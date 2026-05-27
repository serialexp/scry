//! End-to-end smoke test for the v0.3 step 5 binschema query daemon.
//!
//! This is the post-Flight version of the original `flight_e2e` test.
//! Same blocks, same matcher + SQL shapes, same row-count assertions —
//! all that changed is the transport. The test now speaks the
//! length-prefixed binschema `QueryFrame` protocol from
//! `proto/query.schema.json` directly: open a raw `TcpStream`, write
//! one `QueryFrame::QueryRequest`, drain `SchemaMsg` + `BatchMsg`
//! frames into `arrow_ipc::reader::StreamDecoder`, terminate on
//! `EndOfStream` (assert the server-reported `total_rows` matches our
//! client-side count) or `StreamError` (fail).
//!
//! What this proves:
//!
//! - The TCP listener accepts a request, runs the query, and emits
//!   the right `RecordBatch`es on the wire.
//! - `QueryRequest` round-trips through binschema intact (matchers,
//!   sql, request_id).
//! - Both the default `SELECT *` and the explicit-SQL paths fire
//!   through `register_metrics_table_from_candidates` and produce
//!   row counts identical to the local CLI.
//! - `EndOfStream.total_rows` matches the client-side row tally —
//!   the new wire-level trailer is honest.
//! - The postings cache hits the second time around (same
//!   carried-forward assertion as before).
//! - Shutdown via the oneshot signal exits the server task cleanly.
//!
//! What it does NOT prove (still deferred to manual smoke):
//!
//! - The `scan_complete` tracing event fires with the right fields.
//!   We don't wire a custom tracing subscriber here.
//! - Per-query pool stats are non-zero. The InMemory store doesn't
//!   exercise the pool.
//! - The mid-stream `StreamError(QUERY_ERR_RESOURCES)` path. That's
//!   verified by the manual budget-bust smoke in step 5's plan.

use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, UInt64Array};
use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamDecoder;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use object_store::{memory::InMemory, ObjectStore};
use scry_block::{BlockBuilder, BlockBuilderConfig, MetricsBlockBuilder};
use scry_catalog::Catalog;
use scry_objstore::BufPool;
use scry_proto::framing::{read_frame, write_frame};
use scry_proto::streaming::MetricsAppender;
use scry_proto::{QueryFrame, QueryFrameMsg};
use scry_query::{MetricsQuery, PostingsCache, QueryRequest};
use scry_server::QueryService;
use tempfile::TempDir;
use tokio::io::{AsyncWriteExt, BufReader as TokioBufReader, BufWriter as TokioBufWriter};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use uuid::Uuid;

const METRIC_TYPE_COUNTER: u8 = 1;
const BUCKET: &str = "test";

fn test_cfg() -> BlockBuilderConfig {
    BlockBuilderConfig {
        max_rows: 1_000_000,
        target_bytes: 128 * 1024 * 1024,
        row_group_size: 100,
    }
}

fn labels(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect()
}

fn samples_for(b: &mut MetricsBlockBuilder, fp: u64, ts_start: u64, n: u64, step: u64) {
    for i in 0..n {
        b.append_sample(fp, ts_start + i * step, i as f64);
    }
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Result of draining one query connection: the decoded record batches
/// the server streamed back, plus the server-reported `EndOfStream`
/// total. Errors propagate up.
struct QueryResult {
    batches: Vec<RecordBatch>,
    server_total_rows: u64,
}

/// Open a TCP connection to `addr`, send `req`, drain the response
/// stream, return both the batches and the server-reported trailer.
async fn run_query(addr: std::net::SocketAddr, req: QueryRequest) -> QueryResult {
    let sock = TcpStream::connect(addr).await.expect("connect");
    let (r, w) = sock.into_split();
    let mut r = TokioBufReader::new(r);
    let mut w = TokioBufWriter::new(w);

    let request_frame = QueryFrame {
        msg: QueryFrameMsg::QueryRequest(req.to_wire().into()),
    };
    write_frame(&mut w, &request_frame).await.expect("write request");
    w.flush().await.expect("flush request");

    let mut decoder = StreamDecoder::new();
    let mut batches: Vec<RecordBatch> = Vec::new();

    let server_total_rows: u64 = loop {
        let frame: QueryFrame = read_frame(&mut r).await.expect("read frame");
        match frame.msg {
            QueryFrameMsg::SchemaMsg(s) => {
                let mut buf = Buffer::from(s.ipc_bytes);
                while !buf.is_empty() {
                    if let Some(b) = decoder.decode(&mut buf).expect("decode schema") {
                        batches.push(b);
                    }
                }
            }
            QueryFrameMsg::BatchMsg(b) => {
                let mut buf = Buffer::from(b.ipc_bytes);
                while !buf.is_empty() {
                    if let Some(rb) = decoder.decode(&mut buf).expect("decode batch") {
                        batches.push(rb);
                    }
                }
            }
            QueryFrameMsg::EndOfStream(end) => break end.total_rows,
            QueryFrameMsg::StreamError(err) => {
                panic!(
                    "server returned StreamError code={:#06x} message={}",
                    err.code, err.message
                );
            }
            QueryFrameMsg::QueryRequest(_) => {
                panic!("server sent QueryRequest as response (protocol violation)");
            }
        }
    };

    QueryResult {
        batches,
        server_total_rows,
    }
}

#[tokio::test]
async fn query_round_trip() {
    // ── Plant two blocks (same shape as the query crate's e2e test) ─
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // Block A: 3 series × 100 samples = 300 rows.
    let a1: u64 = 0x100; // foo, prod
    let a2: u64 = 0x200; // foo, stage
    let a3: u64 = 0x300; // bar, prod
    let mut block_a = MetricsBlockBuilder::new(writer, test_cfg());
    block_a.observe_series(
        a1,
        METRIC_TYPE_COUNTER,
        labels(&[("__name__", "foo"), ("env", "prod")]),
    );
    block_a.observe_series(
        a2,
        METRIC_TYPE_COUNTER,
        labels(&[("__name__", "foo"), ("env", "stage")]),
    );
    block_a.observe_series(
        a3,
        METRIC_TYPE_COUNTER,
        labels(&[("__name__", "bar"), ("env", "prod")]),
    );
    samples_for(&mut block_a, a1, 1_000_000, 100, 1);
    samples_for(&mut block_a, a2, 1_000_100, 100, 1);
    samples_for(&mut block_a, a3, 1_000_200, 100, 1);
    let meta_a = block_a
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("block A non-empty");
    assert_eq!(meta_a.row_count, 300);

    // Block B: 2 series × 100 samples = 200 rows.
    let b1: u64 = 0x1000; // baz, prod
    let b2: u64 = 0x2000; // foo, prod
    let mut block_b = MetricsBlockBuilder::new(writer, test_cfg());
    block_b.observe_series(
        b1,
        METRIC_TYPE_COUNTER,
        labels(&[("__name__", "baz"), ("env", "prod")]),
    );
    block_b.observe_series(
        b2,
        METRIC_TYPE_COUNTER,
        labels(&[("__name__", "foo"), ("env", "prod")]),
    );
    samples_for(&mut block_b, b1, 2_000_000, 100, 1);
    samples_for(&mut block_b, b2, 2_000_100, 100, 1);
    let meta_b = block_b
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("block B non-empty");
    assert_eq!(meta_b.row_count, 200);

    // Catalog under the conventional `BUCKET` name. The service routes
    // the InMemory store under `s3://test` via
    // `MetricsTable::object_store_url`.
    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    assert!(catalog.insert_block(&meta_a).unwrap());
    assert!(catalog.insert_block(&meta_b).unwrap());

    // ── Stand up the QueryService ──────────────────────────────────
    //
    // Same constructor signature as the Flight version — only the
    // transport changed.
    let pool = BufPool::new();
    let postings_cache = Arc::new(PostingsCache::with_budget_bytes(16 * 1024 * 1024));
    let memory_pool = Arc::new(GreedyMemoryPool::new(256 * 1024 * 1024));
    let runtime_env = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool.clone())
            .build()
            .expect("build RuntimeEnv"),
    );
    let service = Arc::new(QueryService::new(
        Arc::new(Mutex::new(catalog)),
        store.clone(),
        pool,
        postings_cache.clone(),
        runtime_env,
        memory_pool,
    ));

    // Pre-bind to capture the chosen port before spawning the serve
    // task. Same loopback-handoff trick as the Flight version: bind,
    // peek `local_addr`, drop, hand the addr to `serve_with_shutdown`
    // which will re-bind. Cleaner would be a `tx`-returning variant
    // of `serve_with_shutdown`, but that's API churn for a test
    // convenience.
    let bind = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let svc_for_task = service.clone();

    let probe = tokio::net::TcpListener::bind(bind).await.unwrap();
    let listen_addr = probe.local_addr().unwrap();
    drop(probe);

    let serve_handle = tokio::spawn(async move {
        svc_for_task
            .serve_with_shutdown(listen_addr, async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // Tiny delay so the server's bind/listen completes before we
    // dial. Polling for connect would be more robust but loopback +
    // unbounded backlog makes this fine in practice.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // ── Matcher-only query ─────────────────────────────────────────
    let req = QueryRequest {
        metrics_query: MetricsQuery {
            matchers: vec![("__name__".into(), "foo".into())],
            ts_min: None,
            ts_max: None,
        },
        sql: None,
        limit: None,
        request_id: Some("test-matcher".into()),
    };
    let result = run_query(listen_addr, req).await;
    assert_eq!(
        total_rows(&result.batches),
        300,
        "matcher __name__=foo should yield 300 rows (A1=100 + A2=100 + B2=100)"
    );
    assert_eq!(
        result.server_total_rows, 300,
        "EndOfStream.total_rows should match the client-side row count",
    );
    // Sanity: every returned fingerprint is one we expect.
    let mut seen_fps = Vec::new();
    for b in &result.batches {
        let col = b.schema().index_of("series_fingerprint").unwrap();
        let arr = b
            .column(col)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        seen_fps.extend(arr.values().iter().copied());
    }
    for fp in &seen_fps {
        assert!(
            *fp == a1 || *fp == a2 || *fp == b2,
            "unexpected fingerprint {fp:#x} in matcher result"
        );
    }

    // ── SQL query ──────────────────────────────────────────────────
    let req_sql = QueryRequest {
        metrics_query: MetricsQuery::default(),
        sql: Some("SELECT count(*) AS n FROM metrics".into()),
        limit: None,
        request_id: Some("test-sql".into()),
    };
    let result_sql = run_query(listen_addr, req_sql).await;
    assert_eq!(result_sql.batches.len(), 1, "count(*) returns one batch");
    let batch = &result_sql.batches[0];
    assert_eq!(batch.num_rows(), 1);
    // DataFusion picks Int64 for count(*). The column is named per
    // our `AS n` alias.
    let n_col = batch.schema().index_of("n").unwrap();
    let n_arr = batch
        .column(n_col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count(*) column should be Int64");
    assert_eq!(
        n_arr.value(0),
        500,
        "count(*) across both blocks = 300 + 200 = 500"
    );
    assert_eq!(
        result_sql.server_total_rows, 1,
        "SQL count(*) returns one summary row",
    );

    // ── Cache warmth check ─────────────────────────────────────────
    //
    // After the matcher + SQL queries above we should have populated
    // the postings cache. Re-running the matcher query must hit the
    // cache for every block, not miss any of them — same assertion
    // the Flight test had, transport-agnostic.
    let cache_before = postings_cache.stats();
    let req_replay = QueryRequest {
        metrics_query: MetricsQuery {
            matchers: vec![("__name__".into(), "foo".into())],
            ts_min: None,
            ts_max: None,
        },
        sql: None,
        limit: None,
        request_id: Some("test-matcher-replay".into()),
    };
    let result_replay = run_query(listen_addr, req_replay).await;
    assert_eq!(
        total_rows(&result_replay.batches),
        300,
        "replay should return the same 300 rows"
    );
    assert_eq!(result_replay.server_total_rows, 300);

    let cache_after = postings_cache.stats();
    let delta = cache_after.delta(cache_before);
    assert_eq!(
        delta.misses, 0,
        "replay should not miss the cache for any block (got misses={})",
        delta.misses
    );
    assert!(
        delta.hits >= 2,
        "replay should hit the cache for both blocks (got hits={})",
        delta.hits
    );

    // ── Clean shutdown ─────────────────────────────────────────────
    let _ = shutdown_tx.send(());
    serve_handle.await.expect("serve task join").unwrap();
}
