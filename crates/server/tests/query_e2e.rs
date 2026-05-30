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

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, UInt64Array};
use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamDecoder;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use object_store::{memory::InMemory, ObjectStore};
use scry_block::{
    BlockBuilder, BlockBuilderConfig, LogsBlockBuilder, MetricsBlockBuilder, TracesBlockBuilder,
};
use scry_catalog::Catalog;
use scry_objstore::BufPool;
use scry_proto::framing::{read_frame, write_frame};
use scry_proto::constants::Signal;
use scry_proto::streaming::{DecodedSpan, LogsAppender, MetricsAppender, TracesAppender};
use scry_proto::{QueryFrame, QueryFrameMsg};
use scry_query::{BloomCache, PostingsCache, Query, QueryRequest};
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
        ..Default::default()
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
    let bloom_cache = Arc::new(BloomCache::with_budget_bytes(16 * 1024 * 1024));
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
        bloom_cache.clone(),
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
        signal: Signal::Metrics as u8,
        query: Query {
            matchers: vec![("__name__".into(), "foo".into())],
            ts_min: None,
            ts_max: None,
            trace_id: None,
            body_contains: None,
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
        signal: Signal::Metrics as u8,
        query: Query::default(),
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
        signal: Signal::Metrics as u8,
        query: Query {
            matchers: vec![("__name__".into(), "foo".into())],
            ts_min: None,
            ts_max: None,
            trace_id: None,
            body_contains: None,
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

// ─────────────────────────────────────────────────────────────────────
// Logs end-to-end. Symmetric structure with the metrics test above:
// plant one block via `LogsBlockBuilder`, stand up the same
// `QueryService` (which is signal-agnostic — the `Signal::Logs` arm
// reaches into `register_logs_table_from_candidates` and produces an
// identical `RecordBatch`-streaming response), drain matcher + SQL
// responses through the same wire path. Catches any place that still
// hardcodes "metrics" string-wise: the postings sidecar resolver, the
// table-name default, the `scan_complete` field, etc.
// ─────────────────────────────────────────────────────────────────────

fn logs_labels(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    labels(pairs)
}

/// Helper: append `n` log entries for `fp` starting at `ts_start`,
/// stepping by 1ns, with a fixed severity and trivial body/attribute
/// shape. The body deliberately varies per-entry (`row {i}`) so a
/// later substring-filter test would have something to match; today
/// the body is just data.
fn entries_for(
    b: &mut LogsBlockBuilder,
    fp: u64,
    ts_start: u64,
    n: u64,
    severity: u8,
) {
    for i in 0..n {
        let body = format!("row {i} fp={fp:#x}");
        b.append_entry(
            fp,
            ts_start + i,
            severity,
            body.into_bytes(),
            vec![
                (b"trace_id".to_vec(), format!("t{i:04}").into_bytes()),
                (b"status".to_vec(), b"ok".to_vec()),
            ],
        );
    }
}

#[tokio::test]
async fn logs_round_trip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // ── Plant one logs block: 3 streams × 20 entries = 60 rows ─────
    let l_api: u64 = 0xA001; // service=api, env=prod
    let l_db: u64 = 0xA002; // service=db, env=prod
    let l_cache: u64 = 0xA003; // service=cache, env=stage
    let mut block = LogsBlockBuilder::new(writer, test_cfg());
    block.observe_stream(l_api, logs_labels(&[("service", "api"), ("env", "prod")]));
    block.observe_stream(l_db, logs_labels(&[("service", "db"), ("env", "prod")]));
    block.observe_stream(
        l_cache,
        logs_labels(&[("service", "cache"), ("env", "stage")]),
    );
    entries_for(&mut block, l_api, 3_000_000, 20, 9);
    entries_for(&mut block, l_db, 3_000_100, 20, 6);
    entries_for(&mut block, l_cache, 3_000_200, 20, 3);
    let meta = block
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("logs block non-empty");
    assert_eq!(meta.row_count, 60);
    assert_eq!(meta.signal, "logs");

    // ── Catalog ────────────────────────────────────────────────────
    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    assert!(catalog.insert_block(&meta).unwrap());

    // ── Service ────────────────────────────────────────────────────
    let pool = BufPool::new();
    let postings_cache = Arc::new(PostingsCache::with_budget_bytes(16 * 1024 * 1024));
    let bloom_cache = Arc::new(BloomCache::with_budget_bytes(16 * 1024 * 1024));
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
        bloom_cache.clone(),
        runtime_env,
        memory_pool,
    ));

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

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // ── Matcher: service=api → 20 rows from L_api ──────────────────
    let req = QueryRequest {
        signal: Signal::Logs as u8,
        query: Query {
            matchers: vec![("service".into(), "api".into())],
            ts_min: None,
            ts_max: None,
            trace_id: None,
            body_contains: None,
        },
        sql: None,
        limit: None,
        request_id: Some("logs-matcher".into()),
    };
    let result = run_query(listen_addr, req).await;
    assert_eq!(
        total_rows(&result.batches),
        20,
        "service=api should yield 20 rows (only L_api matches)"
    );
    assert_eq!(result.server_total_rows, 20);
    // Sanity: every returned stream_fingerprint is L_api.
    for b in &result.batches {
        let col = b.schema().index_of("stream_fingerprint").unwrap();
        let arr = b
            .column(col)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for &fp in arr.values().iter() {
            assert_eq!(fp, l_api, "service=api row carries unexpected fp {fp:#x}");
        }
    }

    // ── SQL on logs: count(*) → 60 ─────────────────────────────────
    let req_sql = QueryRequest {
        signal: Signal::Logs as u8,
        query: Query::default(),
        sql: Some("SELECT count(*) AS n FROM logs".into()),
        limit: None,
        request_id: Some("logs-sql".into()),
    };
    let result_sql = run_query(listen_addr, req_sql).await;
    assert_eq!(result_sql.batches.len(), 1, "count(*) returns one batch");
    let batch = &result_sql.batches[0];
    assert_eq!(batch.num_rows(), 1);
    let n_col = batch.schema().index_of("n").unwrap();
    let n_arr = batch
        .column(n_col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count(*) column should be Int64");
    assert_eq!(n_arr.value(0), 60, "count(*) across the logs block = 60");
    assert_eq!(result_sql.server_total_rows, 1);

    // ── Default SELECT * (no SQL, no matcher) → 60 rows ────────────
    //
    // Exercises the default-SQL path's `SELECT * FROM logs` resolution
    // (i.e. the per-signal `default_table_name` branch in the
    // service). Also confirms the empty-matcher postings fallback
    // walks `meta.all_fingerprints` rather than the metrics-only
    // `series_types`.
    let req_all = QueryRequest {
        signal: Signal::Logs as u8,
        query: Query::default(),
        sql: None,
        limit: None,
        request_id: Some("logs-default-sql".into()),
    };
    let result_all = run_query(listen_addr, req_all).await;
    assert_eq!(
        total_rows(&result_all.batches),
        60,
        "default SELECT * over the logs block must include every entry"
    );
    assert_eq!(result_all.server_total_rows, 60);

    // ── Clean shutdown ─────────────────────────────────────────────
    let _ = shutdown_tx.send(());
    serve_handle.await.expect("serve task join").unwrap();
}

// Traces end-to-end over the daemon. Proves the `Signal::Traces` arm of
// the QueryService dispatch (candidate listing + table registration +
// default-table resolution) and — crucially — that `query.trace_id`
// survives the binschema wire round-trip and prunes to exactly one
// trace's spans on the server. Traces carry no postings sidecar, so the
// matcher / trace-id / time filters are pushed as parquet row predicates;
// this is the remote counterpart to the local `scry-query` smoke leg.
fn append_test_span(
    b: &mut TracesBlockBuilder,
    trace_id: &[u8; 16],
    span_id: &[u8; 8],
    service_name: &str,
    start: u64,
) {
    let resource_labels: Vec<(Vec<u8>, Vec<u8>)> =
        vec![(b"service.name".to_vec(), service_name.as_bytes().to_vec())];
    let span = DecodedSpan {
        trace_id: &trace_id[..],
        span_id: &span_id[..],
        parent_span_id: None,
        resource_labels: &resource_labels,
        scope_name: b"test-scope",
        scope_version: b"1.0",
        name: b"op",
        kind: 1,
        start_unix_nano: start,
        end_unix_nano: start + 1_000,
        status_code: 0,
        status_message: b"",
        attributes: &[],
        events: &[],
        links: &[],
    };
    b.append_span(&span);
}

#[tokio::test]
async fn traces_round_trip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // ── Plant one traces block: trace A (3 spans, service=api) +
    //    trace B (2 spans, service=db) = 5 spans total. ──────────────
    let trace_a: [u8; 16] = [0xAA; 16];
    let trace_b: [u8; 16] = [0xBB; 16];
    let mut block = <TracesBlockBuilder as BlockBuilder>::new(writer, test_cfg());
    append_test_span(&mut block, &trace_a, &[0x01; 8], "api", 5_000_000);
    append_test_span(&mut block, &trace_a, &[0x02; 8], "api", 5_000_100);
    append_test_span(&mut block, &trace_a, &[0x03; 8], "api", 5_000_200);
    append_test_span(&mut block, &trace_b, &[0x04; 8], "db", 5_000_300);
    append_test_span(&mut block, &trace_b, &[0x05; 8], "db", 5_000_400);
    let meta = block
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("traces block non-empty");
    assert_eq!(meta.row_count, 5);
    assert_eq!(meta.signal, "traces");
    // Traces carry no postings sidecar.
    assert!(!meta.has_postings, "traces blocks must not carry postings");

    // ── Catalog + service ──────────────────────────────────────────
    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    assert!(catalog.insert_block(&meta).unwrap());

    let pool = BufPool::new();
    let postings_cache = Arc::new(PostingsCache::with_budget_bytes(16 * 1024 * 1024));
    let bloom_cache = Arc::new(BloomCache::with_budget_bytes(16 * 1024 * 1024));
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
        bloom_cache.clone(),
        runtime_env,
        memory_pool,
    ));

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
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // ── Default SELECT * → all 5 spans ─────────────────────────────
    let req_all = QueryRequest {
        signal: Signal::Traces as u8,
        query: Query::default(),
        sql: None,
        limit: None,
        request_id: Some("traces-default".into()),
    };
    let result_all = run_query(listen_addr, req_all).await;
    assert_eq!(
        total_rows(&result_all.batches),
        5,
        "default SELECT * over the traces block must include every span"
    );
    assert_eq!(result_all.server_total_rows, 5);

    // ── trace_id A (over the wire) → exactly trace A's 3 spans ─────
    let req_by_id = QueryRequest {
        signal: Signal::Traces as u8,
        query: Query {
            trace_id: Some(trace_a),
            ..Default::default()
        },
        sql: None,
        limit: None,
        request_id: Some("traces-by-id".into()),
    };
    let result_by_id = run_query(listen_addr, req_by_id).await;
    assert_eq!(
        total_rows(&result_by_id.batches),
        3,
        "trace_id=A must prune to A's 3 spans only"
    );
    assert_eq!(result_by_id.server_total_rows, 3);
    // Every returned trace_id must be A — proves the FixedSizeBinary
    // predicate filtered, not just that the count happened to match.
    for b in &result_by_id.batches {
        let col = b.schema().index_of("trace_id").unwrap();
        let arr = b
            .column(col)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("trace_id column is FixedSizeBinary");
        for i in 0..arr.len() {
            assert_eq!(arr.value(i), &trace_a[..], "by-id row carries a non-A trace_id");
        }
    }

    // ── Promoted matcher service.name=db → trace B's 2 spans ───────
    let req_matcher = QueryRequest {
        signal: Signal::Traces as u8,
        query: Query {
            matchers: vec![("service.name".into(), "db".into())],
            ..Default::default()
        },
        sql: None,
        limit: None,
        request_id: Some("traces-matcher".into()),
    };
    let result_matcher = run_query(listen_addr, req_matcher).await;
    assert_eq!(
        total_rows(&result_matcher.batches),
        2,
        "service.name=db must select only trace B's 2 spans"
    );
    assert_eq!(result_matcher.server_total_rows, 2);

    // ── Clean shutdown ─────────────────────────────────────────────
    let _ = shutdown_tx.send(());
    serve_handle.await.expect("serve task join").unwrap();
}
