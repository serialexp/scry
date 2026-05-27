//! End-to-end Arrow Flight smoke test for the v0.3 step 2 query
//! daemon.
//!
//! Plants two metrics blocks in an `InMemory` object store, registers
//! them in a TempDir-backed SQLite catalog, spins up a `QueryService`
//! on `127.0.0.1:0`, connects an Arrow Flight client, and sends two
//! shapes of `QueryRequest`:
//!
//! 1. **Matcher-only.** `__name__=foo` → expect 300 rows (the same
//!    answer the local-mode end-to-end test verifies).
//! 2. **SQL.** `SELECT count(*) FROM metrics` → expect a single
//!    `RecordBatch` with `count = 500` (the union of both blocks).
//!
//! What this proves:
//!
//! - The Flight server starts, accepts a `DoGet`, and streams the
//!   correct `RecordBatch`es back.
//! - The JSON-in-Ticket `QueryRequest` round-trips intact.
//! - Both the default `SELECT *` and the explicit-SQL paths fire
//!   through `register_metrics_table_from_candidates` and produce
//!   row counts identical to the local CLI.
//! - Shutdown via the oneshot signal exits the server task cleanly.
//!
//! What it does NOT prove (deferred to manual smoke / v0.3.x):
//!
//! - The `scan_complete` tracing event fires with the right fields.
//!   We don't wire a custom tracing subscriber here; eyeballing the
//!   daemon's log against the smoke bucket is the v0.3 verification
//!   path for that.
//! - Per-query pool stats are non-zero. Pool warmth shows up over
//!   multiple queries against a single daemon — this test only
//!   sends two and the InMemory store doesn't exercise the pool
//!   (no real HTTP fetches).

use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::StreamExt;
use object_store::{memory::InMemory, ObjectStore};
use scry_block::{BlockBuilder, BlockBuilderConfig, MetricsBlockBuilder};
use scry_catalog::Catalog;
use scry_objstore::BufPool;
use scry_proto::streaming::MetricsAppender;
use scry_query::{MetricsQuery, PostingsCache, QueryRequest};
use scry_server::QueryService;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tonic::transport::Channel;
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

#[tokio::test]
async fn flight_query_round_trip() {
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

    // Catalog under the conventional `BUCKET` name. The Flight
    // service routes the InMemory store under `s3://test` via the
    // `MetricsTable::object_store_url`.
    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    assert!(catalog.insert_block(&meta_a).unwrap());
    assert!(catalog.insert_block(&meta_b).unwrap());

    // ── Stand up the QueryService ──────────────────────────────────
    //
    // Empty pool config — no warmup, no autoscale. The InMemory
    // object store doesn't exercise the pool anyway; this test only
    // proves Flight wire correctness.
    let pool = BufPool::new();
    let postings_cache = Arc::new(PostingsCache::with_budget_bytes(16 * 1024 * 1024));
    // Generous memory budget — the test should not be the thing that
    // hits this cap. 256 MiB is comfortably above what 500 rows of
    // matcher + count(*) ever touches under DataFusion's accounting.
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
    // task — otherwise the test would race the server's listener
    // setup.
    let bind = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let svc_for_task = service.clone();

    // `serve_with_shutdown` is responsible for binding + serving.
    // To get the chosen port back, bind ourselves first, get the
    // local_addr, then drop the listener and pass the addr — the
    // race between drop and re-bind is tiny on loopback. (A cleaner
    // alternative would be returning the bound addr via channel
    // from inside `serve_with_shutdown`, but that's API churn for
    // a test convenience.)
    //
    // Simpler: bind here, peek the addr, hand it off; on loopback
    // the kernel will hand it back to us in serve_with_shutdown.
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
    // an unbounded backlog makes this fine in practice.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // ── Client side: matcher-only query ────────────────────────────
    let endpoint = format!("http://{listen_addr}");
    let channel = Channel::from_shared(endpoint.clone())
        .unwrap()
        .connect()
        .await
        .expect("Flight client connect");
    let mut client = FlightServiceClient::new(channel);

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
    let ticket = Ticket {
        ticket: req.to_ticket_bytes().unwrap(),
    };

    let stream = client
        .do_get(ticket)
        .await
        .expect("DoGet matcher query")
        .into_inner()
        .map(|r| r.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))));
    let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(stream);

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = batch_stream.next().await {
        batches.push(batch.expect("decode FlightData batch"));
    }
    assert_eq!(
        total_rows(&batches),
        300,
        "matcher __name__=foo should yield 300 rows (A1=100 + A2=100 + B2=100)"
    );
    // Sanity: every returned fingerprint is one we expect.
    let mut seen_fps = Vec::new();
    for b in &batches {
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

    // ── Client side: SQL query ─────────────────────────────────────
    let req_sql = QueryRequest {
        metrics_query: MetricsQuery::default(),
        sql: Some("SELECT count(*) AS n FROM metrics".into()),
        limit: None,
        request_id: Some("test-sql".into()),
    };
    let ticket_sql = Ticket {
        ticket: req_sql.to_ticket_bytes().unwrap(),
    };
    let stream_sql = client
        .do_get(ticket_sql)
        .await
        .expect("DoGet SQL query")
        .into_inner()
        .map(|r| r.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))));
    let mut batch_stream_sql = FlightRecordBatchStream::new_from_flight_data(stream_sql);

    let mut sql_batches: Vec<RecordBatch> = Vec::new();
    while let Some(b) = batch_stream_sql.next().await {
        sql_batches.push(b.expect("decode SQL batch"));
    }
    assert_eq!(sql_batches.len(), 1, "count(*) returns one batch");
    let batch = &sql_batches[0];
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

    // ── Cache warmth check ─────────────────────────────────────────
    //
    // After the matcher + SQL queries above we should have populated
    // the postings cache. Re-running the matcher query must hit the
    // cache for every block, not miss any of them — that's the whole
    // point of v0.3 step 3.
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
    let ticket_replay = Ticket {
        ticket: req_replay.to_ticket_bytes().unwrap(),
    };
    let replay_stream = client
        .do_get(ticket_replay)
        .await
        .expect("DoGet replay")
        .into_inner()
        .map(|r| r.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))));
    let mut replay_batches = FlightRecordBatchStream::new_from_flight_data(replay_stream);
    let mut replay_rows = 0;
    while let Some(b) = replay_batches.next().await {
        replay_rows += b.expect("decode replay batch").num_rows();
    }
    assert_eq!(replay_rows, 300, "replay should return the same 300 rows");

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
