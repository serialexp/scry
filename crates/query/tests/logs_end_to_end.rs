//! End-to-end querier test for the logs signal against the
//! DataFusion-backed `LogsTable`. Mirrors `end_to_end.rs` (which
//! covers metrics) — same harness shape, same InMemory + TempDir
//! catalog, same `run_query` helper — but plants logs blocks and
//! exercises the postings + Map column on the logs schema.
//!
//! Coverage:
//! 1. `service=api`             → 200 rows, all fp ∈ {L_api, L_api2}
//! 2. `env=prod`                → 400 rows (everything except L_cache)
//! 3. `service=api, env=stage`  → 0 rows (intersect empty in both blocks)
//! 4. `nonexistent=x`           → 0 rows (every block postings-pruned)
//! 5. Empty matchers            → 500 rows (300 + 200)
//! 6. Time-bounded              → block A skipped at catalog plan, row
//!    count drops to the in-range prefix of block B
//!
//! Coverage parity with `end_to_end.rs` is deliberate. The two tests
//! together prove the v0.4 step-1 promise: that the shared `Query`
//! envelope + postings cache + table-provider abstraction behaves
//! identically for both signals, modulo schema.

use std::sync::Arc;

use arrow::array::UInt64Array;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use object_store::{memory::InMemory, ObjectStore};
use scry_block::{BlockBuilder, BlockBuilderConfig, LogsBlockBuilder};
use scry_catalog::Catalog;
use scry_proto::streaming::LogsAppender;
use scry_query::{
    build_logs_table_from_candidates, list_logs_candidates, register_logs_table, Query,
    LOGS_TABLE_NAME,
};
use tempfile::TempDir;
use uuid::Uuid;

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

fn entries_for(b: &mut LogsBlockBuilder, fp: u64, ts_start: u64, n: u64, severity: u8) {
    for i in 0..n {
        // The body / attribute shape exercises both the Utf8 column
        // and the Map<Utf8,Utf8> column. attribute keys vary across
        // entries so the schema's nullable-values handling stays
        // honest (parquet writes a non-null value every time today,
        // but the schema must permit nulls — see the long comment in
        // `LogsBlockBuilder::main_schema`).
        let body = format!("row {i} fp={fp:#x}");
        b.append_entry(
            fp,
            ts_start + i,
            severity,
            body.into_bytes(),
            vec![
                (b"trace_id".to_vec(), format!("t{i:06}").into_bytes()),
                (b"status".to_vec(), b"ok".to_vec()),
            ],
        );
    }
}

/// Build a SessionContext, register the InMemory store under
/// `s3://{BUCKET}`, register the logs table, run the implicit
/// `SELECT * FROM logs`, and return the collected batches + plan.
async fn run_query(
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
) -> (Vec<arrow::record_batch::RecordBatch>, Arc<dyn ExecutionPlan>) {
    let ctx = SessionContext::new();
    register_logs_table(&ctx, catalog, store, q).await.unwrap();
    let df = ctx.table(LOGS_TABLE_NAME).await.unwrap();
    let physical = df.create_physical_plan().await.unwrap();
    let batches = datafusion::physical_plan::collect(physical.clone(), ctx.task_ctx())
        .await
        .unwrap();
    (batches, physical)
}

fn collect_u64(batches: &[arrow::record_batch::RecordBatch], col: &str) -> Vec<u64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(col).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        out.extend(arr.values().iter().copied());
    }
    out
}

fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn logs_querier_end_to_end() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // ── Block A: 3 streams × 100 entries each = 300 rows ──────────
    //   L_api    (service=api, env=prod)
    //   L_db     (service=db,  env=prod)
    //   L_cache  (service=cache, env=stage)
    let l_api: u64 = 0xA001;
    let l_db: u64 = 0xA002;
    let l_cache: u64 = 0xA003;
    let mut block_a = LogsBlockBuilder::new(writer, test_cfg());
    block_a.observe_stream(l_api, labels(&[("service", "api"), ("env", "prod")]));
    block_a.observe_stream(l_db, labels(&[("service", "db"), ("env", "prod")]));
    block_a.observe_stream(
        l_cache,
        labels(&[("service", "cache"), ("env", "stage")]),
    );
    entries_for(&mut block_a, l_api, 1_000_000, 100, 9);
    entries_for(&mut block_a, l_db, 1_000_100, 100, 6);
    entries_for(&mut block_a, l_cache, 1_000_200, 100, 3);
    let meta_a = block_a
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("block A non-empty");
    assert_eq!(meta_a.row_count, 300);
    assert_eq!(meta_a.signal, "logs");

    // ── Block B: 2 streams × 100 entries each = 200 rows ──────────
    //   L_api2   (service=api, env=prod)  ← shares service/env with
    //                                      block A's L_api but a
    //                                      different fingerprint
    //                                      (different additional
    //                                      labels in a real workload —
    //                                      here just a distinct fp).
    //   L_worker (service=worker, env=prod)
    let l_api2: u64 = 0xB001;
    let l_worker: u64 = 0xB002;
    let mut block_b = LogsBlockBuilder::new(writer, test_cfg());
    block_b.observe_stream(l_api2, labels(&[("service", "api"), ("env", "prod")]));
    block_b.observe_stream(l_worker, labels(&[("service", "worker"), ("env", "prod")]));
    entries_for(&mut block_b, l_api2, 2_000_000, 100, 9);
    entries_for(&mut block_b, l_worker, 2_000_100, 100, 6);
    let meta_b = block_b
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("block B non-empty");
    assert_eq!(meta_b.row_count, 200);

    // ── Catalog ───────────────────────────────────────────────────
    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    assert!(catalog.insert_block(&meta_a).unwrap());
    assert!(catalog.insert_block(&meta_b).unwrap());
    assert_eq!(catalog.block_count().unwrap(), 2);

    // ── 1. service=api → 200 rows, all fp ∈ {l_api, l_api2} ───────
    let q1 = Query {
        matchers: vec![("service".into(), "api".into())],
        ts_min: None,
        ts_max: None,
    };
    let (batches, _plan) = run_query(&catalog, store.clone(), &q1).await;
    assert_eq!(total_rows(&batches), 200);
    for fp in collect_u64(&batches, "stream_fingerprint") {
        assert!(
            fp == l_api || fp == l_api2,
            "unexpected fp {fp:#x} in service=api result"
        );
    }

    // ── 2. env=prod → 400 rows (every stream except L_cache) ──────
    let q2 = Query {
        matchers: vec![("env".into(), "prod".into())],
        ts_min: None,
        ts_max: None,
    };
    let (batches, _plan) = run_query(&catalog, store.clone(), &q2).await;
    assert_eq!(total_rows(&batches), 400);

    // ── 3. service=api + env=stage → 0 rows; block A & B both ────
    //    postings-prune cleanly. (L_api has env=prod, L_cache has
    //    env=stage but not service=api → intersect empty in A;
    //    block B has no env=stage stream at all → intersect empty.)
    let q3 = Query {
        matchers: vec![
            ("service".into(), "api".into()),
            ("env".into(), "stage".into()),
        ],
        ts_min: None,
        ts_max: None,
    };
    let candidates = list_logs_candidates(&catalog, &q3).unwrap();
    let table = build_logs_table_from_candidates(candidates, store.clone(), None, &q3)
        .await
        .unwrap();
    assert_eq!(
        table.blocks().len(),
        0,
        "both blocks should be postings-pruned before scan"
    );
    let (batches, _plan) = run_query(&catalog, store.clone(), &q3).await;
    assert_eq!(total_rows(&batches), 0);

    // ── 4. nonexistent=x → 0 rows ─────────────────────────────────
    let q4 = Query {
        matchers: vec![("nonexistent".into(), "x".into())],
        ts_min: None,
        ts_max: None,
    };
    let (batches, _plan) = run_query(&catalog, store.clone(), &q4).await;
    assert_eq!(total_rows(&batches), 0);

    // ── 5. Empty matchers → all 500 rows (300 + 200) ──────────────
    //
    // Drives the postings.rs empty-matcher fallback through
    // `meta.all_fingerprints` (the new signal-agnostic field), not
    // the metrics-only `series_types`.
    let q5 = Query::default();
    let (batches, _plan) = run_query(&catalog, store.clone(), &q5).await;
    assert_eq!(total_rows(&batches), 500);

    // ── 6. Time-bounded query → block A catalog-pruned ────────────
    //
    // Block A's ts range is 1_000_000..=1_000_299.
    // Block B's ts range is 2_000_000..=2_000_199.
    // Pick a window inside block B only.
    let q6 = Query {
        matchers: vec![("env".into(), "prod".into())],
        ts_min: Some(2_000_050),
        ts_max: Some(2_000_150),
    };
    let candidates = list_logs_candidates(&catalog, &q6).unwrap();
    let table = build_logs_table_from_candidates(candidates, store.clone(), None, &q6)
        .await
        .unwrap();
    assert_eq!(
        table.blocks().len(),
        1,
        "block A should be catalog-pruned by ts overlap"
    );
    assert_eq!(table.blocks()[0].entry.meta.uuid, meta_b.uuid);

    let (batches, _plan) = run_query(&catalog, store.clone(), &q6).await;
    // L_api2:   ts 2_000_000..2_000_099 → 2_000_050..=2_000_099 = 50
    // L_worker: ts 2_000_100..2_000_199 → 2_000_100..=2_000_150 = 51
    assert_eq!(total_rows(&batches), 101);
    for ts in collect_u64(&batches, "ts_unix_nano") {
        assert!((2_000_050..=2_000_150).contains(&ts));
    }
}
