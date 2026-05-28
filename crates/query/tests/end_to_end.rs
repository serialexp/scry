//! End-to-end querier test against the DataFusion-backed `MetricsTable`.
//!
//! Plants two metrics blocks in an InMemory object store, registers
//! them in a TempDir-backed catalog, and asserts the same six
//! behaviours the v0.2 hand-written querier covered — rephrased
//! against `DataFrame.collect()` and `ExecutionPlan` `MetricsSet`.
//!
//! Coverage:
//! 1. `__name__=foo`               → 300 rows, fps ⊆ {a1,a2,b2}, row-group pruning visible
//! 2. `env=prod`                   → 400 rows
//! 3. `__name__=foo, env=stage`    → block B postings-pruned before scan; 100 rows, all fp=a2
//! 4. `nonexistent=x`              → 0 rows (every block postings-pruned)
//! 5. Empty matchers               → 500 rows
//! 6. ts_min/ts_max bounded        → block A catalog-pruned; 101 rows, ts in [2_000_050, 2_000_150]

use std::sync::Arc;

use arrow::array::{Float64Array, UInt64Array};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::ExecutionPlan;
use object_store::{memory::InMemory, ObjectStore};
use scry_block::{BlockBuilder, BlockBuilderConfig, MetricsBlockBuilder};
use scry_catalog::Catalog;
use scry_proto::streaming::MetricsAppender;
use scry_query::{build_metrics_table, register_metrics_table, Query, METRICS_TABLE_NAME};
use tempfile::TempDir;
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

/// Build a fresh SessionContext, register the InMemory store under
/// `s3://{BUCKET}`, register `metrics`, run the implicit
/// `SELECT * FROM metrics`, and return the collected batches + the
/// physical plan (so callers can mine `MetricsSet` for pruning
/// evidence).
async fn run_query(
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
) -> (Vec<arrow::record_batch::RecordBatch>, Arc<dyn ExecutionPlan>) {
    let ctx = SessionContext::new();
    register_metrics_table(&ctx, catalog, store, q).await.unwrap();
    let df = ctx.table(METRICS_TABLE_NAME).await.unwrap();
    let physical = df.create_physical_plan().await.unwrap();
    let batches = datafusion::physical_plan::collect(physical.clone(), ctx.task_ctx())
        .await
        .unwrap();
    (batches, physical)
}

/// Walk the plan tree, returning the deepest `MetricsSet` (typically
/// the `DataSourceExec` wrapping `ParquetSource`).
fn scan_metrics(plan: &dyn ExecutionPlan) -> Option<MetricsSet> {
    let mut out = None;
    fn walk(p: &dyn ExecutionPlan, out: &mut Option<MetricsSet>) {
        if let Some(m) = p.metrics() {
            *out = Some(m);
        }
        for c in p.children() {
            walk(c.as_ref(), out);
        }
    }
    walk(plan, &mut out);
    out
}

/// Count of items pruned for a `PruningMetrics`-shaped counter
/// (e.g. `row_groups_pruned_statistics`). `MetricValue::as_usize`
/// deliberately returns 0 for these — pruning metrics aggregate
/// inside `MetricsSet` itself, not through the scalar accessor.
fn pruned(set: &MetricsSet, name: &str) -> usize {
    let agg = set.aggregate_by_name();
    for m in agg.iter() {
        if m.value().name() == name {
            if let MetricValue::PruningMetrics {
                pruning_metrics, ..
            } = m.value()
            {
                return pruning_metrics.pruned();
            }
        }
    }
    0
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

#[allow(dead_code)]
fn collect_f64(batches: &[arrow::record_batch::RecordBatch], col: &str) -> Vec<f64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(col).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        out.extend(arr.values().iter().copied());
    }
    out
}

fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn querier_end_to_end() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // ── Block A: 3 series × 100 samples each = 300 rows ───────────
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

    // ── Block B: 2 series × 100 samples each = 200 rows ───────────
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

    // ── Catalog ───────────────────────────────────────────────────
    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    assert!(catalog.insert_block(&meta_a).unwrap());
    assert!(catalog.insert_block(&meta_b).unwrap());
    assert_eq!(catalog.block_count().unwrap(), 2);

    // ── 1. __name__=foo → 300 rows; fps ⊆ {a1,a2,b2}; pruning ─────
    let q1 = Query {
        matchers: vec![("__name__".into(), "foo".into())],
        ts_min: None,
        ts_max: None,
    };
    let (batches, plan) = run_query(&catalog, store.clone(), &q1).await;
    assert_eq!(total_rows(&batches), 300);
    let fps = collect_u64(&batches, "series_fingerprint");
    for fp in &fps {
        assert!(
            *fp == a1 || *fp == a2 || *fp == b2,
            "unexpected fp {fp:#x} in __name__=foo result"
        );
    }
    let metrics = scan_metrics(plan.as_ref()).expect("DataSourceExec exposes MetricsSet");
    assert!(
        pruned(&metrics, "row_groups_pruned_statistics") >= 1,
        "expected at least one row group pruned by stats; \
         block A's A3 (bar) group should be skipped by fp IN-list"
    );

    // ── 2. env=prod → 400 rows ────────────────────────────────────
    let q2 = Query {
        matchers: vec![("env".into(), "prod".into())],
        ts_min: None,
        ts_max: None,
    };
    let (batches, _plan) = run_query(&catalog, store.clone(), &q2).await;
    assert_eq!(total_rows(&batches), 400);

    // ── 3. foo+stage → block B postings-pruned before scan ────────
    //
    // Block B has `foo` (B2) but no `stage` row, so its postings
    // intersect is empty → it never makes it into the block list at
    // all. Inspect that via `build_metrics_table` directly.
    let q3 = Query {
        matchers: vec![
            ("__name__".into(), "foo".into()),
            ("env".into(), "stage".into()),
        ],
        ts_min: None,
        ts_max: None,
    };
    let table = build_metrics_table(&catalog, store.clone(), &q3)
        .await
        .unwrap();
    assert_eq!(
        table.blocks().len(),
        1,
        "block B should be postings-pruned and dropped before scan"
    );
    assert_eq!(table.blocks()[0].entry.meta.uuid, meta_a.uuid);

    let (batches, _plan) = run_query(&catalog, store.clone(), &q3).await;
    assert_eq!(total_rows(&batches), 100);
    for fp in collect_u64(&batches, "series_fingerprint") {
        assert_eq!(fp, a2, "all rows for foo+stage must be A2");
    }

    // ── 4. nonexistent=x → 0 rows ─────────────────────────────────
    let q4 = Query {
        matchers: vec![("nonexistent".into(), "x".into())],
        ts_min: None,
        ts_max: None,
    };
    let (batches, _plan) = run_query(&catalog, store.clone(), &q4).await;
    assert_eq!(total_rows(&batches), 0);

    // ── 5. Empty matchers → all 500 rows ──────────────────────────
    let q5 = Query::default();
    let (batches, _plan) = run_query(&catalog, store.clone(), &q5).await;
    assert_eq!(total_rows(&batches), 500, "300 (A) + 200 (B)");

    // ── 6. Time-bounded: block A skipped at catalog plan ──────────
    let q6 = Query {
        matchers: vec![("env".into(), "prod".into())],
        ts_min: Some(2_000_050),
        ts_max: Some(2_000_150),
    };
    let table = build_metrics_table(&catalog, store.clone(), &q6)
        .await
        .unwrap();
    assert_eq!(
        table.blocks().len(),
        1,
        "block A should be catalog-pruned by ts overlap"
    );
    assert_eq!(table.blocks()[0].entry.meta.uuid, meta_b.uuid);

    let (batches, _plan) = run_query(&catalog, store.clone(), &q6).await;
    // B1: ts 2_000_000..2_000_099 → 2_000_050..=2_000_099 = 50 rows
    // B2: ts 2_000_100..2_000_199 → 2_000_100..=2_000_150 = 51 rows
    assert_eq!(total_rows(&batches), 101);
    for ts in collect_u64(&batches, "ts_unix_nano") {
        assert!(ts >= 2_000_050 && ts <= 2_000_150);
    }
}
