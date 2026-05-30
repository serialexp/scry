//! End-to-end retention test against an in-memory object store.
//!
//! Mirrors `crates/compact/tests/compaction_e2e.rs`: InMemory store +
//! TempDir catalog, blocks built via the real `LogsBlockBuilder` /
//! `MetricsBlockBuilder`, and the surviving set queried back through
//! `scry-query`. Timestamps are controlled and `now` is injected, so the
//! age cutoff is exact.
//!
//! Proves the load-bearing properties:
//! - **dry-run is inert** — it reports the candidate but the catalog and
//!   bucket are untouched;
//! - **apply reaps the aged block and only it** — the old block's objects
//!   are gone and its row dropped, the recent block survives and still
//!   queries back losslessly;
//! - **signal-scoping** — a metrics block with no configured TTL is never
//!   reaped, even when it's older than every logs block.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, StringArray, UInt64Array};
use datafusion::execution::context::SessionContext;
use object_store::{memory::InMemory, ObjectStore, ObjectStoreExt};
use scry_block::{
    block_path, BlockBuilder, BlockBuilderConfig, Fence, LogsBlockBuilder, MetricsBlockBuilder,
    NoopSink,
};
use scry_catalog::Catalog;
use scry_proto::streaming::{LogsAppender, MetricsAppender};
use scry_query::{register_logs_table, Query, LOGS_TABLE_NAME};
use scry_retention::{plan_reaping, retain_once, retain_planned, RetentionConfig};
use tempfile::TempDir;
use uuid::Uuid;

const BUCKET: &str = "test";
const DAY: u64 = 86_400 * 1_000_000_000;
/// Reference "now" the policy ages blocks against. ~1000 days past epoch;
/// the absolute value is irrelevant, only the deltas matter.
const NOW: u64 = 1_000 * DAY;

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

fn logs_entries(b: &mut LogsBlockBuilder, fp: u64, ts_start: u64, n: u64) {
    for i in 0..n {
        let body = format!("row {i} fp={fp:#x}");
        b.append_entry(
            fp,
            ts_start + i,
            6,
            body.into_bytes(),
            vec![(b"status".to_vec(), b"ok".to_vec())],
        );
    }
}

fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
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

fn collect_strings(batches: &[arrow::record_batch::RecordBatch], col: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(col).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

async fn run_logs_query(
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
) -> Vec<arrow::record_batch::RecordBatch> {
    let ctx = SessionContext::new();
    register_logs_table(&ctx, catalog, store, q).await.unwrap();
    let df = ctx.table(LOGS_TABLE_NAME).await.unwrap();
    let physical = df.create_physical_plan().await.unwrap();
    datafusion::physical_plan::collect(physical, ctx.task_ctx())
        .await
        .unwrap()
}

/// True if the block's main parquet still exists in the bucket.
async fn parquet_present(store: &Arc<dyn ObjectStore>, meta: &scry_block::BlockMeta) -> bool {
    let p = block_path(
        &meta.signal,
        meta.ts_min_unix_nano,
        meta.writer_id,
        meta.uuid,
        "parquet",
    );
    !matches!(
        store.get(&p.as_str().into()).await,
        Err(object_store::Error::NotFound { .. })
    )
}

fn ttl_logs(days: u64, apply: bool) -> RetentionConfig {
    let mut overrides = BTreeMap::new();
    overrides.insert("logs".to_string(), Duration::from_nanos(days * DAY));
    RetentionConfig {
        default_ttl: None,
        overrides,
        grace: Duration::ZERO,
        apply,
    }
}

#[tokio::test]
async fn logs_retention_dry_run_then_apply() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // Old logs block: newest record ~90 days ago → past a 7-day TTL.
    let fp_old: u64 = 0xA001;
    let mut old = LogsBlockBuilder::new(writer, test_cfg());
    old.observe_stream(fp_old, labels(&[("service", "api"), ("env", "prod")]));
    logs_entries(&mut old, fp_old, NOW - 90 * DAY, 50);
    let m_old = old
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("old block");

    // Recent logs block: newest record ~1 hour ago → within the TTL.
    let fp_recent: u64 = 0xB001;
    let mut recent = LogsBlockBuilder::new(writer, test_cfg());
    recent.observe_stream(fp_recent, labels(&[("service", "db"), ("env", "prod")]));
    logs_entries(&mut recent, fp_recent, NOW - 3600 * 1_000_000_000, 50);
    let m_recent = recent
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("recent block");

    // Ancient metrics block (older than every logs block) with NO TTL —
    // must never be touched (signal-scoping).
    let fp_metric: u64 = 0xC001;
    let mut metrics = MetricsBlockBuilder::new(writer, test_cfg());
    metrics.observe_series(fp_metric, 1, labels(&[("__name__", "up"), ("svc", "api")]));
    for i in 0..40u64 {
        metrics.append_sample(fp_metric, NOW - 200 * DAY + i, i as f64);
    }
    let m_metric = metrics
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("metrics block");

    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    for m in [&m_old, &m_recent, &m_metric] {
        assert!(catalog.insert_block(m).unwrap());
    }
    assert_eq!(catalog.block_count().unwrap(), 3);

    // ── Dry-run: reports the old logs block, touches nothing. ────────
    let dry = retain_once(store.clone(), &catalog, &ttl_logs(7, false), NOW)
        .await
        .unwrap();
    assert!(dry.dry_run);
    assert_eq!(dry.reaped, 1, "only the aged logs block is a candidate");
    assert_eq!(dry.by_signal.get("logs").copied(), Some((1, m_old.byte_size)));
    assert_eq!(
        catalog.list_blocks().unwrap().len(),
        3,
        "dry-run must not mutate the catalog"
    );
    assert!(
        parquet_present(&store, &m_old).await,
        "dry-run must not delete objects"
    );

    // ── Apply: reap the aged block and only it. ──────────────────────
    let applied = retain_once(store.clone(), &catalog, &ttl_logs(7, true), NOW)
        .await
        .unwrap();
    assert!(!applied.dry_run);
    assert_eq!(applied.reaped, 1);

    // Old block: row dropped + objects gone.
    assert!(catalog.get_block(m_old.uuid).unwrap().is_none());
    assert!(!parquet_present(&store, &m_old).await, "aged block reaped");

    // Recent logs + metrics survive (metrics had no TTL).
    let live = catalog.list_blocks().unwrap();
    assert_eq!(live.len(), 2);
    assert!(catalog.get_block(m_recent.uuid).unwrap().is_some());
    assert!(
        catalog.get_block(m_metric.uuid).unwrap().is_some(),
        "metrics block has no TTL and must survive"
    );
    assert!(parquet_present(&store, &m_metric).await);

    // ── Lossless: the surviving logs query returns exactly the recent
    //    block's rows, none of the reaped block's. ────────────────────
    let post = run_logs_query(&catalog, store.clone(), &Query::default()).await;
    assert_eq!(total_rows(&post), 50);
    for fp in collect_u64(&post, "stream_fingerprint") {
        assert_eq!(fp, fp_recent, "only the recent stream survives");
    }
    for body in collect_strings(&post, "body") {
        assert!(body.contains(&format!("{fp_recent:#x}")));
    }

    // ── Idempotent: a second apply finds nothing new to reap. ────────
    let again = retain_once(store.clone(), &catalog, &ttl_logs(7, true), NOW)
        .await
        .unwrap();
    assert_eq!(again.reaped, 0);
}

/// A [`Fence`] that is always lost — the retention lease was never (or no
/// longer) held.
struct LostFence;
impl Fence for LostFence {
    fn check(&self) -> anyhow::Result<()> {
        anyhow::bail!("retention lease lost")
    }
}

#[tokio::test]
async fn retention_aborts_under_a_lost_lease_leaving_blocks_intact() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // One aged logs block — a genuine reaping candidate under a 7-day TTL.
    let fp_old: u64 = 0xA001;
    let mut old = LogsBlockBuilder::new(writer, test_cfg());
    old.observe_stream(fp_old, labels(&[("service", "api")]));
    logs_entries(&mut old, fp_old, NOW - 90 * DAY, 50);
    let m_old = old
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("old block");

    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    assert!(catalog.insert_block(&m_old).unwrap());

    // The block is genuinely expired (plan would reap it).
    let cfg = ttl_logs(7, true);
    let live = catalog.list_blocks().unwrap();
    let expired = plan_reaping(&live, &cfg, NOW);
    assert_eq!(expired.len(), 1, "block is a reaping candidate");

    // But the lease is lost → retain_planned must abort and touch nothing.
    let aborted = retain_planned(&expired, store.clone(), &catalog, &cfg, NOW, &LostFence, &NoopSink)
        .await
        .unwrap();
    assert!(aborted, "a lost lease aborts the pass");

    // Block survives: row present, objects present.
    assert!(
        catalog.get_block(m_old.uuid).unwrap().is_some(),
        "catalog row must survive a fenced abort"
    );
    assert!(
        parquet_present(&store, &m_old).await,
        "objects must survive a fenced abort"
    );
}
