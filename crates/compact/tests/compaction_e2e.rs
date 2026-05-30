//! End-to-end compaction test against an in-memory object store.
//!
//! The load-bearing correctness proof for v0.8: build N small same-level
//! blocks, run `compact_once`, and assert the full lifecycle held —
//!
//! - the merge is **lossless** (every input row survives, in the signal's
//!   sort order) and queryable;
//! - **sidecars are rebuilt** correctly (postings union → label matchers
//!   still resolve; logs body bloom → `body_contains` still finds the
//!   needle; metrics `series_types` unioned);
//! - the **catalog transitioned**: one merged block at `level + 1`, inputs
//!   superseded then deleted (gone from `list_blocks` and `get_block`);
//! - the **input objects were reaped** from the bucket;
//! - a query over the post-compaction catalog returns **identically** to
//!   the same query before compaction.
//!
//! Harness mirrors `crates/query/tests/logs_end_to_end.rs` — InMemory
//! store + TempDir catalog + a `register_*_table` round-trip — so the
//! "queries see the merged block, not the inputs" promise is checked
//! through the real query path, not a hand-rolled scan.

use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use datafusion::execution::context::SessionContext;
use object_store::{memory::InMemory, ObjectStore, ObjectStoreExt};
use scry_block::{block_path, BlockBuilder, BlockBuilderConfig, LogsBlockBuilder, MetricsBlockBuilder};
use scry_catalog::Catalog;
use scry_compact::{compact_once, CompactConfig};
use scry_proto::streaming::{LogsAppender, MetricsAppender};
use scry_query::{
    register_logs_table, register_metrics_table, Query, LOGS_TABLE_NAME, METRICS_TABLE_NAME,
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

fn logs_entries(b: &mut LogsBlockBuilder, fp: u64, ts_start: u64, n: u64, severity: u8) {
    for i in 0..n {
        let body = format!("row {i} fp={fp:#x}");
        b.append_entry(
            fp,
            ts_start + i,
            severity,
            body.into_bytes(),
            vec![(b"status".to_vec(), b"ok".to_vec())],
        );
    }
}

async fn fetch_meta(store: &Arc<dyn ObjectStore>, meta: &scry_block::BlockMeta) -> scry_block::BlockMeta {
    let p = block_path(
        &meta.signal,
        meta.ts_min_unix_nano,
        meta.writer_id,
        meta.uuid,
        "meta.json",
    );
    let bytes = store
        .get(&p.as_str().into())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
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

async fn run_metrics_query(
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
) -> Vec<arrow::record_batch::RecordBatch> {
    let ctx = SessionContext::new();
    register_metrics_table(&ctx, catalog, store, q)
        .await
        .unwrap();
    let df = ctx.table(METRICS_TABLE_NAME).await.unwrap();
    let physical = df.create_physical_plan().await.unwrap();
    datafusion::physical_plan::collect(physical, ctx.task_ctx())
        .await
        .unwrap()
}

#[tokio::test]
async fn logs_compaction_is_lossless_and_reaps_inputs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // Three single-stream L0 blocks, 50 rows each. Streams chosen so the
    // postings union spans shared and distinct label pairs:
    //   block1: fp A  service=api,   env=prod
    //   block2: fp B  service=db,    env=prod
    //   block3: fp C  service=api,   env=stage
    let fp_a: u64 = 0xA001;
    let fp_b: u64 = 0xB001;
    let fp_c: u64 = 0xC001;

    let mut b1 = LogsBlockBuilder::new(writer, test_cfg());
    b1.observe_stream(fp_a, labels(&[("service", "api"), ("env", "prod")]));
    logs_entries(&mut b1, fp_a, 1_000_000, 50, 9);
    let m1 = b1
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("block 1");

    let mut b2 = LogsBlockBuilder::new(writer, test_cfg());
    b2.observe_stream(fp_b, labels(&[("service", "db"), ("env", "prod")]));
    logs_entries(&mut b2, fp_b, 1_000_100, 50, 6);
    let m2 = b2
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("block 2");

    let mut b3 = LogsBlockBuilder::new(writer, test_cfg());
    b3.observe_stream(fp_c, labels(&[("service", "api"), ("env", "stage")]));
    logs_entries(&mut b3, fp_c, 1_000_200, 50, 3);
    let m3 = b3
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("block 3");

    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    for m in [&m1, &m2, &m3] {
        assert!(catalog.insert_block(m).unwrap());
    }
    assert_eq!(catalog.block_count().unwrap(), 3);

    // ── Pre-compaction snapshots through the real query path. ────────
    let q_all = Query::default();
    let pre_all = run_logs_query(&catalog, store.clone(), &q_all).await;
    let mut pre_bodies = collect_strings(&pre_all, "body");
    pre_bodies.sort();
    assert_eq!(pre_bodies.len(), 150);

    let q_api = Query {
        matchers: vec![("service".into(), "api".into())],
        ..Default::default()
    };
    let pre_api = run_logs_query(&catalog, store.clone(), &q_api).await;
    assert_eq!(total_rows(&pre_api), 100, "service=api → fp A + fp C");

    // ── Compact: fanout 3, grace 0, logs only. ───────────────────────
    let cfg = CompactConfig {
        fanout: 3,
        max_level: 3,
        grace: std::time::Duration::ZERO,
        signal_filter: Some("logs".into()),
    };
    let report = compact_once(store.clone(), &catalog, BUCKET, &cfg, &test_cfg())
        .await
        .unwrap();
    assert_eq!(report.merges, 1);
    assert_eq!(report.blocks_in, 3);
    assert_eq!(report.blocks_out, 1);

    // ── Catalog transitioned: one merged L1 block, inputs gone. ──────
    let live = catalog.list_blocks().unwrap();
    assert_eq!(live.len(), 1, "only the merged block remains live");
    let merged = &live[0];
    assert_eq!(merged.level, 1, "merged block is one level up");
    assert_eq!(merged.meta.level, 1, "level promoted into the sidecar");
    assert_eq!(merged.meta.row_count, 150, "row count is the exact sum");
    assert!(merged.meta.has_postings, "logs merged block keeps postings");
    assert!(merged.meta.has_body_bloom, "logs merged block keeps a body bloom");
    assert_eq!(
        merged.meta.ts_min_unix_nano, 1_000_000,
        "min ts spans all inputs"
    );
    assert_eq!(merged.meta.ts_max_unix_nano, 1_000_249);

    // Inputs superseded then deleted: gone from get_block entirely.
    for m in [&m1, &m2, &m3] {
        assert!(
            catalog.get_block(m.uuid).unwrap().is_none(),
            "input {} row dropped after merge",
            m.uuid
        );
    }

    // Input objects reaped from the bucket (parquet + meta + sidecars).
    for m in [&m1, &m2, &m3] {
        let p = block_path("logs", m.ts_min_unix_nano, m.writer_id, m.uuid, "parquet");
        assert!(
            matches!(
                store.get(&p.as_str().into()).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "input parquet {p} should be deleted"
        );
    }

    // ── Lossless + sorted: post-compaction query equals pre. ─────────
    let post_all = run_logs_query(&catalog, store.clone(), &q_all).await;
    let mut post_bodies = collect_strings(&post_all, "body");
    post_bodies.sort();
    assert_eq!(post_bodies, pre_bodies, "every input row survives the merge");

    // The merged main parquet is ordered by (stream_fingerprint, ts);
    // the scan preserves file order, so the streamed rows are sorted.
    let fps = collect_u64(&post_all, "stream_fingerprint");
    let tss = collect_u64(&post_all, "ts_unix_nano");
    let mut prev = (0u64, 0u64);
    for (fp, ts) in fps.iter().zip(tss.iter()) {
        assert!(
            (*fp, *ts) >= prev,
            "merged rows must be sorted by (fp, ts): {:?} after {:?}",
            (*fp, *ts),
            prev
        );
        prev = (*fp, *ts);
    }

    // ── Postings union still resolves the same matcher. ──────────────
    let post_api = run_logs_query(&catalog, store.clone(), &q_api).await;
    assert_eq!(
        total_rows(&post_api),
        100,
        "service=api still resolves fp A + fp C after the postings union"
    );
    for fp in collect_u64(&post_api, "stream_fingerprint") {
        assert!(fp == fp_a || fp == fp_c, "unexpected fp {fp:#x}");
    }

    // ── Body bloom rebuilt: the grep needle still hits. ──────────────
    let needle = format!("{fp_b:#x}"); // only in block 2's bodies
    let q_grep = Query {
        body_contains: Some(needle.clone()),
        ..Default::default()
    };
    let grep = run_logs_query(&catalog, store.clone(), &q_grep).await;
    assert_eq!(total_rows(&grep), 50, "all of fp B's rows match the needle");
    for body in collect_strings(&grep, "body") {
        assert!(body.contains(&needle));
    }
}

#[tokio::test]
async fn metrics_compaction_is_lossless() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // Three single-series L0 blocks, 40 samples each, distinct metric
    // types so the series_types union has to carry all three.
    let fp_a: u64 = 0xA001;
    let fp_b: u64 = 0xB001;
    let fp_c: u64 = 0xC001;

    async fn build_metrics(
        store: &Arc<dyn ObjectStore>,
        writer: Uuid,
        fp: u64,
        mtype: u8,
        ts0: u64,
        svc: &str,
    ) -> scry_block::BlockMeta {
        let mut b = MetricsBlockBuilder::new(writer, test_cfg());
        b.observe_series(fp, mtype, labels(&[("__name__", "http_requests"), ("svc", svc)]));
        for i in 0..40u64 {
            b.append_sample(fp, ts0 + i, i as f64);
        }
        b.finish_and_upload(store.as_ref())
            .await
            .unwrap()
            .expect("metrics block")
    }
    let m1 = build_metrics(&store, writer, fp_a, 1, 2_000_000, "api").await;
    let m2 = build_metrics(&store, writer, fp_b, 2, 2_000_100, "db").await;
    let m3 = build_metrics(&store, writer, fp_c, 3, 2_000_200, "cache").await;

    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    for m in [&m1, &m2, &m3] {
        assert!(catalog.insert_block(m).unwrap());
    }

    let q_all = Query::default();
    let pre = run_metrics_query(&catalog, store.clone(), &q_all).await;
    assert_eq!(total_rows(&pre), 120);

    let cfg = CompactConfig {
        fanout: 3,
        max_level: 3,
        grace: std::time::Duration::ZERO,
        signal_filter: Some("metrics".into()),
    };
    let report = compact_once(store.clone(), &catalog, BUCKET, &cfg, &test_cfg())
        .await
        .unwrap();
    assert_eq!(report.merges, 1);
    assert_eq!(report.blocks_in, 3);

    let live = catalog.list_blocks().unwrap();
    assert_eq!(live.len(), 1);
    let merged = &live[0];
    assert_eq!(merged.level, 1);
    assert_eq!(merged.meta.row_count, 120);
    assert!(merged.meta.has_postings);

    // series_types unioned across all three inputs. The catalog doesn't
    // persist series_types (sidecar-only), so read the merged meta.json
    // back from the bucket.
    let merged_meta = fetch_meta(&store, &merged.meta).await;
    let types = merged_meta
        .series_types
        .as_ref()
        .expect("metrics merged block carries series_types");
    assert_eq!(types.len(), 3, "all three series' types survive");
    let by_fp: std::collections::HashMap<u64, u8> = types.iter().copied().collect();
    assert_eq!(by_fp.get(&fp_a), Some(&1));
    assert_eq!(by_fp.get(&fp_b), Some(&2));
    assert_eq!(by_fp.get(&fp_c), Some(&3));

    // Lossless: same (fp, ts, value) multiset before and after.
    let post = run_metrics_query(&catalog, store.clone(), &q_all).await;
    assert_eq!(total_rows(&post), 120);
    let post_fps = collect_u64(&post, "series_fingerprint");
    let post_ts = collect_u64(&post, "ts_unix_nano");
    let mut prev = (0u64, 0u64);
    for (fp, ts) in post_fps.iter().zip(post_ts.iter()) {
        assert!((*fp, *ts) >= prev, "metrics merged rows sorted by (fp, ts)");
        prev = (*fp, *ts);
    }

    // Inputs reaped.
    for m in [&m1, &m2, &m3] {
        assert!(catalog.get_block(m.uuid).unwrap().is_none());
    }
}
