//! `BlockBuilder::merge` + `reset` semantics for all three builders.
//!
//! The decode-out-of-lock ingest path decodes each batch into a private
//! scratch builder (no pipeline lock held) and then `merge`s that scratch
//! into the shared builder under the lock. These tests lock down the
//! contract that makes that safe:
//!
//! - merging adds the scratch's rows to the shared builder,
//! - `ts_min`/`ts_max` span both sides,
//! - the series/stream dictionary dedups *across* the merge boundary
//!   (cross-batch dedup must match decoding straight into the shared
//!   builder),
//! - the drained scratch is left empty and reusable for the next batch,
//! - the merged buffers still encode to correct, sorted parquet (for
//!   dummy we also reconstruct the CSR key/value bytes to prove the
//!   offset rebasing is right),
//! - `reset` empties a partially-filled scratch (the failed-batch path).

use std::sync::Arc;

use arrow::array::{Array, BinaryArray, Float64Array, StringArray, UInt64Array};
use bytes::Bytes;
use object_store::{memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use scry_block::{
    BlockBuilder, BlockBuilderConfig, BlockMeta, DummyBlockBuilder, LogsBlockBuilder,
    MetricsBlockBuilder,
};
use scry_proto::streaming::{DummyAppender, LogsAppender, MetricsAppender};
use uuid::Uuid;

const METRIC_TYPE_COUNTER: u8 = 1;
const METRIC_TYPE_GAUGE: u8 = 2;

fn labels(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect()
}

fn read_parquet(bytes: Bytes) -> arrow::record_batch::RecordBatch {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let mut reader = builder.build().unwrap();
    let batch = reader.next().expect("at least one batch").unwrap();
    assert!(reader.next().is_none(), "test data fits in one batch");
    batch
}

/// Fetch the main `*.parquet` (not `*.postings.parquet`) and the
/// `*.meta.json` for a freshly uploaded block by listing the bucket.
async fn fetch_main_and_meta(
    store: &Arc<dyn ObjectStore>,
) -> (arrow::record_batch::RecordBatch, BlockMeta) {
    use futures::StreamExt;
    let mut main: Option<Bytes> = None;
    let mut meta: Option<Bytes> = None;
    let mut listing = store.list(None);
    while let Some(obj) = listing.next().await {
        let path = obj.unwrap().location;
        let s = path.to_string();
        if s.ends_with(".meta.json") {
            meta = Some(store.get(&path).await.unwrap().bytes().await.unwrap());
        } else if s.ends_with(".parquet") && !s.ends_with(".postings.parquet") {
            main = Some(store.get(&path).await.unwrap().bytes().await.unwrap());
        }
    }
    let meta: BlockMeta = serde_json::from_slice(&meta.expect("meta.json uploaded")).unwrap();
    (read_parquet(main.expect("main parquet uploaded")), meta)
}

// ─────────────────────────────────────────────────────────────────────
// Dummy
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn dummy_merge_rebases_csr_and_roundtrips() {
    let writer = Uuid::now_v7();
    let cfg = BlockBuilderConfig::default();
    let mut shared = DummyBlockBuilder::new(writer, cfg);
    let mut scratch = DummyBlockBuilder::new(writer, cfg);

    // Shared gets two records; scratch gets two more (out-of-order ts so
    // the encode sort is exercised). Distinct key/value lengths so a
    // wrong CSR offset rebase would corrupt the reconstructed bytes.
    shared.append_raw(300, b"key-a", b"VALUE-AAAA");
    shared.append_raw(100, b"kb", b"v");
    scratch.append_raw(400, b"key-ccc", b"VAL-C");
    scratch.append_raw(200, b"dddd", b"value-DDDDDD");

    shared.merge(&mut scratch);

    // Drained scratch is empty and reusable.
    assert!(scratch.is_empty(), "scratch drained empty after merge");
    assert_eq!(scratch.row_count(), 0);
    scratch.append_raw(999, b"reuse", b"ok"); // must not panic / corrupt
    assert_eq!(scratch.row_count(), 1);

    assert_eq!(shared.row_count(), 4, "merge adds scratch rows");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta = shared
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("non-empty");
    assert_eq!(meta.row_count, 4);
    assert_eq!(meta.ts_min_unix_nano, 100, "ts_min spans both sides");
    assert_eq!(meta.ts_max_unix_nano, 400, "ts_max spans both sides");

    let batch = read_parquet(
        store
            .get(&ObjPath::from(format!(
                "dummy/1970/01/01/{}/{}.parquet",
                meta.writer_id, meta.uuid
            )))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    );
    let ts = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let keys = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let vals = batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();

    // Sorted by ts ascending; CSR slices reconstruct intact after rebase.
    let expected: [(u64, &str, &[u8]); 4] = [
        (100, "kb", b"v"),
        (200, "dddd", b"value-DDDDDD"),
        (300, "key-a", b"VALUE-AAAA"),
        (400, "key-ccc", b"VAL-C"),
    ];
    for (i, (t, k, v)) in expected.iter().enumerate() {
        assert_eq!(ts.value(i), *t, "row {i} ts");
        assert_eq!(keys.value(i), *k, "row {i} key");
        assert_eq!(vals.value(i), *v, "row {i} value");
    }
}

#[test]
fn dummy_reset_empties_partial_scratch() {
    let writer = Uuid::now_v7();
    let mut b = DummyBlockBuilder::new(writer, BlockBuilderConfig::default());
    b.append_raw(10, b"k", b"v");
    b.append_raw(20, b"kk", b"vv");
    assert_eq!(b.row_count(), 2);
    b.reset();
    assert!(b.is_empty());
    assert_eq!(b.row_count(), 0);
    // Reusable: offsets back to the leading-0 sentinel, so appends work.
    b.append_raw(30, b"after", b"reset");
    assert_eq!(b.row_count(), 1);
}

// ─────────────────────────────────────────────────────────────────────
// Metrics
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn metrics_merge_dedups_series_across_boundary() {
    let writer = Uuid::now_v7();
    let cfg = BlockBuilderConfig::default();
    let mut shared = MetricsBlockBuilder::new(writer, cfg);
    let mut scratch = MetricsBlockBuilder::new(writer, cfg);

    let fp_a: u64 = 0x0000_0000_0000_000A;
    let fp_b: u64 = 0x0000_0000_0000_000B;

    // Shared knows series A; scratch re-sends A (dup across the merge)
    // plus a new series B. After merge the dict must hold A and B once.
    shared.observe_series(fp_a, METRIC_TYPE_COUNTER, labels(&[("__name__", "a")]));
    shared.append_sample(fp_a, 100, 1.0);

    scratch.observe_series(fp_a, METRIC_TYPE_COUNTER, labels(&[("__name__", "a")]));
    scratch.observe_series(fp_b, METRIC_TYPE_GAUGE, labels(&[("__name__", "b")]));
    scratch.append_sample(fp_b, 50, 2.0);
    scratch.append_sample(fp_a, 200, 3.0);

    shared.merge(&mut scratch);

    assert!(scratch.is_empty(), "scratch drained empty after merge");
    assert_eq!(scratch.row_count(), 0);
    assert_eq!(shared.row_count(), 3, "all samples merged");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta = shared
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("non-empty");
    assert_eq!(meta.row_count, 3);
    assert_eq!(meta.ts_min_unix_nano, 50);
    assert_eq!(meta.ts_max_unix_nano, 200);
    let series_types = meta.series_types.as_ref().unwrap();
    assert_eq!(series_types.len(), 2, "series A deduped across the merge");
    // Insertion order preserved: A (from shared), then B (from scratch).
    assert_eq!(series_types[0], (fp_a, METRIC_TYPE_COUNTER));
    assert_eq!(series_types[1], (fp_b, METRIC_TYPE_GAUGE));

    let (batch, _) = fetch_main_and_meta(&store).await;
    let fps = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let tss = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let vals = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    // Sorted by (fp, ts): (A,100),(A,200),(B,50).
    let expected = [(fp_a, 100u64, 1.0f64), (fp_a, 200, 3.0), (fp_b, 50, 2.0)];
    for (i, (fp, ts, v)) in expected.iter().enumerate() {
        assert_eq!(fps.value(i), *fp, "row {i} fp");
        assert_eq!(tss.value(i), *ts, "row {i} ts");
        assert!((vals.value(i) - *v).abs() < 1e-9, "row {i} value");
    }
}

#[test]
fn metrics_reset_empties_partial_scratch() {
    let writer = Uuid::now_v7();
    let mut b = MetricsBlockBuilder::new(writer, BlockBuilderConfig::default());
    b.observe_series(7, METRIC_TYPE_COUNTER, labels(&[("__name__", "x")]));
    b.append_sample(7, 1, 1.0);
    assert_eq!(b.row_count(), 1);
    b.reset();
    assert!(b.is_empty());
    // series_seen cleared too: re-observing the same fp is accepted again.
    b.observe_series(7, METRIC_TYPE_COUNTER, labels(&[("__name__", "x")]));
    b.append_sample(7, 2, 2.0);
    assert_eq!(b.row_count(), 1);
}

// ─────────────────────────────────────────────────────────────────────
// Logs
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn logs_merge_dedups_streams_across_boundary() {
    let writer = Uuid::now_v7();
    let cfg = BlockBuilderConfig::default();
    let mut shared = LogsBlockBuilder::new(writer, cfg);
    let mut scratch = LogsBlockBuilder::new(writer, cfg);

    let s_a: u64 = 0x0000_0000_0000_00A0;
    let s_b: u64 = 0x0000_0000_0000_00B0;

    shared.observe_stream(s_a, labels(&[("service", "api")]));
    shared.append_entry(s_a, 100, 1, b"first".to_vec(), labels(&[("k", "1")]));

    scratch.observe_stream(s_a, labels(&[("service", "api")])); // dup
    scratch.observe_stream(s_b, labels(&[("service", "worker")]));
    scratch.append_entry(s_b, 50, 2, b"second".to_vec(), vec![]);
    scratch.append_entry(s_a, 200, 3, b"third".to_vec(), labels(&[("k", "2")]));

    shared.merge(&mut scratch);

    assert!(scratch.is_empty(), "scratch drained empty after merge");
    assert_eq!(scratch.row_count(), 0);
    assert_eq!(shared.row_count(), 3);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta = shared
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("non-empty");
    assert_eq!(meta.row_count, 3);
    assert_eq!(meta.ts_min_unix_nano, 50);
    assert_eq!(meta.ts_max_unix_nano, 200);
    let all_fps = meta.all_fingerprints.as_ref().unwrap();
    assert_eq!(all_fps.len(), 2, "stream A deduped across the merge");

    let (batch, _) = fetch_main_and_meta(&store).await;
    let fps = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let tss = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let bodies = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Sorted by (fp, ts): (A,100,"first"),(A,200,"third"),(B,50,"second").
    let expected = [
        (s_a, 100u64, "first"),
        (s_a, 200, "third"),
        (s_b, 50, "second"),
    ];
    for (i, (fp, ts, body)) in expected.iter().enumerate() {
        assert_eq!(fps.value(i), *fp, "row {i} fp");
        assert_eq!(tss.value(i), *ts, "row {i} ts");
        assert_eq!(bodies.value(i), *body, "row {i} body");
    }

    // Postings present and non-empty (two streams → two label rows).
    assert!(meta.postings_size_bytes.is_some_and(|n| n > 0));
}

#[test]
fn logs_reset_empties_partial_scratch() {
    let writer = Uuid::now_v7();
    let mut b = LogsBlockBuilder::new(writer, BlockBuilderConfig::default());
    b.observe_stream(9, labels(&[("service", "x")]));
    b.append_entry(9, 1, 1, b"msg".to_vec(), vec![]);
    assert_eq!(b.row_count(), 1);
    b.reset();
    assert!(b.is_empty());
    b.observe_stream(9, labels(&[("service", "x")]));
    b.append_entry(9, 2, 1, b"again".to_vec(), vec![]);
    assert_eq!(b.row_count(), 1);
}
