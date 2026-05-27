//! End-to-end roundtrip for `MetricsBlockBuilder`: feed a known set
//! of series + samples through the appender, upload to an in-memory
//! object store, fetch all three artefacts back (main parquet,
//! postings parquet, meta.json), and verify their contents.
//!
//! The goal is to lock down:
//! - main parquet column shape + intra-block sort by (fp, ts)
//! - postings parquet shape: one row per (label_name, label_value),
//!   sorted by name then value, fingerprint list sorted+deduped
//! - sidecar carries has_postings, postings_size_bytes, series_types
//! - upload order doesn't strand the catalog (meta.json reaches the
//!   bucket last)

use std::sync::Arc;

use arrow::array::{Array, Float64Array, ListArray, StringArray, UInt64Array};
use bytes::Bytes;
use object_store::{memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use scry_block::{BlockBuilder, BlockBuilderConfig, BlockMeta, MetricsBlockBuilder};
use scry_proto::streaming::MetricsAppender;
use uuid::Uuid;

/// `metric_type` byte. We don't import the proto enum just to read
/// one constant back; the wire spec pins counter=1, gauge=2.
const METRIC_TYPE_COUNTER: u8 = 1;
const METRIC_TYPE_GAUGE: u8 = 2;

fn labels(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect()
}

/// Read a parquet payload back into a single concatenated RecordBatch.
/// Test data is tiny (handful of rows); one batch is fine.
fn read_parquet(bytes: Bytes) -> arrow::record_batch::RecordBatch {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let mut reader = builder.build().unwrap();
    let batch = reader.next().expect("at least one batch").unwrap();
    assert!(reader.next().is_none(), "test data fits in one batch");
    batch
}

#[tokio::test]
async fn metrics_block_roundtrip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let mut b = MetricsBlockBuilder::new(writer, BlockBuilderConfig::default());

    // Three series. Fingerprints chosen so the unsorted insertion
    // order differs from the sorted (fp, ts) order — the test would
    // pass trivially otherwise.
    //
    // FP_HIGH appears first in insertion order but should come *last*
    // in the sorted main parquet.
    let fp_high: u64 = 0xFFFF_0000_0000_0001;
    let fp_mid: u64 = 0x0000_FFFF_0000_0002;
    let fp_low: u64 = 0x0000_0000_FFFF_0003;

    // Series dict order: HIGH first, then MID, then LOW. Postings
    // build sorts by (label_name, label_value), not insertion order.
    b.observe_series(
        fp_high,
        METRIC_TYPE_COUNTER,
        labels(&[("__name__", "http_requests_total"), ("service", "api")]),
    );
    b.observe_series(
        fp_mid,
        METRIC_TYPE_GAUGE,
        labels(&[("__name__", "memory_bytes"), ("service", "api")]),
    );
    b.observe_series(
        fp_low,
        METRIC_TYPE_COUNTER,
        labels(&[("__name__", "http_requests_total"), ("service", "worker")]),
    );

    // Duplicate series observation in a "later batch": must be a
    // no-op. We can't directly assert that observe_series didn't run
    // (it doesn't return anything), but the postings shape downstream
    // would diverge if it double-counted, so the test would notice.
    b.observe_series(
        fp_high,
        METRIC_TYPE_COUNTER,
        labels(&[("__name__", "http_requests_total"), ("service", "api")]),
    );

    // Samples — intentional out-of-order insertion. Sorted order
    // should be (fp_low, ts=100), (fp_low, ts=200), (fp_mid, ts=150),
    // (fp_high, ts=50), (fp_high, ts=75).
    b.append_sample(fp_high, 50, 1.0);
    b.append_sample(fp_mid, 150, 2.5);
    b.append_sample(fp_low, 200, 3.0);
    b.append_sample(fp_high, 75, 1.5);
    b.append_sample(fp_low, 100, 2.0);

    assert_eq!(b.row_count(), 5);
    assert!(!b.is_empty());

    let meta = b
        .finish_and_upload(store.as_ref())
        .await
        .expect("upload OK")
        .expect("non-empty block → Some(meta)");

    // Sidecar invariants.
    assert_eq!(meta.signal, "metrics");
    assert_eq!(meta.writer_id, writer);
    assert_eq!(meta.row_count, 5);
    assert_eq!(meta.ts_min_unix_nano, 50);
    assert_eq!(meta.ts_max_unix_nano, 200);
    assert!(meta.has_postings, "metrics blocks always carry postings");
    assert!(
        meta.postings_size_bytes.is_some_and(|n| n > 0),
        "postings parquet should be non-empty"
    );
    let series_types = meta
        .series_types
        .as_ref()
        .expect("metrics blocks carry series_types");
    assert_eq!(series_types.len(), 3, "dedup leaves three unique series");
    // Series_types order tracks insertion (dedup-by-fingerprint).
    assert_eq!(series_types[0], (fp_high, METRIC_TYPE_COUNTER));
    assert_eq!(series_types[1], (fp_mid, METRIC_TYPE_GAUGE));
    assert_eq!(series_types[2], (fp_low, METRIC_TYPE_COUNTER));

    // All three artefacts uploaded under the canonical path. Pull
    // them by listing the bucket — sidecar last, per upload-order
    // contract, but we don't have a way to test ordering with an
    // InMemory store; trust the code's `await` discipline.
    let prefix = ObjPath::from(format!(
        "metrics/{}/{}/{}/{}/{}",
        // The block_path helper uses YYYY/MM/DD from ts_min=50ns →
        // epoch 1970-01-01. Hardcode rather than re-derive — if the
        // path scheme ever changes this assertion is the first place
        // to notice.
        "1970",
        "01",
        "01",
        meta.writer_id,
        meta.uuid,
    ));
    let main_path = ObjPath::from(format!("{prefix}.parquet"));
    let postings_path = ObjPath::from(format!("{prefix}.postings.parquet"));
    let meta_path = ObjPath::from(format!("{prefix}.meta.json"));

    let main_bytes: Bytes = store.get(&main_path).await.unwrap().bytes().await.unwrap();
    let postings_bytes: Bytes =
        store.get(&postings_path).await.unwrap().bytes().await.unwrap();
    let meta_bytes: Bytes = store.get(&meta_path).await.unwrap().bytes().await.unwrap();

    // ── Main parquet shape ────────────────────────────────────────
    let main_batch = read_parquet(main_bytes);
    assert_eq!(main_batch.num_rows(), 5);
    assert_eq!(main_batch.schema().field(0).name(), "series_fingerprint");
    assert_eq!(main_batch.schema().field(1).name(), "ts_unix_nano");
    assert_eq!(main_batch.schema().field(2).name(), "value");

    let fps = main_batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let tss = main_batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let vals = main_batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    // Expected (fp, ts, value) tuples in sorted (fp, ts) order.
    let expected = [
        (fp_low, 100u64, 2.0f64),
        (fp_low, 200, 3.0),
        (fp_mid, 150, 2.5),
        (fp_high, 50, 1.0),
        (fp_high, 75, 1.5),
    ];
    for (i, (fp, ts, v)) in expected.iter().enumerate() {
        assert_eq!(fps.value(i), *fp, "row {i} fingerprint");
        assert_eq!(tss.value(i), *ts, "row {i} ts");
        assert!((vals.value(i) - *v).abs() < 1e-9, "row {i} value");
    }

    // ── Postings parquet shape ────────────────────────────────────
    let postings_batch = read_parquet(postings_bytes);
    let names = postings_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = postings_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let lists = postings_batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    // Expected postings rows (sorted by (name, value)):
    //   __name__ → http_requests_total   → [fp_low, fp_high]  (sorted)
    //   __name__ → memory_bytes          → [fp_mid]
    //   service  → api                   → [fp_mid, fp_high]  (sorted)
    //   service  → worker                → [fp_low]
    let expected_rows: Vec<(&str, &str, Vec<u64>)> = vec![
        ("__name__", "http_requests_total", {
            let mut v = vec![fp_low, fp_high];
            v.sort();
            v
        }),
        ("__name__", "memory_bytes", vec![fp_mid]),
        ("service", "api", {
            let mut v = vec![fp_mid, fp_high];
            v.sort();
            v
        }),
        ("service", "worker", vec![fp_low]),
    ];
    assert_eq!(postings_batch.num_rows(), expected_rows.len());
    for (i, (n, v, fps_expected)) in expected_rows.iter().enumerate() {
        assert_eq!(names.value(i), *n, "row {i} label_name");
        assert_eq!(values.value(i), *v, "row {i} label_value");
        let arr = lists.value(i);
        let got = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
        let got_vec: Vec<u64> = (0..got.len()).map(|j| got.value(j)).collect();
        assert_eq!(&got_vec, fps_expected, "row {i} fingerprint list");
    }

    // ── Sidecar JSON parses back into the same BlockMeta ──────────
    let parsed: BlockMeta = serde_json::from_slice(&meta_bytes).unwrap();
    assert_eq!(parsed.uuid, meta.uuid);
    assert_eq!(parsed.row_count, 5);
    assert!(parsed.has_postings);
    assert_eq!(parsed.postings_size_bytes, meta.postings_size_bytes);
    assert_eq!(parsed.series_types, meta.series_types);
}

#[tokio::test]
async fn metrics_empty_builder_uploads_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let b = MetricsBlockBuilder::new(writer, BlockBuilderConfig::default());

    assert!(b.is_empty());
    let res = b.finish_and_upload(store.as_ref()).await.unwrap();
    assert!(res.is_none(), "empty builder skips upload");

    // Bucket should be untouched.
    let mut listing = store.list(None);
    use futures::StreamExt;
    let mut count = 0;
    while let Some(_) = listing.next().await {
        count += 1;
    }
    assert_eq!(count, 0, "no objects uploaded for empty builder");
}
