//! End-to-end roundtrip for `ProfilesBlockBuilder`: feed a handful of
//! profile blobs through the appender, upload to an in-memory object
//! store, fetch the parquet + meta.json back, and verify contents.
//!
//! Locks down:
//! - main parquet column shape + intra-block sort by ts
//! - the pprof bytes survive verbatim in the opaque `data` Binary column
//! - labels round-trip through the `Map<Utf8,Utf8>` column
//! - sidecar has has_postings=false, no postings file in the bucket

use std::sync::Arc;

use arrow::array::{Array, BinaryArray, MapArray, StringArray, UInt8Array, UInt64Array};
use bytes::Bytes;
use object_store::{memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use scry_block::{BlockBuilder, BlockBuilderConfig, BlockMeta, ProfilesBlockBuilder};
use scry_proto::streaming::ProfilesAppender;
use uuid::Uuid;

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

#[tokio::test]
async fn profiles_block_roundtrip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let mut b = ProfilesBlockBuilder::new(writer, BlockBuilderConfig::default());

    // Three blobs, inserted out of ts order; sorted order is 100, 200, 300.
    let blob_a = vec![0x1f, 0x8b, 0x08, 0x00]; // gzip-ish magic; opaque to us
    let blob_b = vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x01];
    let blob_c = vec![0x42];

    b.append_blob(
        300,
        50,
        labels(&[("profile.type", "cpu"), ("service", "worker")]),
        2,
        blob_c.clone(),
    );
    b.append_blob(
        100,
        10,
        labels(&[("profile.type", "cpu"), ("service", "api")]),
        1,
        blob_a.clone(),
    );
    b.append_blob(
        200,
        20,
        labels(&[("profile.type", "heap"), ("service", "api")]),
        1,
        blob_b.clone(),
    );

    assert_eq!(b.row_count(), 3);
    assert!(!b.is_empty());

    let meta = b
        .finish_and_upload(store.as_ref())
        .await
        .expect("upload OK")
        .expect("non-empty block → Some(meta)");

    assert_eq!(meta.signal, "profiles");
    assert_eq!(meta.writer_id, writer);
    assert_eq!(meta.row_count, 3);
    assert_eq!(meta.ts_min_unix_nano, 100);
    assert_eq!(meta.ts_max_unix_nano, 300);
    assert!(!meta.has_postings, "profiles carry no postings");
    assert!(meta.postings_size_bytes.is_none());
    assert!(meta.series_types.is_none());
    assert!(meta.all_fingerprints.is_none());

    let prefix = ObjPath::from(format!(
        "profiles/{}/{}/{}/{}/{}",
        "1970", "01", "01", meta.writer_id, meta.uuid,
    ));
    let main_path = ObjPath::from(format!("{prefix}.parquet"));
    let postings_path = ObjPath::from(format!("{prefix}.postings.parquet"));
    let meta_path = ObjPath::from(format!("{prefix}.meta.json"));

    // No postings object should exist.
    assert!(
        store.get(&postings_path).await.is_err(),
        "profiles block must not write a postings file"
    );

    let main_bytes: Bytes = store.get(&main_path).await.unwrap().bytes().await.unwrap();
    let meta_bytes: Bytes = store.get(&meta_path).await.unwrap().bytes().await.unwrap();

    let batch = read_parquet(main_bytes);
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.schema().field(0).name(), "ts_unix_nano");
    assert_eq!(batch.schema().field(1).name(), "duration_nano");
    assert_eq!(batch.schema().field(2).name(), "labels");
    assert_eq!(batch.schema().field(3).name(), "format");
    assert_eq!(batch.schema().field(4).name(), "data");

    let ts = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let dur = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let fmt = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let data = batch
        .column(4)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let labels_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap();

    // Sorted by ts: (100, blob_a), (200, blob_b), (300, blob_c).
    let expected: [(u64, u64, u8, &Vec<u8>); 3] = [
        (100, 10, 1, &blob_a),
        (200, 20, 1, &blob_b),
        (300, 50, 2, &blob_c),
    ];
    for (i, (ets, edur, efmt, eblob)) in expected.iter().enumerate() {
        assert_eq!(ts.value(i), *ets, "row {i} ts");
        assert_eq!(dur.value(i), *edur, "row {i} duration");
        assert_eq!(fmt.value(i), *efmt, "row {i} format");
        assert_eq!(data.value(i), eblob.as_slice(), "row {i} blob bytes verbatim");
    }

    // Row 0's labels: profile.type=cpu, service=api.
    let entries = labels_col.value(0);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut pairs: Vec<(String, String)> = (0..keys.len())
        .map(|j| (keys.value(j).to_string(), values.value(j).to_string()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("profile.type".to_string(), "cpu".to_string()),
            ("service".to_string(), "api".to_string()),
        ]
    );

    let parsed: BlockMeta = serde_json::from_slice(&meta_bytes).unwrap();
    assert_eq!(parsed.uuid, meta.uuid);
    assert_eq!(parsed.row_count, 3);
    assert!(!parsed.has_postings);
}

#[tokio::test]
async fn profiles_empty_builder_uploads_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let b = ProfilesBlockBuilder::new(writer, BlockBuilderConfig::default());

    assert!(b.is_empty());
    let res = b.finish_and_upload(store.as_ref()).await.unwrap();
    assert!(res.is_none(), "empty builder skips upload");

    use futures::StreamExt;
    let mut listing = store.list(None);
    let mut count = 0;
    while listing.next().await.is_some() {
        count += 1;
    }
    assert_eq!(count, 0, "no objects uploaded for empty builder");
}
