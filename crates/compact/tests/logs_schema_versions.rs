use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, StringArray, UInt16Array, UInt64Array, UInt8Array,
};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use scry_block::{
    block_path, logs_physical_schema_v2, AlwaysValid, BlockBuilder, BlockBuilderConfig,
    LogsBlockBuilder,
};
use scry_catalog::CatalogEntry;
use scry_compact::{merge_blocks, CompactResources, ResourceConfig};
use scry_proto::streaming::LogsAppender;
use uuid::Uuid;

fn cfg() -> BlockBuilderConfig {
    BlockBuilderConfig {
        row_group_size: 32,
        ..Default::default()
    }
}

fn path(meta: &scry_block::BlockMeta, suffix: &str) -> ObjPath {
    ObjPath::from(block_path(
        &meta.signal,
        meta.ts_min_unix_nano,
        meta.writer_id,
        meta.uuid,
        suffix,
    ))
}

async fn make_v1(
    store: &Arc<dyn ObjectStore>,
    writer: Uuid,
    fp: u64,
    ts: u64,
) -> scry_block::BlockMeta {
    let mut builder = LogsBlockBuilder::new(writer, cfg());
    builder.observe_stream(fp, vec![(b"service".to_vec(), b"api".to_vec())]);
    builder.append_entry(fp, ts, 9, b"body".to_vec(), vec![]);
    builder
        .finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .unwrap()
}

async fn relabel_as_v2_with_raw(
    store: &Arc<dyn ObjectStore>,
    mut meta: scry_block::BlockMeta,
    raw: &[u8],
) -> scry_block::BlockMeta {
    let main_path = path(&meta, "parquet");
    let parquet = store.get(&main_path).await.unwrap().bytes().await.unwrap();
    let batch = ParquetRecordBatchReaderBuilder::try_new(parquet)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let n = batch.num_rows();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.extend([
        Arc::new(UInt64Array::new_null(n)) as ArrayRef,
        Arc::new(StringArray::new_null(n)),
        Arc::new(StringArray::new_null(n)),
        Arc::new(FixedSizeBinaryArray::new_null(16, n)),
        Arc::new(FixedSizeBinaryArray::new_null(8, n)),
        Arc::new(UInt8Array::new_null(n)),
        Arc::new(UInt16Array::from(vec![Some(1)])),
        Arc::new(BinaryArray::from(vec![Some(raw)])),
    ]);
    let batch = RecordBatch::try_new(logs_physical_schema_v2(), columns).unwrap();
    let mut output = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut output, logs_physical_schema_v2(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    store
        .put(&main_path, Bytes::from(output).into())
        .await
        .unwrap();
    meta.schema_version = 2;
    let meta_path = path(&meta, "meta.json");
    store
        .put(
            &meta_path,
            Bytes::from(serde_json::to_vec(&meta).unwrap()).into(),
        )
        .await
        .unwrap();
    meta
}

fn entry(meta: scry_block::BlockMeta) -> CatalogEntry {
    CatalogEntry {
        level: meta.level,
        bucket: "test".into(),
        date: "1970-01-01".into(),
        meta,
    }
}

async fn put_meta(store: &Arc<dyn ObjectStore>, meta: &scry_block::BlockMeta) {
    store
        .put(
            &path(meta, "meta.json"),
            Bytes::from(serde_json::to_vec(meta).unwrap()).into(),
        )
        .await
        .unwrap();
}

async fn object_count(store: &Arc<dyn ObjectStore>) -> usize {
    store
        .list(None)
        .fold(0, |count, object| async move {
            object.unwrap();
            count + 1
        })
        .await
}

#[tokio::test]
async fn logs_v2_merge_preserves_opaque_raw_bytes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let raw_a = [0, 0xff, 0x80, 1, 2, 0];
    let raw_b = [0x53, 0x4c, 0x32, 0, 9, 8, 7];
    let a = relabel_as_v2_with_raw(&store, make_v1(&store, writer, 1, 10).await, &raw_a).await;
    let b = relabel_as_v2_with_raw(&store, make_v1(&store, writer, 2, 20).await, &raw_b).await;
    let inputs = vec![entry(a), entry(b)];
    let resources = CompactResources::new(ResourceConfig::default()).unwrap();
    let merged = merge_blocks(
        store.clone(),
        "test",
        "logs",
        &inputs,
        1,
        Uuid::now_v7(),
        &cfg(),
        &AlwaysValid,
        &resources,
        resources.config().non_datafusion_memory_bytes,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(merged.schema_version, 2);

    let parquet = store
        .get(&path(&merged, "parquet"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let batches: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(parquet)
        .unwrap()
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let mut raws = Vec::new();
    for batch in batches {
        let raw = batch
            .column_by_name("raw_record")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        raws.extend(raw.iter().flatten().map(<[u8]>::to_vec));
    }
    raws.sort();
    let mut expected = vec![raw_a.to_vec(), raw_b.to_vec()];
    expected.sort();
    assert_eq!(raws, expected);
}

#[tokio::test]
async fn mixed_logs_versions_are_rejected_by_merge_entrypoint() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let v1 = make_v1(&store, writer, 1, 10).await;
    let v2 = relabel_as_v2_with_raw(&store, make_v1(&store, writer, 2, 20).await, b"opaque").await;
    let inputs = vec![entry(v1), entry(v2)];
    let resources = CompactResources::new(ResourceConfig::default()).unwrap();

    let error = merge_blocks(
        store.clone(),
        "test",
        "logs",
        &inputs,
        1,
        Uuid::now_v7(),
        &cfg(),
        &AlwaysValid,
        &resources,
        resources.config().non_datafusion_memory_bytes,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("schema version 2, expected 1"),
        "{error:#}"
    );
}

#[tokio::test]
async fn unknown_and_mislabeled_logs_schemas_are_rejected_without_output() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let a = make_v1(&store, writer, 1, 10).await;
    let b = make_v1(&store, writer, 2, 20).await;
    let resources = CompactResources::new(ResourceConfig::default()).unwrap();

    for version in [2, 99] {
        let mut inputs = vec![entry(a.clone()), entry(b.clone())];
        for input in &mut inputs {
            input.meta.schema_version = version;
            put_meta(&store, &input.meta).await;
        }
        let objects_before = object_count(&store).await;
        let error = merge_blocks(
            store.clone(),
            "test",
            "logs",
            &inputs,
            1,
            Uuid::now_v7(),
            &cfg(),
            &AlwaysValid,
            &resources,
            resources.config().non_datafusion_memory_bytes,
        )
        .await
        .unwrap_err();
        let message = format!("{error:#}");
        if version == 2 {
            assert!(
                message.contains("parquet schema does not match logs schema version 2"),
                "{message}"
            );
        } else {
            assert!(
                message.contains("unsupported logs block schema version 99"),
                "{message}"
            );
        }
        assert_eq!(
            object_count(&store).await,
            objects_before,
            "schema rejection must not stage output objects"
        );
    }
}
