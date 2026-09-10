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
    block_path, logs_physical_schema_v2, logs_physical_schema_v3, AlwaysValid, BlockBuilder,
    BlockBuilderConfig, LogsBlockBuilder,
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

async fn relabel_with_fidelity(
    store: &Arc<dyn ObjectStore>,
    mut meta: scry_block::BlockMeta,
    schema_version: u32,
    raw_version: u16,
    raw: &[u8],
    received_ts: Option<u64>,
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
        Arc::new(UInt16Array::from(vec![Some(raw_version)])),
        Arc::new(BinaryArray::from(vec![Some(raw)])),
    ]);
    let schema = if schema_version == 3 {
        columns.push(Arc::new(UInt64Array::from(vec![received_ts])));
        logs_physical_schema_v3()
    } else {
        logs_physical_schema_v2()
    };
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut output = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut output, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    store
        .put(&main_path, Bytes::from(output).into())
        .await
        .unwrap();
    meta.schema_version = schema_version;
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
    let a = relabel_with_fidelity(
        &store,
        make_v1(&store, writer, 1, 10).await,
        2,
        1,
        &raw_a,
        None,
    )
    .await;
    let b = relabel_with_fidelity(
        &store,
        make_v1(&store, writer, 2, 20).await,
        2,
        1,
        &raw_b,
        None,
    )
    .await;
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
async fn logs_v3_merge_preserves_opaque_raw_and_receipt_values() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let raw_a = [0, 0xff, 0x80, 1];
    let raw_b = [0x53, 0x4c, 0x32, 0, 9];
    let a = relabel_with_fidelity(
        &store,
        make_v1(&store, writer, 1, 10).await,
        3,
        scry_proto::constants::LOGS_RAW_VERSION_V2,
        &raw_a,
        Some(101),
    )
    .await;
    let b = relabel_with_fidelity(
        &store,
        make_v1(&store, writer, 2, 20).await,
        3,
        scry_proto::constants::LOGS_RAW_VERSION_V2,
        &raw_b,
        Some(202),
    )
    .await;
    let resources = CompactResources::new(ResourceConfig::default()).unwrap();
    let merged = merge_blocks(
        store.clone(),
        "test",
        "logs",
        &[entry(a), entry(b)],
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
    assert_eq!(merged.schema_version, 3);
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
    assert!(batches
        .iter()
        .all(|batch| batch.schema() == logs_physical_schema_v3()));
    let mut values = Vec::new();
    for batch in batches {
        let raws = batch
            .column_by_name("raw_record")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let receipts = batch
            .column_by_name("received_ts_unix_nano")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        values.extend(
            raws.iter()
                .zip(receipts.iter())
                .map(|(raw, receipt)| (raw.unwrap().to_vec(), receipt.unwrap())),
        );
    }
    values.sort();
    assert_eq!(values, vec![(raw_a.to_vec(), 101), (raw_b.to_vec(), 202)]);
}

#[tokio::test]
async fn mixed_logs_versions_are_rejected_by_merge_entrypoint() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let v1 = make_v1(&store, writer, 1, 10).await;
    let v2 = relabel_with_fidelity(
        &store,
        make_v1(&store, writer, 2, 20).await,
        2,
        1,
        b"opaque",
        None,
    )
    .await;
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

    for version in [2, 3, 99] {
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
        if matches!(version, 2 | 3) {
            assert!(
                message.contains(&format!(
                    "parquet schema does not match logs schema version {version}"
                )),
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
