//! Real logs-v2 writer path: canonical wire bytes -> server decoder -> WAL/pipeline
//! -> homogeneous parquet blocks/catalog -> scry-query projection.

use std::sync::{Arc, Mutex};

use arrow::array::{Array, BinaryArray, StringArray, UInt16Array, UInt64Array, UInt8Array};
use datafusion::execution::context::SessionContext;
use futures::StreamExt;
use object_store::{memory::InMemory, ObjectStore, ObjectStoreExt};
use scry_block::{BlockBuilderConfig, BlockMeta, LogsBlockBuilder};
use scry_catalog::Catalog;
use scry_proto::streaming_logs_v2::{ANY_INT64, ANY_MAP, ANY_STRING};
use scry_proto::{LabelPair, LogEntry, LogStream, LogsBatch, LogsBatchV2Input, OpaqueLogRecordV2};
use scry_query::{register_logs_table, Query, LOGS_TABLE_NAME};
use scry_server::{decode, Pipeline};
use tempfile::TempDir;
use uuid::Uuid;

const BUCKET: &str = "test";

fn string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn any_string(out: &mut Vec<u8>, value: &str) {
    out.push(ANY_STRING);
    string(out, value);
}

/// One small canonical raw log record. Keeping this local makes the test
/// independent of decoder test-only helpers while avoiding a general encoder.
fn canonical_v2_record() -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&0u16.to_be_bytes()); // reserved record flags
    string(&mut out, "resource/v1");
    out.extend_from_slice(&0u32.to_be_bytes()); // dropped resource attrs
    out.push(ANY_MAP);
    out.extend_from_slice(&1u32.to_be_bytes());
    string(&mut out, "service");
    any_string(&mut out, "api-v2");

    out.push(1); // instrumentation scope is present
    string(&mut out, "writer-test");
    string(&mut out, "1.0");
    out.extend_from_slice(&0u32.to_be_bytes());
    out.push(ANY_MAP);
    out.extend_from_slice(&0u32.to_be_bytes());
    string(&mut out, "scope/v1");

    out.extend_from_slice(&2_000u64.to_be_bytes());
    out.extend_from_slice(&2_005u64.to_be_bytes());
    out.extend_from_slice(&17i32.to_be_bytes());
    string(&mut out, "ERROR");
    string(&mut out, "exception");
    any_string(&mut out, "canonical v2 body");
    out.extend_from_slice(&0u32.to_be_bytes()); // dropped log attrs
    out.push(ANY_MAP);
    out.extend_from_slice(&1u32.to_be_bytes());
    string(&mut out, "answer");
    out.push(ANY_INT64);
    out.extend_from_slice(&42i64.to_be_bytes());
    out.extend_from_slice(&1u32.to_be_bytes()); // trace flags
    out.push(16);
    out.extend_from_slice(&[0x11; 16]);
    out.push(8);
    out.extend_from_slice(&[0x22; 8]);
    out
}

fn v1_payload(fp: u64, ts: u64, body: &str) -> Vec<u8> {
    LogsBatch {
        streams: vec![LogStream {
            fingerprint: fp,
            labels: vec![LabelPair {
                key: "service".into(),
                value: "legacy".into(),
            }],
            entries: vec![LogEntry {
                ts_unix_nano: ts,
                severity: 9,
                body: body.into(),
                attributes: vec![],
            }],
        }],
    }
    .encode()
    .unwrap()
}

async fn stored_meta(store: &Arc<dyn ObjectStore>) -> Vec<BlockMeta> {
    let mut objects = store.list(None);
    let mut metas: Vec<BlockMeta> = Vec::new();
    while let Some(object) = objects.next().await {
        let location = object.unwrap().location;
        if location.as_ref().ends_with("meta.json") {
            let bytes = store.get(&location).await.unwrap().bytes().await.unwrap();
            metas.push(serde_json::from_slice(&bytes).unwrap());
        }
    }
    metas.sort_by_key(|meta| meta.ts_min_unix_nano);
    metas
}

#[tokio::test]
async fn canonical_v2_crosses_pipeline_schema_boundaries_restarts_and_queries() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let catalog = Arc::new(Mutex::new(
        Catalog::open(&tmp.path().join("catalog.sqlite"), BUCKET).unwrap(),
    ));
    let writer = Uuid::now_v7();
    // Two rows would normally fit. Thus v1 -> v2 -> v1 rotates specifically at
    // both content-schema boundaries; the final v1 is deliberately replayed.
    let cfg = BlockBuilderConfig {
        max_rows: 2,
        target_bytes: 1024 * 1024,
        row_group_size: 1,
        ..Default::default()
    };
    let raw = canonical_v2_record();
    let v2 = LogsBatchV2Input {
        records: vec![OpaqueLogRecordV2 { value: raw.clone() }],
    }
    .encode()
    .unwrap();

    let mut pipeline = Pipeline::<LogsBlockBuilder>::open_with_config(
        wal_dir.clone(),
        store.clone(),
        Some(catalog.clone()),
        writer,
        decode::logs,
        cfg,
    )
    .await
    .unwrap();
    assert_eq!(
        pipeline
            .ingest(&v1_payload(0x101, 1_000, "legacy before"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(pipeline.ingest(&v2).await.unwrap(), 1);
    assert_eq!(
        pipeline
            .ingest(&v1_payload(0x303, 3_000, "legacy after"))
            .await
            .unwrap(),
        1
    );

    // Drain boundary-triggered uploads and the tiny final block, then reopen the
    // same WAL to exercise the normal restart path and prove released frames do
    // not create duplicate blocks.
    pipeline.flush().await.unwrap();
    assert_eq!(stored_meta(&store).await.len(), 3);
    drop(pipeline);

    let mut reopened = Pipeline::<LogsBlockBuilder>::open_with_config(
        wal_dir,
        store.clone(),
        Some(catalog.clone()),
        writer,
        decode::logs,
        cfg,
    )
    .await
    .unwrap();
    reopened.flush().await.unwrap();

    // Read the actual object-store sidecars, not just in-memory builder state.
    let metas = stored_meta(&store).await;
    assert_eq!(metas.len(), 3);
    assert_eq!(
        metas
            .iter()
            .map(|meta| meta.schema_version)
            .collect::<Vec<_>>(),
        vec![1, 2, 1]
    );
    assert!(metas.iter().all(|meta| meta.row_count == 1));

    let entries = catalog.lock().unwrap().list_blocks().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.meta.schema_version)
            .collect::<Vec<_>>(),
        vec![1, 2, 1]
    );

    // Query through the existing scry-query table/catalog path. Selecting only
    // v2 columns also checks schema normalization/projection around v1 blocks.
    let ctx = SessionContext::new();
    {
        let guard = catalog.lock().unwrap();
        register_logs_table(&ctx, &guard, store.clone(), &Query::default())
            .await
            .unwrap();
    }
    let batches = ctx
        .sql(&format!(
            "SELECT ts_unix_nano, body, observed_ts_unix_nano, severity_text, \
             event_name, trace_flags, raw_record_version, raw_record FROM {LOGS_TABLE_NAME} \
             WHERE event_name = 'exception'"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        2_000
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "canonical v2 body"
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        2_005
    );
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "ERROR"
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "exception"
    );
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(6)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(7)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        raw.as_slice()
    );
}
