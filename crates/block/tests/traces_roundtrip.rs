//! End-to-end roundtrip for `TracesBlockBuilder`: feed spans (with
//! nested events/links) through the appender, upload to an in-memory
//! object store, fetch the parquet + meta.json back, and verify the
//! shape — including that the native nested `events`/`links`
//! `List<Struct<…>>` columns survive the round trip populated.
//!
//! Locks down:
//! - main parquet column shape + intra-block sort by (trace_id, start)
//! - denormalised resource_labels / scope name+version per row
//! - nested events[] (ts, name, attributes Map) and links[]
//!   (trace_id, span_id, attributes Map) survive verbatim
//! - parent_span_id nullability
//! - sidecar has_postings=false, no postings file in the bucket

use std::sync::Arc;

use arrow::array::{
    Array, FixedSizeBinaryArray, ListArray, MapArray, StringArray, StructArray, UInt64Array,
    UInt8Array,
};
use bytes::Bytes;
use object_store::{memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use scry_block::{BlockBuilder, BlockBuilderConfig, BlockMeta, TracesBlockBuilder};
use scry_proto::streaming::{DecodedEvent, DecodedLink, DecodedSpan, TracesAppender};
use uuid::Uuid;

fn attrs(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
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

/// Read the (sorted) key→value pairs out of one Map row.
fn map_pairs(entries: &StructArray) -> Vec<(String, String)> {
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
    pairs
}

#[tokio::test]
async fn traces_block_roundtrip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let mut b = TracesBlockBuilder::new(writer, BlockBuilderConfig::default());

    let trace_a = [0xAAu8; 16];
    let trace_b = [0xBBu8; 16];
    let span_root = [0x10u8; 8]; // trace A, start 200, has 1 event + 1 link
    let span_child = [0x11u8; 8]; // trace A, start 100, has 2 events, parent=span_root
    let span_b = [0x20u8; 8]; // trace B, start 50, no events/links

    // ── Span 1: trace A, start 200 (inserted first; sorts last within A) ──
    {
        let res = attrs(&[
            ("service.name", "api"),
            ("service.namespace", "shop"),
            ("deployment.environment", "prod"),
        ]);
        let span_attrs = attrs(&[("http.method", "GET")]);
        let ev_attrs = attrs(&[("exception.type", "IOError")]);
        let events = vec![DecodedEvent {
            ts_unix_nano: 205,
            name: b"exception".to_vec(),
            attributes: ev_attrs,
        }];
        let link_attrs = attrs(&[("link.kind", "follows")]);
        let links = vec![DecodedLink {
            trace_id: vec![0xCCu8; 16],
            span_id: vec![0xDDu8; 8],
            attributes: link_attrs,
        }];
        let span = DecodedSpan {
            trace_id: &trace_a,
            span_id: &span_root,
            parent_span_id: None,
            resource_labels: &res,
            scope_name: b"tracer-a",
            scope_version: b"1.0",
            name: b"root-span",
            kind: 2,
            start_unix_nano: 200,
            end_unix_nano: 260,
            status_code: 1,
            status_message: b"ok",
            attributes: &span_attrs,
            events: &events,
            links: &links,
        };
        b.append_span(&span);
    }

    // ── Span 2: trace B, start 50 (sorts last overall — trace_b > trace_a) ──
    {
        let res = attrs(&[
            ("service.name", "worker"),
            ("service.namespace", "shop"),
            ("deployment.environment", "staging"),
        ]);
        let span = DecodedSpan {
            trace_id: &trace_b,
            span_id: &span_b,
            parent_span_id: None,
            resource_labels: &res,
            scope_name: b"tracer-b",
            scope_version: b"2.1",
            name: b"b-span",
            kind: 1,
            start_unix_nano: 50,
            end_unix_nano: 60,
            status_code: 0,
            status_message: b"",
            attributes: &[],
            events: &[],
            links: &[],
        };
        b.append_span(&span);
    }

    // ── Span 3: trace A, start 100 (sorts first within A) ──
    {
        let res = attrs(&[("service.name", "api")]);
        let span_attrs = attrs(&[("http.method", "POST"), ("http.status", "200")]);
        let events = vec![
            DecodedEvent {
                ts_unix_nano: 110,
                name: b"cache.miss".to_vec(),
                attributes: attrs(&[("key", "abc")]),
            },
            DecodedEvent {
                ts_unix_nano: 120,
                name: b"db.query".to_vec(),
                attributes: vec![],
            },
        ];
        let span = DecodedSpan {
            trace_id: &trace_a,
            span_id: &span_child,
            parent_span_id: Some(&span_root),
            resource_labels: &res,
            scope_name: b"tracer-a",
            scope_version: b"1.0",
            name: b"child-span",
            kind: 3,
            start_unix_nano: 100,
            end_unix_nano: 150,
            status_code: 2,
            status_message: b"error",
            attributes: &span_attrs,
            events: &events,
            links: &[],
        };
        b.append_span(&span);
    }

    assert_eq!(b.row_count(), 3);

    let meta = b
        .finish_and_upload(store.as_ref())
        .await
        .expect("upload OK")
        .expect("non-empty block → Some(meta)");

    assert_eq!(meta.signal, "traces");
    assert_eq!(meta.writer_id, writer);
    assert_eq!(meta.row_count, 3);
    assert_eq!(meta.ts_min_unix_nano, 50, "ts_min over start_unix_nano");
    assert_eq!(meta.ts_max_unix_nano, 200);
    assert!(!meta.has_postings, "traces carry no postings");
    assert!(meta.postings_size_bytes.is_none());

    let prefix = ObjPath::from(format!(
        "traces/{}/{}/{}/{}/{}",
        "1970", "01", "01", meta.writer_id, meta.uuid,
    ));
    let main_path = ObjPath::from(format!("{prefix}.parquet"));
    let postings_path = ObjPath::from(format!("{prefix}.postings.parquet"));
    let meta_path = ObjPath::from(format!("{prefix}.meta.json"));

    assert!(
        store.get(&postings_path).await.is_err(),
        "traces block must not write a postings file"
    );

    let main_bytes: Bytes = store.get(&main_path).await.unwrap().bytes().await.unwrap();
    let meta_bytes: Bytes = store.get(&meta_path).await.unwrap().bytes().await.unwrap();

    let batch = read_parquet(main_bytes);
    assert_eq!(batch.num_rows(), 3);
    // Column order matches the schema.
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec![
            "trace_id",
            "span_id",
            "parent_span_id",
            "resource_labels",
            "service_name",
            "service_namespace",
            "deployment_environment",
            "scope_name",
            "scope_version",
            "name",
            "kind",
            "start_unix_nano",
            "end_unix_nano",
            "status_code",
            "status_message",
            "attributes",
            "events",
            "links",
        ]
    );

    let trace_id = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let span_id = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let parent = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let service_name = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let service_namespace = batch
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let deployment_environment = batch
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let scope_name = batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let scope_version = batch
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let span_name = batch
        .column(9)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let kind = batch
        .column(10)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let start = batch
        .column(11)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let status_code = batch
        .column(13)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let events = batch
        .column(16)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let links = batch
        .column(17)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    // Sorted (trace_id, start): row0 = A/100 (child), row1 = A/200 (root),
    // row2 = B/50.
    assert_eq!(trace_id.value(0), &trace_a);
    assert_eq!(trace_id.value(1), &trace_a);
    assert_eq!(trace_id.value(2), &trace_b);
    assert_eq!(span_id.value(0), &span_child);
    assert_eq!(span_id.value(1), &span_root);
    assert_eq!(span_id.value(2), &span_b);
    assert_eq!(start.value(0), 100);
    assert_eq!(start.value(1), 200);
    assert_eq!(start.value(2), 50);

    // parent_span_id: row0 has parent=span_root, rows 1+2 are null.
    assert!(!parent.is_null(0));
    assert_eq!(parent.value(0), &span_root);
    assert!(parent.is_null(1), "root span has no parent");
    assert!(parent.is_null(2), "b span has no parent");

    assert_eq!(scope_name.value(0), "tracer-a");
    assert_eq!(scope_version.value(0), "1.0");
    assert_eq!(span_name.value(0), "child-span");
    assert_eq!(kind.value(0), 3);
    assert_eq!(status_code.value(0), 2);
    assert_eq!(span_name.value(2), "b-span");

    // ── Promoted resource attributes (denormalised copies) ──
    // row0 = child (service.name=api only → namespace/env null),
    // row1 = root (all three), row2 = b (service worker, ns shop, env staging).
    assert_eq!(service_name.value(0), "api");
    assert_eq!(service_name.value(1), "api");
    assert_eq!(service_name.value(2), "worker");
    assert!(
        service_namespace.is_null(0),
        "child span has no service.namespace → null"
    );
    assert_eq!(service_namespace.value(1), "shop");
    assert_eq!(service_namespace.value(2), "shop");
    assert!(
        deployment_environment.is_null(0),
        "child span has no deployment.environment → null"
    );
    assert_eq!(deployment_environment.value(1), "prod");
    assert_eq!(deployment_environment.value(2), "staging");
    // The promoted values are *copies* — the originals remain in the Map.
    let resource_labels = batch.column(3).as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(
        map_pairs(
            resource_labels
                .value(1)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        ),
        vec![
            ("deployment.environment".to_string(), "prod".to_string()),
            ("service.name".to_string(), "api".to_string()),
            ("service.namespace".to_string(), "shop".to_string()),
        ]
    );

    // ── Nested events on row0 (child span): 2 events ──
    let ev0 = events.value(0);
    let ev0 = ev0.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(ev0.len(), 2, "child span has 2 events");
    let ev_ts = ev0
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let ev_name = ev0
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ev_attr = ev0.column(2).as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(ev_ts.value(0), 110);
    assert_eq!(ev_name.value(0), "cache.miss");
    assert_eq!(
        map_pairs(
            ev_attr
                .value(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        ),
        vec![("key".to_string(), "abc".to_string())]
    );
    assert_eq!(ev_ts.value(1), 120);
    assert_eq!(ev_name.value(1), "db.query");
    assert_eq!(
        ev_attr.value(1).len(),
        0,
        "db.query event has no attributes"
    );

    // Row1 (root span): 1 event, 1 link.
    let ev1 = events.value(1);
    let ev1 = ev1.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(ev1.len(), 1);
    assert_eq!(
        ev1.column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "exception"
    );

    let ln1 = links.value(1);
    let ln1 = ln1.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(ln1.len(), 1, "root span has 1 link");
    let ln_tid = ln1
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let ln_sid = ln1
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let ln_attr = ln1.column(2).as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(ln_tid.value(0), &[0xCCu8; 16]);
    assert_eq!(ln_sid.value(0), &[0xDDu8; 8]);
    assert_eq!(
        map_pairs(
            ln_attr
                .value(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        ),
        vec![("link.kind".to_string(), "follows".to_string())]
    );

    // Row0 has no links; row2 (b span) has no events and no links.
    assert_eq!(links.value(0).len(), 0);
    assert_eq!(events.value(2).len(), 0);
    assert_eq!(links.value(2).len(), 0);

    let parsed: BlockMeta = serde_json::from_slice(&meta_bytes).unwrap();
    assert_eq!(parsed.uuid, meta.uuid);
    assert_eq!(parsed.row_count, 3);
    assert!(!parsed.has_postings);
}

#[tokio::test]
async fn traces_empty_builder_uploads_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let b = TracesBlockBuilder::new(writer, BlockBuilderConfig::default());

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
