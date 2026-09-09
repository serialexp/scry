//! OTLP logs -> gateway canonical-v2 mapping -> real native ingest server ->
//! WAL/block/catalog -> query.  Everything is process-local.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use arrow::array::{Array, BinaryArray, StringArray, UInt16Array, UInt64Array, UInt8Array};
use datafusion::execution::context::SessionContext;
use futures::StreamExt;
use object_store::{memory::InMemory, ObjectStore, ObjectStoreExt};
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
use scry_block::{BlockBuilderConfig, BlockMeta};
use scry_catalog::Catalog;
use scry_gateway::{
    metrics::{GatewayMetrics, GatewaySignal, QueueSnapshot, SinkKind, SinkReporter},
    otlp_logs::{accept, map_logs, sample_request},
    sink::{spawn_sink_instrumented, AppState, ACCEPT_ALL},
    sink_scry::{ScryConnect, ScrySink},
};
use scry_proto::{constants::SIGNAL_BIT_LOGS, LabelPair};
use scry_query::{register_logs_table, Query, LOGS_TABLE_NAME};
use scry_server::{decode, Server, ServerConfig, ShardedPipeline};
use tempfile::TempDir;
use tokio::sync::{oneshot, Semaphore};
use uuid::Uuid;

const BUCKET: &str = "test";

async fn stored_meta(store: &Arc<dyn ObjectStore>) -> Vec<BlockMeta> {
    let mut objects = store.list(None);
    let mut metas = Vec::new();
    while let Some(object) = objects.next().await {
        let location = object.unwrap().location;
        if location.as_ref().ends_with("meta.json") {
            let bytes = store.get(&location).await.unwrap().bytes().await.unwrap();
            metas.push(serde_json::from_slice(&bytes).unwrap());
        }
    }
    metas
}

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn rich_otlp_log_crosses_gateway_and_real_ingest_into_queryable_v2_block() {
    let tmp = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog_path = tmp.path().join("catalog.sqlite");
    let catalog = Arc::new(Mutex::new(Catalog::open(&catalog_path, BUCKET).unwrap()));
    let writer_uuid = Uuid::now_v7();
    let logs = ShardedPipeline::open_with_config(
        8,
        tmp.path().join("wal"),
        store.clone(),
        Some(catalog.clone()),
        writer_uuid,
        decode::logs,
        BlockBuilderConfig {
            max_rows: 100,
            row_group_size: 1,
            ..Default::default()
        },
        Arc::new(Semaphore::new(1)),
        None,
        false,
    )
    .await
    .unwrap();

    // Server currently owns its bind operation. Reserve an ephemeral loopback port,
    // release it immediately, then wait until this in-process server accepts probes.
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = reservation.local_addr().unwrap();
    drop(reservation);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = Server::new(
        ServerConfig {
            listen_addr: addr.to_string(),
            writer_id: "gateway-e2e".into(),
            writer_uuid,
            enable_logs_v2: true,
        },
        None,
        None,
        Some(logs),
        None,
        None,
    )
    .with_live_ring(Some(scry_server::LiveRing::new(
        Duration::from_secs(30),
        1024 * 1024,
    )));
    let server_task = tokio::spawn(async move {
        server
            .serve_with_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let metrics = Arc::new(GatewayMetrics::default());
    let connect = ScryConnect {
        addr: addr.to_string(),
        agent_id: [0x77; 16],
        hostname: "gateway-e2e".into(),
        signals: SIGNAL_BIT_LOGS,
        resource_attrs: vec![LabelPair {
            key: "gateway".into(),
            value: "test".into(),
        }],
    };
    let reporter = SinkReporter::new(Some(metrics.clone()), SinkKind::Scry);
    let sink = spawn_sink_instrumented("scry", ACCEPT_ALL, 8, Some(metrics.clone()), move |rx| {
        ScrySink::new(connect, reporter).run(rx)
    });
    let state = AppState::with_metrics(vec![sink], metrics.clone());

    let mut request = sample_request(1);
    let resource_logs = &mut request.resource_logs[0];
    resource_logs.schema_url = "resource/schema/v1".into();
    let resource = resource_logs.resource.as_mut().unwrap();
    resource.dropped_attributes_count = 2;
    resource
        .attributes
        .push(string_attr("deployment.environment", "integration"));
    let scope_logs = &mut resource_logs.scope_logs[0];
    scope_logs.schema_url = "scope/schema/v1".into();
    let scope = scope_logs.scope.as_mut().unwrap();
    scope.attributes.push(string_attr("scope.attr", "kept"));
    scope.dropped_attributes_count = 3;
    let record = &mut scope_logs.log_records[0];
    record.observed_time_unix_nano = record.time_unix_nano + 9;
    record.severity_number = 17;
    record.severity_text = "ERROR".into();
    record.event_name = "gateway.exception".into();
    record.flags = 1;
    record.dropped_attributes_count = 4;
    record.body = Some(AnyValue {
        value: Some(Value::KvlistValue(
            opentelemetry_proto::tonic::common::v1::KeyValueList {
                values: vec![string_attr("message", "rich canonical body")],
            },
        )),
    });

    // Capture the mapper's exact canonical record independently of storage. The
    // payload prefix is magic(4), raw-version(2), count(4), record-length(4).
    let mapped = map_logs(request.clone());
    assert_eq!(mapped.canonical.record_count, 1);
    let raw_len = u32::from_be_bytes(mapped.canonical.payload[10..14].try_into().unwrap()) as usize;
    let expected_raw = mapped.canonical.payload[14..14 + raw_len].to_vec();

    let response = accept(&state, request);
    assert!(response.partial_success.is_none());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = metrics.snapshot(&[QueueSnapshot {
                kind: SinkKind::Scry,
                depth: 0,
                capacity: 8,
            }]);
            if snapshot["sinks"]["scry"]["signals"][GatewaySignal::Logs.name()]["delivered"] == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // Closing the gateway queue drops its native client after the accepted batch;
    // server shutdown then performs the public, awaited final pipeline flush.
    drop(state);
    tokio::task::yield_now().await;
    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();

    let metas = stored_meta(&store).await;
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].schema_version, 2);
    assert_eq!(metas[0].row_count, 1);
    let entries = catalog.lock().unwrap().list_blocks().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].meta.schema_version, 2);

    let ctx = SessionContext::new();
    // Use a separate read connection so no synchronous mutex guard crosses the
    // async table-registration boundary.
    let query_catalog = Catalog::open(&catalog_path, BUCKET).unwrap();
    register_logs_table(&ctx, &query_catalog, store.clone(), &Query::default())
        .await
        .unwrap();
    let batches = ctx
        .sql(&format!(
            "SELECT ts_unix_nano, observed_ts_unix_nano, severity_text, event_name, \
         trace_flags, raw_record_version, raw_record FROM {LOGS_TABLE_NAME}"
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
        1_700_100_000_000_000_000
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        1_700_100_000_000_000_009
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "ERROR"
    );
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "gateway.exception"
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(6)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        expected_raw
    );
}
