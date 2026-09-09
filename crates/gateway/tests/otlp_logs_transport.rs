//! OTLP logs transport parity.
//!
//! These tests deliberately invoke the tonic service directly. The server's gzip
//! setting is tonic transport configuration; exercising compressed gRPC on a real
//! socket would mostly test tonic and require lifecycle/port plumbing. HTTP gzip is
//! covered through the actual Axum router below.

use std::io::Write;

use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use flate2::{write::GzEncoder, Compression};
use opentelemetry_proto::tonic::{
    collector::logs::v1::{
        logs_service_server::LogsService, ExportLogsServiceRequest, ExportLogsServiceResponse,
    },
    common::v1::{any_value::Value, AnyValue, ArrayValue, KeyValue, KeyValueList},
    logs::v1::LogRecord,
};
use prost::Message;
use scry_gateway::{
    otlp_grpc::OtlpLogsService,
    otlp_logs::sample_request,
    router,
    sink::{spawn_sink, Fanout},
    AppState,
};
use scry_proto::{constants::SIGNAL_BIT_LOGS, generated::LogsBatch};
use tokio::sync::mpsc;
use tonic::{codegen::Service, Request as GrpcRequest};

#[derive(Debug, PartialEq)]
struct CapturedLogs {
    projection: LogsBatch,
    payload: Vec<u8>,
    record_count: u32,
    ts_min: u64,
    ts_max: u64,
}

fn capture_state() -> (AppState, mpsc::UnboundedReceiver<CapturedLogs>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let sink = spawn_sink(
        "capture",
        SIGNAL_BIT_LOGS,
        8,
        move |mut fanout| async move {
            while let Some(item) = fanout.recv().await {
                let Fanout::Logs(logs) = item else {
                    panic!("logs-only sink received another signal");
                };
                let canonical = logs
                    .canonical
                    .as_ref()
                    .expect("OTLP logs must retain their canonical payload");
                tx.send(CapturedLogs {
                    projection: logs.projection.clone(),
                    payload: canonical.payload.clone(),
                    record_count: canonical.record_count,
                    ts_min: canonical.ts_min,
                    ts_max: canonical.ts_max,
                })
                .expect("capture receiver remains live");
            }
        },
    );
    (AppState::new(vec![sink]), rx)
}

fn some_value(inner: Value) -> SomeValue {
    Some(AnyValue { value: Some(inner) })
}

type SomeValue = Option<AnyValue>;

fn attribute(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: some_value(value),
        ..Default::default()
    }
}

/// Includes every AnyValue shape, source metadata, trace context, an observed-time
/// fallback, and one independently rejected record.
fn rich_request() -> ExportLogsServiceRequest {
    let mut request = sample_request(2);
    let resource_logs = &mut request.resource_logs[0];
    resource_logs.schema_url = "https://example.test/resource-schema".into();
    let resource = resource_logs.resource.as_mut().unwrap();
    resource.dropped_attributes_count = 2;
    resource.attributes.push(attribute(
        "deployment.environment",
        Value::StringValue("test".into()),
    ));
    resource.attributes.sort_by(|a, b| a.key.cmp(&b.key));

    let scope_logs = &mut resource_logs.scope_logs[0];
    scope_logs.schema_url = "https://example.test/scope-schema".into();
    let scope = scope_logs.scope.as_mut().unwrap();
    scope.dropped_attributes_count = 3;
    scope.attributes = vec![attribute("scope.bool", Value::BoolValue(true))];

    let first = &mut scope_logs.log_records[0];
    first.observed_time_unix_nano = first.time_unix_nano + 10;
    first.event_name = "checkout.completed".into();
    first.flags = 1;
    first.dropped_attributes_count = 4;
    first.body = some_value(Value::KvlistValue(KeyValueList {
        values: vec![
            attribute("attempt", Value::IntValue(2)),
            attribute("message", Value::StringValue("paid".into())),
        ],
    }));
    first.attributes = vec![
        attribute(
            "array",
            Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("x".into())),
                    },
                    AnyValue {
                        value: Some(Value::IntValue(7)),
                    },
                ],
            }),
        ),
        attribute("bool", Value::BoolValue(false)),
        attribute("bytes", Value::BytesValue(vec![0, 1, 254, 255])),
        attribute("double", Value::DoubleValue(1.25)),
        attribute("int", Value::IntValue(-42)),
        attribute("string", Value::StringValue("attribute".into())),
    ];

    let second = &mut scope_logs.log_records[1];
    second.observed_time_unix_nano = second.time_unix_nano;
    second.time_unix_nano = 0;

    // No event or observed timestamp: rejected without affecting the two valid records.
    scope_logs.log_records.push(LogRecord::default());
    request
}

fn partial(response: &ExportLogsServiceResponse) -> (i64, String) {
    response
        .partial_success
        .as_ref()
        .map(|partial| (partial.rejected_log_records, partial.error_message.clone()))
        .unwrap_or_default()
}

#[derive(Clone, Copy)]
enum HttpEncoding {
    Protobuf,
    Json,
}

async fn send_http(
    request: &ExportLogsServiceRequest,
    encoding: HttpEncoding,
    gzip: bool,
) -> (CapturedLogs, ExportLogsServiceResponse) {
    let (state, mut captured) = capture_state();
    let bytes = match encoding {
        HttpEncoding::Protobuf => request.encode_to_vec(),
        HttpEncoding::Json => serde_json::to_vec(request).unwrap(),
    };
    let body = if gzip {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&bytes).unwrap();
        encoder.finish().unwrap()
    } else {
        bytes
    };
    let content_type = match encoding {
        HttpEncoding::Protobuf => "application/x-protobuf",
        HttpEncoding::Json => "application/json",
    };
    let mut builder = Request::post("/v1/logs").header(header::CONTENT_TYPE, content_type);
    if gzip {
        builder = builder.header(header::CONTENT_ENCODING, "gzip");
    }
    let mut app = router(state);
    let response = app
        .call(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], content_type);
    let response_body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let response = match encoding {
        HttpEncoding::Protobuf => ExportLogsServiceResponse::decode(response_body).unwrap(),
        HttpEncoding::Json => serde_json::from_slice(&response_body).unwrap(),
    };
    let fanout = captured
        .recv()
        .await
        .expect("handler fans out one logs batch");
    (fanout, response)
}

async fn send_grpc(request: ExportLogsServiceRequest) -> (CapturedLogs, ExportLogsServiceResponse) {
    let (state, mut captured) = capture_state();
    let response = OtlpLogsService::new(state)
        .export(GrpcRequest::new(request))
        .await
        .unwrap()
        .into_inner();
    let fanout = captured
        .recv()
        .await
        .expect("service fans out one logs batch");
    (fanout, response)
}

#[tokio::test]
async fn http_protobuf_json_and_gzip_match_plain_grpc() {
    let request = rich_request();
    let (expected_fanout, expected_response) = send_grpc(request.clone()).await;
    assert_eq!(expected_fanout.record_count, 2);
    assert_eq!(partial(&expected_response).0, 1);

    for (encoding, gzip) in [
        (HttpEncoding::Protobuf, false),
        (HttpEncoding::Json, false),
        (HttpEncoding::Protobuf, true),
        (HttpEncoding::Json, true),
    ] {
        let (fanout, response) = send_http(&request, encoding, gzip).await;
        assert_eq!(fanout, expected_fanout);
        assert_eq!(partial(&response), partial(&expected_response));
    }
}
