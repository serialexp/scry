//! OTLP/HTTP protobuf trace ingest: `POST /v1/traces`
//! (`Content-Type: application/x-protobuf`).
//!
//! Decodes an `ExportTraceServiceRequest`, maps it to our `TracesBatch`, and
//! forwards it upstream. OTel resource attributes are carried through to
//! `ResourceEntry.labels` **verbatim** (keys preserved) so the traces block's
//! promoted columns (`service.name`, `service.namespace`,
//! `deployment.environment[.name]`) populate from the Map.

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use scry_proto::generated::{ResourceEntry, ScopeEntry, Span, SpanEvent, SpanLink, TracesBatch};

use crate::{
    otlp_common::{decode_request, encode_response, kv_to_labels},
    sink::AppState,
};

/// Handle one OTLP/HTTP trace export (protobuf or JSON, optionally gzip-compressed).
pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let (req, encoding) =
        decode_request::<ExportTraceServiceRequest>(&headers, body).inspect_err(|_| {
            if let Some(metrics) = state.metrics() {
                metrics.inbound_rejected(crate::metrics::Inbound::OtlpHttp);
            }
        })?;
    if let Some(metrics) = state.metrics() {
        metrics.inbound_accepted(crate::metrics::Inbound::OtlpHttp);
    }
    accept(&state, req);
    Ok(encode_response(
        &ExportTraceServiceResponse::default(),
        encoding,
    ))
}

/// Accept one decoded OTLP trace export from either HTTP or gRPC.
///
/// Both transports deliberately converge here so mapping, empty-batch handling,
/// fan-out, and best-effort acknowledgement semantics cannot drift.
pub fn accept(state: &AppState, req: ExportTraceServiceRequest) {
    state.offer_traces(map_traces(req));
}

/// Pure mapping: OTLP `ExportTraceServiceRequest` → our `TracesBatch`.
///
/// One `ResourceEntry` per `ResourceSpans` and one `ScopeEntry` per `ScopeSpans`
/// (no dedup in v0 — correctness over dictionary size). Span scalar fields map
/// directly; OTel enum ids (`SpanKind` 0–5, `StatusCode` 0–2) fit our `u8`s;
/// attribute `AnyValue`s are stringified into `LabelPair`s.
pub fn map_traces(req: ExportTraceServiceRequest) -> TracesBatch {
    let mut resources: Vec<ResourceEntry> = Vec::new();
    let mut scopes: Vec<ScopeEntry> = Vec::new();
    let mut spans: Vec<Span> = Vec::new();

    for rs in req.resource_spans {
        let resource_idx = resources.len() as u16;
        let labels = rs
            .resource
            .map(|r| kv_to_labels(&r.attributes))
            .unwrap_or_default();
        resources.push(ResourceEntry { labels });

        for ss in rs.scope_spans {
            let scope_idx = scopes.len() as u16;
            let (name, version) = ss.scope.map(|s| (s.name, s.version)).unwrap_or_default();
            scopes.push(ScopeEntry { name, version });

            for sp in ss.spans {
                let parent_span_id = if sp.parent_span_id.is_empty() {
                    None
                } else {
                    Some(sp.parent_span_id)
                };
                let (status_code, status_message) = match sp.status {
                    Some(s) => (s.code as u8, s.message),
                    None => (0, String::new()),
                };

                spans.push(Span {
                    resource_idx,
                    scope_idx,
                    trace_id: sp.trace_id,
                    span_id: sp.span_id,
                    parent_span_id,
                    name: sp.name,
                    kind: sp.kind as u8,
                    start_unix_nano: sp.start_time_unix_nano,
                    end_unix_nano: sp.end_time_unix_nano,
                    status_code,
                    status_message,
                    attributes: kv_to_labels(&sp.attributes),
                    events: sp
                        .events
                        .into_iter()
                        .map(|e| SpanEvent {
                            ts_unix_nano: e.time_unix_nano,
                            name: e.name,
                            attributes: kv_to_labels(&e.attributes),
                        })
                        .collect(),
                    links: sp
                        .links
                        .into_iter()
                        .map(|l| SpanLink {
                            trace_id: l.trace_id,
                            span_id: l.span_id,
                            attributes: kv_to_labels(&l.attributes),
                        })
                        .collect(),
                });
            }
        }
    }

    TracesBatch {
        resources,
        scopes,
        spans,
    }
}

/// Build a sample OTLP request with `n_spans` spans under one resource + scope.
/// Used by the probe binary and the mapping tests. Resource carries the three
/// OTel discovery attributes so the promoted columns are exercised.
pub fn sample_request(n_spans: usize) -> ExportTraceServiceRequest {
    use opentelemetry_proto::tonic::{
        common::v1::{any_value::Value, AnyValue, InstrumentationScope, KeyValue},
        resource::v1::Resource,
        trace::v1::{
            span::{Event, Link, SpanKind},
            status::StatusCode,
            ResourceSpans, ScopeSpans, Span, Status,
        },
    };

    fn str_attr(key: &str, val: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(val.to_string())),
            }),
            ..Default::default()
        }
    }

    let resource = Resource {
        attributes: vec![
            str_attr("service.name", "api"),
            str_attr("service.namespace", "shop"),
            str_attr("deployment.environment", "prod"),
            str_attr("host.name", "host-1"),
        ],
        ..Default::default()
    };

    let scope = InstrumentationScope {
        name: "scry.gateway.probe".to_string(),
        version: "0.1.0".to_string(),
        ..Default::default()
    };

    let trace_id = vec![0x11u8; 16];
    let root_span_id = vec![0x22u8; 8];

    let mut spans = Vec::with_capacity(n_spans);
    for i in 0..n_spans {
        let is_root = i == 0;
        let span_id = if is_root {
            root_span_id.clone()
        } else {
            vec![(0x30 + i) as u8; 8]
        };
        let start = 1_700_000_000_000_000_000u64 + (i as u64) * 1_000_000;
        spans.push(Span {
            trace_id: trace_id.clone(),
            span_id,
            parent_span_id: if is_root {
                vec![]
            } else {
                root_span_id.clone()
            },
            name: format!("op.{i}"),
            kind: SpanKind::Server as i32,
            start_time_unix_nano: start,
            end_time_unix_nano: start + 5_000_000,
            attributes: vec![str_attr("http.method", "GET")],
            events: vec![Event {
                time_unix_nano: start + 1_000_000,
                name: "checkpoint".to_string(),
                attributes: vec![str_attr("phase", "mid")],
                ..Default::default()
            }],
            links: vec![Link {
                trace_id: trace_id.clone(),
                span_id: root_span_id.clone(),
                ..Default::default()
            }],
            status: Some(Status {
                message: String::new(),
                code: StatusCode::Ok as i32,
            }),
            ..Default::default()
        });
    }

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![ScopeSpans {
                scope: Some(scope),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}
