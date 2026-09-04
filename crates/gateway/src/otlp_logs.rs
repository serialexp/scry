//! OTLP logs ingestion and projection into scry log streams.

use std::collections::HashMap;

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use opentelemetry_proto::tonic::{
    collector::logs::v1::{
        ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
    },
    logs::v1::LogRecord,
};
use scry_proto::{
    fingerprint::fingerprint,
    generated::{LogEntry, LogStream, LogsBatch},
};

use crate::{
    otlp_common::{
        anyvalue_to_string, decode_request, encode_response, hex_lower, insert_if_absent,
        kv_to_labels,
    },
    sink::AppState,
};

#[derive(Debug)]
pub struct LogsMapping {
    pub batch: LogsBatch,
    pub rejected: u64,
}

pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let (request, encoding) = decode_request::<ExportLogsServiceRequest>(&headers, body)
        .inspect_err(|_| {
            if let Some(metrics) = state.metrics() {
                metrics.inbound_rejected(crate::metrics::Inbound::OtlpHttp);
            }
        })?;
    if let Some(metrics) = state.metrics() {
        metrics.inbound_accepted(crate::metrics::Inbound::OtlpHttp);
    }
    let response = accept(&state, request);
    Ok(encode_response(&response, encoding))
}

pub fn accept(state: &AppState, request: ExportLogsServiceRequest) -> ExportLogsServiceResponse {
    let mapped = map_logs(request);
    state.offer_logs(mapped.batch);
    response(mapped.rejected)
}

fn response(rejected: u64) -> ExportLogsServiceResponse {
    ExportLogsServiceResponse {
        partial_success: (rejected != 0).then(|| ExportLogsPartialSuccess {
            rejected_log_records: rejected.min(i64::MAX as u64) as i64,
            error_message: "log records without an event or observed timestamp were rejected"
                .into(),
        }),
    }
}

pub fn map_logs(request: ExportLogsServiceRequest) -> LogsMapping {
    let mut streams = Vec::<LogStream>::new();
    let mut by_fingerprint = HashMap::<u64, usize>::new();
    let mut rejected = 0u64;

    for resource_logs in request.resource_logs {
        let resource_labels = resource_logs
            .resource
            .map(|resource| kv_to_labels(&resource.attributes))
            .unwrap_or_default();
        for scope_logs in resource_logs.scope_logs {
            let mut labels = resource_labels.clone();
            if let Some(scope) = scope_logs.scope {
                insert_if_absent(&mut labels, "otel.scope.name", &scope.name);
                insert_if_absent(&mut labels, "otel.scope.version", &scope.version);
            }
            labels.sort_by(|a, b| a.key.cmp(&b.key).then(a.value.cmp(&b.value)));
            labels.dedup_by(|right, left| right.key == left.key);
            let fp = fingerprint(&labels);
            let stream_index = match by_fingerprint.get(&fp).copied() {
                Some(index) if streams[index].labels == labels => index,
                _ => {
                    let index = streams.len();
                    streams.push(LogStream {
                        fingerprint: fp,
                        labels,
                        entries: Vec::with_capacity(scope_logs.log_records.len()),
                    });
                    by_fingerprint.insert(fp, index);
                    index
                }
            };
            for record in scope_logs.log_records {
                match map_record(record) {
                    Some(entry) => streams[stream_index].entries.push(entry),
                    None => rejected += 1,
                }
            }
        }
    }
    streams.retain(|stream| !stream.entries.is_empty());
    LogsMapping {
        batch: LogsBatch { streams },
        rejected,
    }
}

fn map_record(record: LogRecord) -> Option<LogEntry> {
    let ts_unix_nano = if record.time_unix_nano != 0 {
        record.time_unix_nano
    } else {
        record.observed_time_unix_nano
    };
    if ts_unix_nano == 0 {
        return None;
    }
    let mut attributes = kv_to_labels(&record.attributes);
    insert_if_absent(&mut attributes, "otel.severity_text", &record.severity_text);
    if record.trace_id.len() == 16 && record.trace_id.iter().any(|byte| *byte != 0) {
        insert_if_absent(
            &mut attributes,
            "otel.trace_id",
            &hex_lower(&record.trace_id),
        );
    }
    if record.span_id.len() == 8 && record.span_id.iter().any(|byte| *byte != 0) {
        insert_if_absent(&mut attributes, "otel.span_id", &hex_lower(&record.span_id));
    }
    if record.flags != 0 {
        insert_if_absent(&mut attributes, "otel.flags", &record.flags.to_string());
    }
    Some(LogEntry {
        ts_unix_nano,
        severity: u8::try_from(record.severity_number).unwrap_or(0),
        body: record
            .body
            .as_ref()
            .map(anyvalue_to_string)
            .unwrap_or_default(),
        attributes,
    })
}

pub fn sample_request(records: usize) -> ExportLogsServiceRequest {
    use opentelemetry_proto::tonic::{
        common::v1::{any_value::Value, AnyValue, InstrumentationScope, KeyValue},
        logs::v1::{ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };
    let attr = |key: &str, value: &str| KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..Default::default()
    };
    let mut log_records = Vec::with_capacity(records);
    for index in 0..records {
        log_records.push(LogRecord {
            time_unix_nano: 1_700_100_000_000_000_000 + index as u64,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!("otlp log {index}"))),
            }),
            attributes: vec![attr("request.id", &index.to_string())],
            trace_id: vec![0x11; 16],
            span_id: vec![0x22; 8],
            ..Default::default()
        });
    }
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![attr("service.name", "otlp-logs")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "probe".into(),
                    version: "1".into(),
                    ..Default::default()
                }),
                log_records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn maps_records_and_reports_missing_timestamps() {
        let mut request = sample_request(2);
        request.resource_logs[0].scope_logs[0]
            .log_records
            .push(LogRecord::default());
        let mapped = map_logs(request);
        assert_eq!(mapped.rejected, 1);
        assert_eq!(mapped.batch.streams.len(), 1);
        assert_eq!(mapped.batch.streams[0].entries.len(), 2);
        assert_eq!(mapped.batch.streams[0].entries[0].body, "otlp log 0");
        assert!(mapped.batch.streams[0].entries[0]
            .attributes
            .iter()
            .any(|a| a.key == "otel.trace_id"));
    }
}
