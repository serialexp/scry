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
    common::v1::{any_value::Value, AnyValue, KeyValue},
    logs::v1::LogRecord,
};
use scry_proto::{
    constants::{LOGS_BATCH_V2_MAGIC, LOGS_RAW_VERSION_V1},
    encode_log_record_v2_from_source_into,
    fingerprint::fingerprint,
    generated::{LogEntry, LogStream, LogsBatch},
    LogsV2AnyValueAdapter, LogsV2BorrowedAnyValue, LogsV2DecodeLimits, LogsV2EncodeError,
    LogsV2EncodeScratch, LogsV2SourceLogRecordInput, LogsV2SourceScopeInput,
};

use crate::{
    otlp_common::{
        anyvalue_to_string, decode_request, encode_response, hex_lower, insert_if_absent,
        kv_to_labels,
    },
    sink::{AppState, CanonicalLogsBatch, LogsFanout},
};

const REASON_COUNT: usize = 8;
const REASON_NAMES: [&str; REASON_COUNT] = [
    "missing_timestamp",
    "severity_out_of_range",
    "flags_out_of_range",
    "invalid_id",
    "resource_entity_refs",
    "profiling_dictionary_reference",
    "invalid_map_key",
    "encoder_bounds",
];

#[derive(Debug)]
pub struct LogsMapping {
    pub batch: LogsBatch,
    pub canonical: CanonicalLogsBatch,
    pub rejected: u64,
    reasons: [u64; REASON_COUNT],
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
    let response = response(mapped.rejected, &mapped.reasons);
    state.offer_logs_fanout(LogsFanout {
        projection: mapped.batch,
        canonical: Some(mapped.canonical),
    });
    response
}

fn response(rejected: u64, reasons: &[u64; REASON_COUNT]) -> ExportLogsServiceResponse {
    ExportLogsServiceResponse {
        partial_success: (rejected != 0).then(|| {
            // Fixed vocabulary, fixed ordering, and decimal counters make this bounded and
            // deterministic; input keys and values are deliberately never reflected.
            let error_message = REASON_NAMES
                .iter()
                .zip(reasons)
                .filter(|(_, count)| **count != 0)
                .map(|(name, count)| format!("{name}={count}"))
                .collect::<Vec<_>>()
                .join(",");
            ExportLogsPartialSuccess {
                rejected_log_records: rejected.min(i64::MAX as u64) as i64,
                error_message,
            }
        }),
    }
}

#[derive(Clone, Copy)]
enum Reject {
    Timestamp = 0,
    Severity = 1,
    Flags = 2,
    Id = 3,
    Entity = 4,
    Dictionary = 5,
    MapKey = 6,
    Bounds = 7,
}

#[derive(Clone, Copy)]
enum OtlpHandle<'a> {
    Value(Option<&'a AnyValue>),
    Array(&'a [AnyValue]),
    Map(&'a [KeyValue]),
}

struct OtlpAdapter;

impl<'a> LogsV2AnyValueAdapter<'a> for OtlpAdapter {
    type Handle = OtlpHandle<'a>;

    fn value(&self, handle: Self::Handle) -> LogsV2BorrowedAnyValue<'a, Self::Handle> {
        let OtlpHandle::Value(value) = handle else {
            return LogsV2BorrowedAnyValue::Null;
        };
        match value.and_then(|value| value.value.as_ref()) {
            None => LogsV2BorrowedAnyValue::Null,
            Some(Value::StringValue(value)) => LogsV2BorrowedAnyValue::String(value),
            Some(Value::BoolValue(value)) => LogsV2BorrowedAnyValue::Bool(*value),
            Some(Value::IntValue(value)) => LogsV2BorrowedAnyValue::Int64(*value),
            Some(Value::DoubleValue(value)) => LogsV2BorrowedAnyValue::Double(*value),
            Some(Value::BytesValue(value)) => LogsV2BorrowedAnyValue::Bytes(value),
            Some(Value::ArrayValue(value)) => {
                LogsV2BorrowedAnyValue::Array(OtlpHandle::Array(&value.values))
            }
            Some(Value::KvlistValue(value)) => {
                LogsV2BorrowedAnyValue::Map(OtlpHandle::Map(&value.values))
            }
            // Rejected by validation before this adapter is called.
            Some(Value::StringValueStrindex(_)) => LogsV2BorrowedAnyValue::Null,
        }
    }

    fn array_len(&self, array: Self::Handle) -> usize {
        match array {
            OtlpHandle::Array(values) => values.len(),
            _ => 0,
        }
    }
    fn array_value(&self, array: Self::Handle, index: usize) -> Option<Self::Handle> {
        match array {
            OtlpHandle::Array(values) => values.get(index).map(|v| OtlpHandle::Value(Some(v))),
            _ => None,
        }
    }
    fn map_len(&self, map: Self::Handle) -> usize {
        match map {
            OtlpHandle::Map(values) => values.len(),
            _ => 0,
        }
    }
    fn map_key(&self, map: Self::Handle, index: usize) -> Option<&'a str> {
        match map {
            OtlpHandle::Map(values) => values.get(index).map(|v| v.key.as_str()),
            _ => None,
        }
    }
    fn map_value(&self, map: Self::Handle, index: usize) -> Option<Self::Handle> {
        match map {
            OtlpHandle::Map(values) => values
                .get(index)
                .map(|v| OtlpHandle::Value(v.value.as_ref())),
            _ => None,
        }
    }
}

fn validate_map(values: &[KeyValue]) -> Result<(), Reject> {
    validate_map_at(values, 0)
}

fn validate_map_at(values: &[KeyValue], depth: u8) -> Result<(), Reject> {
    if depth > LogsV2DecodeLimits::default().max_depth {
        return Err(Reject::Bounds);
    }
    // Map ordering, empty keys, and duplicates are checked by the encoder after its
    // O(n log n) index sort. This walk is only needed because dictionary references
    // are an OTLP extension that the generic encoder adapter cannot represent.
    for kv in values {
        if kv.key_strindex != 0 {
            return Err(Reject::Dictionary);
        }
        validate_value_at(kv.value.as_ref(), depth)?;
    }
    Ok(())
}

fn validate_value(value: Option<&AnyValue>) -> Result<(), Reject> {
    validate_value_at(value, 0)
}

fn validate_value_at(value: Option<&AnyValue>, depth: u8) -> Result<(), Reject> {
    match value.and_then(|value| value.value.as_ref()) {
        Some(Value::StringValueStrindex(_)) => Err(Reject::Dictionary),
        Some(Value::ArrayValue(array)) => {
            let next = depth.checked_add(1).ok_or(Reject::Bounds)?;
            if next > LogsV2DecodeLimits::default().max_depth {
                return Err(Reject::Bounds);
            }
            for value in &array.values {
                validate_value_at(Some(value), next)?;
            }
            Ok(())
        }
        Some(Value::KvlistValue(map)) => {
            let next = depth.checked_add(1).ok_or(Reject::Bounds)?;
            validate_map_at(&map.values, next)
        }
        _ => Ok(()),
    }
}

type TraceContext<'a> = (Option<&'a [u8; 16]>, Option<&'a [u8; 8]>);

fn classify_encode_error(error: LogsV2EncodeError) -> Reject {
    match error {
        LogsV2EncodeError::EmptyMapKey | LogsV2EncodeError::NonCanonicalMap => Reject::MapKey,
        LogsV2EncodeError::MissingTimestamp => Reject::Timestamp,
        LogsV2EncodeError::ZeroTraceId
        | LogsV2EncodeError::ZeroSpanId
        | LogsV2EncodeError::OrphanSpanId => Reject::Id,
        LogsV2EncodeError::PayloadLimit
        | LogsV2EncodeError::RecordCountLimit
        | LogsV2EncodeError::RecordLimit
        | LogsV2EncodeError::VariableBytesLimit
        | LogsV2EncodeError::StringLimit
        | LogsV2EncodeError::KeyLimit
        | LogsV2EncodeError::BytesLimit
        | LogsV2EncodeError::DepthLimit
        | LogsV2EncodeError::NodeLimit
        | LogsV2EncodeError::KeyValueLimit
        | LogsV2EncodeError::ContainerLimit
        | LogsV2EncodeError::LengthOverflow
        | LogsV2EncodeError::InvalidSource => Reject::Bounds,
    }
}

fn increment_reason(reasons: &mut [u64; REASON_COUNT], reason: Reject) {
    reasons[reason as usize] = reasons[reason as usize]
        .checked_add(1)
        .expect("rejection count cannot exceed the number of addressable records");
}

fn validate_record(record: &LogRecord) -> Result<TraceContext<'_>, Reject> {
    if record.time_unix_nano == 0 && record.observed_time_unix_nano == 0 {
        return Err(Reject::Timestamp);
    }
    if u8::try_from(record.severity_number).is_err() {
        return Err(Reject::Severity);
    }
    if u8::try_from(record.flags).is_err() {
        return Err(Reject::Flags);
    }
    let trace = match record.trace_id.len() {
        0 => None,
        16 if record.trace_id.iter().any(|byte| *byte != 0) => {
            Some(record.trace_id.as_slice().try_into().unwrap())
        }
        _ => return Err(Reject::Id),
    };
    let span = match record.span_id.len() {
        0 => None,
        8 if record.span_id.iter().any(|byte| *byte != 0) => {
            Some(record.span_id.as_slice().try_into().unwrap())
        }
        _ => return Err(Reject::Id),
    };
    if span.is_some() && trace.is_none() {
        return Err(Reject::Id);
    }
    validate_map(&record.attributes)?;
    validate_value(record.body.as_ref())?;
    Ok((trace, span))
}

pub fn map_logs(request: ExportLogsServiceRequest) -> LogsMapping {
    let limits = LogsV2DecodeLimits::default();
    let mut payload = Vec::with_capacity(4096);
    payload.extend_from_slice(&LOGS_BATCH_V2_MAGIC.to_be_bytes());
    payload.extend_from_slice(&LOGS_RAW_VERSION_V1.to_be_bytes());
    payload.extend_from_slice(&0u32.to_be_bytes());
    let mut raw = Vec::new();
    let mut scratch = LogsV2EncodeScratch::default();
    let mut streams = Vec::<LogStream>::new();
    // A hash is only an accelerator. Every colliding candidate is compared exactly.
    let mut by_fingerprint = HashMap::<u64, Vec<usize>>::new();
    let mut reasons = [0u64; REASON_COUNT];
    let mut record_count = 0u32;
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;

    for resource_logs in request.resource_logs {
        let resource_invalid = resource_logs.resource.as_ref().and_then(|resource| {
            if !resource.entity_refs.is_empty() {
                Some(Reject::Entity)
            } else {
                validate_map(&resource.attributes).err()
            }
        });
        let resource_attributes = resource_logs
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or_default();
        for scope_logs in resource_logs.scope_logs {
            let scope_invalid = scope_logs
                .scope
                .as_ref()
                .and_then(|scope| validate_map(&scope.attributes).err());
            let adapter = OtlpAdapter;
            let mut labels = resource_logs
                .resource
                .as_ref()
                .map(|r| kv_to_labels(&r.attributes))
                .unwrap_or_default();
            if let Some(scope) = &scope_logs.scope {
                insert_if_absent(&mut labels, "otel.scope.name", &scope.name);
                insert_if_absent(&mut labels, "otel.scope.version", &scope.version);
            }
            labels.sort_by(|a, b| a.key.cmp(&b.key).then(a.value.cmp(&b.value)));
            labels.dedup_by(|right, left| right.key == left.key);
            let fp = fingerprint(&labels);
            let mut stream_index = None;

            for record in scope_logs.log_records {
                let result = resource_invalid
                    .or(scope_invalid)
                    .map_or_else(|| validate_record(&record), Err);
                let (trace_id, span_id) = match result {
                    Ok(ids) => ids,
                    Err(reason) => {
                        increment_reason(&mut reasons, reason);
                        continue;
                    }
                };
                let scope = scope_logs
                    .scope
                    .as_ref()
                    .map(|value| LogsV2SourceScopeInput {
                        name: &value.name,
                        version: &value.version,
                        dropped_attributes_count: value.dropped_attributes_count,
                        attributes: OtlpHandle::Map(&value.attributes),
                    });
                let input = LogsV2SourceLogRecordInput {
                    adapter: &adapter,
                    resource_schema_url: &resource_logs.schema_url,
                    resource_dropped_attributes_count: resource_logs
                        .resource
                        .as_ref()
                        .map_or(0, |r| r.dropped_attributes_count),
                    resource_attributes: OtlpHandle::Map(resource_attributes),
                    scope,
                    scope_schema_url: &scope_logs.schema_url,
                    time_unix_nano: record.time_unix_nano,
                    observed_time_unix_nano: record.observed_time_unix_nano,
                    severity: record.severity_number as u8,
                    severity_text: &record.severity_text,
                    event_name: &record.event_name,
                    body: OtlpHandle::Value(record.body.as_ref()),
                    dropped_attributes_count: record.dropped_attributes_count,
                    attributes: OtlpHandle::Map(&record.attributes),
                    trace_flags: record.flags as u8,
                    trace_id,
                    span_id,
                };
                raw.clear();
                if let Err(error) =
                    encode_log_record_v2_from_source_into(&input, limits, &mut scratch, &mut raw)
                {
                    increment_reason(&mut reasons, classify_encode_error(error));
                    continue;
                }
                let Some(next_count) = record_count.checked_add(1) else {
                    increment_reason(&mut reasons, Reject::Bounds);
                    continue;
                };
                let payload_len = payload
                    .len()
                    .checked_add(4)
                    .and_then(|len| len.checked_add(raw.len()));
                let Ok(raw_len) = u32::try_from(raw.len()) else {
                    increment_reason(&mut reasons, Reject::Bounds);
                    continue;
                };
                if next_count > limits.max_records
                    || payload_len.is_none_or(|len| len > limits.max_payload_bytes)
                {
                    increment_reason(&mut reasons, Reject::Bounds);
                    continue;
                }
                payload.extend_from_slice(&raw_len.to_be_bytes());
                payload.extend_from_slice(&raw);
                record_count = next_count;
                let timestamp = if record.time_unix_nano != 0 {
                    record.time_unix_nano
                } else {
                    record.observed_time_unix_nano
                };
                ts_min = ts_min.min(timestamp);
                ts_max = ts_max.max(timestamp);

                let stream_index = *stream_index.get_or_insert_with(|| {
                    by_fingerprint
                        .get(&fp)
                        .and_then(|indexes| {
                            indexes
                                .iter()
                                .copied()
                                .find(|&i| streams[i].labels == labels)
                        })
                        .unwrap_or_else(|| {
                            let index = streams.len();
                            streams.push(LogStream {
                                fingerprint: fp,
                                labels: labels.clone(),
                                entries: Vec::new(),
                            });
                            by_fingerprint.entry(fp).or_default().push(index);
                            index
                        })
                });
                streams[stream_index]
                    .entries
                    .push(project_record(record, timestamp));
            }
        }
    }
    payload[6..10].copy_from_slice(&record_count.to_be_bytes());
    let rejected = reasons.iter().copied().fold(0u64, |total, count| {
        total
            .checked_add(count)
            .expect("rejection count cannot exceed the number of addressable records")
    });
    LogsMapping {
        batch: LogsBatch { streams },
        canonical: CanonicalLogsBatch {
            payload,
            record_count,
            ts_min: if record_count == 0 { 0 } else { ts_min },
            ts_max,
        },
        rejected,
        reasons,
    }
}

fn project_record(record: LogRecord, ts_unix_nano: u64) -> LogEntry {
    let mut attributes = kv_to_labels(&record.attributes);
    insert_if_absent(&mut attributes, "otel.severity_text", &record.severity_text);
    if !record.trace_id.is_empty() {
        insert_if_absent(
            &mut attributes,
            "otel.trace_id",
            &hex_lower(&record.trace_id),
        );
    }
    if !record.span_id.is_empty() {
        insert_if_absent(&mut attributes, "otel.span_id", &hex_lower(&record.span_id));
    }
    if record.flags != 0 {
        insert_if_absent(&mut attributes, "otel.flags", &record.flags.to_string());
    }
    LogEntry {
        ts_unix_nano,
        severity: record.severity_number as u8,
        body: record
            .body
            .as_ref()
            .map(anyvalue_to_string)
            .unwrap_or_default(),
        attributes,
    }
}

pub fn sample_request(records: usize) -> ExportLogsServiceRequest {
    use opentelemetry_proto::tonic::{
        common::v1::InstrumentationScope,
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
    use opentelemetry_proto::tonic::common::v1::{
        ArrayValue, EntityRef, InstrumentationScope, KeyValueList,
    };
    use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use scry_proto::{decode_logs_batch_v2_into, CanonicalLogRecord, LogsV2Appender};

    fn value(value: Value) -> AnyValue {
        AnyValue { value: Some(value) }
    }

    fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(self::value(value)),
            ..Default::default()
        }
    }

    #[test]
    fn maps_records_and_reports_missing_timestamps() {
        let mut request = sample_request(2);
        request.resource_logs[0].scope_logs[0]
            .log_records
            .push(LogRecord::default());
        let mapped = map_logs(request);
        assert_eq!(mapped.rejected, 1);
        assert_eq!(mapped.canonical.record_count, 2);
        #[derive(Default)]
        struct Count(u32);
        impl LogsV2Appender for Count {
            fn record(&mut self, _: CanonicalLogRecord<'_>) -> Result<(), String> {
                self.0 += 1;
                Ok(())
            }
        }
        let mut decoded = Count::default();
        decode_logs_batch_v2_into(
            &mapped.canonical.payload,
            LogsV2DecodeLimits::default(),
            &mut decoded,
        )
        .unwrap();
        assert_eq!(decoded.0, 2);
        assert_eq!(mapped.batch.streams.len(), 1);
        assert_eq!(mapped.batch.streams[0].entries.len(), 2);
        assert_eq!(mapped.batch.streams[0].entries[0].body, "otlp log 0");
        assert!(mapped.batch.streams[0].entries[0]
            .attributes
            .iter()
            .any(|a| a.key == "otel.trace_id"));
    }

    #[test]
    fn invalid_records_are_independent() {
        let mut request = sample_request(4);
        let records = &mut request.resource_logs[0].scope_logs[0].log_records;
        records[0].severity_number = 256;
        records[1].flags = 256;
        records[2].trace_id = vec![0; 16];
        assert_eq!(map_logs(request).canonical.record_count, 1);
    }

    #[test]
    fn canonical_payload_preserves_otlp_fidelity() {
        #[derive(Default)]
        struct Capture(Vec<String>);
        impl LogsV2Appender for Capture {
            fn record(&mut self, record: CanonicalLogRecord<'_>) -> Result<(), String> {
                let mut resource = String::new();
                let mut scope = String::new();
                let mut attributes = String::new();
                let mut body = String::new();
                record
                    .resource_attributes
                    .write_canonical_text(&mut resource);
                record
                    .scope_attributes
                    .unwrap()
                    .write_canonical_text(&mut scope);
                record.attributes.write_canonical_text(&mut attributes);
                record.body.write_canonical_text(&mut body);
                self.0.push(format!(
                    "{}|{}|{}|{}|{:?}|{:?}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
                    record.resource_schema_url,
                    record.scope_schema_url,
                    record.resource_dropped_attributes_count,
                    record.scope_dropped_attributes_count.unwrap(),
                    record.scope_name,
                    record.scope_version,
                    record.time_unix_nano,
                    record.observed_time_unix_nano,
                    record.severity_number,
                    record.severity_text,
                    record.event_name,
                    record.dropped_attributes_count,
                    record.trace_flags,
                    resource,
                    scope,
                    attributes,
                    body
                ));
                assert_eq!(record.trace_id, Some(&[1; 16]));
                assert_eq!(record.span_id, Some(&[2; 8]));
                Ok(())
            }
        }

        let nested = Value::KvlistValue(KeyValueList {
            values: vec![
                kv("z", Value::StringValue("text".into())),
                kv(
                    "a",
                    Value::ArrayValue(ArrayValue {
                        values: vec![
                            value(Value::BoolValue(true)),
                            value(Value::IntValue(-7)),
                            value(Value::DoubleValue(-0.0)),
                            value(Value::BytesValue(vec![0, 255])),
                            AnyValue::default(),
                        ],
                    }),
                ),
            ],
        });
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                schema_url: "resource-schema".into(),
                resource: Some(Resource {
                    attributes: vec![kv("z", Value::IntValue(2)), kv("a", Value::IntValue(1))],
                    dropped_attributes_count: 3,
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    schema_url: "scope-schema".into(),
                    scope: Some(InstrumentationScope {
                        name: "scope".into(),
                        version: "v1".into(),
                        attributes: vec![kv("z", Value::BoolValue(false)), kv("a", nested)],
                        dropped_attributes_count: 4,
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 11,
                        observed_time_unix_nano: 12,
                        severity_number: 24,
                        severity_text: "FATAL".into(),
                        event_name: "event".into(),
                        body: None,
                        attributes: vec![
                            kv("z", Value::IntValue(9)),
                            kv("a", Value::DoubleValue(f64::NAN)),
                        ],
                        dropped_attributes_count: 5,
                        flags: 255,
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                    }],
                }],
            }],
        };
        let mapped = map_logs(request);
        let mut capture = Capture::default();
        assert_eq!(
            decode_logs_batch_v2_into(
                &mapped.canonical.payload,
                LogsV2DecodeLimits::default(),
                &mut capture
            )
            .unwrap(),
            1
        );
        assert_eq!(capture.0, ["resource-schema|scope-schema|3|4|Some(\"scope\")|Some(\"v1\")|11|12|24|FATAL|event|5|255|{\"a\":1,\"z\":2}|{\"a\":{\"a\":[true,-7,0.0,hex\"00ff\",null],\"z\":\"text\"},\"z\":false}|{\"a\":NaN,\"z\":9}|null"]);
    }

    #[test]
    fn rejects_recursive_duplicate_and_dictionary_values() {
        let mut request = sample_request(2);
        let records = &mut request.resource_logs[0].scope_logs[0].log_records;
        records[0].body = Some(AnyValue {
            value: Some(Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList {
                    values: vec![
                        KeyValue {
                            key: "x".into(),
                            ..Default::default()
                        },
                        KeyValue {
                            key: "x".into(),
                            ..Default::default()
                        },
                    ],
                },
            )),
        });
        records[1].body = Some(AnyValue {
            value: Some(Value::StringValueStrindex(1)),
        });
        let mapped = map_logs(request);
        assert_eq!(mapped.rejected, 2);
        assert_eq!(mapped.canonical.record_count, 0);
        assert_eq!(mapped.reasons[Reject::MapKey as usize], 1);
        assert_eq!(mapped.reasons[Reject::Dictionary as usize], 1);
    }

    #[test]
    fn classifies_every_rejection_and_response_is_deterministic() {
        let mut request = sample_request(8);
        let records = &mut request.resource_logs[0].scope_logs[0].log_records;
        records[0].time_unix_nano = 0;
        records[0].observed_time_unix_nano = 0;
        records[1].severity_number = -1;
        records[2].flags = 256;
        records[3].trace_id = vec![1];
        records[4].trace_id.clear();
        records[4].span_id = vec![2; 8];
        records[5].attributes = vec![kv("", Value::IntValue(1))];
        records[6].body = Some(value(Value::KvlistValue(KeyValueList {
            values: vec![kv("x", Value::IntValue(1)), kv("x", Value::IntValue(2))],
        })));
        records[7].body = Some(value(Value::ArrayValue(ArrayValue {
            values: vec![value(Value::KvlistValue(KeyValueList {
                values: vec![KeyValue {
                    key: "x".into(),
                    key_strindex: 1,
                    ..Default::default()
                }],
            }))],
        })));
        let mapped = map_logs(request);
        assert_eq!(mapped.rejected, 8);
        assert_eq!(&mapped.reasons[..7], &[1, 1, 1, 2, 0, 1, 2]);
        assert_eq!(response(mapped.rejected, &mapped.reasons).partial_success.unwrap().error_message,
            "missing_timestamp=1,severity_out_of_range=1,flags_out_of_range=1,invalid_id=2,profiling_dictionary_reference=1,invalid_map_key=2");

        let mut entity = sample_request(1);
        entity.resource_logs[0]
            .resource
            .as_mut()
            .unwrap()
            .entity_refs = vec![EntityRef::default()];
        let mapped = map_logs(entity);
        assert_eq!(mapped.reasons[Reject::Entity as usize], 1);

        let mut nested_empty = sample_request(1);
        nested_empty.resource_logs[0].scope_logs[0].log_records[0].body =
            Some(value(Value::KvlistValue(KeyValueList {
                values: vec![kv("", Value::BoolValue(true))],
            })));
        assert_eq!(map_logs(nested_empty).reasons[Reject::MapKey as usize], 1);
    }
}
