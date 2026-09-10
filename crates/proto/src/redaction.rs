//! Uniform, key-based sensitive-value redaction for encoded ingest batches.
//!
//! Only structured map/attribute values are inspected. Log bodies, span names,
//! status messages, and every other free-text field are deliberately untouched.

use std::borrow::Cow;

use binschema_runtime::BinSchemaError;

use crate::constants::{DEFAULT_MAX_BATCH_BYTES, LOGS_BATCH_V2_MAGIC};
use crate::streaming_logs_v2::{
    decode_logs_batch_v2_into, AnyValue, AnyValueRef, CanonicalLogRecord, DecodeError,
    DecodeLimits, LogsV2Appender, ANY_ARRAY, ANY_MAP, ANY_STRING,
};

pub const REDACTED: &str = "[REDACTED]";

#[derive(Debug, thiserror::Error)]
pub enum RedactionError {
    #[error("malformed logs-v1 batch: {0}")]
    LogsV1(#[source] BinSchemaError),
    #[error("malformed traces-v1 batch: {0}")]
    TracesV1(#[source] BinSchemaError),
    #[error("malformed logs-v2 batch: {0}")]
    LogsV2(#[source] DecodeError),
}

/// Returns whether a structured key denotes sensitive data.
///
/// Matching is ASCII-case-insensitive. Non-ASCII-alphanumeric bytes delimit
/// segments, so `http.request.header.Authorization` matches while
/// `authorizationStatus` does not. Conventional multi-segment names may occur
/// within a qualified key; `token` and `secret` match only as the final segment.
pub fn is_sensitive_key(key: &str) -> bool {
    let mut segments = Segments::new(key.as_bytes());
    let mut previous = None;
    while let Some(segment) = segments.next() {
        let single = eq(segment, b"authorization")
            || eq(segment, b"cookie")
            || eq(segment, b"password")
            || eq(segment, b"passphrase");
        let pair = previous.is_some_and(|p| {
            (eq(p, b"proxy") && eq(segment, b"authorization"))
                || (eq(p, b"set") && eq(segment, b"cookie"))
                || (eq(p, b"api") && eq(segment, b"key"))
                || (eq(p, b"access") && eq(segment, b"token"))
                || (eq(p, b"refresh") && eq(segment, b"token"))
                || (eq(p, b"id") && eq(segment, b"token"))
                || (eq(p, b"client") && eq(segment, b"secret"))
                || (eq(p, b"private") && eq(segment, b"key"))
        });
        let is_final = segments.peek().is_none();
        if is_final && (single || pair || eq(segment, b"token") || eq(segment, b"secret")) {
            return true;
        }
        previous = Some(segment);
    }
    false
}

struct Segments<'a> {
    rest: &'a [u8],
}
impl<'a> Segments<'a> {
    fn new(rest: &'a [u8]) -> Self {
        Self { rest }
    }
    fn peek(&self) -> Option<&'a [u8]> {
        let mut copy = Self { rest: self.rest };
        copy.next()
    }
}
impl<'a> Iterator for Segments<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        let start = self.rest.iter().position(u8::is_ascii_alphanumeric)?;
        self.rest = &self.rest[start..];
        let end = self
            .rest
            .iter()
            .position(|b| !b.is_ascii_alphanumeric())
            .unwrap_or(self.rest.len());
        let segment = &self.rest[..end];
        self.rest = &self.rest[end..];
        Some(segment)
    }
}
fn eq(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

type Emit<'a> = dyn FnMut(&[u8]) -> Result<(), BinSchemaError> + 'a;

/// Allocation-free cursor used by the v1 redactors. Collection counts are
/// checked against both the bytes still available and an aggregate item budget
/// derived from the transport-sized payload before any loop is entered.
struct V1Cursor<'a> {
    payload: &'a [u8],
    pos: usize,
    items_left: usize,
    changed: bool,
}

impl<'a> V1Cursor<'a> {
    fn new(payload: &'a [u8]) -> Result<Self, BinSchemaError> {
        if payload.len() > DEFAULT_MAX_BATCH_BYTES as usize {
            return Err(BinSchemaError::InvalidValue(
                "batch exceeds transport byte limit".into(),
            ));
        }
        Ok(Self {
            payload,
            pos: 0,
            items_left: payload.len(),
            changed: false,
        })
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], BinSchemaError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(BinSchemaError::UnexpectedEof)?;
        let bytes = self
            .payload
            .get(self.pos..end)
            .ok_or(BinSchemaError::UnexpectedEof)?;
        self.pos = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, BinSchemaError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<usize, BinSchemaError> {
        let bytes: [u8; 2] = self.take(2)?.try_into().expect("fixed slice");
        Ok(u16::from_be_bytes(bytes) as usize)
    }

    fn u32(&mut self) -> Result<usize, BinSchemaError> {
        let bytes: [u8; 4] = self.take(4)?.try_into().expect("fixed slice");
        Ok(u32::from_be_bytes(bytes) as usize)
    }

    fn count(&mut self, count: usize, minimum_bytes: usize) -> Result<(), BinSchemaError> {
        let required = count
            .checked_mul(minimum_bytes)
            .ok_or(BinSchemaError::UnexpectedEof)?;
        if required > self.payload.len().saturating_sub(self.pos) || count > self.items_left {
            return Err(BinSchemaError::UnexpectedEof);
        }
        self.items_left -= count;
        Ok(())
    }

    fn utf8(&mut self, len: usize) -> Result<&'a [u8], BinSchemaError> {
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes).map_err(|_| BinSchemaError::InvalidUtf8)?;
        Ok(bytes)
    }

    fn raw(&mut self, len: usize, emit: &mut Emit<'_>) -> Result<(), BinSchemaError> {
        emit(self.take(len)?)
    }

    fn string_u8(&mut self, emit: &mut Emit<'_>, validate: bool) -> Result<(), BinSchemaError> {
        let start = self.pos;
        let len = self.u8()? as usize;
        if validate {
            self.utf8(len)?;
        } else {
            self.take(len)?;
        }
        emit(&self.payload[start..self.pos])
    }

    fn string_u16(&mut self, emit: &mut Emit<'_>) -> Result<(), BinSchemaError> {
        let start = self.pos;
        let len = self.u16()?;
        self.utf8(len)?;
        emit(&self.payload[start..self.pos])
    }

    fn pair(&mut self, emit: &mut Emit<'_>) -> Result<(), BinSchemaError> {
        let start = self.pos;
        let key_len = self.u8()? as usize;
        let key = self.utf8(key_len)?;
        let value_length_pos = self.pos;
        let value_len = self.u16()?;
        let value = self.utf8(value_len)?;
        if is_sensitive_key(std::str::from_utf8(key).expect("validated"))
            && value != REDACTED.as_bytes()
        {
            emit(&self.payload[start..value_length_pos])?;
            emit(&(REDACTED.len() as u16).to_be_bytes())?;
            emit(REDACTED.as_bytes())?;
            self.changed = true;
        } else {
            emit(&self.payload[start..self.pos])?;
        }
        Ok(())
    }

    fn pairs(&mut self, count: usize, emit: &mut Emit<'_>) -> Result<(), BinSchemaError> {
        self.count(count, 3)?;
        for _ in 0..count {
            self.pair(emit)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<bool, BinSchemaError> {
        if self.pos != self.payload.len() {
            return Err(BinSchemaError::InvalidEncoding("trailing bytes".into()));
        }
        Ok(self.changed)
    }
}

fn parse_logs_v1(payload: &[u8], emit: &mut Emit<'_>) -> Result<bool, BinSchemaError> {
    let mut cursor = V1Cursor::new(payload)?;
    let start = cursor.pos;
    let streams = cursor.u32()?;
    emit(&payload[start..cursor.pos])?;
    cursor.count(streams, 14)?;
    for _ in 0..streams {
        cursor.raw(8, emit)?;
        let start = cursor.pos;
        let labels = cursor.u16()?;
        emit(&payload[start..cursor.pos])?;
        cursor.pairs(labels, emit)?;
        let start = cursor.pos;
        let entries = cursor.u32()?;
        emit(&payload[start..cursor.pos])?;
        cursor.count(entries, 15)?;
        for _ in 0..entries {
            cursor.raw(9, emit)?;
            let start = cursor.pos;
            let body_len = cursor.u32()?;
            cursor.utf8(body_len)?;
            emit(&payload[start..cursor.pos])?;
            let start = cursor.pos;
            let attrs = cursor.u16()?;
            emit(&payload[start..cursor.pos])?;
            cursor.pairs(attrs, emit)?;
        }
    }
    cursor.finish()
}

fn parse_traces_v1(payload: &[u8], emit: &mut Emit<'_>) -> Result<bool, BinSchemaError> {
    let mut cursor = V1Cursor::new(payload)?;
    let start = cursor.pos;
    let resources = cursor.u16()?;
    emit(&payload[start..cursor.pos])?;
    cursor.count(resources, 2)?;
    for _ in 0..resources {
        let start = cursor.pos;
        let labels = cursor.u16()?;
        emit(&payload[start..cursor.pos])?;
        cursor.pairs(labels, emit)?;
    }

    let start = cursor.pos;
    let scopes = cursor.u16()?;
    emit(&payload[start..cursor.pos])?;
    cursor.count(scopes, 2)?;
    for _ in 0..scopes {
        cursor.string_u8(emit, true)?;
        // Generated encoding treats this field as bytes mapped to ASCII chars;
        // every byte representation is canonical, including bytes above 0x7f.
        cursor.string_u8(emit, false)?;
    }

    let start = cursor.pos;
    let spans = cursor.u32()?;
    emit(&payload[start..cursor.pos])?;
    cursor.count(spans, 56)?;
    for _ in 0..spans {
        cursor.raw(28, emit)?; // dictionary indexes and trace/span ids
        let start = cursor.pos;
        match cursor.u8()? {
            0 => {}
            1 => {
                cursor.take(8)?;
            }
            _ => {
                return Err(BinSchemaError::InvalidEncoding(
                    "non-canonical optional".into(),
                ))
            }
        }
        emit(&payload[start..cursor.pos])?;
        cursor.string_u16(emit)?;
        cursor.raw(18, emit)?; // kind, timestamps, status
        cursor.string_u16(emit)?;

        let start = cursor.pos;
        let attrs = cursor.u16()?;
        emit(&payload[start..cursor.pos])?;
        cursor.pairs(attrs, emit)?;

        let start = cursor.pos;
        let events = cursor.u16()?;
        emit(&payload[start..cursor.pos])?;
        cursor.count(events, 11)?;
        for _ in 0..events {
            cursor.raw(8, emit)?;
            cursor.string_u16(emit)?;
            let start = cursor.pos;
            let attrs = cursor.u8()? as usize;
            emit(&payload[start..cursor.pos])?;
            cursor.pairs(attrs, emit)?;
        }

        let start = cursor.pos;
        let links = cursor.u8()? as usize;
        emit(&payload[start..cursor.pos])?;
        cursor.count(links, 25)?;
        for _ in 0..links {
            cursor.raw(24, emit)?;
            let start = cursor.pos;
            let attrs = cursor.u8()? as usize;
            emit(&payload[start..cursor.pos])?;
            cursor.pairs(attrs, emit)?;
        }
    }
    cursor.finish()
}

fn redact_v1<'a>(
    payload: &'a [u8],
    parse: fn(&[u8], &mut Emit<'_>) -> Result<bool, BinSchemaError>,
) -> Result<Cow<'a, [u8]>, BinSchemaError> {
    let mut output_len = 0usize;
    let changed = parse(payload, &mut |bytes| {
        output_len = output_len
            .checked_add(bytes.len())
            .ok_or_else(|| BinSchemaError::InvalidValue("redacted batch too large".into()))?;
        if output_len > DEFAULT_MAX_BATCH_BYTES as usize {
            return Err(BinSchemaError::InvalidValue(
                "redacted batch exceeds transport byte limit".into(),
            ));
        }
        Ok(())
    })?;
    if !changed {
        return Ok(Cow::Borrowed(payload));
    }
    // The first pass validated every count and length, and computed the exact
    // aggregate output size. No attacker-declared collection count is used as
    // allocation capacity.
    let mut output = Vec::with_capacity(output_len);
    parse(payload, &mut |bytes| {
        output.extend_from_slice(bytes);
        Ok(())
    })?;
    debug_assert_eq!(output.len(), output_len);
    Ok(Cow::Owned(output))
}

/// Validates and redacts an encoded logs-v1 batch without materialising it.
pub fn redact_logs_v1(payload: &[u8]) -> Result<Cow<'_, [u8]>, RedactionError> {
    redact_v1(payload, parse_logs_v1).map_err(RedactionError::LogsV1)
}

/// Validates and redacts an encoded traces-v1 batch without materialising it.
pub fn redact_traces_v1(payload: &[u8]) -> Result<Cow<'_, [u8]>, RedactionError> {
    redact_v1(payload, parse_traces_v1).map_err(RedactionError::TracesV1)
}

/// Reusable output storage for logs-v2 redaction.
#[derive(Default)]
pub struct LogsV2RedactionScratch {
    output: Vec<u8>,
    record: Vec<u8>,
}

/// Validates and redacts all structured maps in a canonical logs-v2 envelope.
/// Values under sensitive keys become typed strings; nested arrays and maps are
/// traversed directly from borrowed wire views without constructing an owned AST.
pub fn redact_logs_v2<'a>(
    payload: &'a [u8],
    limits: DecodeLimits,
    scratch: &mut LogsV2RedactionScratch,
) -> Result<Cow<'a, [u8]>, RedactionError> {
    scratch.output.clear();
    scratch.record.clear();
    let mut sink = V2Sink {
        output: &mut scratch.output,
        record: &mut scratch.record,
        changed: false,
    };
    decode_logs_batch_v2_into(payload, limits, &mut sink).map_err(RedactionError::LogsV2)?;
    if sink.changed {
        Ok(Cow::Owned(std::mem::take(sink.output)))
    } else {
        if sink.output.as_slice() != payload {
            return Err(RedactionError::LogsV2(DecodeError::Appender(
                "logs-v2 redaction did not preserve an unchanged batch".into(),
            )));
        }
        sink.output.clear();
        Ok(Cow::Borrowed(payload))
    }
}

struct V2Sink<'a> {
    output: &'a mut Vec<u8>,
    record: &'a mut Vec<u8>,
    changed: bool,
}
impl LogsV2Appender for V2Sink<'_> {
    fn begin_batch_version(&mut self, raw_version: u16, count: u32) -> Result<(), String> {
        self.output
            .extend_from_slice(&LOGS_BATCH_V2_MAGIC.to_be_bytes());
        self.output.extend_from_slice(&raw_version.to_be_bytes());
        self.output.extend_from_slice(&count.to_be_bytes());
        Ok(())
    }
    fn record(&mut self, record: CanonicalLogRecord<'_>) -> Result<(), String> {
        self.record.clear();
        let changed = encode_record(&record, self.record);
        if !changed && self.record != record.encoded {
            return Err("logs-v2 redaction did not preserve an unchanged record".into());
        }
        self.changed |= changed;
        let record_len = u32::try_from(self.record.len())
            .map_err(|_| "redacted logs-v2 record length exceeds u32".to_owned())?;
        self.output.extend_from_slice(&record_len.to_be_bytes());
        self.output.extend_from_slice(self.record);
        Ok(())
    }
}

fn bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(value);
}
fn string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    bytes(out, value.as_bytes());
}
fn value(value: AnyValueRef<'_>, out: &mut Vec<u8>) -> bool {
    match value.value() {
        AnyValue::Array(array) => {
            out.push(ANY_ARRAY);
            out.extend_from_slice(&array.len().to_be_bytes());
            let mut changed = false;
            for item in array {
                changed |= self::value(item, out);
            }
            changed
        }
        AnyValue::Map(map) => {
            out.push(ANY_MAP);
            out.extend_from_slice(&map.len().to_be_bytes());
            let mut changed = false;
            for (key, item) in map {
                string(out, key);
                if is_sensitive_key(key) {
                    out.push(ANY_STRING);
                    string(out, REDACTED);
                    changed |= !matches!(item.value(), AnyValue::String(REDACTED));
                } else {
                    changed |= self::value(item, out);
                }
            }
            changed
        }
        _ => {
            bytes(out, value.encoded());
            false
        }
    }
}
fn encode_record(record: &CanonicalLogRecord<'_>, out: &mut Vec<u8>) -> bool {
    out.extend_from_slice(&0u16.to_be_bytes());
    string(out, record.resource_schema_url);
    out.extend_from_slice(&record.resource_dropped_attributes_count.to_be_bytes());
    let mut changed = value(record.resource_attributes, out);
    match (
        record.scope_name,
        record.scope_version,
        record.scope_dropped_attributes_count,
        record.scope_attributes,
    ) {
        (Some(name), Some(version), Some(dropped), Some(attributes)) => {
            out.push(1);
            string(out, name);
            string(out, version);
            out.extend_from_slice(&dropped.to_be_bytes());
            changed |= value(attributes, out);
        }
        _ => out.push(0),
    }
    string(out, record.scope_schema_url);
    out.extend_from_slice(&record.time_unix_nano.to_be_bytes());
    out.extend_from_slice(&record.observed_time_unix_nano.to_be_bytes());
    if let Some(received) = record.received_time_unix_nano {
        out.extend_from_slice(&received.to_be_bytes());
    }
    out.extend_from_slice(&record.severity_number.to_be_bytes());
    string(out, record.severity_text);
    string(out, record.event_name);
    changed |= value(record.body, out);
    out.extend_from_slice(&record.dropped_attributes_count.to_be_bytes());
    changed |= value(record.attributes, out);
    out.extend_from_slice(&record.trace_flags.to_be_bytes());
    if let Some(id) = record.trace_id {
        out.push(16);
        bytes(out, id);
    } else {
        out.push(0);
    }
    if let Some(id) = record.span_id {
        out.push(8);
        bytes(out, id);
    } else {
        out.push(0);
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::{
        LabelPair, LogEntry, LogStream, LogsBatch, ResourceEntry, ScopeEntry, Span, SpanEvent,
        SpanLink, TracesBatch,
    };
    use crate::logs_v2_encoder::{
        encode_logs_batch_v2_into, AnyValueInput, KeyValueInput, LogRecordInput, ScopeInput,
    };

    fn pair(key: &str, value: &str) -> LabelPair {
        LabelPair {
            key: key.into(),
            value: value.into(),
        }
    }

    #[test]
    fn sensitive_key_matching_is_segment_aware_and_exhaustive() {
        for key in [
            "authorization",
            "PROXY.Authorization",
            "cookie",
            "set-cookie",
            "password",
            "passphrase",
            "api-key",
            "api_token",
            "access-token",
            "refresh_token",
            "id.token",
            "client_secret",
            "private-key",
            "token",
            "auth.token",
            "secret",
            "db.secret",
        ] {
            assert!(is_sensitive_key(key), "expected sensitive key: {key}");
        }
        for key in [
            "",
            "authorizations",
            "authorizationStatus",
            "cookies",
            "passwordless",
            "passphrases",
            "api-key-id",
            "api_token_count",
            "access-token-kind",
            "refresh-tokenized",
            "id-token-type",
            "client-secret-name",
            "private-key-id",
            "token-count",
            "secretary",
            "mytoken",
            "not_secret_value",
        ] {
            assert!(!is_sensitive_key(key), "sensitive lookalike: {key}");
        }
    }

    #[test]
    fn logs_v1_redacts_only_structured_values_and_is_deterministic() {
        let batch = LogsBatch {
            streams: vec![LogStream {
                fingerprint: 7,
                labels: vec![
                    pair("Authorization", "stream-secret"),
                    pair("token-count", "2"),
                ],
                entries: vec![LogEntry {
                    ts_unix_nano: 9,
                    severity: 4,
                    body: "authorization=free-text-secret".into(),
                    attributes: vec![pair("http.api_token", "entry-secret"), pair("user", "bart")],
                }],
            }],
        };
        let encoded = batch.encode().unwrap();
        let redacted = redact_logs_v1(&encoded).unwrap().into_owned();
        let got = LogsBatch::decode(&redacted).unwrap();
        assert_eq!(got.streams[0].labels[0].value, REDACTED);
        assert_eq!(got.streams[0].labels[1].value, "2");
        assert_eq!(got.streams[0].entries[0].attributes[0].value, REDACTED);
        assert_eq!(got.streams[0].entries[0].attributes[1].value, "bart");
        assert_eq!(
            got.streams[0].entries[0].body,
            "authorization=free-text-secret"
        );
        assert!(matches!(
            redact_logs_v1(&redacted).unwrap(),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn traces_v1_redacts_every_attribute_location_but_not_free_text() {
        let batch = TracesBatch {
            resources: vec![ResourceEntry {
                labels: vec![pair("client.secret", "resource")],
            }],
            scopes: vec![ScopeEntry {
                name: "authorization".into(),
                version: "secret".into(),
            }],
            spans: vec![Span {
                resource_idx: 0,
                scope_idx: 0,
                trace_id: vec![1; 16],
                span_id: vec![2; 8],
                parent_span_id: None,
                name: "password=free-text".into(),
                kind: 1,
                start_unix_nano: 10,
                end_unix_nano: 20,
                status_code: 2,
                status_message: "token=free-text".into(),
                attributes: vec![pair("password", "span")],
                events: vec![SpanEvent {
                    ts_unix_nano: 12,
                    name: "cookie".into(),
                    attributes: vec![pair("cookie", "event")],
                }],
                links: vec![SpanLink {
                    trace_id: vec![3; 16],
                    span_id: vec![4; 8],
                    attributes: vec![pair("private_key", "link")],
                }],
            }],
        };
        let encoded = batch.encode().unwrap();
        let redacted = redact_traces_v1(&encoded).unwrap().into_owned();
        let got = TracesBatch::decode(&redacted).unwrap();
        assert_eq!(got.resources[0].labels[0].value, REDACTED);
        assert_eq!(got.spans[0].attributes[0].value, REDACTED);
        assert_eq!(got.spans[0].events[0].attributes[0].value, REDACTED);
        assert_eq!(got.spans[0].links[0].attributes[0].value, REDACTED);
        assert_eq!(got.spans[0].name, "password=free-text");
        assert_eq!(got.spans[0].status_message, "token=free-text");
        assert_eq!(got.scopes[0].name, "authorization");
        assert!(matches!(
            redact_traces_v1(&redacted).unwrap(),
            Cow::Borrowed(_)
        ));
    }

    fn v2_record<'a>(
        resource: &'a [KeyValueInput<'a>],
        scope_attrs: &'a [KeyValueInput<'a>],
        body: AnyValueInput<'a>,
        attributes: &'a [KeyValueInput<'a>],
        trace_id: &'a [u8; 16],
        span_id: &'a [u8; 8],
    ) -> LogRecordInput<'a> {
        LogRecordInput {
            resource_schema_url: "resource-schema",
            resource_dropped_attributes_count: 1,
            resource_attributes: resource,
            scope: Some(ScopeInput {
                name: "scope",
                version: "v1",
                dropped_attributes_count: 2,
                attributes: scope_attrs,
            }),
            scope_schema_url: "scope-schema",
            time_unix_nano: 3,
            observed_time_unix_nano: 4,
            severity: 255,
            severity_text: "authorization=free-text",
            event_name: "cookie",
            body,
            dropped_attributes_count: 5,
            attributes,
            trace_flags: 255,
            trace_id: Some(trace_id),
            span_id: Some(span_id),
        }
    }

    #[derive(Default)]
    struct V2Collected {
        values: Vec<(String, String, String, String)>,
        metadata: Vec<(i32, u32, bool, bool)>,
    }
    impl LogsV2Appender for V2Collected {
        fn record(&mut self, record: CanonicalLogRecord<'_>) -> Result<(), String> {
            let mut resource = String::new();
            record
                .resource_attributes
                .write_typed_canonical_text(&mut resource);
            let mut scope = String::new();
            record
                .scope_attributes
                .expect("scope is present")
                .write_typed_canonical_text(&mut scope);
            let mut body = String::new();
            record.body.write_typed_canonical_text(&mut body);
            let mut attrs = String::new();
            record.attributes.write_typed_canonical_text(&mut attrs);
            self.values.push((resource, scope, body, attrs));
            self.metadata.push((
                record.severity_number,
                record.trace_flags,
                record.trace_id.is_some(),
                record.span_id.is_some(),
            ));
            assert_eq!(record.severity_text, "authorization=free-text");
            assert_eq!(record.event_name, "cookie");
            Ok(())
        }
    }

    #[test]
    fn logs_v2_recursively_redacts_typed_maps_and_preserves_scalars() {
        let nested = [
            KeyValueInput {
                key: "access-token",
                value: AnyValueInput::Int64(42),
            },
            KeyValueInput {
                key: "safe",
                value: AnyValueInput::Bytes(&[0xab, 0xcd]),
            },
        ];
        let array = [
            AnyValueInput::Bool(true),
            AnyValueInput::Map(&nested),
            AnyValueInput::Double(1.5),
        ];
        let resource = [KeyValueInput {
            key: "api_key",
            value: AnyValueInput::Null,
        }];
        let scope = [
            KeyValueInput {
                key: "refresh_token",
                value: AnyValueInput::Bool(false),
            },
            KeyValueInput {
                key: "safe",
                value: AnyValueInput::String("secret free text"),
            },
        ];
        let body_map = [KeyValueInput {
            key: "array",
            value: AnyValueInput::Array(&array),
        }];
        let attrs = [
            KeyValueInput {
                key: "password",
                value: AnyValueInput::Map(&nested),
            },
            KeyValueInput {
                key: "token-count",
                value: AnyValueInput::Int64(-7),
            },
        ];
        let trace = [1; 16];
        let span = [2; 8];
        let mut encoded = Vec::new();
        encode_logs_batch_v2_into(
            &[v2_record(
                &resource,
                &scope,
                AnyValueInput::Map(&body_map),
                &attrs,
                &trace,
                &span,
            )],
            DecodeLimits::default(),
            &mut encoded,
        )
        .unwrap();
        let mut scratch = LogsV2RedactionScratch::default();
        let redacted = redact_logs_v2(&encoded, DecodeLimits::default(), &mut scratch)
            .unwrap()
            .into_owned();
        let mut got = V2Collected::default();
        decode_logs_batch_v2_into(&redacted, DecodeLimits::default(), &mut got).unwrap();
        assert_eq!(got.values[0].0, r#"{"api_key":"[REDACTED]"}"#);
        assert_eq!(
            got.values[0].1,
            r#"{"refresh_token":"[REDACTED]","safe":"secret free text"}"#
        );
        assert_eq!(
            got.values[0].2,
            r#"{"array":[true,{"access-token":"[REDACTED]","safe":hex"abcd"},1.5]}"#
        );
        assert_eq!(
            got.values[0].3,
            r#"{"password":"[REDACTED]","token-count":-7}"#
        );
        assert_eq!(got.metadata[0], (255, 255, true, true));
        let repeated = redact_logs_v2(&redacted, DecodeLimits::default(), &mut scratch).unwrap();
        assert!(matches!(repeated, Cow::Borrowed(_)));
        assert_eq!(repeated.as_ref(), redacted);
    }

    #[test]
    fn unchanged_batches_borrow_and_malformed_batches_are_rejected() {
        let logs = LogsBatch { streams: vec![] }.encode().unwrap();
        let traces = TracesBatch {
            resources: vec![],
            scopes: vec![],
            spans: vec![],
        }
        .encode()
        .unwrap();
        assert!(matches!(redact_logs_v1(&logs).unwrap(), Cow::Borrowed(_)));
        assert!(matches!(
            redact_traces_v1(&traces).unwrap(),
            Cow::Borrowed(_)
        ));
        let mut bad_logs = logs;
        bad_logs.push(0);
        assert!(redact_logs_v1(&bad_logs).is_err());
        let mut bad_traces = traces;
        bad_traces.push(0);
        assert!(redact_traces_v1(&bad_traces).is_err());
        let mut scratch = LogsV2RedactionScratch::default();
        assert!(redact_logs_v2(&[0; 12], DecodeLimits::default(), &mut scratch).is_err());
    }

    #[test]
    fn v1_redaction_rejects_impossible_u32_counts_without_allocating() {
        assert!(matches!(
            redact_logs_v1(&u32::MAX.to_be_bytes()),
            Err(RedactionError::LogsV1(BinSchemaError::UnexpectedEof))
        ));

        // One stream with no labels, followed by an attacker-sized entry count.
        let mut huge_entries = Vec::new();
        huge_entries.extend_from_slice(&1_u32.to_be_bytes());
        huge_entries.extend_from_slice(&0_u64.to_be_bytes());
        huge_entries.extend_from_slice(&0_u16.to_be_bytes());
        huge_entries.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            redact_logs_v1(&huge_entries),
            Err(RedactionError::LogsV1(BinSchemaError::UnexpectedEof))
        ));

        // Empty dictionaries followed by an attacker-sized span count.
        let mut huge_spans = Vec::new();
        huge_spans.extend_from_slice(&0_u16.to_be_bytes());
        huge_spans.extend_from_slice(&0_u16.to_be_bytes());
        huge_spans.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            redact_traces_v1(&huge_spans),
            Err(RedactionError::TracesV1(BinSchemaError::UnexpectedEof))
        ));
    }
}
