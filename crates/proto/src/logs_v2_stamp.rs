//! Canonical logs raw-record receipt stamping and replay-safe redaction.
//!
//! Both paths stream borrowed decoded values into reusable byte buffers. They do
//! not construct an owned per-scalar tree. Live acceptance admits producer v1
//! only and inserts one trusted receipt timestamp into every output record;
//! replay preserves the input grammar version and any existing receipt.

use std::borrow::Cow;

use crate::constants::{LOGS_BATCH_V2_MAGIC, LOGS_RAW_VERSION_V1, LOGS_RAW_VERSION_V2};
use crate::redaction::{is_sensitive_key, REDACTED};
use crate::streaming_logs_v2::{
    decode_logs_batch_v2_into, AnyValue, AnyValueRef, CanonicalLogRecord, DecodeError,
    DecodeLimits, LogsV2Appender, ANY_ARRAY, ANY_MAP, ANY_STRING,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LogsV2StampError {
    #[error("server receipt timestamp must be nonzero")]
    ZeroReceivedTime,
    #[error("live receipt stamping requires producer raw-record v1, got version {0}")]
    AlreadyStamped(u16),
    #[error("malformed canonical logs batch: {0}")]
    Decode(#[from] DecodeError),
}

/// Reusable storage for receipt stamping and replay redaction.
#[derive(Default)]
pub struct LogsV2StampScratch {
    output: Vec<u8>,
    record: Vec<u8>,
}

/// Validates producer-v1 records, recursively redacts structured sensitive
/// values, and emits canonical server-stamped v2 records.
///
/// `received_ns` is copied unchanged into every record. Zero is rejected, and
/// an input v2 envelope is rejected so a producer cannot provide trusted receipt
/// metadata. Output is checked against the same payload and record byte limits.
pub fn redact_and_stamp_logs_v2<'s>(
    payload: &[u8],
    received_ns: u64,
    limits: DecodeLimits,
    scratch: &'s mut LogsV2StampScratch,
) -> Result<&'s [u8], LogsV2StampError> {
    if received_ns == 0 {
        return Err(LogsV2StampError::ZeroReceivedTime);
    }
    if let Some(version) = envelope_version(payload) {
        if version == LOGS_RAW_VERSION_V2 {
            return Err(LogsV2StampError::AlreadyStamped(version));
        }
    }
    encode(payload, limits, scratch, Mode::Stamp(received_ns))?;
    Ok(&scratch.output)
}

/// Validates and recursively redacts canonical v1 or v2 records for replay.
/// The envelope version and every v2 receipt timestamp are preserved exactly.
pub fn redact_replayed_logs_v2<'a>(
    payload: &'a [u8],
    limits: DecodeLimits,
    scratch: &mut LogsV2StampScratch,
) -> Result<Cow<'a, [u8]>, LogsV2StampError> {
    let changed = encode(payload, limits, scratch, Mode::Preserve)?;
    if changed {
        Ok(Cow::Owned(std::mem::take(&mut scratch.output)))
    } else {
        if scratch.output.as_slice() != payload {
            return Err(DecodeError::Appender(
                "unchanged replay redaction did not preserve canonical bytes".into(),
            )
            .into());
        }
        scratch.output.clear();
        Ok(Cow::Borrowed(payload))
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Stamp(u64),
    Preserve,
}

fn envelope_version(payload: &[u8]) -> Option<u16> {
    payload
        .get(4..6)
        .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn encode(
    payload: &[u8],
    limits: DecodeLimits,
    scratch: &mut LogsV2StampScratch,
    mode: Mode,
) -> Result<bool, LogsV2StampError> {
    scratch.output.clear();
    scratch.record.clear();
    let mut sink = Sink {
        output: &mut scratch.output,
        record: &mut scratch.record,
        limits,
        mode,
        changed: matches!(mode, Mode::Stamp(_)),
        output_version: 0,
    };
    decode_logs_batch_v2_into(payload, limits, &mut sink)?;
    Ok(sink.changed)
}

struct Sink<'a> {
    output: &'a mut Vec<u8>,
    record: &'a mut Vec<u8>,
    limits: DecodeLimits,
    mode: Mode,
    changed: bool,
    output_version: u16,
}

impl LogsV2Appender for Sink<'_> {
    fn begin_batch_version(&mut self, raw_version: u16, count: u32) -> Result<(), String> {
        self.output_version = match self.mode {
            Mode::Stamp(_) if raw_version == LOGS_RAW_VERSION_V1 => LOGS_RAW_VERSION_V2,
            Mode::Stamp(_) => {
                return Err(format!(
                    "live receipt stamping requires producer raw-record v1, got {raw_version}"
                ));
            }
            Mode::Preserve => raw_version,
        };
        append(
            self.output,
            &LOGS_BATCH_V2_MAGIC.to_be_bytes(),
            self.limits.max_payload_bytes,
        )?;
        append(
            self.output,
            &self.output_version.to_be_bytes(),
            self.limits.max_payload_bytes,
        )?;
        append(
            self.output,
            &count.to_be_bytes(),
            self.limits.max_payload_bytes,
        )
    }

    fn record(&mut self, record: CanonicalLogRecord<'_>) -> Result<(), String> {
        let expected_input = match self.mode {
            Mode::Stamp(_) => LOGS_RAW_VERSION_V1,
            Mode::Preserve => self.output_version,
        };
        if record.raw_version != expected_input {
            return Err("record grammar version differs from envelope version".into());
        }
        self.record.clear();
        let changed = encode_record(&record, self.mode, self.record);
        if !changed && self.record.as_slice() != record.encoded {
            return Err("unchanged replay record was not preserved byte-for-byte".into());
        }
        self.changed |= changed;
        if self.record.len() > self.limits.max_record_bytes {
            return Err("redacted canonical record exceeds configured byte limit".into());
        }
        let record_len = u32::try_from(self.record.len())
            .map_err(|_| "canonical record length exceeds u32".to_owned())?;
        append(
            self.output,
            &record_len.to_be_bytes(),
            self.limits.max_payload_bytes,
        )?;
        append(self.output, self.record, self.limits.max_payload_bytes)
    }
}

fn append(out: &mut Vec<u8>, bytes: &[u8], limit: usize) -> Result<(), String> {
    let new_len = out
        .len()
        .checked_add(bytes.len())
        .ok_or_else(|| "canonical logs output length overflow".to_owned())?;
    if new_len > limit {
        return Err("canonical logs output exceeds configured payload byte limit".into());
    }
    out.extend_from_slice(bytes);
    Ok(())
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

fn encode_record(record: &CanonicalLogRecord<'_>, mode: Mode, out: &mut Vec<u8>) -> bool {
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
    match mode {
        Mode::Stamp(received) => out.extend_from_slice(&received.to_be_bytes()),
        Mode::Preserve => {
            if let Some(received) = record.received_time_unix_nano {
                out.extend_from_slice(&received.to_be_bytes());
            }
        }
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
    changed || matches!(mode, Mode::Stamp(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logs_v2_encoder::{
        encode_logs_batch_v2_into, AnyValueInput, KeyValueInput, LogRecordInput, ScopeInput,
    };

    fn input<'a>(
        resource_attributes: &'a [KeyValueInput<'a>],
        scope_attributes: &'a [KeyValueInput<'a>],
        attributes: &'a [KeyValueInput<'a>],
        trace_id: &'a [u8; 16],
        span_id: &'a [u8; 8],
    ) -> LogRecordInput<'a> {
        LogRecordInput {
            resource_schema_url: "resource/schema",
            resource_dropped_attributes_count: 11,
            resource_attributes,
            scope: Some(ScopeInput {
                name: "scope-name",
                version: "scope-version",
                dropped_attributes_count: 12,
                attributes: scope_attributes,
            }),
            scope_schema_url: "scope/schema",
            time_unix_nano: 13,
            observed_time_unix_nano: 14,
            severity: 15,
            severity_text: "severity-text",
            event_name: "event-name",
            body: AnyValueInput::Bytes(&[16, 17]),
            dropped_attributes_count: 18,
            attributes,
            trace_flags: 19,
            trace_id: Some(trace_id),
            span_id: Some(span_id),
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Fields {
        resource_schema_url: String,
        resource_dropped: u32,
        resource: String,
        scope_name: String,
        scope_version: String,
        scope_dropped: u32,
        scope: String,
        scope_schema_url: String,
        time: u64,
        observed: u64,
        severity: i32,
        severity_text: String,
        event_name: String,
        body: Vec<u8>,
        dropped: u32,
        attributes: String,
        trace_flags: u32,
        trace_id: [u8; 16],
        span_id: [u8; 8],
    }

    #[derive(Default)]
    struct Collected {
        count: usize,
        raw_version: u16,
        received: Option<u64>,
        fields: Option<Fields>,
    }

    impl LogsV2Appender for Collected {
        fn begin_batch_version(&mut self, raw_version: u16, _: u32) -> Result<(), String> {
            self.raw_version = raw_version;
            Ok(())
        }

        fn record(&mut self, r: CanonicalLogRecord<'_>) -> Result<(), String> {
            self.count += 1;
            self.received = r.received_time_unix_nano;
            let mut resource = String::new();
            r.resource_attributes
                .write_typed_canonical_text(&mut resource);
            let mut scope = String::new();
            r.scope_attributes
                .unwrap()
                .write_typed_canonical_text(&mut scope);
            let mut attrs = String::new();
            r.attributes.write_typed_canonical_text(&mut attrs);
            self.fields = Some(Fields {
                resource_schema_url: r.resource_schema_url.into(),
                resource_dropped: r.resource_dropped_attributes_count,
                resource,
                scope_name: r.scope_name.unwrap().into(),
                scope_version: r.scope_version.unwrap().into(),
                scope_dropped: r.scope_dropped_attributes_count.unwrap(),
                scope,
                scope_schema_url: r.scope_schema_url.into(),
                time: r.time_unix_nano,
                observed: r.observed_time_unix_nano,
                severity: r.severity_number,
                severity_text: r.severity_text.into(),
                event_name: r.event_name.into(),
                body: r.body.encoded().to_vec(),
                dropped: r.dropped_attributes_count,
                attributes: attrs,
                trace_flags: r.trace_flags,
                trace_id: *r.trace_id.unwrap(),
                span_id: *r.span_id.unwrap(),
            });
            Ok(())
        }
    }

    #[test]
    fn stamps_v1_as_v2_redacts_and_preserves_every_other_field() {
        let resource = [KeyValueInput {
            key: "api_key",
            value: AnyValueInput::Int64(1),
        }];
        let scope = [KeyValueInput {
            key: "safe",
            value: AnyValueInput::Bool(true),
        }];
        let attrs = [KeyValueInput {
            key: "password",
            value: AnyValueInput::String("bad"),
        }];
        let trace = [20; 16];
        let span = [21; 8];
        let record = input(&resource, &scope, &attrs, &trace, &span);
        let mut producer = Vec::new();
        encode_logs_batch_v2_into(&[record, record], DecodeLimits::default(), &mut producer)
            .unwrap();

        let mut scratch = LogsV2StampScratch::default();
        let stamped =
            redact_and_stamp_logs_v2(&producer, 22, DecodeLimits::default(), &mut scratch).unwrap();
        assert_eq!(
            u16::from_be_bytes([stamped[4], stamped[5]]),
            LOGS_RAW_VERSION_V2
        );
        let mut got = Collected::default();
        decode_logs_batch_v2_into(stamped, DecodeLimits::default(), &mut got).unwrap();
        assert_eq!(got.count, 2);
        assert_eq!(
            (got.raw_version, got.received),
            (LOGS_RAW_VERSION_V2, Some(22))
        );
        assert_eq!(
            got.fields.unwrap(),
            Fields {
                resource_schema_url: "resource/schema".into(),
                resource_dropped: 11,
                resource: r#"{"api_key":"[REDACTED]"}"#.into(),
                scope_name: "scope-name".into(),
                scope_version: "scope-version".into(),
                scope_dropped: 12,
                scope: r#"{"safe":true}"#.into(),
                scope_schema_url: "scope/schema".into(),
                time: 13,
                observed: 14,
                severity: 15,
                severity_text: "severity-text".into(),
                event_name: "event-name".into(),
                body: vec![5, 0, 0, 0, 2, 16, 17],
                dropped: 18,
                attributes: r#"{"password":"[REDACTED]"}"#.into(),
                trace_flags: 19,
                trace_id: trace,
                span_id: span,
            }
        );
    }

    #[test]
    fn v1_decode_compatibility_and_live_rejections() {
        let mut producer = Vec::new();
        encode_logs_batch_v2_into(&[], DecodeLimits::default(), &mut producer).unwrap();
        let mut got = Collected::default();
        decode_logs_batch_v2_into(&producer, DecodeLimits::default(), &mut got).unwrap();
        assert_eq!(got.raw_version, LOGS_RAW_VERSION_V1);

        let mut scratch = LogsV2StampScratch::default();
        assert_eq!(
            redact_and_stamp_logs_v2(&producer, 0, DecodeLimits::default(), &mut scratch),
            Err(LogsV2StampError::ZeroReceivedTime)
        );
        let stamped = redact_and_stamp_logs_v2(&producer, 1, DecodeLimits::default(), &mut scratch)
            .unwrap()
            .to_vec();
        assert_eq!(
            redact_and_stamp_logs_v2(&stamped, 2, DecodeLimits::default(), &mut scratch),
            Err(LogsV2StampError::AlreadyStamped(LOGS_RAW_VERSION_V2))
        );
    }

    #[test]
    fn replay_preserves_v2_receipt_and_unchanged_bytes() {
        let mut producer = Vec::new();
        encode_logs_batch_v2_into(&[], DecodeLimits::default(), &mut producer).unwrap();
        let mut scratch = LogsV2StampScratch::default();
        let stamped =
            redact_and_stamp_logs_v2(&producer, u64::MAX, DecodeLimits::default(), &mut scratch)
                .unwrap()
                .to_vec();
        let replayed =
            redact_replayed_logs_v2(&stamped, DecodeLimits::default(), &mut scratch).unwrap();
        assert!(matches!(replayed, Cow::Borrowed(_)));
        assert_eq!(replayed.as_ref(), stamped);
    }

    #[test]
    fn decoder_rejects_zero_v2_receipt() {
        let resource = [];
        let scope = [];
        let attrs = [];
        let trace = [1; 16];
        let span = [2; 8];
        let mut producer = Vec::new();
        encode_logs_batch_v2_into(
            &[input(&resource, &scope, &attrs, &trace, &span)],
            DecodeLimits::default(),
            &mut producer,
        )
        .unwrap();
        let mut scratch = LogsV2StampScratch::default();
        let mut stamped =
            redact_and_stamp_logs_v2(&producer, 1, DecodeLimits::default(), &mut scratch)
                .unwrap()
                .to_vec();
        // Locate the unique receipt value in this fixture and zero it.
        let position = stamped
            .windows(8)
            .position(|window| window == 1u64.to_be_bytes())
            .unwrap();
        stamped[position..position + 8].fill(0);
        assert_eq!(
            decode_logs_batch_v2_into(&stamped, DecodeLimits::default(), &mut Collected::default()),
            Err(DecodeError::ZeroReceivedTime)
        );
    }
}
