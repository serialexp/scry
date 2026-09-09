//! Bounded, allocation-free validation and borrowed decoding for canonical logs v2.
//!
//! `LogsBatchV2` is the big-endian envelope
//! `magic:u32, raw_version:u16, records:u32, (record_len:u32, record...)*`.
//! The canonical raw record is encoded, in order, as:
//!
//! ```text
//! canonical_flags:u16 (= 0),
//! resource_schema_url:utf8<u32>, resource_dropped_attributes_count:u32,
//! resource_attributes:map,
//! scope_present:u8,
//!   if present: scope_name:utf8<u32>, scope_version:utf8<u32>,
//!               scope_dropped_attributes_count:u32, scope_attributes:map,
//! scope_schema_url:utf8<u32>,
//! time_unix_nano:u64, observed_time_unix_nano:u64, severity_number:i32,
//! severity_text:utf8<u32>, event_name:utf8<u32>, body:AnyValue,
//! dropped_attributes_count:u32, attributes:map, trace_flags:u32,
//! trace_id_len:u8, trace_id:[u8; trace_id_len],
//! span_id_len:u8, span_id:[u8; span_id_len]
//! ```
//!
//! `scope_present` is exactly zero or one. ID lengths are exactly 0/16 and 0/8;
//! present IDs must not be all zero, and a span ID requires a trace ID. Thus
//! absent and present values (including an explicitly present empty scope) remain
//! distinguishable.
//!
//! `AnyValue` tags are null=0, string=1, bool=2, int64=3, double=4, bytes=5,
//! array=6, and map=7. Strings and bytes are `len:u32` followed by bytes. Arrays
//! are `count:u32` then values; maps are `count:u32` then `(key, value)` pairs.
//! Map keys must be nonempty and strictly increasing by UTF-8 bytes. Bool is exactly
//! zero or one. Integers, doubles, and lengths are big-endian. Negative zero and
//! all NaNs except `0x7ff8_0000_0000_0000` are noncanonical.
//!
//! Limits cover the complete payload, record size, aggregate variable-width bytes,
//! total AnyValue nodes, total map key/value pairs, each container, keys, and each
//! string/bytes value. Validation is two-pass: the complete envelope is validated
//! before any callback. Parsing creates only borrowed views and no per-value AST.

use crate::constants::{LOGS_BATCH_V2_MAGIC, LOGS_RAW_VERSION_V1};

pub const ANY_NULL: u8 = 0;
pub const ANY_STRING: u8 = 1;
pub const ANY_BOOL: u8 = 2;
pub const ANY_INT64: u8 = 3;
pub const ANY_DOUBLE: u8 = 4;
pub const ANY_BYTES: u8 = 5;
pub const ANY_ARRAY: u8 = 6;
pub const ANY_MAP: u8 = 7;
pub const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

#[derive(Debug, Clone, Copy)]
pub struct DecodeLimits {
    pub max_payload_bytes: usize,
    pub max_records: u32,
    pub max_record_bytes: usize,
    pub max_variable_bytes_per_record: usize,
    pub max_depth: u8,
    pub max_nodes_per_record: u32,
    pub max_key_value_pairs_per_record: u32,
    pub max_container_elements: u32,
    pub max_key_bytes: usize,
    pub max_string_bytes: usize,
    pub max_bytes_value: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 16 * 1024 * 1024,
            max_records: 65_536,
            max_record_bytes: 1024 * 1024,
            max_variable_bytes_per_record: 1024 * 1024,
            max_depth: 32,
            max_nodes_per_record: 65_536,
            max_key_value_pairs_per_record: 65_536,
            max_container_elements: 4096,
            max_key_bytes: 1024,
            max_string_bytes: 1024 * 1024,
            max_bytes_value: 1024 * 1024,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("logs-v2 payload exceeds configured limit")]
    PayloadLimit,
    #[error("logs-v2 record count exceeds configured limit")]
    RecordCountLimit,
    #[error("logs-v2 record exceeds configured limit")]
    RecordLimit,
    #[error("aggregate variable-width data exceeds configured limit")]
    VariableBytesLimit,
    #[error("truncated logs-v2 input")]
    Truncated,
    #[error("wrong logs-v2 magic: {0:#010x}")]
    BadMagic(u32),
    #[error("unsupported canonical raw log version: {0}")]
    UnsupportedVersion(u16),
    #[error("canonical record flags must be zero")]
    InvalidRecordFlags,
    #[error("scope presence byte must be zero or one")]
    InvalidScopePresence,
    #[error("trailing bytes after logs-v2 envelope or record")]
    TrailingBytes,
    #[error("invalid UTF-8 string")]
    InvalidUtf8,
    #[error("string exceeds configured limit")]
    StringLimit,
    #[error("map key exceeds configured limit")]
    KeyLimit,
    #[error("map key must not be empty")]
    EmptyMapKey,
    #[error("byte value exceeds configured limit")]
    BytesLimit,
    #[error("AnyValue nesting exceeds configured limit")]
    DepthLimit,
    #[error("AnyValue node count exceeds configured limit")]
    NodeLimit,
    #[error("map key/value pair count exceeds configured limit")]
    KeyValueLimit,
    #[error("container element count exceeds configured limit")]
    ContainerLimit,
    #[error("unknown AnyValue tag: {0}")]
    UnknownValueTag(u8),
    #[error("boolean byte must be zero or one")]
    InvalidBool,
    #[error("double is not in canonical form")]
    NonCanonicalDouble,
    #[error("map keys are duplicate or not in canonical byte order")]
    NonCanonicalMap,
    #[error("trace_id length must be zero or 16")]
    InvalidTraceIdLength,
    #[error("span_id length must be zero or 8")]
    InvalidSpanIdLength,
    #[error("a present trace_id must be nonzero")]
    ZeroTraceId,
    #[error("a present span_id must be nonzero")]
    ZeroSpanId,
    #[error("span_id is present without trace_id")]
    OrphanSpanId,
    #[error("appender rejected logs-v2 record: {0}")]
    Appender(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnyValueRef<'a> {
    encoded: &'a [u8],
}
impl<'a> AnyValueRef<'a> {
    pub fn encoded(self) -> &'a [u8] {
        self.encoded
    }
    pub fn tag(self) -> u8 {
        self.encoded[0]
    }

    /// Returns a typed, borrowed view of this already-validated value.
    ///
    /// Containers are lazy views over the canonical encoding; this does not
    /// allocate or construct a recursive value tree.
    pub fn value(self) -> AnyValue<'a> {
        decode_value(self.encoded)
    }

    /// Returns an iterator over this value when it is a top-level map.
    pub fn map_iter(self) -> Option<MapIter<'a>> {
        match self.value() {
            AnyValue::Map(map) => Some(map.iter()),
            _ => None,
        }
    }

    /// Appends deterministic canonical text to `output`.
    ///
    /// A string at the root is appended verbatim, which preserves the familiar
    /// log-body projection. Strings nested in arrays or maps (and map keys) use
    /// JSON quoting and escaping. Existing contents of `output` are retained.
    pub fn write_canonical_text(self, output: &mut String) {
        match self.value() {
            AnyValue::String(value) => output.push_str(value),
            value => write_nested_value(value, output),
        }
    }

    /// Appends a type-injective canonical representation to `output`.
    ///
    /// Unlike [`write_canonical_text`](Self::write_canonical_text), root strings
    /// are JSON-quoted too. Use this for labels and other identity-bearing
    /// projections so `"null"` cannot collide with the canonical null value.
    pub fn write_typed_canonical_text(self, output: &mut String) {
        write_nested_value(self.value(), output);
    }
}

/// A typed borrowed view into a validated canonical `AnyValue`.
///
/// Canonical text is `null`, `true`/`false`, signed decimal integers, and
/// deterministic decimal doubles (`.0` is retained for integral finite
/// doubles, with `NaN`, `Infinity`, and `-Infinity` for non-finite values).
/// Bytes are `hex"0123abcd"`; arrays are `[value,...]`; maps are
/// `{"key":value,...}`. Nested strings and map keys are JSON-escaped.
#[derive(Debug, Clone, Copy)]
pub enum AnyValue<'a> {
    Null,
    String(&'a str),
    Bool(bool),
    Int64(i64),
    Double(f64),
    Bytes(&'a [u8]),
    Array(ArrayRef<'a>),
    Map(MapRef<'a>),
}

#[derive(Debug, Clone, Copy)]
pub struct ArrayRef<'a> {
    encoded_values: &'a [u8],
    len: u32,
}

impl<'a> ArrayRef<'a> {
    pub fn len(self) -> u32 {
        self.len
    }

    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    pub fn iter(self) -> ArrayIter<'a> {
        ArrayIter {
            remaining: self.len,
            encoded: self.encoded_values,
        }
    }
}

impl<'a> IntoIterator for ArrayRef<'a> {
    type Item = AnyValueRef<'a>;
    type IntoIter = ArrayIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MapRef<'a> {
    encoded_entries: &'a [u8],
    len: u32,
}

impl<'a> MapRef<'a> {
    pub fn len(self) -> u32 {
        self.len
    }

    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    pub fn iter(self) -> MapIter<'a> {
        MapIter {
            remaining: self.len,
            encoded: self.encoded_entries,
        }
    }
}

impl<'a> IntoIterator for MapRef<'a> {
    type Item = (&'a str, AnyValueRef<'a>);
    type IntoIter = MapIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ArrayIter<'a> {
    remaining: u32,
    encoded: &'a [u8],
}

impl<'a> Iterator for ArrayIter<'a> {
    type Item = AnyValueRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let len = encoded_value_len(self.encoded);
        let (value, rest) = self.encoded.split_at(len);
        self.encoded = rest;
        self.remaining -= 1;
        Some(AnyValueRef { encoded: value })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

impl ExactSizeIterator for ArrayIter<'_> {}
impl std::iter::FusedIterator for ArrayIter<'_> {}

#[derive(Debug, Clone, Copy)]
pub struct MapIter<'a> {
    remaining: u32,
    encoded: &'a [u8],
}

impl<'a> Iterator for MapIter<'a> {
    type Item = (&'a str, AnyValueRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let key_len = read_u32(self.encoded) as usize;
        let key_end = 4 + key_len;
        let key = std::str::from_utf8(&self.encoded[4..key_end])
            .expect("AnyValueRef was UTF-8 validated");
        let value_len = encoded_value_len(&self.encoded[key_end..]);
        let value_end = key_end + value_len;
        let value = AnyValueRef {
            encoded: &self.encoded[key_end..value_end],
        };
        self.encoded = &self.encoded[value_end..];
        self.remaining -= 1;
        Some((key, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

impl ExactSizeIterator for MapIter<'_> {}
impl std::iter::FusedIterator for MapIter<'_> {}

fn read_u32(input: &[u8]) -> u32 {
    u32::from_be_bytes(input[..4].try_into().expect("validated u32"))
}

fn encoded_value_len(input: &[u8]) -> usize {
    match input[0] {
        ANY_NULL => 1,
        ANY_STRING | ANY_BYTES => 5 + read_u32(&input[1..]) as usize,
        ANY_BOOL => 2,
        ANY_INT64 | ANY_DOUBLE => 9,
        ANY_ARRAY => {
            let count = read_u32(&input[1..]);
            let mut offset = 5;
            for _ in 0..count {
                offset += encoded_value_len(&input[offset..]);
            }
            offset
        }
        ANY_MAP => {
            let count = read_u32(&input[1..]);
            let mut offset = 5;
            for _ in 0..count {
                let key_len = read_u32(&input[offset..]) as usize;
                offset += 4 + key_len;
                offset += encoded_value_len(&input[offset..]);
            }
            offset
        }
        _ => unreachable!("AnyValueRef has a validated tag"),
    }
}

fn decode_value(input: &[u8]) -> AnyValue<'_> {
    match input[0] {
        ANY_NULL => AnyValue::Null,
        ANY_STRING => {
            let len = read_u32(&input[1..]) as usize;
            AnyValue::String(
                std::str::from_utf8(&input[5..5 + len]).expect("AnyValueRef was UTF-8 validated"),
            )
        }
        ANY_BOOL => AnyValue::Bool(input[1] != 0),
        ANY_INT64 => AnyValue::Int64(i64::from_be_bytes(input[1..9].try_into().unwrap())),
        ANY_DOUBLE => AnyValue::Double(f64::from_bits(u64::from_be_bytes(
            input[1..9].try_into().unwrap(),
        ))),
        ANY_BYTES => {
            let len = read_u32(&input[1..]) as usize;
            AnyValue::Bytes(&input[5..5 + len])
        }
        ANY_ARRAY => AnyValue::Array(ArrayRef {
            len: read_u32(&input[1..]),
            encoded_values: &input[5..],
        }),
        ANY_MAP => AnyValue::Map(MapRef {
            len: read_u32(&input[1..]),
            encoded_entries: &input[5..],
        }),
        _ => unreachable!("AnyValueRef has a validated tag"),
    }
}

fn write_json_string(value: &str, output: &mut String) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{00}'..='\u{1f}' => {
                use std::fmt::Write;
                write!(output, "\\u{:04x}", character as u32).unwrap();
            }
            _ => output.push(character),
        }
    }
    output.push('"');
}

fn write_nested_value(value: AnyValue<'_>, output: &mut String) {
    use std::fmt::Write;

    match value {
        AnyValue::Null => output.push_str("null"),
        AnyValue::String(value) => write_json_string(value, output),
        AnyValue::Bool(value) => output.push_str(if value { "true" } else { "false" }),
        AnyValue::Int64(value) => write!(output, "{value}").unwrap(),
        AnyValue::Double(value) if value.is_nan() => output.push_str("NaN"),
        AnyValue::Double(value) if value == f64::INFINITY => output.push_str("Infinity"),
        AnyValue::Double(value) if value == f64::NEG_INFINITY => output.push_str("-Infinity"),
        AnyValue::Double(value) => {
            let start = output.len();
            write!(output, "{value}").unwrap();
            if !output[start..].contains(['.', 'e', 'E']) {
                output.push_str(".0");
            }
        }
        AnyValue::Bytes(value) => {
            output.push_str("hex\"");
            for byte in value {
                write!(output, "{byte:02x}").unwrap();
            }
            output.push('"');
        }
        AnyValue::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write_nested_value(value.value(), output);
            }
            output.push(']');
        }
        AnyValue::Map(values) => {
            output.push('{');
            for (index, (key, value)) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write_json_string(key, output);
                output.push(':');
                write_nested_value(value.value(), output);
            }
            output.push('}');
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalLogRecord<'a> {
    pub resource_schema_url: &'a str,
    pub resource_dropped_attributes_count: u32,
    pub resource_attributes: AnyValueRef<'a>,
    pub scope_name: Option<&'a str>,
    pub scope_version: Option<&'a str>,
    pub scope_dropped_attributes_count: Option<u32>,
    pub scope_attributes: Option<AnyValueRef<'a>>,
    pub scope_schema_url: &'a str,
    pub time_unix_nano: u64,
    pub observed_time_unix_nano: u64,
    pub severity_number: i32,
    pub severity_text: &'a str,
    pub event_name: &'a str,
    pub body: AnyValueRef<'a>,
    pub dropped_attributes_count: u32,
    pub attributes: AnyValueRef<'a>,
    pub trace_flags: u32,
    pub trace_id: Option<&'a [u8; 16]>,
    pub span_id: Option<&'a [u8; 8]>,
    pub encoded: &'a [u8],
}

pub trait LogsV2Appender {
    fn begin_batch(&mut self, _record_count: u32) -> Result<(), String> {
        Ok(())
    }
    fn record(&mut self, record: CanonicalLogRecord<'_>) -> Result<(), String>;
}

/// Validates the complete envelope before invoking the first callback, then makes
/// one borrowed callback per canonical record.
pub fn decode_logs_batch_v2_into(
    payload: &[u8],
    limits: DecodeLimits,
    appender: &mut impl LogsV2Appender,
) -> Result<u32, DecodeError> {
    if payload.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadLimit);
    }
    let mut first = Cursor::new(payload);
    validate_header(&mut first)?;
    let count = first.u32()?;
    if count > limits.max_records {
        return Err(DecodeError::RecordCountLimit);
    }
    for _ in 0..count {
        parse_record(length_delimited_record(&mut first, limits)?, limits)?;
    }
    if !first.done() {
        return Err(DecodeError::TrailingBytes);
    }

    appender.begin_batch(count).map_err(DecodeError::Appender)?;
    let mut second = Cursor::new(payload);
    validate_header(&mut second)?;
    debug_assert_eq!(second.u32()?, count);
    for _ in 0..count {
        let encoded = length_delimited_record(&mut second, limits)?;
        appender
            .record(parse_record(encoded, limits)?)
            .map_err(DecodeError::Appender)?;
    }
    debug_assert!(second.done());
    Ok(count)
}

/// Validates and borrows one raw canonical record (without an envelope).
pub fn validate_record(
    encoded: &[u8],
    limits: DecodeLimits,
) -> Result<CanonicalLogRecord<'_>, DecodeError> {
    if encoded.len() > limits.max_record_bytes {
        return Err(DecodeError::RecordLimit);
    }
    parse_record(encoded, limits)
}

fn validate_header(c: &mut Cursor<'_>) -> Result<(), DecodeError> {
    let magic = c.u32()?;
    if magic != LOGS_BATCH_V2_MAGIC {
        return Err(DecodeError::BadMagic(magic));
    }
    let version = c.u16()?;
    if version != LOGS_RAW_VERSION_V1 {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    Ok(())
}
fn length_delimited_record<'a>(
    c: &mut Cursor<'a>,
    limits: DecodeLimits,
) -> Result<&'a [u8], DecodeError> {
    let len = c.u32()? as usize;
    if len > limits.max_record_bytes {
        return Err(DecodeError::RecordLimit);
    }
    c.take(len)
}

#[derive(Default)]
struct RecordBudget {
    variable_bytes: usize,
    nodes: u32,
    key_values: u32,
}
impl RecordBudget {
    fn variable(&mut self, len: usize, limits: DecodeLimits) -> Result<(), DecodeError> {
        self.variable_bytes = self
            .variable_bytes
            .checked_add(len)
            .ok_or(DecodeError::VariableBytesLimit)?;
        if self.variable_bytes > limits.max_variable_bytes_per_record {
            return Err(DecodeError::VariableBytesLimit);
        }
        Ok(())
    }
    fn node(&mut self, limits: DecodeLimits) -> Result<(), DecodeError> {
        self.nodes = self.nodes.checked_add(1).ok_or(DecodeError::NodeLimit)?;
        if self.nodes > limits.max_nodes_per_record {
            return Err(DecodeError::NodeLimit);
        }
        Ok(())
    }
    fn key_values(&mut self, count: u32, limits: DecodeLimits) -> Result<(), DecodeError> {
        self.key_values = self
            .key_values
            .checked_add(count)
            .ok_or(DecodeError::KeyValueLimit)?;
        if self.key_values > limits.max_key_value_pairs_per_record {
            return Err(DecodeError::KeyValueLimit);
        }
        Ok(())
    }
}

fn parse_record(
    encoded: &[u8],
    limits: DecodeLimits,
) -> Result<CanonicalLogRecord<'_>, DecodeError> {
    let mut c = Cursor::new(encoded);
    let mut budget = RecordBudget::default();
    if c.u16()? != 0 {
        return Err(DecodeError::InvalidRecordFlags);
    }
    let resource_schema_url = c.value_string(limits.max_string_bytes, &mut budget, limits)?;
    let resource_dropped_attributes_count = c.u32()?;
    let resource_attributes = c.map_value(0, &mut budget, limits)?;
    let scope_present = match c.u8()? {
        0 => false,
        1 => true,
        _ => return Err(DecodeError::InvalidScopePresence),
    };
    let (scope_name, scope_version, scope_dropped_attributes_count, scope_attributes) =
        if scope_present {
            (
                Some(c.value_string(limits.max_string_bytes, &mut budget, limits)?),
                Some(c.value_string(limits.max_string_bytes, &mut budget, limits)?),
                Some(c.u32()?),
                Some(c.map_value(0, &mut budget, limits)?),
            )
        } else {
            (None, None, None, None)
        };
    let scope_schema_url = c.value_string(limits.max_string_bytes, &mut budget, limits)?;
    let time_unix_nano = c.u64()?;
    let observed_time_unix_nano = c.u64()?;
    let severity_number = c.i32()?;
    let severity_text = c.value_string(limits.max_string_bytes, &mut budget, limits)?;
    let event_name = c.value_string(limits.max_string_bytes, &mut budget, limits)?;
    let body = c.any_value(0, &mut budget, limits)?;
    let dropped_attributes_count = c.u32()?;
    let attributes = c.map_value(0, &mut budget, limits)?;
    let trace_flags = c.u32()?;
    let trace_id = match c.u8()? {
        0 => None,
        16 => {
            let id = c.array_16()?;
            if id.iter().all(|&b| b == 0) {
                return Err(DecodeError::ZeroTraceId);
            }
            Some(id)
        }
        _ => return Err(DecodeError::InvalidTraceIdLength),
    };
    let span_id = match c.u8()? {
        0 => None,
        8 => {
            let id = c.array_8()?;
            if id.iter().all(|&b| b == 0) {
                return Err(DecodeError::ZeroSpanId);
            }
            Some(id)
        }
        _ => return Err(DecodeError::InvalidSpanIdLength),
    };
    if trace_id.is_none() && span_id.is_some() {
        return Err(DecodeError::OrphanSpanId);
    }
    if !c.done() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok(CanonicalLogRecord {
        resource_schema_url,
        resource_dropped_attributes_count,
        resource_attributes,
        scope_name,
        scope_version,
        scope_dropped_attributes_count,
        scope_attributes,
        scope_schema_url,
        time_unix_nano,
        observed_time_unix_nano,
        severity_number,
        severity_text,
        event_name,
        body,
        dropped_attributes_count,
        attributes,
        trace_flags,
        trace_id,
        span_id,
        encoded,
    })
}

#[derive(Clone, Copy)]
struct Cursor<'a> {
    input: &'a [u8],
    position: usize,
}
impl<'a> Cursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }
    fn done(self) -> bool {
        self.position == self.input.len()
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(DecodeError::Truncated)?;
        let result = self
            .input
            .get(self.position..end)
            .ok_or(DecodeError::Truncated)?;
        self.position = end;
        Ok(result)
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn array_8(&mut self) -> Result<&'a [u8; 8], DecodeError> {
        self.take(8)?.try_into().map_err(|_| DecodeError::Truncated)
    }
    fn array_16(&mut self) -> Result<&'a [u8; 16], DecodeError> {
        self.take(16)?
            .try_into()
            .map_err(|_| DecodeError::Truncated)
    }
    fn raw_string(
        &mut self,
        max: usize,
        too_long: DecodeError,
        budget: &mut RecordBudget,
        limits: DecodeLimits,
    ) -> Result<&'a str, DecodeError> {
        let len = self.u32()? as usize;
        if len > max {
            return Err(too_long);
        }
        let bytes = self.take(len)?;
        let value = std::str::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)?;
        budget.variable(len, limits)?;
        Ok(value)
    }
    fn value_string(
        &mut self,
        max: usize,
        budget: &mut RecordBudget,
        limits: DecodeLimits,
    ) -> Result<&'a str, DecodeError> {
        self.raw_string(max, DecodeError::StringLimit, budget, limits)
    }
    fn any_value(
        &mut self,
        depth: u8,
        budget: &mut RecordBudget,
        limits: DecodeLimits,
    ) -> Result<AnyValueRef<'a>, DecodeError> {
        if depth > limits.max_depth {
            return Err(DecodeError::DepthLimit);
        }
        budget.node(limits)?;
        let start = self.position;
        match self.u8()? {
            ANY_NULL => {}
            ANY_STRING => {
                self.value_string(limits.max_string_bytes, budget, limits)?;
            }
            ANY_BOOL => {
                if self.u8()? > 1 {
                    return Err(DecodeError::InvalidBool);
                }
            }
            ANY_INT64 => {
                self.take(8)?;
            }
            ANY_DOUBLE => {
                let bits = self.u64()?;
                if bits == (-0.0f64).to_bits()
                    || (f64::from_bits(bits).is_nan() && bits != CANONICAL_NAN_BITS)
                {
                    return Err(DecodeError::NonCanonicalDouble);
                }
            }
            ANY_BYTES => {
                let len = self.u32()? as usize;
                if len > limits.max_bytes_value {
                    return Err(DecodeError::BytesLimit);
                }
                self.take(len)?;
                budget.variable(len, limits)?;
            }
            ANY_ARRAY => self.array(depth, budget, limits)?,
            ANY_MAP => self.map(depth, budget, limits)?,
            other => return Err(DecodeError::UnknownValueTag(other)),
        }
        Ok(AnyValueRef {
            encoded: &self.input[start..self.position],
        })
    }
    fn map_value(
        &mut self,
        depth: u8,
        budget: &mut RecordBudget,
        limits: DecodeLimits,
    ) -> Result<AnyValueRef<'a>, DecodeError> {
        if depth > limits.max_depth {
            return Err(DecodeError::DepthLimit);
        }
        budget.node(limits)?;
        let start = self.position;
        let tag = self.u8()?;
        if tag != ANY_MAP {
            return Err(DecodeError::UnknownValueTag(tag));
        }
        self.map(depth, budget, limits)?;
        Ok(AnyValueRef {
            encoded: &self.input[start..self.position],
        })
    }
    fn array(
        &mut self,
        depth: u8,
        budget: &mut RecordBudget,
        limits: DecodeLimits,
    ) -> Result<(), DecodeError> {
        if depth >= limits.max_depth {
            return Err(DecodeError::DepthLimit);
        }
        let count = self.u32()?;
        if count > limits.max_container_elements {
            return Err(DecodeError::ContainerLimit);
        }
        for _ in 0..count {
            self.any_value(depth + 1, budget, limits)?;
        }
        Ok(())
    }
    fn map(
        &mut self,
        depth: u8,
        budget: &mut RecordBudget,
        limits: DecodeLimits,
    ) -> Result<(), DecodeError> {
        if depth >= limits.max_depth {
            return Err(DecodeError::DepthLimit);
        }
        let count = self.u32()?;
        if count > limits.max_container_elements {
            return Err(DecodeError::ContainerLimit);
        }
        budget.key_values(count, limits)?;
        let mut previous: Option<&[u8]> = None;
        for _ in 0..count {
            let key =
                self.raw_string(limits.max_key_bytes, DecodeError::KeyLimit, budget, limits)?;
            if key.is_empty() {
                return Err(DecodeError::EmptyMapKey);
            }
            if previous.is_some_and(|old| old >= key.as_bytes()) {
                return Err(DecodeError::NonCanonicalMap);
            }
            previous = Some(key.as_bytes());
            self.any_value(depth + 1, budget, limits)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(out: &mut Vec<u8>, value: &[u8]) {
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
    }
    fn null(out: &mut Vec<u8>) {
        out.push(ANY_NULL);
    }
    fn empty_map(out: &mut Vec<u8>) {
        out.push(ANY_MAP);
        out.extend_from_slice(&0u32.to_be_bytes());
    }
    fn record(
        scope: bool,
        trace: Option<[u8; 16]>,
        span: Option<[u8; 8]>,
        body: impl FnOnce(&mut Vec<u8>),
        attrs: impl FnOnce(&mut Vec<u8>),
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&0u16.to_be_bytes());
        string(&mut out, b"resource/v1");
        out.extend_from_slice(&3u32.to_be_bytes());
        empty_map(&mut out);
        out.push(u8::from(scope));
        if scope {
            string(&mut out, b"scope");
            string(&mut out, b"1.0");
            out.extend_from_slice(&4u32.to_be_bytes());
            empty_map(&mut out);
        }
        string(&mut out, b"scope/v1");
        out.extend_from_slice(&11u64.to_be_bytes());
        out.extend_from_slice(&12u64.to_be_bytes());
        out.extend_from_slice(&(-17i32).to_be_bytes());
        string(&mut out, b"ERROR");
        string(&mut out, b"exception");
        body(&mut out);
        out.extend_from_slice(&2u32.to_be_bytes());
        attrs(&mut out);
        out.extend_from_slice(&1u32.to_be_bytes());
        match trace {
            Some(id) => {
                out.push(16);
                out.extend_from_slice(&id);
            }
            None => out.push(0),
        }
        match span {
            Some(id) => {
                out.push(8);
                out.extend_from_slice(&id);
            }
            None => out.push(0),
        }
        out
    }
    fn standard(body: impl FnOnce(&mut Vec<u8>), attrs: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        record(true, Some([1; 16]), Some([2; 8]), body, attrs)
    }
    fn envelope(records: &[Vec<u8>]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&LOGS_BATCH_V2_MAGIC.to_be_bytes());
        out.extend_from_slice(&LOGS_RAW_VERSION_V1.to_be_bytes());
        out.extend_from_slice(&(records.len() as u32).to_be_bytes());
        for r in records {
            out.extend_from_slice(&(r.len() as u32).to_be_bytes());
            out.extend_from_slice(r);
        }
        out
    }
    #[derive(Default)]
    struct Sink {
        begun: bool,
        records: usize,
    }
    impl LogsV2Appender for Sink {
        fn begin_batch(&mut self, _: u32) -> Result<(), String> {
            self.begun = true;
            Ok(())
        }
        fn record(&mut self, r: CanonicalLogRecord<'_>) -> Result<(), String> {
            self.records += 1;
            assert_eq!(r.severity_number, -17);
            assert_eq!(r.body.tag(), ANY_NULL);
            Ok(())
        }
    }

    #[test]
    fn decodes_authoritative_order_and_signed_severity() {
        let raw = standard(null, empty_map);
        let mut sink = Sink::default();
        assert_eq!(
            decode_logs_batch_v2_into(&envelope(&[raw]), DecodeLimits::default(), &mut sink),
            Ok(1)
        );
        assert!(sink.begun);
        assert_eq!(sink.records, 1);
    }

    #[test]
    fn preserves_absent_and_present_scope_and_ids() {
        let absent = record(false, None, None, null, empty_map);
        let r = validate_record(&absent, DecodeLimits::default()).unwrap();
        assert_eq!(
            (r.scope_name, r.scope_attributes, r.trace_id, r.span_id),
            (None, None, None, None)
        );
        let present = standard(null, empty_map);
        let r = validate_record(&present, DecodeLimits::default()).unwrap();
        assert_eq!(r.scope_name, Some("scope"));
        assert!(r.scope_attributes.is_some());
        assert!(r.trace_id.is_some() && r.span_id.is_some());
    }

    #[test]
    fn validates_all_any_values_and_canonical_maps() {
        let raw = standard(
            |o| {
                o.push(ANY_ARRAY);
                o.extend_from_slice(&7u32.to_be_bytes());
                null(o);
                o.push(ANY_STRING);
                string(o, b"");
                o.extend_from_slice(&[ANY_BOOL, 1, ANY_INT64]);
                o.extend_from_slice(&i64::MIN.to_be_bytes());
                o.push(ANY_DOUBLE);
                o.extend_from_slice(&CANONICAL_NAN_BITS.to_be_bytes());
                o.push(ANY_BYTES);
                o.extend_from_slice(&2u32.to_be_bytes());
                o.extend_from_slice(&[1, 2]);
                empty_map(o);
            },
            |o| {
                o.push(ANY_MAP);
                o.extend_from_slice(&2u32.to_be_bytes());
                string(o, b"a");
                null(o);
                string(o, b"z");
                null(o);
            },
        );
        assert!(validate_record(&raw, DecodeLimits::default()).is_ok());
    }

    #[test]
    fn rejects_presence_flags_and_ids() {
        let mut bad_flags = standard(null, empty_map);
        bad_flags[1] = 1;
        assert_eq!(
            validate_record(&bad_flags, DecodeLimits::default()),
            Err(DecodeError::InvalidRecordFlags)
        );
        let mut bad_scope = standard(null, empty_map);
        let scope_offset = 2 + 4 + "resource/v1".len() + 4 + 5;
        bad_scope[scope_offset] = 2;
        assert_eq!(
            validate_record(&bad_scope, DecodeLimits::default()),
            Err(DecodeError::InvalidScopePresence)
        );
        assert_eq!(
            validate_record(
                &record(false, None, Some([2; 8]), null, empty_map),
                DecodeLimits::default()
            ),
            Err(DecodeError::OrphanSpanId)
        );
        assert_eq!(
            validate_record(
                &record(false, Some([0; 16]), None, null, empty_map),
                DecodeLimits::default()
            ),
            Err(DecodeError::ZeroTraceId)
        );
        assert_eq!(
            validate_record(
                &record(false, Some([1; 16]), Some([0; 8]), null, empty_map),
                DecodeLimits::default()
            ),
            Err(DecodeError::ZeroSpanId)
        );
        let mut invalid_len = record(false, None, None, null, empty_map);
        let n = invalid_len.len();
        invalid_len[n - 2] = 15;
        assert_eq!(
            validate_record(&invalid_len, DecodeLimits::default()),
            Err(DecodeError::InvalidTraceIdLength)
        );
    }

    #[test]
    fn first_pass_prevents_callbacks() {
        let good = standard(null, empty_map);
        let mut bad = standard(null, empty_map);
        bad.push(0);
        let mut sink = Sink::default();
        assert_eq!(
            decode_logs_batch_v2_into(&envelope(&[good, bad]), DecodeLimits::default(), &mut sink),
            Err(DecodeError::TrailingBytes)
        );
        assert!(!sink.begun);
        assert_eq!(sink.records, 0);
    }

    #[test]
    fn enforces_aggregate_node_kv_container_key_and_value_limits() {
        let keyed = standard(null, |o| {
            o.push(ANY_MAP);
            o.extend_from_slice(&1u32.to_be_bytes());
            string(o, b"key");
            null(o);
        });
        assert_eq!(
            validate_record(
                &keyed,
                DecodeLimits {
                    max_key_bytes: 2,
                    ..DecodeLimits::default()
                }
            ),
            Err(DecodeError::KeyLimit)
        );
        assert_eq!(
            validate_record(
                &keyed,
                DecodeLimits {
                    max_key_value_pairs_per_record: 0,
                    ..DecodeLimits::default()
                }
            ),
            Err(DecodeError::KeyValueLimit)
        );
        assert_eq!(
            validate_record(
                &keyed,
                DecodeLimits {
                    max_nodes_per_record: 3,
                    ..DecodeLimits::default()
                }
            ),
            Err(DecodeError::NodeLimit)
        );
        assert_eq!(
            validate_record(
                &keyed,
                DecodeLimits {
                    max_variable_bytes_per_record: 1,
                    ..DecodeLimits::default()
                }
            ),
            Err(DecodeError::VariableBytesLimit)
        );
        assert_eq!(
            validate_record(
                &keyed,
                DecodeLimits {
                    max_container_elements: 0,
                    ..DecodeLimits::default()
                }
            ),
            Err(DecodeError::ContainerLimit)
        );
        let empty_key = standard(null, |o| {
            o.push(ANY_MAP);
            o.extend_from_slice(&1u32.to_be_bytes());
            string(o, b"");
            null(o);
        });
        assert_eq!(
            validate_record(&empty_key, DecodeLimits::default()),
            Err(DecodeError::EmptyMapKey)
        );
    }

    #[test]
    fn rejects_noncanonical_values_truncation_and_envelope_errors() {
        let bad_bool = standard(|o| o.extend_from_slice(&[ANY_BOOL, 2]), empty_map);
        assert_eq!(
            validate_record(&bad_bool, DecodeLimits::default()),
            Err(DecodeError::InvalidBool)
        );
        let bad_map = standard(null, |o| {
            o.push(ANY_MAP);
            o.extend_from_slice(&2u32.to_be_bytes());
            string(o, b"b");
            null(o);
            string(o, b"a");
            null(o);
        });
        assert_eq!(
            validate_record(&bad_map, DecodeLimits::default()),
            Err(DecodeError::NonCanonicalMap)
        );
        let mut truncated = standard(null, empty_map);
        truncated.pop();
        assert_eq!(
            validate_record(&truncated, DecodeLimits::default()),
            Err(DecodeError::Truncated)
        );
        let mut payload = envelope(&[]);
        payload.push(0);
        assert_eq!(
            decode_logs_batch_v2_into(&payload, DecodeLimits::default(), &mut Sink::default()),
            Err(DecodeError::TrailingBytes)
        );
    }

    fn any_string(out: &mut Vec<u8>, value: &str) {
        out.push(ANY_STRING);
        string(out, value.as_bytes());
    }

    fn map_entry(out: &mut Vec<u8>, key: &str, value: impl FnOnce(&mut Vec<u8>)) {
        string(out, key.as_bytes());
        value(out);
    }

    #[test]
    fn borrowed_typed_values_and_top_level_map_iteration() {
        let raw = standard(null, |out| {
            out.push(ANY_MAP);
            out.extend_from_slice(&3u32.to_be_bytes());
            map_entry(out, "a", |out| {
                out.push(ANY_INT64);
                out.extend_from_slice(&(-42i64).to_be_bytes());
            });
            map_entry(out, "nested", |out| {
                out.push(ANY_ARRAY);
                out.extend_from_slice(&2u32.to_be_bytes());
                out.extend_from_slice(&[ANY_BOOL, 1]);
                any_string(out, "borrowed");
            });
            map_entry(out, "z", null);
        });
        let record = validate_record(&raw, DecodeLimits::default()).unwrap();
        let mut entries = record.attributes.map_iter().unwrap();
        assert_eq!(entries.len(), 3);

        let (key, value) = entries.next().unwrap();
        assert_eq!(key, "a");
        assert!(matches!(value.value(), AnyValue::Int64(-42)));

        let (key, value) = entries.next().unwrap();
        assert_eq!(key, "nested");
        let AnyValue::Array(array) = value.value() else {
            panic!("expected array")
        };
        assert_eq!(array.len(), 2);
        let mut values = array.iter();
        assert!(matches!(
            values.next().unwrap().value(),
            AnyValue::Bool(true)
        ));
        assert!(matches!(
            values.next().unwrap().value(),
            AnyValue::String("borrowed")
        ));
        assert!(values.next().is_none());
        assert_eq!(entries.next().unwrap().0, "z");
        assert!(entries.next().is_none());
        assert!(record.body.map_iter().is_none());
    }

    #[test]
    fn canonical_text_preserves_root_string_and_escapes_nested_strings() {
        let text = "exact body: \\\"line\\n\t雪";
        let raw = standard(|out| any_string(out, text), empty_map);
        let record = validate_record(&raw, DecodeLimits::default()).unwrap();
        let mut output = String::from("prefix|");
        record.body.write_canonical_text(&mut output);
        assert_eq!(output, format!("prefix|{text}"));
        output.clear();
        record.body.write_typed_canonical_text(&mut output);
        assert_eq!(output, r#""exact body: \\\"line\\n\t雪""#);

        let raw = standard(
            |out| {
                out.push(ANY_ARRAY);
                out.extend_from_slice(&2u32.to_be_bytes());
                any_string(out, "quote:\" slash:\\ newline:\n tab:\t nul:\0 雪");
                out.push(ANY_MAP);
                out.extend_from_slice(&1u32.to_be_bytes());
                map_entry(out, "k\n\"", |out| any_string(out, "v\r"));
            },
            empty_map,
        );
        let record = validate_record(&raw, DecodeLimits::default()).unwrap();
        let mut output = String::new();
        record.body.write_canonical_text(&mut output);
        assert_eq!(
            output,
            "[\"quote:\\\" slash:\\\\ newline:\\n tab:\\t nul:\\u0000 雪\",{\"k\\n\\\"\":\"v\\r\"}]"
        );
    }

    #[test]
    fn canonical_text_renders_every_scalar_type_unambiguously() {
        let raw = standard(
            |out| {
                out.push(ANY_ARRAY);
                out.extend_from_slice(&10u32.to_be_bytes());
                null(out);
                out.extend_from_slice(&[ANY_BOOL, 0, ANY_BOOL, 1]);
                out.push(ANY_INT64);
                out.extend_from_slice(&i64::MIN.to_be_bytes());
                for value in [1.0, 1.25, f64::INFINITY, f64::NEG_INFINITY] {
                    out.push(ANY_DOUBLE);
                    out.extend_from_slice(&value.to_bits().to_be_bytes());
                }
                out.push(ANY_DOUBLE);
                out.extend_from_slice(&CANONICAL_NAN_BITS.to_be_bytes());
                out.push(ANY_BYTES);
                out.extend_from_slice(&3u32.to_be_bytes());
                out.extend_from_slice(&[0, 0xab, 0xff]);
            },
            empty_map,
        );
        let record = validate_record(&raw, DecodeLimits::default()).unwrap();
        let mut output = String::new();
        record.body.write_canonical_text(&mut output);
        assert_eq!(
            output,
            "[null,false,true,-9223372036854775808,1.0,1.25,Infinity,-Infinity,NaN,hex\"00abff\"]"
        );
    }

    #[test]
    fn canonical_map_text_retains_wire_sorted_order_and_empty_containers() {
        let raw = standard(
            |out| {
                out.push(ANY_MAP);
                out.extend_from_slice(&3u32.to_be_bytes());
                map_entry(out, "array", |out| {
                    out.push(ANY_ARRAY);
                    out.extend_from_slice(&0u32.to_be_bytes());
                });
                map_entry(out, "bytes", |out| {
                    out.push(ANY_BYTES);
                    out.extend_from_slice(&0u32.to_be_bytes());
                });
                map_entry(out, "map", empty_map);
            },
            empty_map,
        );
        let record = validate_record(&raw, DecodeLimits::default()).unwrap();
        let mut output = String::new();
        record.body.write_canonical_text(&mut output);
        assert_eq!(output, r#"{"array":[],"bytes":hex"","map":{}}"#);
    }

    #[test]
    fn defaults_match_protocol_caps() {
        let l = DecodeLimits::default();
        assert_eq!(l.max_payload_bytes, 16 << 20);
        assert_eq!(l.max_records, 65_536);
        assert_eq!(l.max_record_bytes, 1 << 20);
        assert_eq!(l.max_variable_bytes_per_record, 1 << 20);
        assert_eq!(l.max_depth, 32);
        assert_eq!(l.max_nodes_per_record, 65_536);
        assert_eq!(l.max_key_value_pairs_per_record, 65_536);
        assert_eq!(l.max_container_elements, 4096);
        assert_eq!(l.max_key_bytes, 1024);
        assert_eq!(l.max_string_bytes, 1 << 20);
        assert_eq!(l.max_bytes_value, 1 << 20);
    }
}
