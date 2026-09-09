//! Bounded canonical logs-v2 producer encoding.
//!
//! Inputs borrow producer-owned data and are deliberately independent of OTLP
//! types. The convenient slice-based input is complemented by a generic adapter,
//! allowing another crate to traverse a foreign recursive tree without wrappers
//! or allocations. All public encoders are transactional: on an error the
//! caller's `Vec` is restored to its original length.

use crate::constants::{LOGS_BATCH_V2_MAGIC, LOGS_RAW_VERSION_V1};
use crate::streaming_logs_v2::{
    DecodeLimits, ANY_ARRAY, ANY_BOOL, ANY_BYTES, ANY_DOUBLE, ANY_INT64, ANY_MAP, ANY_NULL,
    ANY_STRING, CANONICAL_NAN_BITS,
};

#[derive(Debug, Clone, Copy)]
pub enum AnyValueInput<'a> {
    Null,
    String(&'a str),
    Bool(bool),
    Int64(i64),
    Double(f64),
    Bytes(&'a [u8]),
    Array(&'a [AnyValueInput<'a>]),
    Map(&'a [KeyValueInput<'a>]),
}

#[derive(Debug, Clone, Copy)]
pub struct KeyValueInput<'a> {
    pub key: &'a str,
    pub value: AnyValueInput<'a>,
}

#[derive(Debug, Clone, Copy)]
pub struct ScopeInput<'a> {
    pub name: &'a str,
    pub version: &'a str,
    pub dropped_attributes_count: u32,
    pub attributes: &'a [KeyValueInput<'a>],
}

#[derive(Debug, Clone, Copy)]
pub struct LogRecordInput<'a> {
    pub resource_schema_url: &'a str,
    pub resource_dropped_attributes_count: u32,
    pub resource_attributes: &'a [KeyValueInput<'a>],
    pub scope: Option<ScopeInput<'a>>,
    pub scope_schema_url: &'a str,
    pub time_unix_nano: u64,
    pub observed_time_unix_nano: u64,
    pub severity: u8,
    pub severity_text: &'a str,
    pub event_name: &'a str,
    pub body: AnyValueInput<'a>,
    pub dropped_attributes_count: u32,
    pub attributes: &'a [KeyValueInput<'a>],
    pub trace_flags: u8,
    pub trace_id: Option<&'a [u8; 16]>,
    pub span_id: Option<&'a [u8; 8]>,
}

/// A borrowed view returned by [`AnyValueAdapter`]. Container variants hold a
/// cheap copied handle rather than a reference to a wrapper object.
pub enum BorrowedAnyValue<'a, H> {
    Null,
    String(&'a str),
    Bool(bool),
    Int64(i64),
    Double(f64),
    Bytes(&'a [u8]),
    Array(H),
    Map(H),
}

/// Allocation-free traversal of an application's recursive value tree.
///
/// An external crate implements this trait for its local adapter, while `Handle`
/// may be `&ExternalAnyValue`. Foreign tree nodes implement no scry traits.
pub trait AnyValueAdapter<'a> {
    type Handle: Copy;

    fn value(&self, handle: Self::Handle) -> BorrowedAnyValue<'a, Self::Handle>;
    fn array_len(&self, array: Self::Handle) -> usize;
    fn array_value(&self, array: Self::Handle, index: usize) -> Option<Self::Handle>;
    fn map_len(&self, map: Self::Handle) -> usize;
    fn map_key(&self, map: Self::Handle, index: usize) -> Option<&'a str>;
    fn map_value(&self, map: Self::Handle, index: usize) -> Option<Self::Handle>;
}

/// Reusable, depth-indexed storage used to canonicalize source maps.
#[derive(Debug, Default)]
pub struct EncodeScratch {
    map_indices: Vec<Vec<usize>>,
}

pub struct SourceScopeInput<'a, A: AnyValueAdapter<'a> + ?Sized> {
    pub name: &'a str,
    pub version: &'a str,
    pub dropped_attributes_count: u32,
    pub attributes: A::Handle,
}

impl<'a, A: AnyValueAdapter<'a> + ?Sized> Clone for SourceScopeInput<'a, A> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'a, A: AnyValueAdapter<'a> + ?Sized> Copy for SourceScopeInput<'a, A> {}

pub struct SourceLogRecordInput<'a, A: AnyValueAdapter<'a> + ?Sized> {
    pub adapter: &'a A,
    pub resource_schema_url: &'a str,
    pub resource_dropped_attributes_count: u32,
    pub resource_attributes: A::Handle,
    pub scope: Option<SourceScopeInput<'a, A>>,
    pub scope_schema_url: &'a str,
    pub time_unix_nano: u64,
    pub observed_time_unix_nano: u64,
    pub severity: u8,
    pub severity_text: &'a str,
    pub event_name: &'a str,
    pub body: A::Handle,
    pub dropped_attributes_count: u32,
    pub attributes: A::Handle,
    pub trace_flags: u8,
    pub trace_id: Option<&'a [u8; 16]>,
    pub span_id: Option<&'a [u8; 8]>,
}

impl<'a, A: AnyValueAdapter<'a> + ?Sized> Clone for SourceLogRecordInput<'a, A> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'a, A: AnyValueAdapter<'a> + ?Sized> Copy for SourceLogRecordInput<'a, A> {}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EncodeError {
    #[error("logs-v2 payload exceeds configured limit")]
    PayloadLimit,
    #[error("logs-v2 record count exceeds configured limit")]
    RecordCountLimit,
    #[error("logs-v2 record exceeds configured limit")]
    RecordLimit,
    #[error("aggregate variable-width data exceeds configured limit")]
    VariableBytesLimit,
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
    #[error("map keys are duplicate or not in canonical byte order")]
    NonCanonicalMap,
    #[error("a length or count cannot be represented by the wire format")]
    LengthOverflow,
    #[error("at least one record timestamp must be nonzero")]
    MissingTimestamp,
    #[error("a present trace_id must be nonzero")]
    ZeroTraceId,
    #[error("a present span_id must be nonzero")]
    ZeroSpanId,
    #[error("span_id is present without trace_id")]
    OrphanSpanId,
    #[error("value source did not provide an entry below its reported length")]
    InvalidSource,
}

/// Append one raw canonical record (without its envelope or length prefix).
pub fn encode_log_record_v2_into(
    record: &LogRecordInput<'_>,
    limits: DecodeLimits,
    output: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let original = output.len();
    let result = encode_record(record, limits, output);
    if result.is_err() {
        output.truncate(original);
    }
    result
}

/// Encode a record directly from an application's borrowed recursive value tree.
pub fn encode_log_record_v2_from_source_into<'a, A: AnyValueAdapter<'a> + ?Sized>(
    record: &SourceLogRecordInput<'a, A>,
    limits: DecodeLimits,
    scratch: &mut EncodeScratch,
    output: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let original = output.len();
    let result = encode_source_record(record, limits, scratch, output);
    if result.is_err() {
        output.truncate(original);
    }
    result
}

/// Append a complete envelope directly from borrowed source trees.
pub fn encode_logs_batch_v2_from_sources_into<'a, A: AnyValueAdapter<'a> + ?Sized>(
    records: &[SourceLogRecordInput<'a, A>],
    limits: DecodeLimits,
    scratch: &mut EncodeScratch,
    output: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let original = output.len();
    let result = (|| {
        let count = u32::try_from(records.len()).map_err(|_| EncodeError::LengthOverflow)?;
        if count > limits.max_records {
            return Err(EncodeError::RecordCountLimit);
        }
        append_payload(output, &LOGS_BATCH_V2_MAGIC.to_be_bytes(), original, limits)?;
        append_payload(output, &LOGS_RAW_VERSION_V1.to_be_bytes(), original, limits)?;
        append_payload(output, &count.to_be_bytes(), original, limits)?;
        for record in records {
            let prefix = output.len();
            append_payload(output, &[0; 4], original, limits)?;
            let start = output.len();
            encode_source_record(record, limits, scratch, output)?;
            let wire_len =
                u32::try_from(output.len() - start).map_err(|_| EncodeError::LengthOverflow)?;
            output[prefix..prefix + 4].copy_from_slice(&wire_len.to_be_bytes());
            if output.len() - original > limits.max_payload_bytes {
                return Err(EncodeError::PayloadLimit);
            }
        }
        Ok(())
    })();
    if result.is_err() {
        output.truncate(original);
    }
    result
}

/// Append a complete canonical logs-v2 envelope.
pub fn encode_logs_batch_v2_into(
    records: &[LogRecordInput<'_>],
    limits: DecodeLimits,
    output: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let original = output.len();
    let result = (|| {
        let count = u32::try_from(records.len()).map_err(|_| EncodeError::LengthOverflow)?;
        if count > limits.max_records {
            return Err(EncodeError::RecordCountLimit);
        }
        append_payload(output, &LOGS_BATCH_V2_MAGIC.to_be_bytes(), original, limits)?;
        append_payload(output, &LOGS_RAW_VERSION_V1.to_be_bytes(), original, limits)?;
        append_payload(output, &count.to_be_bytes(), original, limits)?;
        for record in records {
            let prefix = output.len();
            append_payload(output, &[0; 4], original, limits)?;
            let start = output.len();
            encode_record(record, limits, output)?;
            let len = output.len() - start;
            let wire_len = u32::try_from(len).map_err(|_| EncodeError::LengthOverflow)?;
            output[prefix..prefix + 4].copy_from_slice(&wire_len.to_be_bytes());
            if output.len() - original > limits.max_payload_bytes {
                return Err(EncodeError::PayloadLimit);
            }
        }
        Ok(())
    })();
    if result.is_err() {
        output.truncate(original);
    }
    result
}

fn append_payload(
    out: &mut Vec<u8>,
    bytes: &[u8],
    start: usize,
    limits: DecodeLimits,
) -> Result<(), EncodeError> {
    let next = out
        .len()
        .checked_sub(start)
        .and_then(|n| n.checked_add(bytes.len()))
        .ok_or(EncodeError::PayloadLimit)?;
    if next > limits.max_payload_bytes {
        return Err(EncodeError::PayloadLimit);
    }
    out.extend_from_slice(bytes);
    Ok(())
}

#[derive(Default)]
struct Budget {
    variable: usize,
    nodes: u32,
    pairs: u32,
}

struct RecordWriter<'a> {
    out: &'a mut Vec<u8>,
    start: usize,
    limits: DecodeLimits,
    budget: Budget,
}

impl RecordWriter<'_> {
    fn bytes(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        let size = self
            .out
            .len()
            .checked_sub(self.start)
            .and_then(|n| n.checked_add(bytes.len()))
            .ok_or(EncodeError::RecordLimit)?;
        if size > self.limits.max_record_bytes {
            return Err(EncodeError::RecordLimit);
        }
        self.out.extend_from_slice(bytes);
        Ok(())
    }
    fn u8(&mut self, value: u8) -> Result<(), EncodeError> {
        self.bytes(&[value])
    }
    fn u16(&mut self, value: u16) -> Result<(), EncodeError> {
        self.bytes(&value.to_be_bytes())
    }
    fn u32(&mut self, value: u32) -> Result<(), EncodeError> {
        self.bytes(&value.to_be_bytes())
    }
    fn u64(&mut self, value: u64) -> Result<(), EncodeError> {
        self.bytes(&value.to_be_bytes())
    }
    fn variable(&mut self, len: usize) -> Result<(), EncodeError> {
        self.budget.variable = self
            .budget
            .variable
            .checked_add(len)
            .ok_or(EncodeError::VariableBytesLimit)?;
        if self.budget.variable > self.limits.max_variable_bytes_per_record {
            return Err(EncodeError::VariableBytesLimit);
        }
        Ok(())
    }
    fn string(&mut self, value: &str, key: bool) -> Result<(), EncodeError> {
        let max = if key {
            self.limits.max_key_bytes
        } else {
            self.limits.max_string_bytes
        };
        if value.len() > max {
            return Err(if key {
                EncodeError::KeyLimit
            } else {
                EncodeError::StringLimit
            });
        }
        let len = u32::try_from(value.len()).map_err(|_| EncodeError::LengthOverflow)?;
        self.variable(value.len())?;
        self.u32(len)?;
        self.bytes(value.as_bytes())
    }
    fn node(&mut self) -> Result<(), EncodeError> {
        self.budget.nodes = self
            .budget
            .nodes
            .checked_add(1)
            .ok_or(EncodeError::NodeLimit)?;
        if self.budget.nodes > self.limits.max_nodes_per_record {
            return Err(EncodeError::NodeLimit);
        }
        Ok(())
    }
    fn value(&mut self, value: AnyValueInput<'_>, depth: u8) -> Result<(), EncodeError> {
        if depth > self.limits.max_depth {
            return Err(EncodeError::DepthLimit);
        }
        self.node()?;
        match value {
            AnyValueInput::Null => self.u8(ANY_NULL),
            AnyValueInput::String(v) => {
                self.u8(ANY_STRING)?;
                self.string(v, false)
            }
            AnyValueInput::Bool(v) => {
                self.u8(ANY_BOOL)?;
                self.u8(u8::from(v))
            }
            AnyValueInput::Int64(v) => {
                self.u8(ANY_INT64)?;
                self.bytes(&v.to_be_bytes())
            }
            AnyValueInput::Double(v) => {
                self.u8(ANY_DOUBLE)?;
                let bits = if v.is_nan() {
                    CANONICAL_NAN_BITS
                } else if v == 0.0 {
                    0
                } else {
                    v.to_bits()
                };
                self.u64(bits)
            }
            AnyValueInput::Bytes(v) => {
                if v.len() > self.limits.max_bytes_value {
                    return Err(EncodeError::BytesLimit);
                }
                let len = u32::try_from(v.len()).map_err(|_| EncodeError::LengthOverflow)?;
                self.variable(v.len())?;
                self.u8(ANY_BYTES)?;
                self.u32(len)?;
                self.bytes(v)
            }
            AnyValueInput::Array(v) => {
                if depth >= self.limits.max_depth {
                    return Err(EncodeError::DepthLimit);
                }
                let count = u32::try_from(v.len()).map_err(|_| EncodeError::LengthOverflow)?;
                if count > self.limits.max_container_elements {
                    return Err(EncodeError::ContainerLimit);
                }
                self.u8(ANY_ARRAY)?;
                self.u32(count)?;
                for item in v {
                    self.value(*item, depth + 1)?;
                }
                Ok(())
            }
            AnyValueInput::Map(v) => self.map(v, depth),
        }
    }
    fn map(&mut self, values: &[KeyValueInput<'_>], depth: u8) -> Result<(), EncodeError> {
        if depth >= self.limits.max_depth {
            return Err(EncodeError::DepthLimit);
        }
        let count = u32::try_from(values.len()).map_err(|_| EncodeError::LengthOverflow)?;
        if count > self.limits.max_container_elements {
            return Err(EncodeError::ContainerLimit);
        }
        self.budget.pairs = self
            .budget
            .pairs
            .checked_add(count)
            .ok_or(EncodeError::KeyValueLimit)?;
        if self.budget.pairs > self.limits.max_key_value_pairs_per_record {
            return Err(EncodeError::KeyValueLimit);
        }
        let mut previous: Option<&[u8]> = None;
        for pair in values {
            if pair.key.is_empty() {
                return Err(EncodeError::EmptyMapKey);
            }
            if previous.is_some_and(|old| old >= pair.key.as_bytes()) {
                return Err(EncodeError::NonCanonicalMap);
            }
            previous = Some(pair.key.as_bytes());
        }
        self.u8(ANY_MAP)?;
        self.u32(count)?;
        for pair in values {
            self.string(pair.key, true)?;
            self.value(pair.value, depth + 1)?;
        }
        Ok(())
    }
    fn top_map(&mut self, values: &[KeyValueInput<'_>]) -> Result<(), EncodeError> {
        self.node()?;
        self.map(values, 0)
    }

    fn source_value<'a, A: AnyValueAdapter<'a> + ?Sized>(
        &mut self,
        adapter: &A,
        value: A::Handle,
        depth: u8,
        scratch: &mut EncodeScratch,
    ) -> Result<(), EncodeError> {
        if depth > self.limits.max_depth {
            return Err(EncodeError::DepthLimit);
        }
        self.node()?;
        match adapter.value(value) {
            BorrowedAnyValue::Null => self.u8(ANY_NULL),
            BorrowedAnyValue::String(v) => {
                self.u8(ANY_STRING)?;
                self.string(v, false)
            }
            BorrowedAnyValue::Bool(v) => {
                self.u8(ANY_BOOL)?;
                self.u8(u8::from(v))
            }
            BorrowedAnyValue::Int64(v) => {
                self.u8(ANY_INT64)?;
                self.bytes(&v.to_be_bytes())
            }
            BorrowedAnyValue::Double(v) => {
                self.u8(ANY_DOUBLE)?;
                let bits = if v.is_nan() {
                    CANONICAL_NAN_BITS
                } else if v == 0.0 {
                    0
                } else {
                    v.to_bits()
                };
                self.u64(bits)
            }
            BorrowedAnyValue::Bytes(v) => {
                if v.len() > self.limits.max_bytes_value {
                    return Err(EncodeError::BytesLimit);
                }
                let len = u32::try_from(v.len()).map_err(|_| EncodeError::LengthOverflow)?;
                self.variable(v.len())?;
                self.u8(ANY_BYTES)?;
                self.u32(len)?;
                self.bytes(v)
            }
            BorrowedAnyValue::Array(values) => {
                if depth >= self.limits.max_depth {
                    return Err(EncodeError::DepthLimit);
                }
                let len = adapter.array_len(values);
                let count = u32::try_from(len).map_err(|_| EncodeError::LengthOverflow)?;
                if count > self.limits.max_container_elements {
                    return Err(EncodeError::ContainerLimit);
                }
                self.u8(ANY_ARRAY)?;
                self.u32(count)?;
                for index in 0..len {
                    let item = adapter
                        .array_value(values, index)
                        .ok_or(EncodeError::InvalidSource)?;
                    self.source_value(adapter, item, depth + 1, scratch)?;
                }
                Ok(())
            }
            BorrowedAnyValue::Map(values) => self.source_map(adapter, values, depth, scratch),
        }
    }

    fn source_map<'a, A: AnyValueAdapter<'a> + ?Sized>(
        &mut self,
        adapter: &A,
        values: A::Handle,
        depth: u8,
        scratch: &mut EncodeScratch,
    ) -> Result<(), EncodeError> {
        if depth >= self.limits.max_depth {
            return Err(EncodeError::DepthLimit);
        }
        let len = adapter.map_len(values);
        let count = u32::try_from(len).map_err(|_| EncodeError::LengthOverflow)?;
        if count > self.limits.max_container_elements {
            return Err(EncodeError::ContainerLimit);
        }
        self.budget.pairs = self
            .budget
            .pairs
            .checked_add(count)
            .ok_or(EncodeError::KeyValueLimit)?;
        if self.budget.pairs > self.limits.max_key_value_pairs_per_record {
            return Err(EncodeError::KeyValueLimit);
        }

        let slot = usize::from(depth);
        if scratch.map_indices.len() <= slot {
            scratch.map_indices.resize_with(slot + 1, Vec::new);
        }
        let mut indices = std::mem::take(&mut scratch.map_indices[slot]);
        indices.clear();
        indices.extend(0..len);
        let mut source_error = false;
        indices.sort_unstable_by(|a, b| {
            match (adapter.map_key(values, *a), adapter.map_key(values, *b)) {
                (Some(a), Some(b)) => a.as_bytes().cmp(b.as_bytes()),
                _ => {
                    source_error = true;
                    a.cmp(b)
                }
            }
        });
        let result = (|| {
            if source_error {
                return Err(EncodeError::InvalidSource);
            }
            let mut previous: Option<&[u8]> = None;
            for &index in &indices {
                let key = adapter
                    .map_key(values, index)
                    .ok_or(EncodeError::InvalidSource)?;
                if key.is_empty() {
                    return Err(EncodeError::EmptyMapKey);
                }
                if previous.is_some_and(|old| old == key.as_bytes()) {
                    return Err(EncodeError::NonCanonicalMap);
                }
                previous = Some(key.as_bytes());
            }
            self.u8(ANY_MAP)?;
            self.u32(count)?;
            for &index in &indices {
                self.string(
                    adapter
                        .map_key(values, index)
                        .ok_or(EncodeError::InvalidSource)?,
                    true,
                )?;
                let value = adapter
                    .map_value(values, index)
                    .ok_or(EncodeError::InvalidSource)?;
                self.source_value(adapter, value, depth + 1, scratch)?;
            }
            Ok(())
        })();
        indices.clear();
        scratch.map_indices[slot] = indices;
        result
    }

    fn source_top_map<'a, A: AnyValueAdapter<'a> + ?Sized>(
        &mut self,
        adapter: &A,
        values: A::Handle,
        scratch: &mut EncodeScratch,
    ) -> Result<(), EncodeError> {
        self.node()?;
        self.source_map(adapter, values, 0, scratch)
    }
}

fn encode_source_record<'a, A: AnyValueAdapter<'a> + ?Sized>(
    record: &SourceLogRecordInput<'a, A>,
    limits: DecodeLimits,
    scratch: &mut EncodeScratch,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    validate_metadata(
        record.time_unix_nano,
        record.observed_time_unix_nano,
        record.trace_id,
        record.span_id,
    )?;
    let start = out.len();
    let mut w = RecordWriter {
        out,
        start,
        limits,
        budget: Budget::default(),
    };
    w.u16(0)?;
    w.string(record.resource_schema_url, false)?;
    w.u32(record.resource_dropped_attributes_count)?;
    w.source_top_map(record.adapter, record.resource_attributes, scratch)?;
    match record.scope {
        None => w.u8(0)?,
        Some(scope) => {
            w.u8(1)?;
            w.string(scope.name, false)?;
            w.string(scope.version, false)?;
            w.u32(scope.dropped_attributes_count)?;
            w.source_top_map(record.adapter, scope.attributes, scratch)?;
        }
    }
    w.string(record.scope_schema_url, false)?;
    w.u64(record.time_unix_nano)?;
    w.u64(record.observed_time_unix_nano)?;
    w.bytes(&i32::from(record.severity).to_be_bytes())?;
    w.string(record.severity_text, false)?;
    w.string(record.event_name, false)?;
    w.source_value(record.adapter, record.body, 0, scratch)?;
    w.u32(record.dropped_attributes_count)?;
    w.source_top_map(record.adapter, record.attributes, scratch)?;
    w.u32(u32::from(record.trace_flags))?;
    match record.trace_id {
        Some(id) => {
            w.u8(16)?;
            w.bytes(id)?;
        }
        None => w.u8(0)?,
    }
    match record.span_id {
        Some(id) => {
            w.u8(8)?;
            w.bytes(id)?;
        }
        None => w.u8(0)?,
    }
    Ok(())
}

fn validate_metadata(
    time: u64,
    observed: u64,
    trace_id: Option<&[u8; 16]>,
    span_id: Option<&[u8; 8]>,
) -> Result<(), EncodeError> {
    if time == 0 && observed == 0 {
        return Err(EncodeError::MissingTimestamp);
    }
    if trace_id.is_some_and(|id| id.iter().all(|b| *b == 0)) {
        return Err(EncodeError::ZeroTraceId);
    }
    if span_id.is_some_and(|id| id.iter().all(|b| *b == 0)) {
        return Err(EncodeError::ZeroSpanId);
    }
    if trace_id.is_none() && span_id.is_some() {
        return Err(EncodeError::OrphanSpanId);
    }
    Ok(())
}

fn encode_record(
    record: &LogRecordInput<'_>,
    limits: DecodeLimits,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    if record.time_unix_nano == 0 && record.observed_time_unix_nano == 0 {
        return Err(EncodeError::MissingTimestamp);
    }
    if record.trace_id.is_some_and(|id| id.iter().all(|b| *b == 0)) {
        return Err(EncodeError::ZeroTraceId);
    }
    if record.span_id.is_some_and(|id| id.iter().all(|b| *b == 0)) {
        return Err(EncodeError::ZeroSpanId);
    }
    if record.trace_id.is_none() && record.span_id.is_some() {
        return Err(EncodeError::OrphanSpanId);
    }
    let start = out.len();
    let mut w = RecordWriter {
        out,
        start,
        limits,
        budget: Budget::default(),
    };
    w.u16(0)?;
    w.string(record.resource_schema_url, false)?;
    w.u32(record.resource_dropped_attributes_count)?;
    w.top_map(record.resource_attributes)?;
    match record.scope {
        None => w.u8(0)?,
        Some(scope) => {
            w.u8(1)?;
            w.string(scope.name, false)?;
            w.string(scope.version, false)?;
            w.u32(scope.dropped_attributes_count)?;
            w.top_map(scope.attributes)?;
        }
    }
    w.string(record.scope_schema_url, false)?;
    w.u64(record.time_unix_nano)?;
    w.u64(record.observed_time_unix_nano)?;
    w.bytes(&i32::from(record.severity).to_be_bytes())?;
    w.string(record.severity_text, false)?;
    w.string(record.event_name, false)?;
    w.value(record.body, 0)?;
    w.u32(record.dropped_attributes_count)?;
    w.top_map(record.attributes)?;
    w.u32(u32::from(record.trace_flags))?;
    match record.trace_id {
        Some(id) => {
            w.u8(16)?;
            w.bytes(id)?;
        }
        None => w.u8(0)?,
    }
    match record.span_id {
        Some(id) => {
            w.u8(8)?;
            w.bytes(id)?;
        }
        None => w.u8(0)?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming_logs_v2::{decode_logs_batch_v2_into, AnyValue, LogsV2Appender};

    static EMPTY: [KeyValueInput<'static>; 0] = [];

    fn record<'a>(body: AnyValueInput<'a>) -> LogRecordInput<'a> {
        LogRecordInput {
            resource_schema_url: "resource",
            resource_dropped_attributes_count: 1,
            resource_attributes: &EMPTY,
            scope: None,
            scope_schema_url: "scope",
            time_unix_nano: 42,
            observed_time_unix_nano: 0,
            severity: 255,
            severity_text: "fatal",
            event_name: "event",
            body,
            dropped_attributes_count: 2,
            attributes: &EMPTY,
            trace_flags: 255,
            trace_id: None,
            span_id: None,
        }
    }

    #[derive(Default)]
    struct Sink {
        count: u32,
        severity: i32,
        flags: u32,
        value: String,
    }
    impl LogsV2Appender for Sink {
        fn record(&mut self, record: crate::CanonicalLogRecord<'_>) -> Result<(), String> {
            self.count += 1;
            self.severity = record.severity_number;
            self.flags = record.trace_flags;
            record.body.write_typed_canonical_text(&mut self.value);
            Ok(())
        }
    }

    fn round_trip(body: AnyValueInput<'_>) -> Sink {
        let mut encoded = vec![];
        encode_logs_batch_v2_into(&[record(body)], DecodeLimits::default(), &mut encoded).unwrap();
        let mut sink = Sink::default();
        decode_logs_batch_v2_into(&encoded, DecodeLimits::default(), &mut sink).unwrap();
        sink
    }

    #[test]
    fn round_trips_every_value_type_and_storage_widths() {
        assert_eq!(round_trip(AnyValueInput::Null).value, "null");
        assert_eq!(round_trip(AnyValueInput::String("x")).value, "\"x\"");
        assert_eq!(round_trip(AnyValueInput::Bool(true)).value, "true");
        assert_eq!(round_trip(AnyValueInput::Int64(-7)).value, "-7");
        assert_eq!(round_trip(AnyValueInput::Double(1.5)).value, "1.5");
        assert_eq!(round_trip(AnyValueInput::Bytes(&[0xab])).value, "hex\"ab\"");
        let array = [AnyValueInput::Null, AnyValueInput::Bool(false)];
        assert_eq!(
            round_trip(AnyValueInput::Array(&array)).value,
            "[null,false]"
        );
        let map = [KeyValueInput {
            key: "a",
            value: AnyValueInput::Int64(1),
        }];
        let sink = round_trip(AnyValueInput::Map(&map));
        assert_eq!(sink.value, "{\"a\":1}");
        assert_eq!((sink.severity, sink.flags), (255, 255));
    }

    #[test]
    fn canonicalizes_negative_zero_and_every_nan() {
        for value in [-0.0, f64::NAN, f64::from_bits(0xfff0_0000_0000_0001)] {
            let mut raw = vec![];
            encode_log_record_v2_into(
                &record(AnyValueInput::Double(value)),
                DecodeLimits::default(),
                &mut raw,
            )
            .unwrap();
            let decoded = crate::validate_logs_v2_record(&raw, DecodeLimits::default()).unwrap();
            match decoded.body.value() {
                AnyValue::Double(v) if value == 0.0 => assert_eq!(v.to_bits(), 0),
                AnyValue::Double(v) => assert_eq!(v.to_bits(), CANONICAL_NAN_BITS),
                _ => panic!("wrong value"),
            }
        }
    }

    #[test]
    fn preserves_scope_and_valid_ids() {
        let attrs = [KeyValueInput {
            key: "a",
            value: AnyValueInput::Null,
        }];
        let trace = [1; 16];
        let span = [2; 8];
        let mut input = record(AnyValueInput::Null);
        input.scope = Some(ScopeInput {
            name: "",
            version: "",
            dropped_attributes_count: 3,
            attributes: &attrs,
        });
        input.trace_id = Some(&trace);
        input.span_id = Some(&span);
        let mut raw = vec![];
        encode_log_record_v2_into(&input, DecodeLimits::default(), &mut raw).unwrap();
        let got = crate::validate_logs_v2_record(&raw, DecodeLimits::default()).unwrap();
        assert_eq!(got.scope_name, Some(""));
        assert_eq!(got.scope_dropped_attributes_count, Some(3));
        assert_eq!(got.trace_id, Some(&trace));
        assert_eq!(got.span_id, Some(&span));
    }

    #[test]
    fn rejects_timestamp_and_identifier_storage_violations_transactionally() {
        let mut input = record(AnyValueInput::Null);
        let mut out = vec![9, 8];
        input.time_unix_nano = 0;
        assert_eq!(
            encode_log_record_v2_into(&input, DecodeLimits::default(), &mut out),
            Err(EncodeError::MissingTimestamp)
        );
        assert_eq!(out, [9, 8]);
        input.time_unix_nano = 1;
        let zero_trace = [0; 16];
        input.trace_id = Some(&zero_trace);
        assert_eq!(
            encode_log_record_v2_into(&input, DecodeLimits::default(), &mut out),
            Err(EncodeError::ZeroTraceId)
        );
        let trace = [1; 16];
        let zero_span = [0; 8];
        input.trace_id = Some(&trace);
        input.span_id = Some(&zero_span);
        assert_eq!(
            encode_log_record_v2_into(&input, DecodeLimits::default(), &mut out),
            Err(EncodeError::ZeroSpanId)
        );
        let span = [1; 8];
        input.trace_id = None;
        input.span_id = Some(&span);
        assert_eq!(
            encode_log_record_v2_into(&input, DecodeLimits::default(), &mut out),
            Err(EncodeError::OrphanSpanId)
        );
        assert_eq!(out, [9, 8]);
    }

    #[test]
    fn requires_nonempty_strictly_sorted_unique_maps() {
        for (map, error) in [
            (
                &[KeyValueInput {
                    key: "",
                    value: AnyValueInput::Null,
                }][..],
                EncodeError::EmptyMapKey,
            ),
            (
                &[
                    KeyValueInput {
                        key: "b",
                        value: AnyValueInput::Null,
                    },
                    KeyValueInput {
                        key: "a",
                        value: AnyValueInput::Null,
                    },
                ][..],
                EncodeError::NonCanonicalMap,
            ),
            (
                &[
                    KeyValueInput {
                        key: "a",
                        value: AnyValueInput::Null,
                    },
                    KeyValueInput {
                        key: "a",
                        value: AnyValueInput::Null,
                    },
                ][..],
                EncodeError::NonCanonicalMap,
            ),
        ] {
            let mut input = record(AnyValueInput::Null);
            input.attributes = map;
            assert_eq!(
                encode_log_record_v2_into(&input, DecodeLimits::default(), &mut vec![]),
                Err(error)
            );
        }
    }

    #[test]
    fn encodes_external_recursive_source_without_an_input_ast() {
        enum ExternalValue {
            Scalar(i64),
            Array(Vec<ExternalValue>),
            Map(Vec<(String, ExternalValue)>),
        }
        struct ExternalAdapter;
        impl<'a> AnyValueAdapter<'a> for ExternalAdapter {
            type Handle = &'a ExternalValue;
            fn value(&self, value: Self::Handle) -> BorrowedAnyValue<'a, Self::Handle> {
                match value {
                    ExternalValue::Scalar(v) => BorrowedAnyValue::Int64(*v),
                    ExternalValue::Array(_) => BorrowedAnyValue::Array(value),
                    ExternalValue::Map(_) => BorrowedAnyValue::Map(value),
                }
            }
            fn array_len(&self, value: Self::Handle) -> usize {
                match value {
                    ExternalValue::Array(v) => v.len(),
                    _ => 0,
                }
            }
            fn array_value(&self, value: Self::Handle, index: usize) -> Option<Self::Handle> {
                match value {
                    ExternalValue::Array(v) => v.get(index),
                    _ => None,
                }
            }
            fn map_len(&self, value: Self::Handle) -> usize {
                match value {
                    ExternalValue::Map(v) => v.len(),
                    _ => 0,
                }
            }
            fn map_key(&self, value: Self::Handle, index: usize) -> Option<&'a str> {
                match value {
                    ExternalValue::Map(v) => v.get(index).map(|e| e.0.as_str()),
                    _ => None,
                }
            }
            fn map_value(&self, value: Self::Handle, index: usize) -> Option<Self::Handle> {
                match value {
                    ExternalValue::Map(v) => v.get(index).map(|e| &e.1),
                    _ => None,
                }
            }
        }

        let empty = ExternalValue::Map(Vec::new());
        // Deliberately unsorted at both levels: only scratch indices are sorted.
        let body = ExternalValue::Map(vec![
            ("z".into(), ExternalValue::Scalar(3)),
            (
                "a".into(),
                ExternalValue::Array(vec![ExternalValue::Map(vec![
                    ("y".into(), ExternalValue::Scalar(2)),
                    ("x".into(), ExternalValue::Scalar(1)),
                ])]),
            ),
        ]);
        let adapter = ExternalAdapter;
        let input = SourceLogRecordInput {
            adapter: &adapter,
            resource_schema_url: "",
            resource_dropped_attributes_count: 0,
            resource_attributes: &empty,
            scope: None,
            scope_schema_url: "",
            time_unix_nano: 1,
            observed_time_unix_nano: 0,
            severity: 0,
            severity_text: "",
            event_name: "",
            body: &body,
            dropped_attributes_count: 0,
            attributes: &empty,
            trace_flags: 0,
            trace_id: None,
            span_id: None,
        };
        let mut scratch = EncodeScratch::default();
        let mut encoded = Vec::new();
        encode_logs_batch_v2_from_sources_into(
            &[input],
            DecodeLimits::default(),
            &mut scratch,
            &mut encoded,
        )
        .unwrap();
        let capacities: Vec<_> = scratch.map_indices.iter().map(Vec::capacity).collect();
        let mut sink = Sink::default();
        decode_logs_batch_v2_into(&encoded, DecodeLimits::default(), &mut sink).unwrap();
        assert_eq!(sink.value, "{\"a\":[{\"x\":1,\"y\":2}],\"z\":3}");

        encoded.clear();
        encode_logs_batch_v2_from_sources_into(
            &[input],
            DecodeLimits::default(),
            &mut scratch,
            &mut encoded,
        )
        .unwrap();
        assert_eq!(
            capacities,
            scratch
                .map_indices
                .iter()
                .map(Vec::capacity)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn enforces_all_configured_bounds_and_rolls_back_batches() {
        fn fails(
            body: AnyValueInput<'_>,
            change: impl FnOnce(&mut DecodeLimits),
            expected: EncodeError,
        ) {
            let mut limits = DecodeLimits::default();
            change(&mut limits);
            let mut out = vec![7];
            assert_eq!(
                encode_logs_batch_v2_into(&[record(body)], limits, &mut out),
                Err(expected)
            );
            assert_eq!(out, [7]);
        }
        fails(
            AnyValueInput::String("xx"),
            |l| l.max_string_bytes = 1,
            EncodeError::StringLimit,
        );
        fails(
            AnyValueInput::Bytes(&[1, 2]),
            |l| l.max_bytes_value = 1,
            EncodeError::BytesLimit,
        );
        fails(
            AnyValueInput::String("xx"),
            |l| l.max_variable_bytes_per_record = 1,
            EncodeError::VariableBytesLimit,
        );
        fails(
            AnyValueInput::Null,
            |l| l.max_nodes_per_record = 2,
            EncodeError::NodeLimit,
        );
        let array = [AnyValueInput::Null, AnyValueInput::Null];
        fails(
            AnyValueInput::Array(&array),
            |l| l.max_container_elements = 1,
            EncodeError::ContainerLimit,
        );
        let nested = [AnyValueInput::Array(&[])];
        fails(
            AnyValueInput::Array(&nested),
            |l| l.max_depth = 1,
            EncodeError::DepthLimit,
        );
        let map = [KeyValueInput {
            key: "aa",
            value: AnyValueInput::Null,
        }];
        fails(
            AnyValueInput::Map(&map),
            |l| l.max_key_bytes = 1,
            EncodeError::KeyLimit,
        );
        fails(
            AnyValueInput::Map(&map),
            |l| l.max_key_value_pairs_per_record = 0,
            EncodeError::KeyValueLimit,
        );
        fails(
            AnyValueInput::Null,
            |l| l.max_record_bytes = 1,
            EncodeError::RecordLimit,
        );
        fails(
            AnyValueInput::Null,
            |l| l.max_payload_bytes = 1,
            EncodeError::PayloadLimit,
        );

        let limits = DecodeLimits {
            max_records: 0,
            ..DecodeLimits::default()
        };
        let mut out = vec![3];
        assert_eq!(
            encode_logs_batch_v2_into(&[record(AnyValueInput::Null)], limits, &mut out),
            Err(EncodeError::RecordCountLimit)
        );
        assert_eq!(out, [3]);
    }
}
