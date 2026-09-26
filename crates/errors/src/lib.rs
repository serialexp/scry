//! Pure domain primitives for extracting and folding error occurrences from
//! authoritative canonical logs-v2 records.
//!
//! `OCC1` is an internal, type-injective identity encoding. Every variable-width
//! component is prefixed by a big-endian `u32`; integers are big-endian. It contains
//! scrubbed semantic log content and logical identity, but no signal/source, block,
//! row, receipt-time, or other physical provenance.

pub mod engine;
pub mod fingerprint;
pub mod follow;
pub mod handle;
pub mod issue;
pub mod manifest;
pub mod occ1;
pub mod projection;
pub mod publication;
pub mod quarantine;
pub mod snapshot;
pub mod sqlite;

use scry_proto::{AnyValue, AnyValueRef, CanonicalLogRecord};
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;

pub const CANONICAL_VERSION: u16 = 1;
pub const SCRUB_POLICY_VERSION: u16 = 1;
pub const REDACTED: &str = "[REDACTED]";

const APP_DOMAIN: &[u8] = b"scry.app.identity.v1\0";
const OCC_MAGIC: &[u8; 4] = b"OCC1";

/// Conservative extraction/canonicalization limits in addition to logs-v2's
/// decoder limits. Identity fields fail rather than being truncated.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_identity_bytes: usize,
    pub max_canonical_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_identity_bytes: 16 * 1024,
            max_canonical_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("record is not an exception event")]
    NotExceptionEvent,
    #[error("exception event has no standard exception content")]
    MissingExceptionContent,
    #[error("required attribute `{0}` is missing")]
    MissingAttribute(&'static str),
    #[error("attribute `{0}` must be a string")]
    AttributeType(&'static str),
    #[error("attribute `{0}` is empty after Unicode normalization and trimming")]
    EmptyAttribute(&'static str),
    #[error("attribute `{0}` is not a strict lowercase non-nil UUID")]
    InvalidUuid(&'static str),
    #[error("identity field exceeds configured limit")]
    IdentityLimit,
    #[error("canonical OCC1 bytes exceed configured limit")]
    CanonicalLimit,
    #[error("a canonical component is too large for OCC1")]
    LengthOverflow,
}

/// A validated UUID represented without formatting or allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainUuid([u8; 16]);

impl DomainUuid {
    pub fn parse(value: &str, field: &'static str) -> Result<Self, Error> {
        let bytes = value.as_bytes();
        if bytes.len() != 36
            || bytes[8] != b'-'
            || bytes[13] != b'-'
            || bytes[18] != b'-'
            || bytes[23] != b'-'
        {
            return Err(Error::InvalidUuid(field));
        }
        let mut out = [0_u8; 16];
        let mut nibble = None;
        let mut index = 0;
        for (position, byte) in bytes.iter().copied().enumerate() {
            if matches!(position, 8 | 13 | 18 | 23) {
                continue;
            }
            let value = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                _ => return Err(Error::InvalidUuid(field)),
            };
            if let Some(high) = nibble.take() {
                out[index] = high << 4 | value;
                index += 1;
            } else {
                nibble = Some(value);
            }
        }
        if out == [0; 16] {
            return Err(Error::InvalidUuid(field));
        }
        Ok(Self(out))
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

pub type DeploymentId = DomainUuid;
pub type EventId = DomainUuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppIdentity {
    /// Complete SHA-256 identity; this is the collision-authoritative value.
    pub digest: [u8; 32],
    /// Compact index/display identity. Equality must still compare `digest`.
    pub app_id: [u8; 16],
}

/// Derives application identity from NFC(trim(namespace)) and NFC(trim(name)).
/// Namespace may be empty; service name may not.
pub fn derive_app_identity(
    service_namespace: &str,
    service_name: &str,
    limits: Limits,
) -> Result<AppIdentity, Error> {
    let namespace = service_namespace.trim();
    let name = service_name.trim();
    let namespace_len = normalized_utf8_len(namespace)?;
    let name_len = normalized_utf8_len(name)?;
    if name_len == 0 {
        return Err(Error::EmptyAttribute("service.name"));
    }
    if namespace_len > limits.max_identity_bytes || name_len > limits.max_identity_bytes {
        return Err(Error::IdentityLimit);
    }
    let mut hash = Sha256::new();
    hash.update(APP_DOMAIN);
    hash_normalized(&mut hash, namespace, namespace_len)?;
    hash_normalized(&mut hash, name, name_len)?;
    let digest: [u8; 32] = hash.finalize().into();
    let mut app_id = [0; 16];
    app_id.copy_from_slice(&digest[..16]);
    Ok(AppIdentity { digest, app_id })
}

fn normalized_utf8_len(value: &str) -> Result<usize, Error> {
    value.nfc().try_fold(0_usize, |length, character| {
        length
            .checked_add(character.len_utf8())
            .ok_or(Error::LengthOverflow)
    })
}

/// Hashes NFC text in a second pass so the required length prefix can be emitted
/// without allocating a normalized `String` for every record.
fn hash_normalized(hash: &mut Sha256, value: &str, length: usize) -> Result<(), Error> {
    let length = u32::try_from(length).map_err(|_| Error::LengthOverflow)?;
    hash.update(length.to_be_bytes());
    let mut encoded = [0_u8; 4];
    for character in value.nfc() {
        hash.update(character.encode_utf8(&mut encoded).as_bytes());
    }
    Ok(())
}

/// Reusable buffers for record-by-record extraction. Call [`Scratch::recycle`]
/// after consuming an occurrence to retain its canonical allocation.
#[derive(Debug, Default)]
pub struct Scratch {
    canonical: Vec<u8>,
}

impl Scratch {
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            canonical: Vec::with_capacity(bytes),
        }
    }

    pub fn recycle(&mut self, mut occurrence: Occurrence) {
        occurrence.canonical.clear();
        if occurrence.canonical.capacity() > self.canonical.capacity() {
            self.canonical = occurrence.canonical;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Occurrence {
    pub deployment_id: DeploymentId,
    pub app: AppIdentity,
    pub event_id: EventId,
    pub trace_id: Option<[u8; 16]>,
    pub span_id: Option<[u8; 8]>,
    pub trace_flags: u32,
    pub canonical_sha256: [u8; 32],
    /// Exact collision-authoritative OCC1 bytes.
    pub canonical: Vec<u8>,
}

/// Extracts an authoritative occurrence from one logs-v2 record.
pub fn extract(
    record: CanonicalLogRecord<'_>,
    deployment_id: DeploymentId,
    limits: Limits,
    scratch: &mut Scratch,
) -> Result<Occurrence, Error> {
    if !is_exception_event_name(record.event_name) {
        return Err(Error::NotExceptionEvent);
    }
    let attributes = record
        .attributes
        .map_iter()
        .expect("logs-v2 attributes are a map");
    let resource = record
        .resource_attributes
        .map_iter()
        .expect("logs-v2 resource attributes are a map");

    let event_text = string_attribute(attributes, "scry.event.id")?
        .ok_or(Error::MissingAttribute("scry.event.id"))?;
    let event_id = EventId::parse(event_text, "scry.event.id")?;
    let name = string_attribute(resource, "service.name")?
        .ok_or(Error::MissingAttribute("service.name"))?;
    let namespace = string_attribute(resource, "service.namespace")?.unwrap_or("");
    let app = derive_app_identity(namespace, name, limits)?;

    if !has_standard_exception_content(attributes)? {
        return Err(Error::MissingExceptionContent);
    }

    scratch.canonical.clear();
    let result = encode_occurrence(
        record,
        deployment_id,
        &app,
        event_id,
        limits,
        &mut scratch.canonical,
    );
    if let Err(error) = result {
        scratch.canonical.clear();
        return Err(error);
    }
    let canonical_sha256 = Sha256::digest(&scratch.canonical).into();
    let canonical = std::mem::take(&mut scratch.canonical);
    Ok(Occurrence {
        deployment_id,
        app,
        event_id,
        trace_id: record.trace_id.copied(),
        span_id: record.span_id.copied(),
        trace_flags: record.trace_flags,
        canonical_sha256,
        canonical,
    })
}

pub fn is_exception_event_name(name: &str) -> bool {
    if name == "exception" {
        return true;
    }
    let Some(prefix) = name.strip_suffix(".exception") else {
        return false;
    };
    !prefix.is_empty() && prefix.split('.').all(valid_event_segment)
}

fn valid_event_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn find_attribute<'a>(
    attributes: impl Iterator<Item = (&'a str, AnyValueRef<'a>)>,
    key: &str,
) -> Option<AnyValueRef<'a>> {
    attributes
        .filter(|(candidate, _)| *candidate == key)
        .map(|(_, value)| value)
        .next()
}

fn string_attribute<'a>(
    attributes: impl Iterator<Item = (&'a str, AnyValueRef<'a>)>,
    key: &'static str,
) -> Result<Option<&'a str>, Error> {
    find_attribute(attributes, key)
        .map(|value| match value.value() {
            AnyValue::String(value) => Ok(value),
            _ => Err(Error::AttributeType(key)),
        })
        .transpose()
}

fn has_standard_exception_content<'a>(
    attributes: impl Iterator<Item = (&'a str, AnyValueRef<'a>)>,
) -> Result<bool, Error> {
    let mut found = false;
    for (key, value) in attributes {
        if matches!(
            key,
            "exception.type" | "exception.message" | "exception.stacktrace"
        ) {
            match value.value() {
                AnyValue::String(value) if !value.is_empty() => found = true,
                AnyValue::String(_) => {}
                _ => {
                    let field = match key {
                        "exception.type" => "exception.type",
                        "exception.message" => "exception.message",
                        _ => "exception.stacktrace",
                    };
                    return Err(Error::AttributeType(field));
                }
            }
        }
    }
    Ok(found)
}

/// Uses the single protocol-level policy shared by every intake and producer.
/// This deliberately inspects keys only, never free text.
pub fn is_sensitive_key(key: &str) -> bool {
    scry_proto::redaction::is_sensitive_key(key)
}

fn encode_occurrence(
    record: CanonicalLogRecord<'_>,
    deployment: DeploymentId,
    app: &AppIdentity,
    event: EventId,
    limits: Limits,
    out: &mut Vec<u8>,
) -> Result<(), Error> {
    put(out, OCC_MAGIC, limits)?;
    put_u16(out, CANONICAL_VERSION, limits)?;
    put_u16(out, SCRUB_POLICY_VERSION, limits)?;
    put(out, deployment.as_bytes(), limits)?;
    put(out, &app.digest, limits)?;
    put(out, event.as_bytes(), limits)?;
    put_str(out, record.resource_schema_url, limits)?;
    put_u32(out, record.resource_dropped_attributes_count, limits)?;
    put_value(out, record.resource_attributes, limits)?;
    put_option_str(out, record.scope_name, limits)?;
    put_option_str(out, record.scope_version, limits)?;
    put_option_u32(out, record.scope_dropped_attributes_count, limits)?;
    put_option_value(out, record.scope_attributes, limits)?;
    put_str(out, record.scope_schema_url, limits)?;
    put_u64(out, record.time_unix_nano, limits)?;
    put_u64(out, record.observed_time_unix_nano, limits)?;
    put_i32(out, record.severity_number, limits)?;
    put_str(out, record.severity_text, limits)?;
    put_str(out, record.event_name, limits)?;
    put_value(out, record.body, limits)?;
    put_u32(out, record.dropped_attributes_count, limits)?;
    put_value(out, record.attributes, limits)?;
    put_u32(out, record.trace_flags, limits)?;
    put_option_fixed(out, record.trace_id.map(|v| v.as_slice()), limits)?;
    put_option_fixed(out, record.span_id.map(|v| v.as_slice()), limits)?;
    Ok(())
}

fn put_value(out: &mut Vec<u8>, value: AnyValueRef<'_>, limits: Limits) -> Result<(), Error> {
    let start = out.len();
    put_u32(out, 0, limits)?;
    match value.value() {
        AnyValue::Null => put_u8(out, 0, limits)?,
        AnyValue::String(value) => {
            put_u8(out, 1, limits)?;
            put_str(out, value, limits)?;
        }
        AnyValue::Bool(value) => {
            put_u8(out, 2, limits)?;
            put_u8(out, u8::from(value), limits)?;
        }
        AnyValue::Int64(value) => {
            put_u8(out, 3, limits)?;
            put(out, &value.to_be_bytes(), limits)?;
        }
        AnyValue::Double(value) => {
            put_u8(out, 4, limits)?;
            put(out, &value.to_bits().to_be_bytes(), limits)?;
        }
        AnyValue::Bytes(value) => {
            put_u8(out, 5, limits)?;
            put_bytes(out, value, limits)?;
        }
        AnyValue::Array(values) => {
            put_u8(out, 6, limits)?;
            put_u32(out, values.len(), limits)?;
            for value in values {
                put_value(out, value, limits)?;
            }
        }
        AnyValue::Map(values) => {
            put_u8(out, 7, limits)?;
            put_u32(out, values.len(), limits)?;
            for (key, value) in values {
                put_str(out, key, limits)?;
                if is_sensitive_key(key) {
                    put_redacted_value(out, limits)?;
                } else {
                    put_value(out, value, limits)?;
                }
            }
        }
    }
    let payload_len = out.len() - start - 4;
    let len = u32::try_from(payload_len).map_err(|_| Error::LengthOverflow)?;
    out[start..start + 4].copy_from_slice(&len.to_be_bytes());
    Ok(())
}

fn put_redacted_value(out: &mut Vec<u8>, limits: Limits) -> Result<(), Error> {
    let len = 1_usize
        .checked_add(4 + REDACTED.len())
        .ok_or(Error::LengthOverflow)?;
    put_u32(
        out,
        u32::try_from(len).map_err(|_| Error::LengthOverflow)?,
        limits,
    )?;
    put_u8(out, 1, limits)?;
    put_str(out, REDACTED, limits)
}

fn put_option_value(
    out: &mut Vec<u8>,
    value: Option<AnyValueRef<'_>>,
    limits: Limits,
) -> Result<(), Error> {
    put_u8(out, u8::from(value.is_some()), limits)?;
    if let Some(value) = value {
        put_value(out, value, limits)?;
    }
    Ok(())
}
fn put_option_str(out: &mut Vec<u8>, value: Option<&str>, limits: Limits) -> Result<(), Error> {
    put_u8(out, u8::from(value.is_some()), limits)?;
    if let Some(value) = value {
        put_str(out, value, limits)?;
    }
    Ok(())
}
fn put_option_u32(out: &mut Vec<u8>, value: Option<u32>, limits: Limits) -> Result<(), Error> {
    put_u8(out, u8::from(value.is_some()), limits)?;
    if let Some(value) = value {
        put_u32(out, value, limits)?;
    }
    Ok(())
}
fn put_option_fixed(out: &mut Vec<u8>, value: Option<&[u8]>, limits: Limits) -> Result<(), Error> {
    put_u8(out, u8::from(value.is_some()), limits)?;
    if let Some(value) = value {
        put(out, value, limits)?;
    }
    Ok(())
}
fn put_str(out: &mut Vec<u8>, value: &str, limits: Limits) -> Result<(), Error> {
    put_bytes(out, value.as_bytes(), limits)
}
fn put_bytes(out: &mut Vec<u8>, value: &[u8], limits: Limits) -> Result<(), Error> {
    put_u32(
        out,
        u32::try_from(value.len()).map_err(|_| Error::LengthOverflow)?,
        limits,
    )?;
    put(out, value, limits)
}
fn put_u8(out: &mut Vec<u8>, value: u8, limits: Limits) -> Result<(), Error> {
    put(out, &[value], limits)
}
fn put_u16(out: &mut Vec<u8>, value: u16, limits: Limits) -> Result<(), Error> {
    put(out, &value.to_be_bytes(), limits)
}
fn put_u32(out: &mut Vec<u8>, value: u32, limits: Limits) -> Result<(), Error> {
    put(out, &value.to_be_bytes(), limits)
}
fn put_i32(out: &mut Vec<u8>, value: i32, limits: Limits) -> Result<(), Error> {
    put(out, &value.to_be_bytes(), limits)
}
fn put_u64(out: &mut Vec<u8>, value: u64, limits: Limits) -> Result<(), Error> {
    put(out, &value.to_be_bytes(), limits)
}
fn put(out: &mut Vec<u8>, value: &[u8], limits: Limits) -> Result<(), Error> {
    let new_len = out
        .len()
        .checked_add(value.len())
        .ok_or(Error::LengthOverflow)?;
    if new_len > limits.max_canonical_bytes {
        return Err(Error::CanonicalLimit);
    }
    out.extend_from_slice(value);
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldOutcome {
    Insert,
    Duplicate,
    Collision,
}

/// Determines the fold action. Digest equality is only an accelerator: exact
/// canonical bytes are always compared before declaring a duplicate.
pub fn fold(existing: Option<&Occurrence>, candidate: &Occurrence) -> FoldOutcome {
    let Some(existing) = existing else {
        return FoldOutcome::Insert;
    };
    if existing.deployment_id != candidate.deployment_id
        || existing.app.digest != candidate.app.digest
        || existing.event_id != candidate.event_id
    {
        return FoldOutcome::Insert;
    }
    if existing.canonical_sha256 == candidate.canonical_sha256
        && existing.canonical == candidate.canonical
    {
        FoldOutcome::Duplicate
    } else {
        FoldOutcome::Collision
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scry_proto::{
        encode_log_record_v2_into, validate_logs_v2_record, LogRecordInput,
        LogsV2AnyValueInput as Value, LogsV2DecodeLimits, LogsV2KeyValueInput as Kv,
    };

    const EVENT_ID: &str = "018f1f8e-7b2c-7a91-8123-0123456789ab";
    const DEPLOYMENT_ID: &str = "018f1f8e-7b2c-7a91-8123-abcdef012345";

    fn encode_fixture(event_name: &str, attributes: &mut [Kv<'_>]) -> Vec<u8> {
        let resource = [
            Kv {
                key: "service.name",
                value: Value::String(" api "),
            },
            Kv {
                key: "service.namespace",
                value: Value::String("prod"),
            },
        ];
        attributes.sort_unstable_by_key(|entry| entry.key.as_bytes());
        let input = LogRecordInput {
            resource_schema_url: "resource-schema",
            resource_dropped_attributes_count: 1,
            resource_attributes: &resource,
            scope: None,
            scope_schema_url: "scope-schema",
            time_unix_nano: 10,
            observed_time_unix_nano: 20,
            severity: 17,
            severity_text: "ERROR",
            event_name,
            body: Value::String("body"),
            dropped_attributes_count: 2,
            attributes,
            trace_flags: 1,
            trace_id: Some(&[1; 16]),
            span_id: Some(&[2; 8]),
        };
        let mut encoded = Vec::new();
        encode_log_record_v2_into(&input, LogsV2DecodeLimits::default(), &mut encoded).unwrap();
        encoded
    }

    fn occurrence(event_name: &str, message: &str, secret_key: &str, secret: &str) -> Occurrence {
        let mut attributes = [
            Kv {
                key: "exception.message",
                value: Value::String(message),
            },
            Kv {
                key: "scry.event.id",
                value: Value::String(EVENT_ID),
            },
            Kv {
                key: secret_key,
                value: Value::String(secret),
            },
        ];
        let encoded = encode_fixture(event_name, &mut attributes);
        let record = validate_logs_v2_record(&encoded, LogsV2DecodeLimits::default()).unwrap();
        extract(
            record,
            DeploymentId::parse(DEPLOYMENT_ID, "deployment_id").unwrap(),
            Limits::default(),
            &mut Scratch::default(),
        )
        .unwrap()
    }

    #[test]
    fn strict_uuid_accepts_only_lowercase_hyphenated_non_nil() {
        assert_eq!(DomainUuid::parse(EVENT_ID, "id").unwrap().as_bytes()[0], 1);
        for invalid in [
            "00000000-0000-0000-0000-000000000000",
            "018F1f8e-7b2c-7a91-8123-0123456789ab",
            "018f1f8e7b2c7a9181230123456789ab",
            "018f1f8e-7b2c-7a91-8123-0123456789ag",
        ] {
            assert_eq!(
                DomainUuid::parse(invalid, "id"),
                Err(Error::InvalidUuid("id"))
            );
        }
    }

    #[test]
    fn application_identity_is_trimmed_nfc_case_sensitive_and_retains_digest() {
        let limits = Limits::default();
        let composed = derive_app_identity(" cafe\u{301} ", " Api ", limits).unwrap();
        let normalized = derive_app_identity("café", "Api", limits).unwrap();
        assert_eq!(composed, normalized);
        assert_eq!(
            normalized.digest,
            [
                0x43, 0xb0, 0x6c, 0x30, 0xbd, 0x87, 0x71, 0x2e, 0xcf, 0xfd, 0x05, 0xd7, 0xf0, 0xea,
                0xea, 0x65, 0xcd, 0xdc, 0xe6, 0x2e, 0x31, 0xfe, 0xaa, 0x7b, 0xe1, 0xc7, 0xa5, 0xde,
                0x23, 0xb1, 0xb0, 0x1d,
            ]
        );
        assert_eq!(&composed.digest[..16], &composed.app_id);
        assert_ne!(
            normalized,
            derive_app_identity("café", "api", limits).unwrap()
        );
        assert!(derive_app_identity("", "api", limits).is_ok());
        assert_eq!(
            derive_app_identity("ns", "  ", limits),
            Err(Error::EmptyAttribute("service.name"))
        );
        assert_eq!(
            derive_app_identity(
                "toolong",
                "api",
                Limits {
                    max_identity_bytes: 3,
                    max_canonical_bytes: 100
                }
            ),
            Err(Error::IdentityLimit)
        );
    }

    #[test]
    fn event_name_requires_well_formed_dot_delimited_exception_suffix() {
        for valid in [
            "exception",
            "http.server.request.exception",
            "a-b.c_2.exception",
        ] {
            assert!(is_exception_event_name(valid), "{valid}");
        }
        for invalid in [
            "Exception",
            ".exception",
            "a..exception",
            "a exception",
            "notexception",
            "exception.extra",
        ] {
            assert!(!is_exception_event_name(invalid), "{invalid}");
        }
    }

    #[test]
    fn extracts_identity_trace_and_occ1_bytes() {
        let got = occurrence("http.request.exception", "boom", "token", "one");
        assert_eq!(&got.canonical[..4], b"OCC1");
        assert_eq!(&got.canonical[4..6], &CANONICAL_VERSION.to_be_bytes());
        assert_eq!(&got.canonical[6..8], &SCRUB_POLICY_VERSION.to_be_bytes());
        assert_eq!(got.trace_id, Some([1; 16]));
        assert_eq!(got.span_id, Some([2; 8]));
        assert_eq!(got.trace_flags, 1);
        let expected_digest: [u8; 32] = Sha256::digest(&got.canonical).into();
        assert_eq!(got.canonical_sha256, expected_digest);
        assert!(got.canonical.len() < Limits::default().max_canonical_bytes);
    }

    #[test]
    fn sensitive_matcher_uses_shared_segment_policy_and_does_not_scan_values() {
        for key in [
            "Authorization",
            "COOKIE",
            "x-api-key",
            "Client_Secret",
            "password",
            "my-token",
        ] {
            assert!(is_sensitive_key(key), "{key}");
        }
        for key in ["authorization_hint", "token_count", "message", ""] {
            assert!(!is_sensitive_key(key), "{key}");
        }
        let text_with_secret_word = occurrence("exception", "password=hunter2", "safe", "token");
        let changed_text = occurrence("exception", "password=different", "safe", "token");
        assert_ne!(text_with_secret_word.canonical, changed_text.canonical);
    }

    #[test]
    fn scrubbing_precedes_hash_and_replaces_entire_typed_value() {
        let first = occurrence("exception", "boom", "ToKeN", "one");
        let second = occurrence("exception", "boom", "ToKeN", "two");
        assert_eq!(first.canonical, second.canonical);
        assert_eq!(first.canonical_sha256, second.canonical_sha256);
        assert!(first
            .canonical
            .windows(REDACTED.len())
            .any(|window| window == REDACTED.as_bytes()));
        assert!(!first.canonical.windows(3).any(|window| window == b"one"));
    }

    #[test]
    fn canonical_identity_excludes_physical_source_but_includes_semantics() {
        let first = occurrence("exception", "one", "token", "secret");
        let same = occurrence("exception", "one", "token", "different-secret");
        let changed = occurrence("exception", "two", "token", "secret");
        assert_eq!(first.canonical, same.canonical);
        assert_ne!(first.canonical, changed.canonical);
    }

    #[test]
    fn fold_compares_scope_digest_and_exact_bytes() {
        let first = occurrence("exception", "one", "token", "secret");
        let duplicate = occurrence("exception", "one", "token", "other");
        let collision = occurrence("exception", "two", "token", "secret");
        assert_eq!(fold(None, &first), FoldOutcome::Insert);
        assert_eq!(fold(Some(&first), &duplicate), FoldOutcome::Duplicate);
        assert_eq!(fold(Some(&first), &collision), FoldOutcome::Collision);

        let mut forged = duplicate.clone();
        forged.canonical.push(0);
        assert_eq!(fold(Some(&first), &forged), FoldOutcome::Collision);

        let mut other_event = duplicate.clone();
        other_event.event_id.0[15] ^= 1;
        assert_eq!(fold(Some(&first), &other_event), FoldOutcome::Insert);
    }

    #[test]
    fn extraction_rejects_non_exception_missing_content_and_small_bound() {
        let mut valid_attributes = [
            Kv {
                key: "exception.message",
                value: Value::String("boom"),
            },
            Kv {
                key: "scry.event.id",
                value: Value::String(EVENT_ID),
            },
        ];
        let raw = encode_fixture("ordinary", &mut valid_attributes);
        let record = validate_logs_v2_record(&raw, LogsV2DecodeLimits::default()).unwrap();
        assert_eq!(
            extract(
                record,
                DomainUuid::parse(DEPLOYMENT_ID, "deployment_id").unwrap(),
                Limits::default(),
                &mut Scratch::default()
            ),
            Err(Error::NotExceptionEvent)
        );

        let mut no_content = [Kv {
            key: "scry.event.id",
            value: Value::String(EVENT_ID),
        }];
        let raw = encode_fixture("exception", &mut no_content);
        let record = validate_logs_v2_record(&raw, LogsV2DecodeLimits::default()).unwrap();
        assert_eq!(
            extract(
                record,
                DomainUuid::parse(DEPLOYMENT_ID, "deployment_id").unwrap(),
                Limits::default(),
                &mut Scratch::default()
            ),
            Err(Error::MissingExceptionContent)
        );

        let mut malformed_content = [
            Kv {
                key: "exception.type",
                value: Value::Int64(1),
            },
            Kv {
                key: "scry.event.id",
                value: Value::String(EVENT_ID),
            },
        ];
        let raw = encode_fixture("exception", &mut malformed_content);
        let record = validate_logs_v2_record(&raw, LogsV2DecodeLimits::default()).unwrap();
        assert_eq!(
            extract(
                record,
                DomainUuid::parse(DEPLOYMENT_ID, "deployment_id").unwrap(),
                Limits::default(),
                &mut Scratch::default()
            ),
            Err(Error::AttributeType("exception.type"))
        );

        let raw = encode_fixture("exception", &mut valid_attributes);
        let record = validate_logs_v2_record(&raw, LogsV2DecodeLimits::default()).unwrap();
        assert_eq!(
            extract(
                record,
                DomainUuid::parse(DEPLOYMENT_ID, "deployment_id").unwrap(),
                Limits {
                    max_identity_bytes: 1024,
                    max_canonical_bytes: 8
                },
                &mut Scratch::default()
            ),
            Err(Error::CanonicalLimit)
        );
    }

    #[test]
    fn scratch_recycles_canonical_allocation() {
        let item = occurrence("exception", "boom", "token", "secret");
        let capacity = item.canonical.capacity();
        let mut scratch = Scratch::default();
        scratch.recycle(item);
        assert!(scratch.canonical.capacity() >= capacity);
    }
}
