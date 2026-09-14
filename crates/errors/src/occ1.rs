//! Zero-copy decoder for the binary OCC1 canonical format.
//!
//! The encoder lives in [`super::encode_occurrence`]. This decoder reads the
//! same format and extracts the fields relevant for fingerprinting: exception
//! type, message, stacktrace, event name, severity, and body.

use std::str;

const OCC_MAGIC: &[u8; 4] = b"OCC1";

/// Error decoding OCC1 canonical bytes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("OCC1 input too short")]
    TooShort,
    #[error("invalid OCC1 magic bytes")]
    BadMagic,
    #[error("unsupported canonical version {0}")]
    UnsupportedVersion(u16),
    #[error("invalid UTF-8 in OCC1 string")]
    InvalidUtf8,
    #[error("unknown value type tag {0}")]
    UnknownValueType(u8),
}

/// Decoded OCC1 canonical record. Borrows from the input bytes.
#[derive(Debug)]
pub struct DecodedOcc1<'a> {
    pub canonical_version: u16,
    pub scrub_policy_version: u16,
    pub deployment_id: &'a [u8; 16],
    pub app_identity_digest: &'a [u8; 32],
    pub event_id: &'a [u8; 16],
    pub severity_number: i32,
    pub severity_text: &'a str,
    pub event_name: &'a str,
    pub body: DecodedValue<'a>,
    pub attributes: Vec<(&'a str, DecodedValue<'a>)>,
}

impl<'a> DecodedOcc1<'a> {
    /// Find an attribute by key. Returns `None` if absent.
    pub fn attribute(&self, key: &str) -> Option<&DecodedValue<'a>> {
        self.attributes
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v)
    }

    /// Convenience: get an attribute as a string, or `None`.
    pub fn string_attribute(&self, key: &str) -> Option<&'a str> {
        match self.attribute(key) {
            Some(DecodedValue::String(s)) => Some(s),
            _ => None,
        }
    }
}

/// A decoded typed value from the OCC1 format.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedValue<'a> {
    Null,
    String(&'a str),
    Bool(bool),
    Int64(i64),
    Double(f64),
    Bytes(&'a [u8]),
    Array(Vec<DecodedValue<'a>>),
    Map(Vec<(&'a str, DecodedValue<'a>)>),
}

/// Decode an OCC1 canonical byte slice.
pub fn decode(data: &[u8]) -> Result<DecodedOcc1<'_>, DecodeError> {
    let mut cursor = Cursor::new(data);

    // Magic
    let magic = cursor.take(4)?;
    if magic != OCC_MAGIC {
        return Err(DecodeError::BadMagic);
    }

    // Versions
    let canonical_version = cursor.u16()?;
    if canonical_version != 1 {
        return Err(DecodeError::UnsupportedVersion(canonical_version));
    }
    let scrub_policy_version = cursor.u16()?;

    // Identity
    let deployment_id = cursor.fixed::<16>()?;
    let app_identity_digest = cursor.fixed::<32>()?;
    let event_id = cursor.fixed::<16>()?;

    // Resource (skip schema_url, dropped_count, attributes)
    cursor.skip_str()?; // resource_schema_url
    cursor.skip(4)?; // resource_dropped_attributes_count
    cursor.skip_value()?; // resource_attributes

    // Scope (all optional)
    cursor.skip_option_str()?; // scope_name
    cursor.skip_option_str()?; // scope_version
    cursor.skip_option_u32()?; // scope_dropped_attributes_count
    cursor.skip_option_value()?; // scope_attributes
    cursor.skip_str()?; // scope_schema_url

    // Timestamps (skip)
    cursor.skip(8)?; // time_unix_nano
    cursor.skip(8)?; // observed_time_unix_nano

    // Severity
    let severity_number = cursor.i32()?;
    let severity_text = cursor.str()?;

    // Event
    let event_name = cursor.str()?;
    let body = cursor.value()?;

    // Dropped attributes count (skip)
    cursor.skip(4)?;

    // Attributes — the main payload for fingerprinting
    let attributes = match cursor.value()? {
        DecodedValue::Map(pairs) => pairs,
        _ => Vec::new(),
    };

    Ok(DecodedOcc1 {
        canonical_version,
        scrub_policy_version,
        deployment_id,
        app_identity_digest,
        event_id,
        severity_number,
        severity_text,
        event_name,
        body,
        attributes,
    })
}

// ---------------------------------------------------------------------------
// Internal cursor for walking the byte slice
// ---------------------------------------------------------------------------

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.remaining() < n {
            return Err(DecodeError::TooShort);
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn skip(&mut self, n: usize) -> Result<(), DecodeError> {
        self.take(n)?;
        Ok(())
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32_val(&mut self) -> Result<u32, DecodeError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        let bytes = self.take(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn fixed<const N: usize>(&mut self) -> Result<&'a [u8; N], DecodeError> {
        let slice = self.take(N)?;
        Ok(slice.try_into().unwrap())
    }

    fn str(&mut self) -> Result<&'a str, DecodeError> {
        let len = self.u32_val()? as usize;
        let bytes = self.take(len)?;
        str::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)
    }

    fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.u32_val()? as usize;
        self.take(len)
    }

    fn skip_str(&mut self) -> Result<(), DecodeError> {
        let len = self.u32_val()? as usize;
        self.skip(len)
    }

    fn option_tag(&mut self) -> Result<bool, DecodeError> {
        let tag = self.take(1)?[0];
        Ok(tag != 0)
    }

    fn skip_option_str(&mut self) -> Result<(), DecodeError> {
        if self.option_tag()? {
            self.skip_str()?;
        }
        Ok(())
    }

    fn skip_option_u32(&mut self) -> Result<(), DecodeError> {
        if self.option_tag()? {
            self.skip(4)?;
        }
        Ok(())
    }

    fn skip_option_value(&mut self) -> Result<(), DecodeError> {
        if self.option_tag()? {
            self.skip_value()?;
        }
        Ok(())
    }

    fn skip_value(&mut self) -> Result<(), DecodeError> {
        // Values are length-prefixed: [u32 payload_len][payload]
        let payload_len = self.u32_val()? as usize;
        self.skip(payload_len)
    }

    fn value(&mut self) -> Result<DecodedValue<'a>, DecodeError> {
        let payload_len = self.u32_val()? as usize;
        let end = self.pos + payload_len;
        if end > self.data.len() {
            return Err(DecodeError::TooShort);
        }
        let result = self.decode_value_body()?;
        // Ensure we consumed exactly the declared payload
        if self.pos != end {
            // Silently advance to the declared end for forward compatibility
            self.pos = end;
        }
        Ok(result)
    }

    fn decode_value_body(&mut self) -> Result<DecodedValue<'a>, DecodeError> {
        let type_tag = self.take(1)?[0];
        match type_tag {
            0 => Ok(DecodedValue::Null),
            1 => {
                let s = self.str()?;
                Ok(DecodedValue::String(s))
            }
            2 => {
                let b = self.take(1)?[0];
                Ok(DecodedValue::Bool(b != 0))
            }
            3 => {
                let bytes = self.take(8)?;
                let v = i64::from_be_bytes(bytes.try_into().unwrap());
                Ok(DecodedValue::Int64(v))
            }
            4 => {
                let bytes = self.take(8)?;
                let bits = u64::from_be_bytes(bytes.try_into().unwrap());
                Ok(DecodedValue::Double(f64::from_bits(bits)))
            }
            5 => {
                let b = self.bytes()?;
                Ok(DecodedValue::Bytes(b))
            }
            6 => {
                let count = self.u32_val()? as usize;
                let mut items = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    items.push(self.value()?);
                }
                Ok(DecodedValue::Array(items))
            }
            7 => {
                let count = self.u32_val()? as usize;
                let mut pairs = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    let key = self.str()?;
                    let val = self.value()?;
                    pairs.push((key, val));
                }
                Ok(DecodedValue::Map(pairs))
            }
            other => Err(DecodeError::UnknownValueType(other)),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{extract, DeploymentId, Limits, Scratch, CANONICAL_VERSION, SCRUB_POLICY_VERSION};
    use scry_proto::{
        encode_log_record_v2_into, validate_logs_v2_record, LogRecordInput,
        LogsV2AnyValueInput as Value, LogsV2DecodeLimits, LogsV2KeyValueInput as Kv,
    };

    pub(crate) fn make_occurrence(
        exception_type: &str,
        exception_message: &str,
    ) -> crate::Occurrence {
        let resource = [Kv {
            key: "service.name",
            value: Value::String("test-service"),
        }];
        let mut attributes = vec![
            Kv {
                key: "exception.type",
                value: Value::String(exception_type),
            },
            Kv {
                key: "exception.message",
                value: Value::String(exception_message),
            },
            Kv {
                key: "scry.event.id",
                value: Value::String("018f1f8e-7b2c-7a91-8123-0123456789ab"),
            },
        ];
        attributes.sort_unstable_by_key(|entry| entry.key.as_bytes().to_vec());
        let input = LogRecordInput {
            resource_schema_url: "",
            resource_dropped_attributes_count: 0,
            resource_attributes: &resource,
            scope: None,
            scope_schema_url: "",
            time_unix_nano: 1_700_000_000_000_000_000,
            observed_time_unix_nano: 1_700_000_000_000_000_100,
            severity: 17,
            severity_text: "ERROR",
            event_name: "exception",
            body: Value::String("request failed"),
            dropped_attributes_count: 0,
            attributes: &attributes,
            trace_flags: 1,
            trace_id: Some(&[5; 16]),
            span_id: Some(&[6; 8]),
        };
        let mut encoded = Vec::new();
        encode_log_record_v2_into(&input, LogsV2DecodeLimits::default(), &mut encoded).unwrap();
        let record = validate_logs_v2_record(&encoded, LogsV2DecodeLimits::default()).unwrap();
        let deployment =
            DeploymentId::parse("018f1f8e-7b2c-7a91-8123-abcdef012345", "test").unwrap();
        extract(
            record,
            deployment,
            Limits::default(),
            &mut Scratch::default(),
        )
        .unwrap()
    }

    #[test]
    fn round_trip_extracts_exception_fields() {
        let occ = make_occurrence("DatabaseError", "connection refused");
        let decoded = decode(&occ.canonical).unwrap();

        assert_eq!(decoded.canonical_version, CANONICAL_VERSION);
        assert_eq!(decoded.scrub_policy_version, SCRUB_POLICY_VERSION);
        assert_eq!(decoded.event_name, "exception");
        assert_eq!(decoded.severity_number, 17);
        assert_eq!(decoded.severity_text, "ERROR");
        assert_eq!(
            decoded.string_attribute("exception.type"),
            Some("DatabaseError")
        );
        assert_eq!(
            decoded.string_attribute("exception.message"),
            Some("connection refused")
        );
        assert_eq!(
            decoded.string_attribute("scry.event.id"),
            Some("018f1f8e-7b2c-7a91-8123-0123456789ab")
        );
        assert!(matches!(
            decoded.body,
            DecodedValue::String("request failed")
        ));
    }

    #[test]
    fn rejects_bad_magic_and_short_input() {
        assert_eq!(decode(b"").unwrap_err(), DecodeError::TooShort);
        assert_eq!(decode(b"OCC").unwrap_err(), DecodeError::TooShort);
        assert_eq!(decode(b"BADM").unwrap_err(), DecodeError::BadMagic);
        let mut bad_version = b"OCC1".to_vec();
        bad_version.extend_from_slice(&99_u16.to_be_bytes());
        bad_version.extend_from_slice(&1_u16.to_be_bytes());
        // Pad enough for the version read
        bad_version.resize(100, 0);
        assert_eq!(
            decode(&bad_version).unwrap_err(),
            DecodeError::UnsupportedVersion(99)
        );
    }

    #[test]
    fn identity_bytes_match_occurrence() {
        let occ = make_occurrence("TypeError", "null is not a function");
        let decoded = decode(&occ.canonical).unwrap();

        assert_eq!(decoded.deployment_id, occ.deployment_id.as_bytes());
        assert_eq!(decoded.app_identity_digest, &occ.app.digest);
        assert_eq!(decoded.event_id, occ.event_id.as_bytes());
    }
}
