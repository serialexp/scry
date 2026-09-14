//! Fingerprint v1: deterministic error grouping from exception type and
//! normalized message.
//!
//! The fingerprint is a SHA-256 digest of domain-tagged, length-prefixed
//! canonical components. Message normalization replaces volatile tokens
//! (UUIDs, numbers, timestamps, hex strings, URLs) with typed placeholders
//! so semantically identical errors produce the same digest regardless of
//! per-occurrence dynamic values.

use std::sync::OnceLock;

use regex::Regex;
use sha2::{Digest, Sha256};

use crate::occ1::{self, DecodeError, DecodedOcc1, DecodedValue};

const FINGERPRINT_DOMAIN_TAG: &[u8] = b"scry.fingerprint.v1\0";
pub const FINGERPRINT_VERSION: u16 = 1;

/// Grouping quality — how much useful signal the fingerprint has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GroupingQuality {
    /// Exception type + normalized message.
    TypeAndMessage = 0,
    /// Exception type only (message was empty or absent).
    TypeOnly = 1,
    /// No useful exception data; app-scoped fallback bucket.
    Fallback = 2,
}

impl GroupingQuality {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::TypeAndMessage),
            1 => Some(Self::TypeOnly),
            2 => Some(Self::Fallback),
            _ => None,
        }
    }
}

/// One component included in (or excluded from) the fingerprint, recorded
/// for later explanation in the UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FingerprintComponent {
    pub kind: ComponentKind,
    pub value: String,
    pub included: bool,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentKind {
    ExceptionType,
    ExceptionMessage,
    Body,
    Severity,
}

/// The result of fingerprinting one occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FingerprintResult {
    /// The 32-byte SHA-256 fingerprint digest.
    pub digest: [u8; 32],
    /// Algorithm version.
    pub version: u16,
    /// How much useful signal went into the fingerprint.
    pub quality: GroupingQuality,
    /// Human-readable title for the issue (exception type, or fallback).
    pub title: String,
    /// Components considered, with inclusion/exclusion reasons.
    pub components: Vec<FingerprintComponent>,
}

/// Compute a fingerprint from OCC1 canonical bytes.
pub fn fingerprint_v1(
    canonical: &[u8],
    app_identity_digest: &[u8; 32],
) -> Result<FingerprintResult, DecodeError> {
    let decoded = occ1::decode(canonical)?;
    Ok(fingerprint_from_decoded(&decoded, app_identity_digest))
}

/// Compute a fingerprint from an already-decoded OCC1 record.
pub fn fingerprint_from_decoded(
    decoded: &DecodedOcc1<'_>,
    app_identity_digest: &[u8; 32],
) -> FingerprintResult {
    let exception_type = decoded.string_attribute("exception.type").unwrap_or("");
    let exception_message = decoded.string_attribute("exception.message").unwrap_or("");

    let mut components = Vec::new();

    // Determine quality and select components
    if !exception_type.is_empty() && !exception_message.is_empty() {
        let normalized = normalize_message(exception_message);
        components.push(FingerprintComponent {
            kind: ComponentKind::ExceptionType,
            value: exception_type.to_owned(),
            included: true,
            reason: "primary grouping component",
        });
        components.push(FingerprintComponent {
            kind: ComponentKind::ExceptionMessage,
            value: normalized.clone(),
            included: true,
            reason: "normalized message",
        });

        let digest = compute_digest(app_identity_digest, Some(exception_type), Some(&normalized));
        FingerprintResult {
            digest,
            version: FINGERPRINT_VERSION,
            quality: GroupingQuality::TypeAndMessage,
            title: exception_type.to_owned(),
            components,
        }
    } else if !exception_type.is_empty() {
        components.push(FingerprintComponent {
            kind: ComponentKind::ExceptionType,
            value: exception_type.to_owned(),
            included: true,
            reason: "primary grouping component",
        });
        if exception_message.is_empty() {
            components.push(FingerprintComponent {
                kind: ComponentKind::ExceptionMessage,
                value: String::new(),
                included: false,
                reason: "empty message excluded",
            });
        }

        let digest = compute_digest(app_identity_digest, Some(exception_type), None);
        FingerprintResult {
            digest,
            version: FINGERPRINT_VERSION,
            quality: GroupingQuality::TypeOnly,
            title: exception_type.to_owned(),
            components,
        }
    } else {
        // Fallback: use body text + severity as last resort
        let body_text = match &decoded.body {
            DecodedValue::String(s) => Some(*s),
            _ => None,
        };
        let fallback_label = if let Some(body) = body_text {
            let normalized = normalize_message(body);
            components.push(FingerprintComponent {
                kind: ComponentKind::Body,
                value: normalized.clone(),
                included: true,
                reason: "fallback: no exception type, using normalized body",
            });
            normalized
        } else {
            components.push(FingerprintComponent {
                kind: ComponentKind::Severity,
                value: decoded.severity_text.to_owned(),
                included: true,
                reason: "fallback: no exception type or body",
            });
            format!("unknown error ({})", decoded.severity_text)
        };

        // Fallback digest includes the body/severity so distinct messages
        // still get distinct issues rather than one giant bucket.
        let digest = compute_digest(app_identity_digest, None, Some(&fallback_label));
        FingerprintResult {
            digest,
            version: FINGERPRINT_VERSION,
            quality: GroupingQuality::Fallback,
            title: fallback_label,
            components,
        }
    }
}

fn compute_digest(
    app_identity_digest: &[u8; 32],
    exception_type: Option<&str>,
    normalized_message: Option<&str>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(FINGERPRINT_DOMAIN_TAG);
    hasher.update(app_identity_digest);
    hasher.update(FINGERPRINT_VERSION.to_be_bytes());

    // Exception type: presence tag + length-prefixed value
    if let Some(t) = exception_type {
        hasher.update([1_u8]);
        hasher.update((t.len() as u32).to_be_bytes());
        hasher.update(t.as_bytes());
    } else {
        hasher.update([0_u8]);
    }

    // Normalized message: presence tag + length-prefixed value
    if let Some(m) = normalized_message {
        hasher.update([1_u8]);
        hasher.update((m.len() as u32).to_be_bytes());
        hasher.update(m.as_bytes());
    } else {
        hasher.update([0_u8]);
    }

    hasher.finalize().into()
}

// ---------------------------------------------------------------------------
// Message normalization
// ---------------------------------------------------------------------------

fn normalizer() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(concat!(
            // UUIDs (hyphenated)
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            // 0x-prefixed hex
            r"|0x[0-9a-fA-F]+",
            // ISO timestamps (yyyy-mm-ddThh:mm:ss...) — before general numbers
            r"|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[^\s]*",
            // URLs
            r"|https?://[^\s]+",
            // IP-like dotted numbers (1.2.3.4, 10.0.0.1:5432)
            r"|[0-9]+(?:\.[0-9]+){2,}(?::[0-9]+)?",
            // Floating point numbers
            r"|[0-9]+\.[0-9]+",
            // Decimal numbers >= 4 digits (anywhere, not just word-bounded)
            r"|[0-9]{4,}",
        ))
        .unwrap()
    })
}

/// Replace volatile tokens in an error message with typed placeholders.
pub fn normalize_message(message: &str) -> String {
    let replaced = normalizer().replace_all(message, "{…}");
    // Collapse consecutive whitespace and trim
    let mut result = String::with_capacity(replaced.len());
    let mut prev_space = true;
    for ch in replaced.chars() {
        if ch.is_whitespace() {
            if !prev_space {
                result.push(' ');
                prev_space = true;
            }
        } else {
            result.push(ch);
            prev_space = false;
        }
    }
    result.truncate(result.trim_end().len());
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_replaces_volatile_tokens() {
        let msg = "connection to 10.0.0.1:5432 failed after 12345ms: \
                   request 018f1f8e-7b2c-7a91-8123-0123456789ab timed out \
                   at 2024-01-15T10:30:00Z see https://example.com/err/42";
        let normalized = normalize_message(msg);
        // UUIDs, timestamps, URLs, large numbers should all be replaced
        assert!(!normalized.contains("018f1f8e"));
        assert!(!normalized.contains("2024-01-15"));
        assert!(!normalized.contains("https://"));
        assert!(!normalized.contains("12345"));
        // But meaningful text remains
        assert!(normalized.contains("connection to"));
        assert!(normalized.contains("failed after"));
        assert!(normalized.contains("timed out"));
    }

    #[test]
    fn normalization_collapses_whitespace() {
        assert_eq!(normalize_message("  hello   world  "), "hello world");
    }

    #[test]
    fn normalization_replaces_hex_and_floats() {
        let msg = "hash 0xdeadbeef01 value 3.14159 code 0xABCDEF01";
        let normalized = normalize_message(msg);
        assert!(!normalized.contains("deadbeef"));
        assert!(!normalized.contains("3.14159"));
        assert!(!normalized.contains("ABCDEF"));
    }

    #[test]
    fn same_type_and_message_produce_same_fingerprint() {
        let app = [0x42_u8; 32];
        let fp1 = compute_digest(&app, Some("TypeError"), Some("null is not a function"));
        let fp2 = compute_digest(&app, Some("TypeError"), Some("null is not a function"));
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn different_volatile_values_normalize_to_same_fingerprint() {
        let app = [0x42_u8; 32];
        let msg1 = "connection to 10.0.0.1 failed after 12345ms";
        let msg2 = "connection to 192.168.1.1 failed after 67890ms";
        let fp1 = compute_digest(&app, Some("DbError"), Some(&normalize_message(msg1)));
        let fp2 = compute_digest(&app, Some("DbError"), Some(&normalize_message(msg2)));
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn different_types_produce_different_fingerprints() {
        let app = [0x42_u8; 32];
        let fp1 = compute_digest(&app, Some("TypeError"), Some("test"));
        let fp2 = compute_digest(&app, Some("ValueError"), Some("test"));
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn different_apps_produce_different_fingerprints() {
        let app1 = [0x42_u8; 32];
        let app2 = [0x43_u8; 32];
        let fp1 = compute_digest(&app1, Some("TypeError"), Some("test"));
        let fp2 = compute_digest(&app2, Some("TypeError"), Some("test"));
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn type_only_differs_from_type_with_message() {
        let app = [0x42_u8; 32];
        let with_msg = compute_digest(&app, Some("TypeError"), Some("hello"));
        let without_msg = compute_digest(&app, Some("TypeError"), None);
        assert_ne!(with_msg, without_msg);
    }

    #[test]
    fn quality_levels_from_occ1() {
        use crate::occ1::tests::make_occurrence;

        let occ = make_occurrence("TypeError", "null is not a function");
        let result = fingerprint_v1(&occ.canonical, &occ.app.digest).unwrap();
        assert_eq!(result.quality, GroupingQuality::TypeAndMessage);
        assert_eq!(result.title, "TypeError");
        assert_eq!(result.version, FINGERPRINT_VERSION);
        assert!(!result.digest.iter().all(|b| *b == 0));
    }
}
