//! Fingerprint v2: deterministic error grouping from exception type and
//! normalized message.
//!
//! The fingerprint is a SHA-256 digest of domain-tagged, length-prefixed
//! canonical components. Message normalization replaces volatile tokens
//! (UUIDs, timestamps, clock times, hex identifiers, URLs, addresses and
//! numbers) with typed placeholders so semantically identical errors produce
//! the same digest regardless of per-occurrence dynamic values.
//!
//! # Version history
//!
//! * **v1** (`fp-v1`): type+message, type-only, or body/severity fallback;
//!   one generic `{…}` placeholder. A message-only exception (no
//!   `exception.type`) fell through to the body fallback, and the normalizer
//!   missed space-separated timestamps, bare clock times, and hex identifiers
//!   without a `0x` prefix.
//! * **v2** (`fp-v2`): adds [`GroupingQuality::MessageOnly`], typed
//!   placeholders (`{uuid}`, `{timestamp}`, `{time}`, `{url}`, `{hex}`, `{ip}`,
//!   `{float}`, `{int}`), the missed token classes above, and the grouping
//!   quality as a digest component so a message-only fingerprint can never
//!   equal a body fallback with the same text. Changing any of these rules
//!   requires a new version: the version is part of the digest and of the
//!   derived issue identity, and the grouping generation string names it.

use std::sync::OnceLock;

use regex::Regex;
use sha2::{Digest, Sha256};

use crate::occ1::{self, DecodeError, DecodedOcc1, DecodedValue};

const FINGERPRINT_DOMAIN_TAG: &[u8] = b"scry.fingerprint.v1\0";
/// Fingerprint algorithm version. Included in every digest and issue identity.
pub const FINGERPRINT_VERSION: u16 = 2;
/// Maximum UTF-8 byte length of a stored issue title. Longer titles are cut
/// on a character boundary and end in `…`. The fingerprint digest always uses
/// the complete normalized text; only the display title is bounded.
pub const MAX_TITLE_BYTES: usize = 512;

/// Grouping quality — how much useful signal the fingerprint has.
///
/// The numeric codes are persisted and sent to clients; they are stable and
/// deliberately not ordered by quality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GroupingQuality {
    /// Exception type + normalized message.
    TypeAndMessage = 0,
    /// Exception type only (message was empty or absent).
    TypeOnly = 1,
    /// No useful exception data; app-scoped fallback bucket.
    Fallback = 2,
    /// Normalized exception message without an exception type (fp-v2+).
    MessageOnly = 3,
}

impl GroupingQuality {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::TypeAndMessage),
            1 => Some(Self::TypeOnly),
            2 => Some(Self::Fallback),
            3 => Some(Self::MessageOnly),
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
    /// Human-readable title for the issue, at most [`MAX_TITLE_BYTES`].
    pub title: String,
}

/// Compute a fingerprint from OCC1 canonical bytes.
pub fn fingerprint_canonical(
    canonical: &[u8],
    app_identity_digest: &[u8; 32],
) -> Result<FingerprintResult, DecodeError> {
    let decoded = occ1::decode(canonical)?;
    Ok(fingerprint_from_decoded(&decoded, app_identity_digest))
}

/// The fingerprint inputs selected from one decoded occurrence.
enum Selection<'a> {
    TypeAndMessage { ty: &'a str, message: &'a str },
    TypeOnly { ty: &'a str },
    MessageOnly { message: &'a str },
    FallbackBody { body: &'a str },
    FallbackSeverity { severity_text: &'a str },
}

fn select<'a>(decoded: &DecodedOcc1<'a>) -> Selection<'a> {
    let ty = decoded.string_attribute("exception.type").unwrap_or("");
    let message = decoded.string_attribute("exception.message").unwrap_or("");
    match (ty.is_empty(), message.is_empty()) {
        (false, false) => Selection::TypeAndMessage { ty, message },
        (false, true) => Selection::TypeOnly { ty },
        (true, false) => Selection::MessageOnly { message },
        (true, true) => match &decoded.body {
            DecodedValue::String(body) if !body.trim().is_empty() => {
                Selection::FallbackBody { body }
            }
            _ => Selection::FallbackSeverity {
                severity_text: decoded.severity_text,
            },
        },
    }
}

/// Compute a fingerprint from an already-decoded OCC1 record.
///
/// Allocates only the normalized message (when one is used) and the bounded
/// title; explanation components are produced separately by
/// [`explain_fingerprint`] so the grouping hot path does not build them.
pub fn fingerprint_from_decoded(
    decoded: &DecodedOcc1<'_>,
    app_identity_digest: &[u8; 32],
) -> FingerprintResult {
    let (quality, digest, title) = match select(decoded) {
        Selection::TypeAndMessage { ty, message } => {
            let normalized = normalize_message(message);
            (
                GroupingQuality::TypeAndMessage,
                compute_digest(
                    app_identity_digest,
                    GroupingQuality::TypeAndMessage,
                    Some(ty),
                    Some(&normalized),
                ),
                bounded_title(ty),
            )
        }
        Selection::TypeOnly { ty } => (
            GroupingQuality::TypeOnly,
            compute_digest(
                app_identity_digest,
                GroupingQuality::TypeOnly,
                Some(ty),
                None,
            ),
            bounded_title(ty),
        ),
        Selection::MessageOnly { message } => {
            let normalized = normalize_message(message);
            let digest = compute_digest(
                app_identity_digest,
                GroupingQuality::MessageOnly,
                None,
                Some(&normalized),
            );
            (
                GroupingQuality::MessageOnly,
                digest,
                bounded_owned(normalized),
            )
        }
        Selection::FallbackBody { body } => {
            let normalized = normalize_message(body);
            let digest = compute_digest(
                app_identity_digest,
                GroupingQuality::Fallback,
                None,
                Some(&normalized),
            );
            (GroupingQuality::Fallback, digest, bounded_owned(normalized))
        }
        Selection::FallbackSeverity { severity_text } => {
            let label = format!("unknown error ({severity_text})");
            let digest = compute_digest(
                app_identity_digest,
                GroupingQuality::Fallback,
                None,
                Some(&label),
            );
            (GroupingQuality::Fallback, digest, bounded_owned(label))
        }
    };
    FingerprintResult {
        digest,
        version: FINGERPRINT_VERSION,
        quality,
        title,
    }
}

/// Explain which components [`fingerprint_from_decoded`] used, for display.
pub fn explain_fingerprint(decoded: &DecodedOcc1<'_>) -> Vec<FingerprintComponent> {
    let component = |kind, value: String, included, reason| FingerprintComponent {
        kind,
        value,
        included,
        reason,
    };
    match select(decoded) {
        Selection::TypeAndMessage { ty, message } => vec![
            component(
                ComponentKind::ExceptionType,
                ty.to_owned(),
                true,
                "primary grouping component",
            ),
            component(
                ComponentKind::ExceptionMessage,
                normalize_message(message),
                true,
                "normalized message",
            ),
        ],
        Selection::TypeOnly { ty } => vec![
            component(
                ComponentKind::ExceptionType,
                ty.to_owned(),
                true,
                "primary grouping component",
            ),
            component(
                ComponentKind::ExceptionMessage,
                String::new(),
                false,
                "empty message excluded",
            ),
        ],
        Selection::MessageOnly { message } => vec![
            component(
                ComponentKind::ExceptionType,
                String::new(),
                false,
                "no exception type",
            ),
            component(
                ComponentKind::ExceptionMessage,
                normalize_message(message),
                true,
                "normalized message without exception type",
            ),
        ],
        Selection::FallbackBody { body } => vec![component(
            ComponentKind::Body,
            normalize_message(body),
            true,
            "fallback: no exception type or message, using normalized body",
        )],
        Selection::FallbackSeverity { severity_text } => vec![component(
            ComponentKind::Severity,
            severity_text.to_owned(),
            true,
            "fallback: no exception type, message, or body",
        )],
    }
}

fn compute_digest(
    app_identity_digest: &[u8; 32],
    quality: GroupingQuality,
    exception_type: Option<&str>,
    normalized_message: Option<&str>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(FINGERPRINT_DOMAIN_TAG);
    hasher.update(app_identity_digest);
    hasher.update(FINGERPRINT_VERSION.to_be_bytes());
    // v2: the selection kind is a component, so equal text selected by
    // different rules (message-only vs. body fallback) never shares an issue.
    hasher.update([quality as u8]);

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

fn bounded_title(value: &str) -> String {
    if value.len() <= MAX_TITLE_BYTES {
        return value.to_owned();
    }
    let mut title = String::with_capacity(MAX_TITLE_BYTES);
    title.push_str(&value[..title_cut(value)]);
    title.push('…');
    title
}

fn bounded_owned(mut value: String) -> String {
    if value.len() > MAX_TITLE_BYTES {
        let cut = title_cut(&value);
        value.truncate(cut);
        value.push('…');
    }
    value
}

/// Largest char boundary that leaves room for the 3-byte ellipsis.
fn title_cut(value: &str) -> usize {
    let mut cut = MAX_TITLE_BYTES - '…'.len_utf8();
    while !value.is_char_boundary(cut) {
        cut -= 1;
    }
    cut
}

// ---------------------------------------------------------------------------
// Message normalization
// ---------------------------------------------------------------------------

/// One pattern for every volatile token class. Alternation is leftmost-first,
/// so more specific shapes precede the generic numeric ones. Word boundaries
/// are ASCII (`(?-u:\b)`) so matching stays on the fast DFA path for non-ASCII
/// messages. The regex crate has no look-around; [`classify`] decides the
/// token type from the matched text (and keeps letter-only hex-alphabet words
/// such as `deadbeef` or `facade` verbatim).
fn normalizer() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(concat!(
            // UUIDs (hyphenated)
            r"(?-u:\b)[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?-u:\b)",
            // Dates with an optional `T` or space separated time, fraction and zone:
            // 2024-01-15, 2024-01-15T10:30:00Z, 2024-01-15 10:30:00.123+02:00
            r"|(?-u:\b)[0-9]{4}-[0-9]{2}-[0-9]{2}(?:[T ][0-9]{2}:[0-9]{2}(?::[0-9]{2})?(?:[.,][0-9]+)?(?:Z|[+-][0-9]{2}:?[0-9]{2})?)?(?-u:\b)",
            // Bare clock times with seconds: 10:30:00, 7:05:09.123
            r"|(?-u:\b)[0-9]{1,2}:[0-9]{2}:[0-9]{2}(?:[.,][0-9]+)?(?-u:\b)",
            // URLs with any scheme
            r"|(?-u:\b)[a-zA-Z][a-zA-Z0-9+.-]*://[^\s]+",
            // 0x-prefixed hex
            r"|(?-u:\b)0[xX][0-9a-fA-F]+(?-u:\b)",
            // Bare hex runs of at least 8 characters (IDs, hashes, addresses)
            r"|(?-u:\b)[0-9a-fA-F]{8,}(?-u:\b)",
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

/// Map one normalizer match to its typed placeholder, or `None` to keep the
/// text (a hex-alphabet word without digits).
fn classify(token: &str) -> Option<&'static str> {
    let bytes = token.as_bytes();
    if token.contains("://") {
        return Some("{url}");
    }
    if bytes.len() == 36 && bytes[8] == b'-' && bytes[13] == b'-' && bytes[18] == b'-' {
        return Some("{uuid}");
    }
    if bytes.len() >= 10 && bytes[4] == b'-' && bytes[7] == b'-' {
        return Some("{timestamp}");
    }
    if bytes.len() > 2 && bytes[0] == b'0' && (bytes[1] == b'x' || bytes[1] == b'X') {
        return Some("{hex}");
    }
    let dots = bytes.iter().filter(|&&b| b == b'.').count();
    if dots >= 2 {
        return Some("{ip}");
    }
    if bytes.contains(&b':') {
        return Some("{time}");
    }
    if dots == 1 {
        return Some("{float}");
    }
    if bytes.iter().all(u8::is_ascii_digit) {
        return Some("{int}");
    }
    if bytes.iter().any(u8::is_ascii_digit) {
        return Some("{hex}");
    }
    None
}

/// Replace volatile tokens in an error message with typed placeholders and
/// collapse whitespace, in one pass over the input.
pub fn normalize_message(message: &str) -> String {
    let mut result = String::with_capacity(message.len());
    let mut prev_space = true;
    let mut last = 0;
    for m in normalizer().find_iter(message) {
        push_collapsed(&mut result, &message[last..m.start()], &mut prev_space);
        push_collapsed(
            &mut result,
            classify(m.as_str()).unwrap_or(m.as_str()),
            &mut prev_space,
        );
        last = m.end();
    }
    push_collapsed(&mut result, &message[last..], &mut prev_space);
    result.truncate(result.trim_end().len());
    result
}

fn push_collapsed(out: &mut String, text: &str, prev_space: &mut bool) {
    for ch in text.chars() {
        if ch.is_whitespace() {
            if !*prev_space {
                out.push(' ');
                *prev_space = true;
            }
        } else {
            out.push(ch);
            *prev_space = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::occ1::tests::{make_occurrence, make_occurrence_with};

    #[test]
    fn normalization_replaces_volatile_tokens() {
        let msg = "connection to 10.0.0.1:5432 failed after 12345ms: \
                   request 018f1f8e-7b2c-7a91-8123-0123456789ab timed out \
                   at 2024-01-15T10:30:00Z see https://example.com/err/42";
        assert_eq!(
            normalize_message(msg),
            "connection to {ip} failed after {int}ms: request {uuid} timed out \
             at {timestamp} see {url}"
        );
    }

    #[test]
    fn normalization_collapses_whitespace() {
        assert_eq!(normalize_message("  hello   world  "), "hello world");
    }

    #[test]
    fn normalization_replaces_hex_and_floats() {
        assert_eq!(
            normalize_message("hash 0xdeadbeef01 value 3.14159 code 0xABCDEF01"),
            "hash {hex} value {float} code {hex}"
        );
    }

    #[test]
    fn normalization_replaces_space_separated_timestamps() {
        for msg in [
            "deadline 2024-01-15 10:30:00 exceeded",
            "deadline 2024-01-15 10:30:00.123456 exceeded",
            "deadline 2024-01-15 10:30:00,5+02:00 exceeded",
            "deadline 2024-01-15 10:30 exceeded",
            "deadline 2024-01-15 exceeded",
        ] {
            assert_eq!(
                normalize_message(msg),
                "deadline {timestamp} exceeded",
                "{msg}"
            );
        }
    }

    #[test]
    fn normalization_replaces_bare_clock_times() {
        assert_eq!(
            normalize_message("job started 10:30:00 stalled at 7:05:09.123"),
            "job started {time} stalled at {time}"
        );
        // Without seconds a colon pair is not assumed to be a time.
        assert_eq!(normalize_message("ratio 3:4 invalid"), "ratio 3:4 invalid");
    }

    #[test]
    fn normalization_replaces_bare_hex_ids_with_min_length_and_boundaries() {
        assert_eq!(
            normalize_message("object 5f2b9c1ad3e4 missing in shard a1b2c3d4"),
            "object {hex} missing in shard {hex}"
        );
        // Letter-only hex-alphabet words are ordinary words.
        assert_eq!(
            normalize_message("deadbeef facade accede"),
            "deadbeef facade accede"
        );
        // Shorter than eight characters: kept.
        assert_eq!(normalize_message("code a1b2c3 bad"), "code a1b2c3 bad");
        // Hex embedded in a longer word is not split out at a non-boundary.
        assert_eq!(
            normalize_message("tokenzz5f2b9c1ad3e4 bad"),
            "tokenzz5f2b9c1ad3e4 bad"
        );
        // A long pure-digit run is an integer, not hex.
        assert_eq!(normalize_message("order 12345678"), "order {int}");
    }

    #[test]
    fn normalization_keeps_unicode_text_intact() {
        assert_eq!(
            normalize_message("échec de connexion après 12345 ms — réessayer"),
            "échec de connexion après {int} ms — réessayer"
        );
    }

    #[test]
    fn same_type_and_message_produce_same_fingerprint() {
        let app = [0x42_u8; 32];
        let q = GroupingQuality::TypeAndMessage;
        let fp1 = compute_digest(&app, q, Some("TypeError"), Some("null is not a function"));
        let fp2 = compute_digest(&app, q, Some("TypeError"), Some("null is not a function"));
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn different_volatile_values_normalize_to_same_fingerprint() {
        let app = [0x42_u8; 32];
        let q = GroupingQuality::TypeAndMessage;
        let msg1 = "connection to 10.0.0.1 failed after 12345ms at 2024-01-15 10:30:00";
        let msg2 = "connection to 192.168.1.1 failed after 67890ms at 2025-11-02 23:01:59";
        let fp1 = compute_digest(&app, q, Some("DbError"), Some(&normalize_message(msg1)));
        let fp2 = compute_digest(&app, q, Some("DbError"), Some(&normalize_message(msg2)));
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn different_types_produce_different_fingerprints() {
        let app = [0x42_u8; 32];
        let q = GroupingQuality::TypeAndMessage;
        let fp1 = compute_digest(&app, q, Some("TypeError"), Some("test"));
        let fp2 = compute_digest(&app, q, Some("ValueError"), Some("test"));
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn different_apps_produce_different_fingerprints() {
        let q = GroupingQuality::TypeAndMessage;
        let fp1 = compute_digest(&[0x42_u8; 32], q, Some("TypeError"), Some("test"));
        let fp2 = compute_digest(&[0x43_u8; 32], q, Some("TypeError"), Some("test"));
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn type_only_differs_from_type_with_message() {
        let app = [0x42_u8; 32];
        let with_msg = compute_digest(
            &app,
            GroupingQuality::TypeAndMessage,
            Some("TypeError"),
            Some("hello"),
        );
        let without_msg = compute_digest(&app, GroupingQuality::TypeOnly, Some("TypeError"), None);
        assert_ne!(with_msg, without_msg);
    }

    #[test]
    fn message_only_differs_from_body_fallback_with_same_text() {
        let app = [0x42_u8; 32];
        let message = compute_digest(&app, GroupingQuality::MessageOnly, None, Some("boom"));
        let body = compute_digest(&app, GroupingQuality::Fallback, None, Some("boom"));
        assert_ne!(message, body);
    }

    #[test]
    fn quality_levels_from_occ1() {
        let occ = make_occurrence("TypeError", "null is not a function");
        let result = fingerprint_canonical(&occ.canonical, &occ.app.digest).unwrap();
        assert_eq!(result.quality, GroupingQuality::TypeAndMessage);
        assert_eq!(result.title, "TypeError");
        assert_eq!(result.version, FINGERPRINT_VERSION);
        assert!(!result.digest.iter().all(|b| *b == 0));

        let occ = make_occurrence_with(Some("TypeError"), None, 17);
        let result = fingerprint_canonical(&occ.canonical, &occ.app.digest).unwrap();
        assert_eq!(result.quality, GroupingQuality::TypeOnly);
        assert_eq!(result.title, "TypeError");
    }

    #[test]
    fn message_only_exception_groups_by_normalized_message() {
        let first = make_occurrence_with(None, Some("user 12345 not found"), 17);
        let second = make_occurrence_with(None, Some("user 67890 not found"), 17);
        let other = make_occurrence_with(None, Some("quota exceeded"), 17);
        let a = fingerprint_canonical(&first.canonical, &first.app.digest).unwrap();
        let b = fingerprint_canonical(&second.canonical, &second.app.digest).unwrap();
        let c = fingerprint_canonical(&other.canonical, &other.app.digest).unwrap();
        assert_eq!(a.quality, GroupingQuality::MessageOnly);
        assert_eq!(a.title, "user {int} not found");
        assert_eq!(a.digest, b.digest);
        assert_ne!(a.digest, c.digest);
    }

    #[test]
    fn fallback_uses_body_then_severity() {
        let occ = make_occurrence("X", "y");
        let mut decoded = occ1::decode(&occ.canonical).unwrap();
        decoded.attributes.clear();
        let body = fingerprint_from_decoded(&decoded, &occ.app.digest);
        assert_eq!(body.quality, GroupingQuality::Fallback);
        assert_eq!(body.title, "request failed");

        decoded.body = DecodedValue::Null;
        let severity = fingerprint_from_decoded(&decoded, &occ.app.digest);
        assert_eq!(severity.quality, GroupingQuality::Fallback);
        assert_eq!(severity.title, "unknown error (ERROR)");
        assert_ne!(body.digest, severity.digest);
    }

    #[test]
    fn titles_are_bounded_on_char_boundaries() {
        let long_type = "é".repeat(MAX_TITLE_BYTES);
        let occ = make_occurrence_with(Some(&long_type), None, 17);
        let result = fingerprint_canonical(&occ.canonical, &occ.app.digest).unwrap();
        assert!(result.title.len() <= MAX_TITLE_BYTES);
        assert!(result.title.ends_with('…'));

        let long_message = "word ".repeat(MAX_TITLE_BYTES);
        let occ = make_occurrence_with(None, Some(&long_message), 17);
        let result = fingerprint_canonical(&occ.canonical, &occ.app.digest).unwrap();
        assert!(result.title.len() <= MAX_TITLE_BYTES);
        assert!(result.title.ends_with('…'));
        // The digest still covers the full message: a different tail differs.
        let mut other = long_message.clone();
        other.push_str("tail");
        let occ2 = make_occurrence_with(None, Some(&other), 17);
        let result2 = fingerprint_canonical(&occ2.canonical, &occ2.app.digest).unwrap();
        assert_eq!(result.title, result2.title);
        assert_ne!(result.digest, result2.digest);
    }

    #[test]
    fn explanation_matches_selection() {
        let occ = make_occurrence_with(None, Some("id 12345"), 17);
        let decoded = occ1::decode(&occ.canonical).unwrap();
        let components = explain_fingerprint(&decoded);
        assert_eq!(components.len(), 2);
        assert!(!components[0].included);
        assert_eq!(components[1].value, "id {int}");
    }
}
