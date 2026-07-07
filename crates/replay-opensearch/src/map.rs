//! Pure mapping from an OpenSearch document `_source` to a scry log record.
//!
//! The mapping is **convention-first, override-friendly**: sensible default
//! field names (`@timestamp`, `message`/`body`, `severity`/`log.level`/`level`,
//! `service.name`/`service`/`app`/`k8s_app`) are used unless the operator points
//! us at different ones. Everything else in the document becomes per-entry
//! attributes so a replay is a faithful copy, not a lossy projection.
//!
//! This module is deliberately free of I/O so the mapping is fully unit-tested;
//! the reader ([`crate::os`]) and sender ([`crate::wire`]) feed it `serde_json`
//! values and consume [`MappedRecord`]s.

use scry_proto::{fingerprint::fingerprint, generated::LogEntry, LabelPair};
use serde_json::Value;

/// Field-name preferences resolved from CLI flags. Each list is tried in order;
/// the first present, usable value wins.
#[derive(Debug, Clone)]
pub struct MappingConfig {
    /// Document field carrying the record timestamp (default `@timestamp`).
    pub timestamp_field: String,
    /// Candidate fields for the log body, in priority order.
    pub body_fields: Vec<String>,
    /// Candidate fields for the severity, in priority order.
    pub severity_fields: Vec<String>,
    /// Candidate fields for the service name, in priority order. The first
    /// present becomes the `service` stream label.
    pub service_fields: Vec<String>,
    /// Extra document fields to promote to (low-cardinality) stream labels.
    pub label_fields: Vec<String>,
    /// Cap on the number of per-entry attributes emitted from a single document
    /// (bounds pathological wide docs).
    pub max_attrs: usize,
}

impl Default for MappingConfig {
    fn default() -> Self {
        Self {
            timestamp_field: "@timestamp".to_string(),
            body_fields: vec!["message".to_string(), "body".to_string()],
            severity_fields: vec![
                "severity".to_string(),
                "log.level".to_string(),
                "level".to_string(),
            ],
            service_fields: vec![
                "service.name".to_string(),
                "service".to_string(),
                "app".to_string(),
                "k8s_app".to_string(),
            ],
            label_fields: Vec::new(),
            max_attrs: 64,
        }
    }
}

/// One mapped document: its stream identity (labels + fingerprint) and the log
/// entry itself. Consecutive records sharing a fingerprint are grouped into one
/// `LogStream` by the sender.
#[derive(Debug, Clone, PartialEq)]
pub struct MappedRecord {
    pub labels: Vec<LabelPair>,
    pub fingerprint: u64,
    pub entry: LogEntry,
}

/// Running tallies over a replay, surfaced in the stats line and final summary.
#[derive(Debug, Default, Clone, Copy)]
pub struct MapCounters {
    /// Documents whose timestamp was missing/unparseable and inherited the
    /// previous document's timestamp (carry-forward).
    pub ts_inherited: u64,
    /// Documents with no usable body field (empty body emitted).
    pub body_missing: u64,
}

/// Map one document `_source` to a [`MappedRecord`].
///
/// `carry_ts` is the running timestamp cursor: documents arrive in ascending
/// timestamp order (the PIT `search_after` sort), so a document missing its
/// timestamp inherits the last good one (monotonic, never travels backwards).
/// Before the first valid timestamp, the caller seeds `carry_ts` with
/// ingest-now.
pub fn doc_to_record(
    source: &Value,
    cfg: &MappingConfig,
    carry_ts: &mut u64,
    counters: &mut MapCounters,
) -> MappedRecord {
    // Track which document fields were consumed so they don't reappear as
    // attributes.
    let mut consumed: Vec<String> = Vec::new();

    // ── timestamp ──────────────────────────────────────────────────────
    let ts = match lookup(source, &cfg.timestamp_field) {
        Some(v) => match parse_timestamp(v) {
            Some(ns) => {
                consumed.push(cfg.timestamp_field.clone());
                *carry_ts = ns;
                ns
            }
            None => {
                counters.ts_inherited += 1;
                *carry_ts
            }
        },
        None => {
            counters.ts_inherited += 1;
            *carry_ts
        }
    };

    // ── body ───────────────────────────────────────────────────────────
    let mut body = String::new();
    for f in &cfg.body_fields {
        if let Some(v) = lookup(source, f) {
            body = scalar_to_string(v);
            consumed.push(f.clone());
            break;
        }
    }
    if body.is_empty() {
        counters.body_missing += 1;
    }

    // ── severity ───────────────────────────────────────────────────────
    let mut severity = 9u8; // INFO default
    for f in &cfg.severity_fields {
        if let Some(v) = lookup(source, f) {
            severity = parse_severity(v);
            consumed.push(f.clone());
            break;
        }
    }

    // ── service label ──────────────────────────────────────────────────
    let mut labels: Vec<LabelPair> = Vec::new();
    for f in &cfg.service_fields {
        if let Some(v) = lookup(source, f) {
            let s = scalar_to_string(v);
            if !s.is_empty() {
                labels.push(LabelPair {
                    key: "service".to_string(),
                    value: s,
                });
                consumed.push(f.clone());
                break;
            }
        }
    }

    // ── extra stream labels ────────────────────────────────────────────
    for f in &cfg.label_fields {
        if let Some(v) = lookup(source, f) {
            let s = scalar_to_string(v);
            labels.push(LabelPair {
                key: f.clone(),
                value: s,
            });
            consumed.push(f.clone());
        }
    }

    let fp = fingerprint(&labels);

    // ── attributes (everything else, flattened) ────────────────────────
    let mut attributes: Vec<LabelPair> = Vec::new();
    flatten_into(source, "", &consumed, cfg.max_attrs, &mut attributes);

    MappedRecord {
        labels,
        fingerprint: fp,
        entry: LogEntry {
            ts_unix_nano: ts,
            severity,
            body,
            attributes,
        },
    }
}

/// Look up a field, accepting either a flat key (`"service.name"` present
/// verbatim) or a dotted path into nested objects (`{service:{name:…}}`).
fn lookup<'a>(source: &'a Value, field: &str) -> Option<&'a Value> {
    if let Some(v) = source.get(field) {
        return Some(v);
    }
    if !field.contains('.') {
        return None;
    }
    let mut cur = source;
    for seg in field.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Render a scalar JSON value to a string. Objects/arrays are JSON-encoded so no
/// data is silently dropped.
fn scalar_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

/// Parse a timestamp value to unix nanoseconds. Accepts an RFC3339 string or an
/// epoch number whose unit (s / ms / µs / ns) is inferred by magnitude.
fn parse_timestamp(v: &Value) -> Option<u64> {
    match v {
        Value::String(s) => {
            let dt = chrono::DateTime::parse_from_rfc3339(s).ok()?;
            let ns = dt.timestamp_nanos_opt()?;
            u64::try_from(ns).ok()
        }
        Value::Number(n) => {
            let f = n.as_f64()?;
            if f <= 0.0 {
                return None;
            }
            Some(epoch_to_nanos(f))
        }
        _ => None,
    }
}

/// Infer the epoch unit from magnitude and convert to nanoseconds. Reference
/// point: 2001-09-09 is ~1e9 s, ~1e12 ms, ~1e15 µs, ~1e18 ns.
fn epoch_to_nanos(v: f64) -> u64 {
    // Thresholds sit an order of magnitude below each unit's "year ~2001+"
    // value so realistic timestamps land in the right bucket.
    if v < 1e11 {
        (v * 1e9) as u64 // seconds
    } else if v < 1e14 {
        (v * 1e6) as u64 // milliseconds
    } else if v < 1e17 {
        (v * 1e3) as u64 // microseconds
    } else {
        v as u64 // nanoseconds
    }
}

/// Map a severity value to the OTel 1–24 scale. A number in range is used
/// as-is; a string level maps to its band start (inverse of the tail's
/// `severity_name`); anything else falls back to INFO(9).
fn parse_severity(v: &Value) -> u8 {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_u64() {
                if (1..=24).contains(&i) {
                    return i as u8;
                }
            }
            9
        }
        Value::String(s) => severity_from_str(s),
        _ => 9,
    }
}

/// Map a textual level to an OTel severity number (band start), tolerating
/// common aliases and casing.
fn severity_from_str(s: &str) -> u8 {
    match s.trim().to_ascii_lowercase().as_str() {
        "trace" => 1,
        "debug" => 5,
        "info" | "information" | "notice" => 9,
        "warn" | "warning" => 13,
        "error" | "err" => 17,
        "fatal" | "crit" | "critical" | "alert" | "emerg" | "emergency" | "panic" => 21,
        _ => 9,
    }
}

/// Flatten `value` into dotted-key `(key, string)` attributes, appending to
/// `out`. Nested objects recurse (`a.b.c`); arrays are JSON-stringified whole.
/// Keys listed in `consumed` (exact dotted match) are skipped. Stops at
/// `max_attrs`.
fn flatten_into(
    value: &Value,
    prefix: &str,
    consumed: &[String],
    max_attrs: usize,
    out: &mut Vec<LabelPair>,
) {
    if out.len() >= max_attrs {
        return;
    }
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                if out.len() >= max_attrs {
                    return;
                }
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                if consumed.iter().any(|c| c == &key) {
                    continue;
                }
                match v {
                    Value::Object(_) => flatten_into(v, &key, consumed, max_attrs, out),
                    Value::Array(_) => out.push(LabelPair {
                        key,
                        value: v.to_string(),
                    }),
                    Value::Null => {}
                    scalar => out.push(LabelPair {
                        key,
                        value: scalar_to_string(scalar),
                    }),
                }
            }
        }
        // A non-object root (rare for _source) is emitted under its prefix.
        other if !prefix.is_empty() => out.push(LabelPair {
            key: prefix.to_string(),
            value: scalar_to_string(other),
        }),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg() -> MappingConfig {
        MappingConfig::default()
    }

    #[test]
    fn parses_rfc3339_timestamp() {
        let v = json!("2023-01-01T00:00:00Z");
        // 2023-01-01T00:00:00Z = 1672531200 s
        assert_eq!(parse_timestamp(&v), Some(1_672_531_200_000_000_000));
    }

    #[test]
    fn parses_rfc3339_with_millis_and_offset() {
        let v = json!("2023-01-01T01:00:00.500+01:00");
        // Same instant as 00:00:00.500Z
        assert_eq!(parse_timestamp(&v), Some(1_672_531_200_500_000_000));
    }

    #[test]
    fn infers_epoch_units() {
        assert_eq!(
            parse_timestamp(&json!(1_672_531_200u64)),
            Some(1_672_531_200_000_000_000)
        ); // s
        assert_eq!(
            parse_timestamp(&json!(1_672_531_200_000u64)),
            Some(1_672_531_200_000_000_000)
        ); // ms
        assert_eq!(
            parse_timestamp(&json!(1_672_531_200_000_000u64)),
            Some(1_672_531_200_000_000_000)
        ); // us
        assert_eq!(
            parse_timestamp(&json!(1_672_531_200_000_000_000u64)),
            Some(1_672_531_200_000_000_000)
        ); // ns
    }

    #[test]
    fn carry_forward_on_missing_ts() {
        let c = cfg();
        let mut carry = 42_000u64;
        let mut counters = MapCounters::default();
        let rec = doc_to_record(&json!({"message": "hi"}), &c, &mut carry, &mut counters);
        assert_eq!(rec.entry.ts_unix_nano, 42_000);
        assert_eq!(counters.ts_inherited, 1);
        // A later doc with a real ts advances the cursor.
        let rec2 = doc_to_record(
            &json!({"@timestamp": "2023-01-01T00:00:00Z", "message": "yo"}),
            &c,
            &mut carry,
            &mut counters,
        );
        assert_eq!(rec2.entry.ts_unix_nano, 1_672_531_200_000_000_000);
        assert_eq!(carry, 1_672_531_200_000_000_000);
    }

    #[test]
    fn severity_string_and_numeric() {
        assert_eq!(parse_severity(&json!("ERROR")), 17);
        assert_eq!(parse_severity(&json!("warn")), 13);
        assert_eq!(parse_severity(&json!("Information")), 9);
        assert_eq!(parse_severity(&json!(21)), 21); // in-range numeric passthrough
        assert_eq!(parse_severity(&json!(99)), 9); // out of range → INFO
        assert_eq!(parse_severity(&json!("nonsense")), 9);
    }

    #[test]
    fn service_key_priority() {
        let c = cfg();
        let mut carry = 0u64;
        let mut counters = MapCounters::default();
        // service.name wins over service/app.
        let rec = doc_to_record(
            &json!({"@timestamp": "2023-01-01T00:00:00Z", "service": {"name": "api"}, "app": "other", "message": "x"}),
            &c,
            &mut carry,
            &mut counters,
        );
        let svc = rec.labels.iter().find(|l| l.key == "service").unwrap();
        assert_eq!(svc.value, "api");
    }

    #[test]
    fn label_and_attribute_split() {
        let mut c = cfg();
        c.label_fields = vec!["env".to_string()];
        let mut carry = 0u64;
        let mut counters = MapCounters::default();
        let rec = doc_to_record(
            &json!({
                "@timestamp": "2023-01-01T00:00:00Z",
                "service": "api",
                "env": "prod",
                "message": "hello",
                "severity": "info",
                "trace_id": "abc",
                "http": {"status": 200, "method": "GET"}
            }),
            &c,
            &mut carry,
            &mut counters,
        );
        // Labels: service + env (low card), fingerprinted.
        assert!(rec
            .labels
            .iter()
            .any(|l| l.key == "service" && l.value == "api"));
        assert!(rec
            .labels
            .iter()
            .any(|l| l.key == "env" && l.value == "prod"));
        // Attributes: everything else, nested flattened, consumed fields absent.
        let attrs: std::collections::HashMap<_, _> = rec
            .entry
            .attributes
            .iter()
            .map(|l| (l.key.as_str(), l.value.as_str()))
            .collect();
        assert_eq!(attrs.get("trace_id"), Some(&"abc"));
        assert_eq!(attrs.get("http.status"), Some(&"200"));
        assert_eq!(attrs.get("http.method"), Some(&"GET"));
        assert!(!attrs.contains_key("@timestamp"));
        assert!(!attrs.contains_key("message"));
        assert!(!attrs.contains_key("severity"));
        assert!(!attrs.contains_key("service"));
        assert!(!attrs.contains_key("env"));
    }

    #[test]
    fn array_field_is_json_stringified() {
        let c = cfg();
        let mut carry = 0u64;
        let mut counters = MapCounters::default();
        let rec = doc_to_record(
            &json!({"@timestamp": "2023-01-01T00:00:00Z", "message": "x", "tags": ["a", "b"]}),
            &c,
            &mut carry,
            &mut counters,
        );
        let tags = rec
            .entry
            .attributes
            .iter()
            .find(|l| l.key == "tags")
            .unwrap();
        assert_eq!(tags.value, "[\"a\",\"b\"]");
    }

    #[test]
    fn max_attrs_cap() {
        let mut c = cfg();
        c.max_attrs = 2;
        let mut carry = 0u64;
        let mut counters = MapCounters::default();
        let rec = doc_to_record(
            &json!({"@timestamp": "2023-01-01T00:00:00Z", "message": "x", "a": 1, "b": 2, "cc": 3, "d": 4}),
            &c,
            &mut carry,
            &mut counters,
        );
        assert_eq!(rec.entry.attributes.len(), 2);
    }

    #[test]
    fn body_missing_counted_and_empty() {
        let c = cfg();
        let mut carry = 0u64;
        let mut counters = MapCounters::default();
        let rec = doc_to_record(
            &json!({"@timestamp": "2023-01-01T00:00:00Z", "service": "api"}),
            &c,
            &mut carry,
            &mut counters,
        );
        assert_eq!(rec.entry.body, "");
        assert_eq!(counters.body_missing, 1);
    }
}
