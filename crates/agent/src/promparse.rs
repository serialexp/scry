//! Hand-rolled parser for the Prometheus **text exposition format** (the classic
//! `text/plain; version=0.0.4` body served on `/metrics`).
//!
//! Deliberately hand-written rather than pulled from a crate: the exposition
//! format is the entry point for everything the agent scrapes, so we want full
//! control to extend it (OpenMetrics, exemplars, …) without fighting a
//! third-party API. It is pure and dependency-free.
//!
//! What it understands:
//! - blank lines (ignored);
//! - `# HELP <name> <text>` (recognised, text discarded — scry has nowhere to
//!   store it);
//! - `# TYPE <name> {counter|gauge|histogram|summary|untyped}`;
//! - other `#` comment lines (ignored);
//! - sample lines `name[{l="v",...}] value [timestamp_ms]`, with Go-style floats
//!   (`1.5`, `1e9`, `+Inf`, `-Inf`, `NaN`) and label-value escapes (`\\`, `\"`,
//!   `\n`).
//!
//! It is **lenient**: a malformed sample line is skipped and counted (see
//! [`Scrape::skipped`]) rather than failing the whole scrape, so one truncated
//! line never discards an otherwise good body.
//!
//! Out of scope (v1): OpenMetrics framing (`# EOF`, `_created`, exemplars), and
//! native (sparse) histograms — neither has a representation in scry's wire.

use std::collections::HashMap;

use scry_proto::{
    constants::{
        METRIC_TYPE_COUNTER, METRIC_TYPE_GAUGE, METRIC_TYPE_HISTOGRAM, METRIC_TYPE_SUMMARY,
        METRIC_TYPE_UNKNOWN,
    },
    LabelPair,
};

/// A declared metric family type (from a `# TYPE` line).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Untyped,
}

impl Kind {
    fn parse(s: &str) -> Option<Kind> {
        Some(match s {
            "counter" => Kind::Counter,
            "gauge" => Kind::Gauge,
            "histogram" => Kind::Histogram,
            "summary" => Kind::Summary,
            "untyped" | "unknown" => Kind::Untyped,
            _ => return None,
        })
    }

    fn metric_type(self) -> u8 {
        match self {
            Kind::Counter => METRIC_TYPE_COUNTER,
            Kind::Gauge => METRIC_TYPE_GAUGE,
            Kind::Histogram => METRIC_TYPE_HISTOGRAM,
            Kind::Summary => METRIC_TYPE_SUMMARY,
            Kind::Untyped => METRIC_TYPE_UNKNOWN,
        }
    }
}

/// One parsed sample line. `labels` are the labels as written (not including the
/// metric name); the scraper adds `__name__` and the target's identifying labels
/// before fingerprinting.
#[derive(Debug, Clone, PartialEq)]
pub struct ScrapedMetric {
    pub name: String,
    pub labels: Vec<LabelPair>,
    pub value: f64,
    pub timestamp_ms: Option<i64>,
}

/// The result of parsing one scrape body.
#[derive(Debug, Clone, Default)]
pub struct Scrape {
    pub metrics: Vec<ScrapedMetric>,
    /// Family name → declared kind (from `# TYPE`).
    pub types: HashMap<String, Kind>,
    /// Count of sample lines that failed to parse and were skipped.
    pub skipped: u64,
}

impl Scrape {
    /// Resolve a series name to a `METRIC_TYPE_*` byte. Tries the name directly,
    /// then strips histogram/summary component suffixes to find the family type.
    pub fn metric_type(&self, name: &str) -> u8 {
        if let Some(k) = self.types.get(name) {
            return k.metric_type();
        }
        for suf in ["_bucket", "_sum", "_count"] {
            if let Some(base) = name.strip_suffix(suf) {
                if matches!(self.types.get(base), Some(Kind::Histogram)) {
                    return METRIC_TYPE_HISTOGRAM;
                }
            }
        }
        for suf in ["_sum", "_count"] {
            if let Some(base) = name.strip_suffix(suf) {
                if matches!(self.types.get(base), Some(Kind::Summary)) {
                    return METRIC_TYPE_SUMMARY;
                }
            }
        }
        METRIC_TYPE_UNKNOWN
    }
}

/// Parse a Prometheus text exposition body.
pub fn parse(body: &str) -> Scrape {
    let mut scrape = Scrape::default();
    for raw in body.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix('#') {
            parse_comment(rest.trim_start(), &mut scrape.types);
            continue;
        }
        match parse_sample_line(line) {
            Some(m) => scrape.metrics.push(m),
            None => scrape.skipped += 1,
        }
    }
    scrape
}

/// Handle the content of a `#`-prefixed line (already stripped of `#`).
fn parse_comment(rest: &str, types: &mut HashMap<String, Kind>) {
    // Only `TYPE` carries information we keep; `HELP` and anything else is noise.
    let mut it = rest
        .splitn(3, char::is_whitespace)
        .filter(|s| !s.is_empty());
    if it.next() != Some("TYPE") {
        return;
    }
    // Re-split the remainder honouring runs of whitespace.
    let mut fields = rest.split_whitespace();
    let _type_kw = fields.next(); // "TYPE"
    let (Some(name), Some(kind)) = (fields.next(), fields.next()) else {
        return;
    };
    if let Some(k) = Kind::parse(kind) {
        types.insert(name.to_string(), k);
    }
}

/// Parse one sample line: `name[{labels}] value [timestamp_ms]`.
fn parse_sample_line(line: &str) -> Option<ScrapedMetric> {
    let bytes = line.as_bytes();
    let mut i = 0;

    // ── metric name ──
    let name_start = i;
    while i < bytes.len() && is_name_byte(bytes[i], i == name_start) {
        i += 1;
    }
    if i == name_start {
        return None;
    }
    let name = &line[name_start..i];

    // ── optional label block ──
    let mut labels = Vec::new();
    if i < bytes.len() && bytes[i] == b'{' {
        let (parsed, end) = parse_label_block(line, i + 1)?;
        labels = parsed;
        i = end; // position just past the closing '}'
    }

    // ── value (and optional timestamp) ──
    let rest = line[i..].trim_start();
    let mut parts = rest.split_whitespace();
    let value = parse_value(parts.next()?)?;
    let timestamp_ms = match parts.next() {
        Some(ts) => Some(ts.parse::<i64>().ok()?),
        None => None,
    };
    // Anything further on the line is malformed.
    if parts.next().is_some() {
        return None;
    }

    Some(ScrapedMetric {
        name: name.to_string(),
        labels,
        value,
        timestamp_ms,
    })
}

/// Parse the label block starting at `start` (the byte just after `{`). Returns
/// the labels and the index just past the closing `}`.
fn parse_label_block(line: &str, start: usize) -> Option<(Vec<LabelPair>, usize)> {
    let bytes = line.as_bytes();
    let mut i = start;
    let mut labels = Vec::new();

    loop {
        // skip whitespace / commas between entries
        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b',') {
            i += 1;
        }
        if i >= bytes.len() {
            return None; // unterminated block
        }
        if bytes[i] == b'}' {
            return Some((labels, i + 1));
        }

        // label name
        let key_start = i;
        while i < bytes.len() && is_name_byte(bytes[i], i == key_start) {
            i += 1;
        }
        if i == key_start {
            return None;
        }
        let key = line[key_start..i].to_string();

        // '='
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            return None;
        }
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }

        // quoted value
        if i >= bytes.len() || bytes[i] != b'"' {
            return None;
        }
        i += 1;
        let mut value = String::new();
        loop {
            if i >= bytes.len() {
                return None; // unterminated string
            }
            match bytes[i] {
                b'"' => {
                    i += 1;
                    break;
                }
                b'\\' => {
                    i += 1;
                    if i >= bytes.len() {
                        return None;
                    }
                    match bytes[i] {
                        b'\\' => value.push('\\'),
                        b'"' => value.push('"'),
                        b'n' => value.push('\n'),
                        // Unknown escape: keep the escaped byte verbatim.
                        other => value.push(other as char),
                    }
                    i += 1;
                }
                _ => {
                    // Copy one UTF-8 char (label values may be non-ASCII).
                    let ch_start = i;
                    i += 1;
                    while i < bytes.len() && (bytes[i] & 0xC0) == 0x80 {
                        i += 1;
                    }
                    value.push_str(&line[ch_start..i]);
                }
            }
        }
        labels.push(LabelPair { key, value });
    }
}

/// A Prometheus value token → f64, honouring the special tokens.
fn parse_value(tok: &str) -> Option<f64> {
    match tok {
        "NaN" | "nan" => Some(f64::NAN),
        "+Inf" | "Inf" | "+inf" | "inf" => Some(f64::INFINITY),
        "-Inf" | "-inf" => Some(f64::NEG_INFINITY),
        _ => tok.parse::<f64>().ok(),
    }
}

/// Metric- and label-name byte test. `first` tightens the first byte to exclude
/// digits. Metric names additionally allow `:`; we accept it for both since a
/// label name never contains `:` in valid input anyway.
fn is_name_byte(b: u8, first: bool) -> bool {
    match b {
        b'a'..=b'z' | b'A'..=b'Z' | b'_' | b':' => true,
        b'0'..=b'9' => !first,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lbl(pairs: &[(&str, &str)]) -> Vec<LabelPair> {
        pairs
            .iter()
            .map(|(k, v)| LabelPair {
                key: (*k).into(),
                value: (*v).into(),
            })
            .collect()
    }

    #[test]
    fn parses_simple_counter_and_gauge() {
        let body = "\
# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method=\"post\",code=\"200\"} 1027 1395066363000
http_requests_total{method=\"post\",code=\"400\"} 3
# TYPE temperature gauge
temperature 21.5
";
        let s = parse(body);
        assert_eq!(s.skipped, 0);
        assert_eq!(s.metrics.len(), 3);

        let m0 = &s.metrics[0];
        assert_eq!(m0.name, "http_requests_total");
        assert_eq!(m0.labels, lbl(&[("method", "post"), ("code", "200")]));
        assert_eq!(m0.value, 1027.0);
        assert_eq!(m0.timestamp_ms, Some(1395066363000));

        assert_eq!(s.metrics[1].timestamp_ms, None);

        let g = &s.metrics[2];
        assert_eq!(g.name, "temperature");
        assert!(g.labels.is_empty());
        assert_eq!(g.value, 21.5);

        assert_eq!(s.metric_type("http_requests_total"), METRIC_TYPE_COUNTER);
        assert_eq!(s.metric_type("temperature"), METRIC_TYPE_GAUGE);
        assert_eq!(s.metric_type("unseen_metric"), METRIC_TYPE_UNKNOWN);
    }

    #[test]
    fn special_float_values() {
        let body = "\
a 1e9
b +Inf
c -Inf
d NaN
e -3.25
f 1.5e-3
";
        let s = parse(body);
        assert_eq!(s.skipped, 0);
        assert_eq!(s.metrics[0].value, 1e9);
        assert_eq!(s.metrics[1].value, f64::INFINITY);
        assert_eq!(s.metrics[2].value, f64::NEG_INFINITY);
        assert!(s.metrics[3].value.is_nan());
        assert_eq!(s.metrics[4].value, -3.25);
        assert_eq!(s.metrics[5].value, 1.5e-3);
    }

    #[test]
    fn label_value_escapes() {
        // backslash, escaped quote, newline, and a brace inside the value.
        let body = r#"m{path="/a\\b",msg="say \"hi\"",multi="x\ny",brace="a}b"} 1"#;
        let s = parse(body);
        assert_eq!(s.skipped, 0);
        let m = &s.metrics[0];
        assert_eq!(
            m.labels[0],
            LabelPair {
                key: "path".into(),
                value: "/a\\b".into()
            }
        );
        assert_eq!(
            m.labels[1],
            LabelPair {
                key: "msg".into(),
                value: "say \"hi\"".into()
            }
        );
        assert_eq!(
            m.labels[2],
            LabelPair {
                key: "multi".into(),
                value: "x\ny".into()
            }
        );
        // The unescaped '}' inside the quoted value must not end the block early.
        assert_eq!(
            m.labels[3],
            LabelPair {
                key: "brace".into(),
                value: "a}b".into()
            }
        );
        assert_eq!(m.value, 1.0);
    }

    #[test]
    fn histogram_and_summary_type_resolution() {
        let body = "\
# TYPE rpc_duration_seconds histogram
rpc_duration_seconds_bucket{le=\"0.1\"} 5
rpc_duration_seconds_bucket{le=\"+Inf\"} 9
rpc_duration_seconds_sum 0.42
rpc_duration_seconds_count 9
# TYPE rpc_latency summary
rpc_latency{quantile=\"0.5\"} 0.01
rpc_latency_sum 1.2
rpc_latency_count 100
";
        let s = parse(body);
        assert_eq!(s.skipped, 0);
        assert_eq!(
            s.metric_type("rpc_duration_seconds_bucket"),
            METRIC_TYPE_HISTOGRAM
        );
        assert_eq!(
            s.metric_type("rpc_duration_seconds_sum"),
            METRIC_TYPE_HISTOGRAM
        );
        assert_eq!(
            s.metric_type("rpc_duration_seconds_count"),
            METRIC_TYPE_HISTOGRAM
        );
        // Summary base series carries the quantile label.
        assert_eq!(s.metric_type("rpc_latency"), METRIC_TYPE_SUMMARY);
        assert_eq!(s.metric_type("rpc_latency_sum"), METRIC_TYPE_SUMMARY);
        assert_eq!(s.metric_type("rpc_latency_count"), METRIC_TYPE_SUMMARY);
        // The le="+Inf" bucket parses its label fine.
        let inf_bucket = s
            .metrics
            .iter()
            .find(|m| m.labels.iter().any(|l| l.value == "+Inf"));
        assert!(inf_bucket.is_some());
    }

    #[test]
    fn skips_malformed_lines_without_aborting() {
        let body = "\
good_metric 1
this is not valid
also_bad{unterminated=\"x 2
another_good 3
missing_value{a=\"b\"}
empty_braces{} 7
";
        let s = parse(body);
        // good_metric, another_good, empty_braces parse; three lines skipped.
        let names: Vec<_> = s.metrics.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"good_metric"));
        assert!(names.contains(&"another_good"));
        assert!(names.contains(&"empty_braces"));
        assert_eq!(s.metrics.len(), 3);
        assert_eq!(s.skipped, 3);
        assert!(s
            .metrics
            .iter()
            .find(|m| m.name == "empty_braces")
            .unwrap()
            .labels
            .is_empty());
    }

    #[test]
    fn ignores_comments_and_blank_lines() {
        // Leading newline is intentional: exercises blank-line handling.
        let body = "
# this is a plain comment
# HELP foo some help
foo 42

# TYPE foo gauge
";
        let s = parse(body);
        assert_eq!(s.metrics.len(), 1);
        assert_eq!(s.metrics[0].value, 42.0);
        assert_eq!(s.metric_type("foo"), METRIC_TYPE_GAUGE);
    }

    #[test]
    fn handles_whitespace_and_trailing_comma() {
        // The brace follows the name directly (per the exposition format), but
        // we tolerate spaces around `=`/`,` and a trailing comma inside the block.
        let body = "metric_name{ a = \"1\" , b = \"2\" , }   3.0\n";
        let s = parse(body);
        assert_eq!(s.skipped, 0);
        let m = &s.metrics[0];
        assert_eq!(m.name, "metric_name");
        assert_eq!(m.labels, lbl(&[("a", "1"), ("b", "2")]));
        assert_eq!(m.value, 3.0);
    }
}
