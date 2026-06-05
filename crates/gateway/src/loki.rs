//! Loki sink: map a fanned-out [`LogsBatch`] to a Grafana Loki push request and
//! POST it to `{base}/loki/api/v1/push`.
//!
//! One Loki *stream* per scry `LogStream`: the stream's labels become Loki
//! stream labels (keys sanitized to Loki's `[a-zA-Z_][a-zA-Z0-9_]*` grammar),
//! and each `LogEntry` becomes a `[ts_unix_nano_string, line]` value. Per-entry
//! data that would explode stream cardinality if promoted to labels — the
//! severity and the entry `attributes` (e.g. `stream=stdout`) — rides along as
//! **structured metadata** (the optional third element of a value tuple), which
//! Loki 3.x stores per-entry. Entries with no metadata emit a plain 2-tuple, so
//! the push is also valid against a Loki with structured metadata disabled.
//!
//! The mapping ([`to_push_request`]) is pure and unit-tested; [`LokiSink`] is a
//! thin worker that serializes it and ships it best-effort.

use std::collections::BTreeMap;

use scry_proto::generated::LogsBatch;
use serde::{ser::SerializeSeq, Serialize, Serializer};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::sink::Fanout;

/// A Loki push request: `{"streams":[…]}`.
#[derive(Debug, Serialize, PartialEq)]
pub struct LokiPushRequest {
    pub streams: Vec<LokiStream>,
}

/// One Loki stream: a label set + its entries.
#[derive(Debug, Serialize, PartialEq)]
pub struct LokiStream {
    pub stream: BTreeMap<String, String>,
    pub values: Vec<LokiValue>,
}

/// A single Loki log line: `[ts_ns, line]` or, when there is per-entry
/// structured metadata, `[ts_ns, line, {meta}]`.
#[derive(Debug, PartialEq)]
pub struct LokiValue {
    pub ts_unix_nano: String,
    pub line: String,
    pub metadata: BTreeMap<String, String>,
}

impl Serialize for LokiValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.metadata.is_empty() {
            let mut seq = serializer.serialize_seq(Some(2))?;
            seq.serialize_element(&self.ts_unix_nano)?;
            seq.serialize_element(&self.line)?;
            seq.end()
        } else {
            let mut seq = serializer.serialize_seq(Some(3))?;
            seq.serialize_element(&self.ts_unix_nano)?;
            seq.serialize_element(&self.line)?;
            seq.serialize_element(&self.metadata)?;
            seq.end()
        }
    }
}

/// Pure mapping: a scry [`LogsBatch`] → a Loki [`LokiPushRequest`].
///
/// One Loki stream per scry `LogStream`. Label keys are sanitized to Loki's
/// label grammar (dots and other illegal chars → `_`, leading digit → `_`
/// prefix). Severity (when non-zero) and entry attributes are emitted as
/// structured metadata.
pub fn to_push_request(batch: &LogsBatch) -> LokiPushRequest {
    let streams = batch
        .streams
        .iter()
        .map(|s| {
            let stream: BTreeMap<String, String> = s
                .labels
                .iter()
                .map(|l| (sanitize_label(&l.key), l.value.clone()))
                .collect();
            let values = s
                .entries
                .iter()
                .map(|e| {
                    let mut metadata: BTreeMap<String, String> = e
                        .attributes
                        .iter()
                        .map(|a| (sanitize_label(&a.key), a.value.clone()))
                        .collect();
                    if e.severity != 0 {
                        metadata.insert("severity_number".to_string(), e.severity.to_string());
                    }
                    LokiValue {
                        ts_unix_nano: e.ts_unix_nano.to_string(),
                        line: e.body.clone(),
                        metadata,
                    }
                })
                .collect();
            LokiStream { stream, values }
        })
        .collect();
    LokiPushRequest { streams }
}

/// Coerce a label key to Loki's `[a-zA-Z_][a-zA-Z0-9_]*` grammar: illegal
/// characters → `_`, a leading digit gains a `_` prefix, empty → `_`.
fn sanitize_label(key: &str) -> String {
    let mut out: String = key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    match out.chars().next() {
        None => "_".to_string(),
        // A leading digit is legal mid-name but not first — prefix, don't drop.
        Some(c) if c.is_ascii_digit() => {
            out.insert(0, '_');
            out
        }
        _ => out,
    }
}

/// Worker that ships fanned-out log batches to a Loki push endpoint.
pub struct LokiSink {
    http: reqwest::Client,
    endpoint: String,
}

impl LokiSink {
    /// `base` is the Loki base URL (e.g. `http://loki:3100`); the push path is
    /// appended.
    pub fn new(http: reqwest::Client, base: &str) -> Self {
        let endpoint = format!("{}/loki/api/v1/push", base.trim_end_matches('/'));
        Self { http, endpoint }
    }

    pub async fn run(self, mut rx: mpsc::Receiver<Fanout>) {
        while let Some(item) = rx.recv().await {
            let Fanout::Logs(batch) = item else {
                continue; // mask is logs-only; ignore anything else defensively
            };
            let req = to_push_request(&batch);
            if let Err(e) = self.ship(&req).await {
                warn!(error = %e, "loki sink push failed; dropping batch");
            }
        }
        info!("loki sink worker exiting (queue closed)");
    }

    async fn ship(&self, req: &LokiPushRequest) -> anyhow::Result<()> {
        let resp = self.http.post(&self.endpoint).json(req).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "loki responded {status}: {}",
                body.chars().take(400).collect::<String>()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scry_proto::{
        generated::{LogEntry, LogStream},
        LabelPair,
    };

    fn lp(k: &str, v: &str) -> LabelPair {
        LabelPair {
            key: k.into(),
            value: v.into(),
        }
    }

    #[test]
    fn sanitizes_label_keys() {
        assert_eq!(sanitize_label("service.name"), "service_name");
        assert_eq!(sanitize_label("k8s_app"), "k8s_app");
        assert_eq!(sanitize_label("9lives"), "_9lives");
        assert_eq!(sanitize_label("a-b.c"), "a_b_c");
        assert_eq!(sanitize_label(""), "_");
    }

    #[test]
    fn maps_one_stream_per_logstream_with_metadata() {
        let batch = LogsBatch {
            streams: vec![LogStream {
                fingerprint: 1,
                labels: vec![lp("namespace", "prod"), lp("service.name", "api")],
                entries: vec![
                    LogEntry {
                        ts_unix_nano: 1_700_000_000_000_000_000,
                        severity: 9,
                        body: "hello".into(),
                        attributes: vec![lp("stream", "stdout")],
                    },
                    LogEntry {
                        ts_unix_nano: 1_700_000_000_000_000_001,
                        severity: 0,
                        body: "plain".into(),
                        attributes: vec![],
                    },
                ],
            }],
        };

        let req = to_push_request(&batch);
        assert_eq!(req.streams.len(), 1);
        let s = &req.streams[0];
        // Labels sanitized.
        assert_eq!(s.stream.get("namespace"), Some(&"prod".to_string()));
        assert_eq!(s.stream.get("service_name"), Some(&"api".to_string()));
        assert_eq!(s.values.len(), 2);

        // First entry: ns-string ts + structured metadata (attr + severity).
        let v0 = &s.values[0];
        assert_eq!(v0.ts_unix_nano, "1700000000000000000");
        assert_eq!(v0.line, "hello");
        assert_eq!(v0.metadata.get("stream"), Some(&"stdout".to_string()));
        assert_eq!(v0.metadata.get("severity_number"), Some(&"9".to_string()));

        // Second entry: no metadata (severity 0, no attrs) → plain 2-tuple.
        assert!(s.values[1].metadata.is_empty());
    }

    #[test]
    fn value_serializes_to_2_or_3_tuple() {
        let plain = LokiValue {
            ts_unix_nano: "10".into(),
            line: "x".into(),
            metadata: BTreeMap::new(),
        };
        assert_eq!(serde_json::to_string(&plain).unwrap(), r#"["10","x"]"#);

        let mut meta = BTreeMap::new();
        meta.insert("k".to_string(), "v".to_string());
        let with = LokiValue {
            ts_unix_nano: "10".into(),
            line: "x".into(),
            metadata: meta,
        };
        assert_eq!(
            serde_json::to_string(&with).unwrap(),
            r#"["10","x",{"k":"v"}]"#
        );
    }
}
