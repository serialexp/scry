//! OpenSearch sink: map a fanned-out [`LogsBatch`] to an OpenSearch `_bulk`
//! NDJSON body and POST it, routing each stream to a **per-service rolling data
//! stream** and (by default) **self-managing** the lifecycle assets.
//!
//! ## Routing — `<prefix>-<service>`
//! `--opensearch-index` is an index *prefix*. Each `LogStream` is written to the
//! data stream `<prefix>-<service>`, where `<service>` is the stream's service
//! name (the first present of [`SERVICE_KEYS`], sanitized) or `general` when no
//! service label is present. So OTLP/Pyroscope logs land under their
//! `service.name`, k8s pod logs under their `app`/`k8s_app` label, and anything
//! unlabelled under `<prefix>-general`. One Loki-style stream per `LogStream`;
//! one document per `LogEntry`. The bulk action is `create` (required by data
//! streams; on a plain index it just auto-generates the doc id).
//!
//! ## Self-management (the service owns the index, not the cluster)
//! Unless `--opensearch-unmanaged` is set, the sink asserts — at startup, on a
//! schedule, and after a write error — the lifecycle assets so a human poking
//! the cluster can't silently break ingest:
//!   - an **ISM rollover policy** (`<prefix>-rollover`): roll a backing index
//!     over by size/age, **no auto-delete**. Its `ism_template.index_patterns`
//!     matches `<prefix>-*` so the policy auto-attaches to every data stream
//!     and, crucially, to every backing index created at rollover (manual
//!     attach does *not* survive rollover — see the OpenSearch ISM docs).
//!   - an **index template** (`<prefix>`): `data_stream` enabled, with explicit
//!     mappings. `labels`/`attributes` are `flat_object` so arbitrary label keys
//!     can never cause a mapping explosion or a type conflict (the usual cause
//!     of silently-dropped docs); `@timestamp` is a date, `body` text,
//!     `severity` a short.
//!   - the per-service **data streams**, created lazily on first sight.
//!
//! The ISM policy is reconciled read-then-write (`if_seq_no`/`if_primary_term`)
//! so drift is corrected; the index template PUT is idempotent.
//!
//! The pure pieces ([`to_bulk_ndjson`], [`data_stream_name`], [`ism_policy_doc`],
//! [`index_template_doc`]) are unit-tested; [`OpenSearchSink`] is the worker that
//! ships + manages best-effort.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use reqwest::{header::CONTENT_TYPE, StatusCode};
use scry_proto::generated::{LogStream, LogsBatch};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tracing::{info, warn};

use crate::{aws_sign::SigV4Signer, sink::Fanout};

/// Label keys that carry a service name, in priority order. The first present,
/// non-empty value becomes the per-service index segment; if none match, the
/// segment is [`DEFAULT_SERVICE`]. Covers the OTLP/Pyroscope convention
/// (`service.name`) and the scry-agent k8s convention (`app` → `k8s_app`).
const SERVICE_KEYS: [&str; 4] = ["service.name", "service", "app", "k8s_app"];

/// Index segment used when a stream carries no recognisable service label.
const DEFAULT_SERVICE: &str = "general";

#[derive(Serialize)]
struct BulkAction<'a> {
    create: BulkTarget<'a>,
}

#[derive(Serialize)]
struct BulkTarget<'a> {
    #[serde(rename = "_index")]
    index: &'a str,
}

#[derive(Serialize)]
struct LogDoc<'a> {
    #[serde(rename = "@timestamp")]
    timestamp: String,
    body: &'a str,
    severity: u8,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    labels: BTreeMap<&'a str, &'a str>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    attributes: BTreeMap<&'a str, &'a str>,
}

// ── Pure routing/mapping ────────────────────────────────────────────────

/// The service segment for a stream: the first present [`SERVICE_KEYS`] value
/// (sanitized), else [`DEFAULT_SERVICE`].
pub fn service_segment(stream: &LogStream) -> String {
    for key in SERVICE_KEYS {
        if let Some(pair) = stream.labels.iter().find(|l| l.key == key) {
            let seg = sanitize_segment(&pair.value);
            if !seg.is_empty() {
                return seg;
            }
        }
    }
    DEFAULT_SERVICE.to_string()
}

/// The data-stream name for a stream: `<prefix>-<service>`, lowercased and
/// sanitized to OpenSearch's index-naming rules.
pub fn data_stream_name(prefix: &str, stream: &LogStream) -> String {
    format!("{}-{}", sanitize_prefix(prefix), service_segment(stream))
}

/// Coerce one name segment to OpenSearch-index-safe characters: lowercase,
/// every char outside `[a-z0-9_-]` → `-`, collapse repeated `-`, trim leading/
/// trailing `-`/`_`. May return `""` (the caller substitutes a default).
fn sanitize_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_dash = false;
    for raw in s.chars() {
        let c = raw.to_ascii_lowercase();
        let keep = c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-';
        let mapped = if keep { c } else { '-' };
        if mapped == '-' {
            if prev_dash {
                continue;
            }
            prev_dash = true;
        } else {
            prev_dash = false;
        }
        out.push(mapped);
    }
    out.trim_matches(|c| c == '-' || c == '_').to_string()
}

/// Sanitize the operator-supplied prefix, falling back to `scry` if it
/// sanitizes to empty (so a name is always valid).
fn sanitize_prefix(prefix: &str) -> String {
    let p = sanitize_segment(prefix);
    if p.is_empty() {
        "scry".to_string()
    } else {
        p
    }
}

/// Pure mapping: a scry [`LogsBatch`] → an OpenSearch `_bulk` NDJSON body. Each
/// stream's docs target its own `<prefix>-<service>` data stream (action line +
/// doc line per entry, trailing newline included).
pub fn to_bulk_ndjson(batch: &LogsBatch, prefix: &str) -> Vec<u8> {
    let mut out = String::new();
    for s in &batch.streams {
        let target = data_stream_name(prefix, s);
        let action = serde_json::to_string(&BulkAction {
            create: BulkTarget { index: &target },
        })
        .expect("bulk action serializes");
        let labels: BTreeMap<&str, &str> = s
            .labels
            .iter()
            .map(|l| (l.key.as_str(), l.value.as_str()))
            .collect();
        for e in &s.entries {
            let attributes: BTreeMap<&str, &str> = e
                .attributes
                .iter()
                .map(|a| (a.key.as_str(), a.value.as_str()))
                .collect();
            let doc = LogDoc {
                timestamp: iso8601(e.ts_unix_nano),
                body: &e.body,
                severity: e.severity,
                labels: labels.clone(),
                attributes,
            };
            out.push_str(&action);
            out.push('\n');
            out.push_str(&serde_json::to_string(&doc).expect("log doc serializes"));
            out.push('\n');
        }
    }
    out.into_bytes()
}

/// The distinct data-stream names referenced by a batch (for lazy creation).
fn distinct_targets(batch: &LogsBatch, prefix: &str) -> Vec<String> {
    batch
        .streams
        .iter()
        .map(|s| data_stream_name(prefix, s))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Format unix-nanos as a UTC ISO-8601 string with millisecond precision and a
/// `Z` suffix (e.g. `2023-11-14T22:13:20.000Z`) — the shape OpenSearch's
/// `strict_date_optional_time` dynamic date detection recognises.
fn iso8601(ts_unix_nano: u64) -> String {
    let secs = (ts_unix_nano / 1_000_000_000) as i64;
    let nanos = (ts_unix_nano % 1_000_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
        .unwrap_or_default()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

// ── Managed-asset documents (pure) ──────────────────────────────────────

/// The ISM policy document: rollover by `min_size`/`min_index_age`, **no delete
/// state**, auto-attaching to `<prefix>-*` via `ism_template` (the only way a
/// rolled-over backing index stays managed).
pub fn ism_policy_doc(prefix: &str, min_size: &str, min_age: &str) -> Value {
    json!({
        "policy": {
            "description": "scry-gateway: roll over backing indices by size/age; no auto-delete",
            "default_state": "hot",
            "states": [{
                "name": "hot",
                "actions": [{ "rollover": { "min_size": min_size, "min_index_age": min_age } }],
                "transitions": []
            }],
            "ism_template": {
                "index_patterns": [format!("{prefix}-*")],
                "priority": 100
            }
        }
    })
}

/// The index template document: `data_stream` enabled, explicit mappings with
/// `flat_object` labels/attributes (no mapping explosion / type conflicts).
pub fn index_template_doc(prefix: &str) -> Value {
    json!({
        "index_patterns": [format!("{prefix}-*")],
        "data_stream": {},
        "priority": 200,
        "template": {
            "mappings": {
                "properties": {
                    "@timestamp": { "type": "date" },
                    "body":       { "type": "text" },
                    "severity":   { "type": "short" },
                    "labels":     { "type": "flat_object" },
                    "attributes": { "type": "flat_object" }
                }
            }
        }
    })
}

// ── Worker ──────────────────────────────────────────────────────────────

/// Configuration for an [`OpenSearchSink`].
pub struct OpenSearchConfig {
    /// OpenSearch base URL (e.g. `http://opensearch:9200`).
    pub base: String,
    /// Index prefix; write target is `<prefix>-<service>`.
    pub prefix: String,
    /// Whether to create/maintain the ISM policy + index template + data streams.
    pub manage: bool,
    /// Rollover trigger size (per backing index), e.g. `30gb`.
    pub rollover_size: String,
    /// Rollover trigger age (per backing index), e.g. `1d`.
    pub rollover_age: String,
    /// How often to re-assert managed assets (drift correction).
    pub reconcile_interval: Duration,
    /// AWS SigV4 signer. `Some` for Amazon OpenSearch Service / Serverless
    /// (which reject unsigned requests); `None` for a self-hosted cluster.
    pub signer: Option<Arc<SigV4Signer>>,
}

/// Worker that ships fanned-out log batches to OpenSearch and (by default)
/// self-manages the per-prefix lifecycle assets.
pub struct OpenSearchSink {
    http: reqwest::Client,
    base: String,
    prefix: String,
    manage: bool,
    rollover_size: String,
    rollover_age: String,
    reconcile_interval: Duration,
    policy_id: String,
    template_name: String,
    /// Data streams we've already ensured exist this session (cleared on every
    /// reconcile so a recreated cluster is re-bootstrapped).
    ensured: HashSet<String>,
    /// Set after a write error so the next loop turn re-asserts managed assets.
    needs_reconcile: bool,
    /// AWS SigV4 signer applied to every request when targeting Amazon
    /// OpenSearch Service / Serverless; `None` for a self-hosted cluster.
    signer: Option<Arc<SigV4Signer>>,
}

impl OpenSearchSink {
    pub fn new(http: reqwest::Client, cfg: OpenSearchConfig) -> Self {
        let prefix = sanitize_prefix(&cfg.prefix);
        Self {
            http,
            base: cfg.base.trim_end_matches('/').to_string(),
            policy_id: format!("{prefix}-rollover"),
            template_name: prefix.clone(),
            prefix,
            manage: cfg.manage,
            rollover_size: cfg.rollover_size,
            rollover_age: cfg.rollover_age,
            reconcile_interval: cfg.reconcile_interval,
            ensured: HashSet::new(),
            needs_reconcile: false,
            signer: cfg.signer,
        }
    }

    /// Build, optionally SigV4-sign, and execute one request. All HTTP the sink
    /// makes (bulk writes + management calls) goes through here so AWS-managed
    /// endpoints are authorized uniformly.
    async fn send(&self, builder: reqwest::RequestBuilder) -> anyhow::Result<reqwest::Response> {
        let mut req = builder.build().context("building OpenSearch request")?;
        if let Some(signer) = &self.signer {
            signer.sign(&mut req).await?;
        }
        self.http.execute(req).await.map_err(Into::into)
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<Fanout>) {
        if self.manage {
            self.reconcile().await;
        }
        let mut ticker = tokio::time::interval(self.reconcile_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await; // consume the immediate first tick (we just reconciled)

        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    let Some(item) = maybe else { break };
                    let Fanout::Logs(batch) = item else {
                        continue; // mask is logs-only; ignore anything else defensively
                    };
                    if let Err(e) = self.handle(&batch).await {
                        warn!(error = %e, "opensearch sink batch failed; dropping batch");
                        if self.manage {
                            self.needs_reconcile = true;
                        }
                    }
                }
                _ = ticker.tick(), if self.manage => {
                    self.reconcile().await;
                }
            }

            if self.manage && self.needs_reconcile {
                self.reconcile().await;
                self.needs_reconcile = false;
            }
        }
        info!("opensearch sink worker exiting (queue closed)");
    }

    async fn handle(&mut self, batch: &LogsBatch) -> anyhow::Result<()> {
        if self.manage {
            for name in distinct_targets(batch, &self.prefix) {
                if self.ensured.contains(&name) {
                    continue;
                }
                match self.ensure_data_stream(&name).await {
                    Ok(()) => {
                        self.ensured.insert(name);
                    }
                    Err(e) => {
                        // Not fatal: a matching template + auto_create may still
                        // let the bulk land. Leave it unensured to retry.
                        warn!(stream = %name, error = %e, "opensearch: ensure data stream failed");
                    }
                }
            }
        }
        self.ship(batch).await
    }

    /// Ship one batch via `_bulk`. Returns `Err` on a transport/non-2xx failure
    /// (which triggers a reconcile); per-item `"errors":true` is logged only.
    async fn ship(&self, batch: &LogsBatch) -> anyhow::Result<()> {
        let body = to_bulk_ndjson(batch, &self.prefix);
        if body.is_empty() {
            return Ok(());
        }
        let resp = self
            .send(
                self.http
                    .post(format!("{}/_bulk", self.base))
                    .header(CONTENT_TYPE, "application/x-ndjson")
                    .body(body),
            )
            .await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("opensearch _bulk {status}: {}", snippet(&text));
        }
        if text.contains("\"errors\":true") {
            warn!(
                "opensearch bulk reported per-item errors: {}",
                snippet(&text)
            );
        }
        Ok(())
    }

    /// Assert the managed assets: ISM policy (drift-corrected), then index
    /// template (idempotent). Order matters — the policy's `ism_template` must
    /// exist before any data stream is created so it auto-attaches. Best-effort.
    async fn reconcile(&mut self) {
        if let Err(e) = self.put_ism_policy().await {
            warn!(error = %e, "opensearch: ISM policy assert failed");
        }
        if let Err(e) = self.put_index_template().await {
            warn!(error = %e, "opensearch: index template assert failed");
        }
        // Force re-ensure of data streams (handles a recreated/wiped cluster).
        self.ensured.clear();
        info!(prefix = %self.prefix, "opensearch: managed assets reconciled");
    }

    async fn put_index_template(&self) -> anyhow::Result<()> {
        let url = format!("{}/_index_template/{}", self.base, self.template_name);
        let resp = self
            .send(self.http.put(&url).json(&index_template_doc(&self.prefix)))
            .await?;
        ensure_success(resp, "put index template").await
    }

    /// Create the ISM policy if absent, else overwrite it with the current
    /// `_seq_no`/`_primary_term` so operator drift is corrected.
    async fn put_ism_policy(&self) -> anyhow::Result<()> {
        let url = format!("{}/_plugins/_ism/policies/{}", self.base, self.policy_id);
        let doc = ism_policy_doc(&self.prefix, &self.rollover_size, &self.rollover_age);

        let get = self.send(self.http.get(&url)).await?;
        if get.status() == StatusCode::NOT_FOUND {
            let resp = self.send(self.http.put(&url).json(&doc)).await?;
            return ensure_success(resp, "create ISM policy").await;
        }
        if !get.status().is_success() {
            let status = get.status();
            anyhow::bail!("get ISM policy {status}: {}", body_snippet(get).await);
        }

        let v: Value = get.json().await?;
        let seq = v.get("_seq_no").and_then(Value::as_i64);
        let term = v.get("_primary_term").and_then(Value::as_i64);
        let put_url = match (seq, term) {
            (Some(seq), Some(term)) => format!("{url}?if_seq_no={seq}&if_primary_term={term}"),
            _ => url.clone(),
        };
        let resp = self.send(self.http.put(&put_url).json(&doc)).await?;
        if resp.status() == StatusCode::CONFLICT {
            // Someone updated it between our GET and PUT; next reconcile retries.
            warn!("opensearch: ISM policy update conflicted; will retry next reconcile");
            return Ok(());
        }
        ensure_success(resp, "update ISM policy").await
    }

    /// Ensure a data stream exists (GET, then PUT on 404). Tolerates a creation
    /// race ("resource_already_exists").
    async fn ensure_data_stream(&self, name: &str) -> anyhow::Result<()> {
        let url = format!("{}/_data_stream/{}", self.base, name);
        if self.send(self.http.get(&url)).await?.status().is_success() {
            return Ok(());
        }
        let put = self.send(self.http.put(&url)).await?;
        let status = put.status();
        if status.is_success() {
            return Ok(());
        }
        let text = body_snippet(put).await;
        if text.contains("resource_already_exists") || text.contains("already exists") {
            return Ok(());
        }
        anyhow::bail!("create data stream {name}: {status}: {text}");
    }
}

fn snippet(s: &str) -> String {
    s.chars().take(400).collect()
}

async fn body_snippet(resp: reqwest::Response) -> String {
    snippet(&resp.text().await.unwrap_or_default())
}

async fn ensure_success(resp: reqwest::Response, what: &str) -> anyhow::Result<()> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    anyhow::bail!("{what}: {status}: {}", body_snippet(resp).await);
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

    fn stream(labels: Vec<LabelPair>, entries: Vec<LogEntry>) -> LogStream {
        LogStream {
            fingerprint: 1,
            labels,
            entries,
        }
    }

    fn entry(ts: u64, severity: u8, body: &str, attrs: Vec<LabelPair>) -> LogEntry {
        LogEntry {
            ts_unix_nano: ts,
            severity,
            body: body.into(),
            attributes: attrs,
        }
    }

    #[test]
    fn iso8601_formats_utc_millis() {
        assert_eq!(
            iso8601(1_700_000_000_000_000_000),
            "2023-11-14T22:13:20.000Z"
        );
    }

    #[test]
    fn service_segment_priority_and_fallback() {
        // service.name wins over other keys.
        let s = stream(
            vec![lp("app", "ignored"), lp("service.name", "api")],
            vec![],
        );
        assert_eq!(service_segment(&s), "api");
        // falls back through the list to k8s_app.
        let s = stream(
            vec![lp("namespace", "prod"), lp("k8s_app", "worker")],
            vec![],
        );
        assert_eq!(service_segment(&s), "worker");
        // no service-ish label → general.
        let s = stream(vec![lp("namespace", "prod"), lp("pod", "p-1")], vec![]);
        assert_eq!(service_segment(&s), "general");
    }

    #[test]
    fn service_segment_sanitizes() {
        // uppercase + illegal chars collapse to a safe lowercase segment.
        assert_eq!(
            service_segment(&stream(vec![lp("service", "My App/v2")], vec![])),
            "my-app-v2"
        );
        // a value that sanitizes to empty is skipped → general.
        assert_eq!(
            service_segment(&stream(vec![lp("service", "///")], vec![])),
            "general"
        );
    }

    #[test]
    fn data_stream_name_combines_prefix_and_service() {
        let s = stream(vec![lp("service.name", "api")], vec![]);
        assert_eq!(data_stream_name("scry-logs", &s), "scry-logs-api");
        let s = stream(vec![lp("namespace", "x")], vec![]);
        assert_eq!(data_stream_name("scry-logs", &s), "scry-logs-general");
    }

    #[test]
    fn bulk_routes_each_stream_to_its_service() {
        let batch = LogsBatch {
            streams: vec![
                stream(
                    vec![lp("service.name", "api")],
                    vec![entry(
                        1_700_000_000_000_000_000,
                        9,
                        "hello",
                        vec![lp("stream", "stdout")],
                    )],
                ),
                stream(
                    vec![lp("namespace", "prod")],
                    vec![entry(1_700_000_000_000_000_001, 0, "plain", vec![])],
                ),
            ],
        };
        let text = String::from_utf8(to_bulk_ndjson(&batch, "scry-logs")).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 4); // 2 entries → action+doc each
        assert!(text.ends_with('\n'));

        let a0: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(a0["create"]["_index"], "scry-logs-api");
        let d0: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(d0["@timestamp"], "2023-11-14T22:13:20.000Z");
        assert_eq!(d0["body"], "hello");
        assert_eq!(d0["severity"], 9);
        assert_eq!(d0["labels"]["service.name"], "api");
        assert_eq!(d0["attributes"]["stream"], "stdout");

        // Second stream → general; empty attributes omitted.
        let a1: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(a1["create"]["_index"], "scry-logs-general");
        let d1: Value = serde_json::from_str(lines[3]).unwrap();
        assert_eq!(d1["body"], "plain");
        assert!(d1.get("attributes").is_none());
    }

    #[test]
    fn ism_policy_is_rollover_only_with_template() {
        let p = ism_policy_doc("scry-logs", "30gb", "1d");
        let policy = &p["policy"];
        // Exactly one state, a rollover action, no delete state.
        let states = policy["states"].as_array().unwrap();
        assert_eq!(states.len(), 1);
        let actions = states[0]["actions"].as_array().unwrap();
        assert_eq!(actions.len(), 1);
        let rollover = &actions[0]["rollover"];
        assert_eq!(rollover["min_size"], "30gb");
        assert_eq!(rollover["min_index_age"], "1d");
        // No delete state and no delete action anywhere.
        assert!(
            states.iter().all(|s| s["name"] != "delete"),
            "no delete state"
        );
        assert!(
            states.iter().all(|s| s["actions"]
                .as_array()
                .is_none_or(|acts| acts.iter().all(|a| a.get("delete").is_none()))),
            "no delete action"
        );
        // Auto-attach pattern matches the prefix family.
        assert_eq!(policy["ism_template"]["index_patterns"][0], "scry-logs-*");
    }

    #[test]
    fn index_template_is_data_stream_with_flat_object_labels() {
        let t = index_template_doc("scry-logs");
        assert_eq!(t["index_patterns"][0], "scry-logs-*");
        assert!(t.get("data_stream").is_some());
        let props = &t["template"]["mappings"]["properties"];
        assert_eq!(props["@timestamp"]["type"], "date");
        assert_eq!(props["labels"]["type"], "flat_object");
        assert_eq!(props["attributes"]["type"], "flat_object");
        assert_eq!(props["severity"]["type"], "short");
    }
}
