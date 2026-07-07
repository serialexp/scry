//! Agent processing-pipeline config (TOML).
//!
//! The agent's *runtime* (where to connect, intervals, batch sizes, scrape
//! targets) stays on CLI flags. The *processing pipeline* — how labels are
//! reshaped and which fields are extracted before records go on the wire —
//! lives here, in a TOML file usually mounted from a Kubernetes ConfigMap. The
//! file is read once at startup; a ConfigMap change is applied by restarting the
//! DaemonSet (no live reload).
//!
//! Six pipeline features, all agent-side (the scry backend already stores
//! arbitrary stream labels → postings and per-entry attributes → a
//! SQL-queryable `Map<Utf8,Utf8>` column, so nothing downstream changes):
//!
//! 1. per-signal `keep` allow-list (the global `--keep` flag becomes per-signal);
//! 2. `label_map` — surface a pod label under a chosen stream-label name (so an
//!    existing `app="…"` query keeps working instead of needing `k8s_app="…"`);
//! 3. `static_labels` — inject fixed labels onto every stream/series;
//! 4. JSON body fields → stream labels (fingerprinted → postings; low card);
//! 5. JSON body fields → per-entry structured metadata (the attributes Map);
//! 6. metric `label_map` — rename exposed metric label keys.
//!
//! **Config owns the pipeline; flags own runtime.** When no `--config` is given
//! the legacy global `--keep` flag is synthesized into a degenerate config
//! (applied to both signals) so there is exactly one downstream code path. When
//! `--config` *is* given, `--keep` must be empty — a loud failure beats silent
//! precedence.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use scry_proto::LabelPair;

use crate::scrape::TlsProfile;
use scry_match::LabelFilter;

// ── File shape (serde) ──────────────────────────────────────────────────────

/// The on-disk TOML document. Every field defaults, so an empty file (or any
/// omitted section) compiles to a no-op pipeline identical to the flags-only
/// default. `deny_unknown_fields` makes a typo'd key (`static_label` vs
/// `static_labels`) fail loudly at startup rather than silently no-op.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileConfig {
    pub logs: LogsSection,
    pub metrics: MetricsSection,
}

/// `[logs]` — the log-stream pipeline.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogsSection {
    /// Keep-only matchers (Prometheus-style; see [`LabelFilter`]). Empty ⇒ keep all.
    pub keep: Vec<String>,
    /// Fixed labels injected on every stream, e.g. `cluster = "gothab-prod"`.
    pub static_labels: BTreeMap<String, String>,
    /// Pod-label key → surfaced stream-label name. A mapped pod label is emitted
    /// under the chosen name *instead of* its `k8s_<key>` form.
    pub label_map: BTreeMap<String, String>,
    /// Optional JSON body extraction.
    pub json: Option<JsonSection>,
}

/// `[logs.json]` — extract fields from a JSON log body.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JsonSection {
    /// JSON fields promoted to stream *labels* (fingerprinted → postings). Keep
    /// these low-cardinality — each distinct value spawns a distinct stream.
    pub labels: Vec<String>,
    /// JSON fields promoted to per-entry *structured metadata* (attributes Map).
    /// High cardinality is fine here.
    pub metadata: Vec<String>,
    /// If set and present as a string, this JSON field replaces the log body.
    pub message_field: Option<String>,
}

/// `[metrics]` — the scraped-series pipeline.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsSection {
    /// Keep-only matchers against the full series label set (incl. `__name__`).
    pub keep: Vec<String>,
    /// Fixed labels injected on every series (and the synthesized `up` /
    /// `scrape_duration_seconds`, since those carry the target labels).
    pub static_labels: BTreeMap<String, String>,
    /// Exposed-label key → renamed key, e.g. `container_name = "container"`.
    /// Applied before the `exported_<key>` collision check.
    pub label_map: BTreeMap<String, String>,
    /// Optional kubelet/cadvisor scraping of *this* node's kubelet.
    pub kubelet: Option<KubeletSection>,
    /// Node-local pod-label service discovery: each job turns matching pods on
    /// this node into scrape targets (no `prometheus.io/scrape` annotation
    /// required). Repeatable as `[[metrics.scrape_pods]]`.
    pub scrape_pods: Vec<PodScrapeJobSection>,
}

/// `[metrics.kubelet]` — scrape the node's own kubelet over HTTPS (:10250).
/// Only present when the table is; each field then defaults.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubeletSection {
    /// Master switch. Default false — the table can exist (pre-staged config)
    /// without scraping until flipped on.
    #[serde(default)]
    pub enabled: bool,
    /// Address template. `${NODE_IP}` / `${NODE_NAME}` are interpolated at
    /// startup from the downward-API values (`--node-ip` / `--node-name`).
    #[serde(default = "default_kubelet_address")]
    pub address: String,
    /// Scrape `/metrics/cadvisor` (per-container resource metrics).
    #[serde(default = "default_true")]
    pub cadvisor: bool,
    /// Scrape `/metrics` (the kubelet's own metrics).
    #[serde(default = "default_true")]
    pub kubelet: bool,
    /// Bearer-token file, re-read per scrape so SA-token rotation is followed.
    #[serde(default = "default_sa_token")]
    pub bearer_file: String,
    /// TLS posture for the HTTPS scrape.
    #[serde(default)]
    pub tls: TlsSection,
}

impl Default for KubeletSection {
    fn default() -> Self {
        KubeletSection {
            enabled: false,
            address: default_kubelet_address(),
            cadvisor: true,
            kubelet: true,
            bearer_file: default_sa_token(),
            tls: TlsSection::default(),
        }
    }
}

/// `[metrics.kubelet.tls]` — defaults to skip-verify (the kubelet serving cert
/// rarely has a SAN matching the IP we dial), matching Prometheus' stock job.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSection {
    #[serde(default = "default_true")]
    pub insecure_skip_verify: bool,
    /// Optional CA bundle (PEM) to verify against instead of skip-verify.
    #[serde(default)]
    pub ca_file: Option<String>,
}

impl Default for TlsSection {
    fn default() -> Self {
        TlsSection {
            insecure_skip_verify: true,
            ca_file: None,
        }
    }
}

/// One `[[metrics.scrape_pods]]` job: scrape pods whose labels match `selector`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PodScrapeJobSection {
    /// `job` label for matched targets. Defaults (like annotation SD) to the
    /// pod's `app.kubernetes.io/name`/`app`/`k8s-app` label, else the pod name.
    #[serde(default)]
    pub job: Option<String>,
    /// Label selector — ALL `key = value` pairs must match the pod's labels.
    pub selector: BTreeMap<String, String>,
    /// Container port to scrape.
    pub port: u16,
    #[serde(default = "default_metrics_path")]
    pub path: String,
    #[serde(default = "default_scheme")]
    pub scheme: String,
}

fn default_true() -> bool {
    true
}
fn default_kubelet_address() -> String {
    "https://${NODE_IP}:10250".to_string()
}
fn default_sa_token() -> String {
    "/var/run/secrets/kubernetes.io/serviceaccount/token".to_string()
}
fn default_metrics_path() -> String {
    "/metrics".to_string()
}
fn default_scheme() -> String {
    "http".to_string()
}

// ── Compiled runtime pipelines ──────────────────────────────────────────────

/// The compiled log pipeline, built once at startup and shared (read-only) by
/// the batcher's `ingest`.
#[derive(Debug, Default)]
pub struct LogPipeline {
    pub keep: LabelFilter,
    pub static_labels: Vec<LabelPair>,
    pub label_map: HashMap<String, String>,
    pub json: Option<JsonPipeline>,
}

/// Compiled JSON extraction config.
#[derive(Debug, Default)]
pub struct JsonPipeline {
    pub labels: Vec<String>,
    pub metadata: Vec<String>,
    pub message_field: Option<String>,
}

impl JsonPipeline {
    /// True when extraction would produce nothing — lets the hot path skip the
    /// per-line JSON parse entirely.
    pub fn is_noop(&self) -> bool {
        self.labels.is_empty() && self.metadata.is_empty() && self.message_field.is_none()
    }

    /// True when extraction can add labels (and thus change a stream's
    /// fingerprint per line). When false, the enriched labels are stable per
    /// stream and can be cached by the base fingerprint.
    pub fn adds_labels(&self) -> bool {
        !self.labels.is_empty()
    }
}

impl LogPipeline {
    /// True when the pipeline reshapes labels/attributes/body — i.e. `enrich`
    /// must run. When false, `ingest` uses the base `stream_labels` output
    /// directly, keeping the default (no-config) path byte-identical to today.
    /// (`keep` is intentionally excluded: it filters, it doesn't reshape.)
    pub fn enriches(&self) -> bool {
        !self.static_labels.is_empty()
            || !self.label_map.is_empty()
            || self.json.as_ref().is_some_and(|j| !j.is_noop())
    }

    /// True when JSON extraction can add stream labels — meaning the fingerprint
    /// varies per line and the enriched labels can't be cached per stream.
    pub fn json_adds_labels(&self) -> bool {
        self.json.as_ref().is_some_and(|j| j.adds_labels())
    }

    /// True when a per-line JSON body parse is needed at all.
    pub fn needs_json(&self) -> bool {
        self.json.as_ref().is_some_and(|j| !j.is_noop())
    }
}

/// The compiled metric pipeline.
#[derive(Debug, Default)]
pub struct MetricPipeline {
    pub keep: LabelFilter,
    pub static_labels: Vec<LabelPair>,
    pub label_map: HashMap<String, String>,
    /// Kubelet/cadvisor scraping of this node (if a `[metrics.kubelet]` table
    /// was present). May be present-but-disabled — check [`KubeletConfig::enabled`].
    pub kubelet: Option<KubeletConfig>,
    /// Node-local pod-label SD jobs.
    pub scrape_pods: Vec<PodScrapeJob>,
}

/// Compiled kubelet scrape config. `address` is still a template
/// (`${NODE_IP}`/`${NODE_NAME}`) — interpolated in `main` where the runtime
/// node identity lives.
#[derive(Debug, Clone)]
pub struct KubeletConfig {
    pub enabled: bool,
    pub address: String,
    pub cadvisor: bool,
    pub kubelet: bool,
    pub bearer_file: PathBuf,
    pub tls: TlsProfile,
}

/// Compiled pod-label SD job (path normalized, scheme defaulted).
#[derive(Debug, Clone)]
pub struct PodScrapeJob {
    pub job: Option<String>,
    pub selector: BTreeMap<String, String>,
    pub port: u16,
    pub path: String,
    pub scheme: String,
}

impl FileConfig {
    /// Synthesize a config from the legacy global `--keep` flag, applied to both
    /// signals. Used when no `--config` file is given so there is one downstream
    /// code path.
    pub fn from_global_keep(keep: &[String]) -> Self {
        FileConfig {
            logs: LogsSection {
                keep: keep.to_vec(),
                ..Default::default()
            },
            metrics: MetricsSection {
                keep: keep.to_vec(),
                ..Default::default()
            },
        }
    }

    /// Compile the file shape into runtime pipelines, validating keep regexes.
    pub fn compile(self) -> Result<(LogPipeline, MetricPipeline)> {
        let logs = LogPipeline {
            keep: LabelFilter::parse(&self.logs.keep).context("compiling [logs] keep")?,
            static_labels: to_label_pairs(self.logs.static_labels),
            label_map: self.logs.label_map.into_iter().collect(),
            json: self.logs.json.map(|j| JsonPipeline {
                labels: j.labels,
                metadata: j.metadata,
                message_field: j.message_field,
            }),
        };
        let kubelet = self.metrics.kubelet.map(|k| KubeletConfig {
            enabled: k.enabled,
            address: k.address,
            cadvisor: k.cadvisor,
            kubelet: k.kubelet,
            bearer_file: PathBuf::from(k.bearer_file),
            tls: TlsProfile {
                insecure_skip_verify: k.tls.insecure_skip_verify,
                ca_file: k.tls.ca_file.map(PathBuf::from),
            },
        });
        let scrape_pods = self
            .metrics
            .scrape_pods
            .into_iter()
            .map(|p| PodScrapeJob {
                job: p.job,
                selector: p.selector,
                port: p.port,
                path: normalize_path(&p.path),
                scheme: p.scheme,
            })
            .collect();
        let metrics = MetricPipeline {
            keep: LabelFilter::parse(&self.metrics.keep).context("compiling [metrics] keep")?,
            static_labels: to_label_pairs(self.metrics.static_labels),
            label_map: self.metrics.label_map.into_iter().collect(),
            kubelet,
            scrape_pods,
        };
        Ok((logs, metrics))
    }
}

/// Read + parse a TOML config file. Thin I/O, kept separate from the pure
/// [`FileConfig::compile`].
pub fn load(path: &Path) -> Result<FileConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading agent config {}", path.display()))?;
    let cfg: FileConfig = toml::from_str(&text)
        .with_context(|| format!("parsing agent config {}", path.display()))?;
    Ok(cfg)
}

/// Resolve the agent's processing pipeline from the optional `--config` path and
/// the legacy global `--keep` flag.
///
/// Config owns the pipeline: when a config file is given, `--keep` must be empty
/// (its per-signal home is the `[logs]` / `[metrics] keep` arrays). When no
/// config is given, the flag is synthesized into a degenerate config.
pub fn resolve(
    config_path: Option<&Path>,
    global_keep: &[String],
) -> Result<(LogPipeline, MetricPipeline)> {
    let file = match config_path {
        Some(p) => {
            if !global_keep.is_empty() {
                bail!(
                    "--keep is ignored when --config is set; move it into the \
                     [logs] / [metrics] `keep` arrays of {}",
                    p.display()
                );
            }
            load(p)?
        }
        None => FileConfig::from_global_keep(global_keep),
    };
    file.compile()
}

/// Convert a sorted `BTreeMap` of static labels into the wire `LabelPair` shape.
fn to_label_pairs(m: BTreeMap<String, String>) -> Vec<LabelPair> {
    m.into_iter()
        .map(|(key, value)| LabelPair { key, value })
        .collect()
}

/// Ensure a scrape path has a leading slash (`metrics` → `/metrics`), matching
/// the annotation-SD path handling in `discovery::build_scrape_target`.
fn normalize_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn empty_file_compiles_to_noop() {
        let cfg: FileConfig = toml::from_str("").unwrap();
        let (logs, metrics) = cfg.compile().unwrap();
        assert!(logs.keep.is_empty());
        assert!(logs.static_labels.is_empty());
        assert!(logs.label_map.is_empty());
        assert!(logs.json.is_none());
        assert!(metrics.keep.is_empty());
        assert!(metrics.static_labels.is_empty());
        assert!(metrics.label_map.is_empty());
    }

    #[test]
    fn full_config_round_trips() {
        let toml_src = r#"
            [logs]
            keep = ['namespace=prod']
            static_labels = { cluster = "gothab-prod" }
            label_map = { app = "app" }

            [logs.json]
            labels = ["level"]
            metadata = ["request_id"]
            message_field = "msg"

            [metrics]
            keep = ['__name__=~"http_.*"']
            static_labels = { cluster = "gothab-prod" }
            label_map = { container_name = "container" }
        "#;
        let cfg: FileConfig = toml::from_str(toml_src).unwrap();
        let (logs, metrics) = cfg.compile().unwrap();

        assert_eq!(logs.keep.len(), 1);
        assert_eq!(
            logs.static_labels,
            vec![LabelPair {
                key: "cluster".into(),
                value: "gothab-prod".into()
            }]
        );
        assert_eq!(logs.label_map.get("app"), Some(&"app".to_string()));
        let j = logs.json.unwrap();
        assert_eq!(j.labels, vec!["level".to_string()]);
        assert_eq!(j.metadata, vec!["request_id".to_string()]);
        assert_eq!(j.message_field.as_deref(), Some("msg"));
        assert!(j.adds_labels());
        assert!(!j.is_noop());

        assert_eq!(metrics.keep.len(), 1);
        assert_eq!(
            metrics.label_map.get("container_name"),
            Some(&"container".to_string())
        );
    }

    #[test]
    fn unknown_field_is_rejected() {
        // `static_label` (singular) is a typo; deny_unknown_fields catches it.
        let err = toml::from_str::<FileConfig>("[logs]\nstatic_label = { a = \"b\" }\n");
        assert!(err.is_err(), "expected unknown-field error, got {err:?}");
    }

    #[test]
    fn bad_keep_regex_fails_compile() {
        let cfg: FileConfig = toml::from_str("[logs]\nkeep = ['a=~(']\n").unwrap();
        assert!(cfg.compile().is_err());
    }

    #[test]
    fn from_global_keep_applies_to_both_signals() {
        let (logs, metrics) = FileConfig::from_global_keep(&["namespace=prod".to_string()])
            .compile()
            .unwrap();
        assert_eq!(logs.keep.len(), 1);
        assert_eq!(metrics.keep.len(), 1);
    }

    #[test]
    fn resolve_bails_on_config_plus_keep() {
        // The bail happens before any file read, so a nonexistent path is fine.
        let err = resolve(
            Some(Path::new("/nonexistent/agent.toml")),
            &["a=b".to_string()],
        );
        let msg = format!("{:#}", err.unwrap_err());
        assert!(msg.contains("--keep"), "unexpected error: {msg}");
    }

    #[test]
    fn resolve_without_config_uses_global_keep() {
        let (logs, metrics) = resolve(None, &["namespace=prod".to_string()]).unwrap();
        assert_eq!(logs.keep.len(), 1);
        assert_eq!(metrics.keep.len(), 1);
    }

    #[test]
    fn kubelet_and_scrape_pods_round_trip() {
        let toml_src = r#"
            [metrics.kubelet]
            enabled = true
            address = "https://${NODE_IP}:10250"
            bearer_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
            [metrics.kubelet.tls]
            insecure_skip_verify = true

            [[metrics.scrape_pods]]
            job = "node-exporter"
            selector = { "app.kubernetes.io/name" = "node-exporter" }
            port = 9100

            [[metrics.scrape_pods]]
            selector = { app = "kube-state-metrics" }
            port = 8080
            path = "metrics"
            scheme = "http"
        "#;
        let cfg: FileConfig = toml::from_str(toml_src).unwrap();
        let (_logs, metrics) = cfg.compile().unwrap();

        let k = metrics.kubelet.expect("kubelet present");
        assert!(k.enabled);
        assert_eq!(k.address, "https://${NODE_IP}:10250");
        // defaults: both endpoints on, skip-verify on, no CA.
        assert!(k.cadvisor && k.kubelet);
        assert!(k.tls.insecure_skip_verify);
        assert!(k.tls.ca_file.is_none());
        assert_eq!(
            k.bearer_file,
            PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount/token")
        );

        assert_eq!(metrics.scrape_pods.len(), 2);
        let ne = &metrics.scrape_pods[0];
        assert_eq!(ne.job.as_deref(), Some("node-exporter"));
        assert_eq!(ne.port, 9100);
        assert_eq!(ne.path, "/metrics"); // defaulted
        assert_eq!(ne.scheme, "http"); // defaulted
        assert_eq!(
            ne.selector
                .get("app.kubernetes.io/name")
                .map(String::as_str),
            Some("node-exporter")
        );
        // `path = "metrics"` gets a leading slash.
        assert_eq!(metrics.scrape_pods[1].path, "/metrics");
        assert!(metrics.scrape_pods[1].job.is_none());
    }

    #[test]
    fn kubelet_table_defaults_when_minimal() {
        // An empty [metrics.kubelet] table: enabled defaults false, endpoints on,
        // skip-verify on, default address + SA token path.
        let cfg: FileConfig = toml::from_str("[metrics.kubelet]\n").unwrap();
        let (_logs, metrics) = cfg.compile().unwrap();
        let k = metrics.kubelet.expect("kubelet present");
        assert!(!k.enabled);
        assert_eq!(k.address, "https://${NODE_IP}:10250");
        assert!(k.cadvisor && k.kubelet);
        assert!(k.tls.insecure_skip_verify);
    }

    #[test]
    fn no_kubelet_table_means_none() {
        let cfg: FileConfig = toml::from_str("[metrics]\nstatic_labels = { a = \"b\" }\n").unwrap();
        let (_logs, metrics) = cfg.compile().unwrap();
        assert!(metrics.kubelet.is_none());
        assert!(metrics.scrape_pods.is_empty());
    }

    #[test]
    fn scrape_pod_unknown_field_is_rejected() {
        let err = toml::from_str::<FileConfig>(
            "[[metrics.scrape_pods]]\nselector = { app = \"x\" }\nport = 9100\nprt = 1\n",
        );
        assert!(err.is_err(), "expected unknown-field error, got {err:?}");
    }

    #[test]
    fn scrape_pod_requires_selector_and_port() {
        // Missing `port` is an error (no default).
        let err =
            toml::from_str::<FileConfig>("[[metrics.scrape_pods]]\nselector = { app = \"x\" }\n");
        assert!(err.is_err(), "expected missing-field error, got {err:?}");
    }
}
