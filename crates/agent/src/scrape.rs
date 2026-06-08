//! Scrape one Prometheus `/metrics` endpoint and turn it into scry wire series.
//!
//! [`scrape_once`] does the impure work (HTTP GET, optional bearer auth, timing)
//! and delegates the format → wire mapping to the pure, unit-tested
//! [`scrape_to_series`]. Each scrape also synthesizes the standard `up` and
//! `scrape_duration_seconds` gauges, exactly like Prometheus, so a target going
//! down is observable rather than just absent.
//!
//! Label handling mirrors Prometheus: the target's identifying labels
//! (`job`, `instance`, and the k8s `namespace`/`pod`/… set) are authoritative —
//! if a scraped sample carries a label that collides with a target label, the
//! scraped one is renamed `exported_<key>` (the `honor_labels: false` default).
//! Every series carries `__name__=<metric>` as a label, matching the
//! remote-write inbound so query semantics are identical across ingest paths.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::Context;
use scry_proto::{
    constants::METRIC_TYPE_GAUGE,
    fingerprint::fingerprint,
    generated::{MetricSample, SeriesDictEntry},
    LabelPair,
};
use tracing::warn;

use crate::promparse::{self, Scrape};

/// A resolved scrape target: where to fetch, the identifying labels every series
/// from it carries, an optional bearer token, the TLS posture, and an optional
/// exposed-label rename map (the metrics `label_map` from the config).
#[derive(Debug, Clone, Default)]
pub struct ScrapeTarget {
    pub url: String,
    pub labels: Vec<LabelPair>,
    /// Bearer credential. A `File` source is re-read per scrape so a rotating
    /// ServiceAccount token (kubelet auth) is always current.
    pub bearer: Option<BearerSource>,
    /// TLS posture used to pick the right client from a [`ClientPool`]. The
    /// `Default` profile verifies certs (plain HTTP targets ignore it).
    pub tls: TlsProfile,
    /// Exposed-label key → renamed key, applied before the `exported_<key>`
    /// collision check in [`scrape_to_series`]. Empty ⇒ no relabeling.
    pub label_map: HashMap<String, String>,
}

/// Where a target's bearer token comes from. `File` is re-read on every scrape
/// (cheap; the kubelet SA token rotates ~hourly).
#[derive(Debug, Clone)]
pub enum BearerSource {
    Literal(String),
    File(PathBuf),
}

impl BearerSource {
    /// Resolve to the current token string (reads the file for `File`).
    fn resolve(&self) -> anyhow::Result<String> {
        match self {
            BearerSource::Literal(s) => Ok(s.clone()),
            BearerSource::File(p) => Ok(std::fs::read_to_string(p)
                .with_context(|| format!("reading bearer token {}", p.display()))?
                .trim()
                .to_string()),
        }
    }
}

/// TLS verification posture for an HTTPS scrape. `Eq + Hash` so a [`ClientPool`]
/// can key one reqwest client per distinct profile — the common case (the
/// verify-on default) collapses every plain/standard target onto a single client,
/// with kubelet adding one skip-verify client.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct TlsProfile {
    pub insecure_skip_verify: bool,
    pub ca_file: Option<PathBuf>,
}

/// A lazily-populated cache of reqwest clients keyed by [`TlsProfile`]. Building
/// a client bakes in TLS config, so we can't vary verification per request on a
/// shared client — instead we build (at most) one client per distinct profile.
#[derive(Debug)]
pub struct ClientPool {
    timeout: Duration,
    clients: Mutex<HashMap<TlsProfile, reqwest::Client>>,
}

impl ClientPool {
    pub fn new(timeout: Duration) -> Self {
        ClientPool {
            timeout,
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// Get (or build + cache) the client for `tls`. reqwest clients are cheap to
    /// clone (Arc inside).
    pub fn client_for(&self, tls: &TlsProfile) -> anyhow::Result<reqwest::Client> {
        let mut guard = self.clients.lock().expect("client pool mutex poisoned");
        if let Some(c) = guard.get(tls) {
            return Ok(c.clone());
        }
        let client = build_client(self.timeout, tls)?;
        guard.insert(tls.clone(), client.clone());
        Ok(client)
    }
}

/// Build a reqwest client for one TLS profile (skip-verify and/or a custom CA
/// bundle, mirroring `crates/gateway/src/tls.rs`).
fn build_client(timeout: Duration, tls: &TlsProfile) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(timeout);
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca) = &tls.ca_file {
        let pem = std::fs::read(ca).with_context(|| format!("reading CA file {}", ca.display()))?;
        let certs = reqwest::Certificate::from_pem_bundle(&pem)
            .with_context(|| format!("parsing CA bundle {}", ca.display()))?;
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }
    builder.build().context("building scrape HTTP client")
}

/// Current wall-clock time as unix nanoseconds (the scrape timestamp for samples
/// that carry none of their own).
pub fn now_unix_nano() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// The series + samples produced by one scrape (already including `up` and
/// `scrape_duration_seconds`).
#[derive(Debug, Default)]
pub struct ScrapeResult {
    pub series: Vec<SeriesDictEntry>,
    pub samples: Vec<MetricSample>,
}

/// Fetch and convert one target. Never fails: on any HTTP/parse error it emits
/// `up=0` (plus the duration gauge) so the failure is recorded as data.
pub async fn scrape_once(
    http: &reqwest::Client,
    target: &ScrapeTarget,
    now_ns: u64,
) -> ScrapeResult {
    let start = Instant::now();
    let outcome = fetch(http, target).await;
    let elapsed_secs = start.elapsed().as_secs_f64();

    let mut result = ScrapeResult::default();
    let up = match outcome {
        Ok(body) => {
            let mut scrape = promparse::parse(&body);
            if scrape.skipped > 0 {
                warn!(target = %target.url, skipped = scrape.skipped, "skipped malformed metric lines");
            }
            // Rename exposed labels (config metrics.label_map) before the
            // exported_<key> collision check inside scrape_to_series.
            relabel_scrape(&mut scrape, &target.label_map);
            let (series, samples) = scrape_to_series(&scrape, &target.labels, now_ns);
            result.series = series;
            result.samples = samples;
            1.0
        }
        Err(e) => {
            warn!(target = %target.url, error = %e, "scrape failed");
            0.0
        }
    };

    // Synthesized gauges carry only the target labels (+ __name__), like Prometheus.
    push_synth(&mut result, "up", up, &target.labels, now_ns);
    push_synth(
        &mut result,
        "scrape_duration_seconds",
        elapsed_secs,
        &target.labels,
        now_ns,
    );
    result
}

/// HTTP GET the target body. Errors on a non-2xx status or transport failure.
async fn fetch(http: &reqwest::Client, target: &ScrapeTarget) -> anyhow::Result<String> {
    let mut req = http.get(&target.url);
    if let Some(src) = &target.bearer {
        req = req.bearer_auth(src.resolve()?);
    }
    let resp = req.send().await?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("target returned HTTP {status}");
    }
    Ok(resp.text().await?)
}

/// Rename exposed metric label keys in place, per the metrics `label_map`
/// (e.g. `container_name` → `container`). Runs *before* [`scrape_to_series`]'s
/// `exported_<key>` collision handling, so a renamed key that then collides with
/// a target label is still demoted to `exported_<renamed>`. A no-op when the map
/// is empty (the common case), so the non-relabeling hot path is untouched.
pub fn relabel_scrape(scrape: &mut Scrape, label_map: &HashMap<String, String>) {
    if label_map.is_empty() {
        return;
    }
    for m in &mut scrape.metrics {
        for l in &mut m.labels {
            if let Some(renamed) = label_map.get(&l.key) {
                l.key = renamed.clone();
            }
        }
    }
}

/// Pure mapping: a parsed [`Scrape`] + the target's identifying labels → wire
/// series and samples.
///
/// Series are deduplicated by fingerprint. A sample with no explicit timestamp
/// uses `now_ns` (the scrape time); an explicit exposition timestamp is in
/// milliseconds and converted to ns.
pub fn scrape_to_series(
    scrape: &Scrape,
    target_labels: &[LabelPair],
    now_ns: u64,
) -> (Vec<SeriesDictEntry>, Vec<MetricSample>) {
    let target_keys: std::collections::HashSet<&str> =
        target_labels.iter().map(|l| l.key.as_str()).collect();

    let mut index: HashMap<u64, usize> = HashMap::new();
    let mut series: Vec<SeriesDictEntry> = Vec::new();
    let mut samples: Vec<MetricSample> = Vec::new();

    for m in &scrape.metrics {
        // labels = target labels (authoritative) + __name__ + exposed labels
        // (collisions renamed exported_<key>).
        let mut labels: Vec<LabelPair> =
            Vec::with_capacity(target_labels.len() + m.labels.len() + 1);
        labels.extend_from_slice(target_labels);
        labels.push(LabelPair {
            key: "__name__".into(),
            value: m.name.clone(),
        });
        for l in &m.labels {
            let key = if target_keys.contains(l.key.as_str()) || l.key == "__name__" {
                format!("exported_{}", l.key)
            } else {
                l.key.clone()
            };
            labels.push(LabelPair {
                key,
                value: l.value.clone(),
            });
        }

        let fp = fingerprint(&labels);
        if let std::collections::hash_map::Entry::Vacant(slot) = index.entry(fp) {
            slot.insert(series.len());
            series.push(SeriesDictEntry {
                fingerprint: fp,
                metric_type: scrape.metric_type(&m.name),
                labels,
            });
        }
        let ts = match m.timestamp_ms {
            Some(ms) => (ms.max(0) as u64).saturating_mul(1_000_000),
            None => now_ns,
        };
        samples.push(MetricSample {
            fingerprint: fp,
            ts_unix_nano: ts,
            value: m.value,
        });
    }

    (series, samples)
}

/// Append one synthesized single-sample gauge series (`up`, `scrape_duration_seconds`).
fn push_synth(
    result: &mut ScrapeResult,
    name: &str,
    value: f64,
    target_labels: &[LabelPair],
    now_ns: u64,
) {
    let mut labels: Vec<LabelPair> = Vec::with_capacity(target_labels.len() + 1);
    labels.extend_from_slice(target_labels);
    labels.push(LabelPair {
        key: "__name__".into(),
        value: name.into(),
    });
    let fp = fingerprint(&labels);
    result.series.push(SeriesDictEntry {
        fingerprint: fp,
        metric_type: METRIC_TYPE_GAUGE,
        labels,
    });
    result.samples.push(MetricSample {
        fingerprint: fp,
        ts_unix_nano: now_ns,
        value,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tlabels() -> Vec<LabelPair> {
        vec![
            LabelPair {
                key: "job".into(),
                value: "node".into(),
            },
            LabelPair {
                key: "instance".into(),
                value: "10.0.0.1:9100".into(),
            },
        ]
    }

    #[test]
    fn maps_series_with_target_and_name_labels() {
        let scrape = promparse::parse(
            "# TYPE http_requests_total counter\n\
             http_requests_total{method=\"get\"} 5\n\
             http_requests_total{method=\"post\"} 7\n",
        );
        let now = 1_700_000_000_000_000_000u64;
        let (series, samples) = scrape_to_series(&scrape, &tlabels(), now);

        assert_eq!(series.len(), 2);
        assert_eq!(samples.len(), 2);

        // Each series carries job, instance, __name__, and its own method label.
        let s0 = &series[0];
        let get = |k: &str| {
            s0.labels
                .iter()
                .find(|l| l.key == k)
                .map(|l| l.value.as_str())
        };
        assert_eq!(get("job"), Some("node"));
        assert_eq!(get("instance"), Some("10.0.0.1:9100"));
        assert_eq!(get("__name__"), Some("http_requests_total"));
        assert!(get("method").is_some());

        // Fingerprint is over the full label set.
        assert_eq!(s0.fingerprint, fingerprint(&s0.labels));
        // No explicit timestamp → scrape time.
        assert_eq!(samples[0].ts_unix_nano, now);
    }

    #[test]
    fn explicit_timestamp_is_ms_to_ns() {
        let scrape = promparse::parse("g 2 1500\n");
        let (_, samples) = scrape_to_series(&scrape, &[], 999);
        assert_eq!(samples[0].ts_unix_nano, 1500 * 1_000_000);
    }

    #[test]
    fn colliding_exposed_label_is_renamed_exported() {
        // The exporter exposes its own `job` label; the target's wins, the
        // exposed one becomes exported_job.
        let scrape = promparse::parse("m{job=\"self\"} 1\n");
        let (series, _) = scrape_to_series(&scrape, &tlabels(), 0);
        let s = &series[0];
        assert_eq!(
            s.labels
                .iter()
                .find(|l| l.key == "job")
                .map(|l| l.value.as_str()),
            Some("node")
        );
        assert_eq!(
            s.labels
                .iter()
                .find(|l| l.key == "exported_job")
                .map(|l| l.value.as_str()),
            Some("self")
        );
    }

    #[test]
    fn synthesized_up_and_duration() {
        let mut result = ScrapeResult::default();
        push_synth(&mut result, "up", 1.0, &tlabels(), 42);
        push_synth(&mut result, "scrape_duration_seconds", 0.01, &tlabels(), 42);
        assert_eq!(result.series.len(), 2);
        assert_eq!(result.series[0].metric_type, METRIC_TYPE_GAUGE);
        let up = &result.series[0];
        assert_eq!(
            up.labels
                .iter()
                .find(|l| l.key == "__name__")
                .map(|l| l.value.as_str()),
            Some("up")
        );
        assert_eq!(result.samples[0].value, 1.0);
        // up and duration have distinct fingerprints.
        assert_ne!(result.series[0].fingerprint, result.series[1].fingerprint);
    }

    #[test]
    fn relabel_renames_exposed_keys() {
        let mut scrape = promparse::parse("m{container_name=\"c1\",method=\"get\"} 1\n");
        let mut map = HashMap::new();
        map.insert("container_name".to_string(), "container".to_string());
        relabel_scrape(&mut scrape, &map);
        let labels = &scrape.metrics[0].labels;
        assert!(labels
            .iter()
            .any(|l| l.key == "container" && l.value == "c1"));
        assert!(!labels.iter().any(|l| l.key == "container_name"));
        // Unmapped labels are untouched.
        assert!(labels.iter().any(|l| l.key == "method" && l.value == "get"));
    }

    #[test]
    fn relabel_then_collision_is_exported() {
        // Exposed `container_name` is renamed to `container`, which collides with
        // a target `container` label → the renamed one is demoted exported_container.
        let mut scrape = promparse::parse("m{container_name=\"exposed\"} 1\n");
        let mut map = HashMap::new();
        map.insert("container_name".to_string(), "container".to_string());
        relabel_scrape(&mut scrape, &map);
        let target = vec![LabelPair {
            key: "container".into(),
            value: "authoritative".into(),
        }];
        let (series, _) = scrape_to_series(&scrape, &target, 0);
        let s = &series[0];
        assert_eq!(
            s.labels
                .iter()
                .find(|l| l.key == "container")
                .map(|l| l.value.as_str()),
            Some("authoritative")
        );
        assert_eq!(
            s.labels
                .iter()
                .find(|l| l.key == "exported_container")
                .map(|l| l.value.as_str()),
            Some("exposed")
        );
    }

    #[test]
    fn relabel_empty_map_is_noop() {
        let mut scrape = promparse::parse("m{a=\"1\"} 1\n");
        let before = scrape.metrics.clone();
        relabel_scrape(&mut scrape, &HashMap::new());
        assert_eq!(scrape.metrics, before);
    }

    #[test]
    fn dedups_repeated_series_into_one_entry() {
        // Same labels twice (two samples) → one series, two samples.
        let scrape = promparse::parse("g 1\ng 2\n");
        let (series, samples) = scrape_to_series(&scrape, &[], 7);
        assert_eq!(series.len(), 1);
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].fingerprint, samples[1].fingerprint);
    }

    #[test]
    fn tls_profile_eq_and_hash_distinguish_skip_verify() {
        use std::collections::HashSet;
        let default = TlsProfile::default();
        let skip = TlsProfile {
            insecure_skip_verify: true,
            ca_file: None,
        };
        assert_ne!(default, skip);
        let set: HashSet<_> = [default.clone(), skip.clone(), default.clone()]
            .into_iter()
            .collect();
        assert_eq!(set.len(), 2); // default + skip, the duplicate default collapses
    }

    #[test]
    fn client_pool_reuses_one_client_per_profile() {
        let pool = ClientPool::new(Duration::from_secs(5));
        let p = TlsProfile {
            insecure_skip_verify: true,
            ca_file: None,
        };
        // Two requests for the same profile cache exactly one client.
        let _ = pool.client_for(&p).unwrap();
        let _ = pool.client_for(&p).unwrap();
        assert_eq!(pool.clients.lock().unwrap().len(), 1);
        // A distinct profile builds (and caches) a second client.
        let _ = pool.client_for(&TlsProfile::default()).unwrap();
        assert_eq!(pool.clients.lock().unwrap().len(), 2);
    }

    #[test]
    fn bearer_source_file_is_reread() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join(format!("scry-bearer-test-{}", std::process::id()));
        {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(f, "  token-v1  ").unwrap(); // surrounding whitespace trimmed
        }
        let src = BearerSource::File(path.clone());
        assert_eq!(src.resolve().unwrap(), "token-v1");
        // Rotate the file; resolve picks up the new value.
        std::fs::write(&path, "token-v2\n").unwrap();
        assert_eq!(src.resolve().unwrap(), "token-v2");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bearer_source_literal_is_verbatim() {
        assert_eq!(
            BearerSource::Literal("abc".into()).resolve().unwrap(),
            "abc"
        );
    }
}
