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
use std::time::Instant;

use scry_proto::{
    constants::METRIC_TYPE_GAUGE,
    fingerprint::fingerprint,
    generated::{MetricSample, SeriesDictEntry},
    LabelPair,
};
use tracing::warn;

use crate::promparse::{self, Scrape};

/// A resolved scrape target: where to fetch, the identifying labels every series
/// from it carries, and an optional bearer token.
#[derive(Debug, Clone)]
pub struct ScrapeTarget {
    pub url: String,
    pub labels: Vec<LabelPair>,
    pub bearer: Option<String>,
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
            let scrape = promparse::parse(&body);
            if scrape.skipped > 0 {
                warn!(target = %target.url, skipped = scrape.skipped, "skipped malformed metric lines");
            }
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
    if let Some(token) = &target.bearer {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("target returned HTTP {status}");
    }
    Ok(resp.text().await?)
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
    fn dedups_repeated_series_into_one_entry() {
        // Same labels twice (two samples) → one series, two samples.
        let scrape = promparse::parse("g 1\ng 2\n");
        let (series, samples) = scrape_to_series(&scrape, &[], 7);
        assert_eq!(series.len(), 1);
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].fingerprint, samples[1].fingerprint);
    }
}
