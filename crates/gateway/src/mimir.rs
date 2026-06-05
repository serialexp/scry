//! Mimir sink: re-emit a fanned-out [`MetricsBatch`] as a Prometheus
//! remote-write request and POST it to a Mimir distributor.
//!
//! This is the inverse of the remote-write **inbound** ([`crate::promwrite`]):
//! the gateway accepts metrics (via remote-write or the native wire) and tees
//! them back out to Mimir in the same v1 format. We reuse the protobuf wire
//! types and the snappy codec from [`crate::promwrite`] rather than redeclaring
//! them, so the encode path is guaranteed symmetric with the decode path.
//!
//! [`to_write_request`] is pure and unit-tested; [`MimirSink`] is a thin worker
//! that serializes it and ships it best-effort (drops on failure, per D-041).
//!
//! Out of scope (same as the inbound): remote-write **v2**, native histograms,
//! exemplars, and per-series type metadata — every sample lands as a plain
//! float series.

use std::collections::HashMap;

use scry_proto::generated::MetricsBatch;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::promwrite::{encode_snappy, Label, Sample, TimeSeries, WriteRequest};
use crate::sink::Fanout;

/// Pure mapping: a scry [`MetricsBatch`] → a remote-write [`WriteRequest`].
///
/// One `TimeSeries` per series fingerprint, labels taken from the batch's series
/// dictionary. Samples are grouped by fingerprint, preserving first-seen order.
/// Sample timestamps are scry nanoseconds and converted to Prometheus
/// **milliseconds**. A sample whose fingerprint is not in the series dictionary
/// is dropped defensively (it would otherwise carry no labels).
pub fn to_write_request(batch: &MetricsBatch) -> WriteRequest {
    // fingerprint → its labels (as remote-write Labels).
    let labels_by_fp: HashMap<u64, Vec<Label>> = batch
        .series
        .iter()
        .map(|s| {
            let labels = s
                .labels
                .iter()
                .map(|l| Label {
                    name: l.key.clone(),
                    value: l.value.clone(),
                })
                .collect();
            (s.fingerprint, labels)
        })
        .collect();

    // fingerprint → index into the output Vec (preserves first-seen order).
    let mut index: HashMap<u64, usize> = HashMap::new();
    let mut timeseries: Vec<TimeSeries> = Vec::new();

    for sample in &batch.samples {
        let Some(labels) = labels_by_fp.get(&sample.fingerprint) else {
            continue; // sample with no series dict entry → no labels → drop
        };
        let idx = *index.entry(sample.fingerprint).or_insert_with(|| {
            timeseries.push(TimeSeries {
                labels: labels.clone(),
                samples: Vec::new(),
            });
            timeseries.len() - 1
        });
        timeseries[idx].samples.push(Sample {
            value: sample.value,
            timestamp: (sample.ts_unix_nano / 1_000_000) as i64,
        });
    }

    WriteRequest { timeseries }
}

/// Worker that ships fanned-out metric batches to a Mimir distributor's
/// remote-write endpoint.
pub struct MimirSink {
    http: reqwest::Client,
    endpoint: String,
    tenant: Option<String>,
}

impl MimirSink {
    /// `base` is the Mimir base URL (e.g. `http://mimir:9009`); the distributor
    /// remote-write path is appended. `tenant`, when set, is sent as the
    /// `X-Scope-OrgID` header for multi-tenant Mimir.
    pub fn new(http: reqwest::Client, base: &str, tenant: Option<String>) -> Self {
        let endpoint = format!("{}/api/v1/push", base.trim_end_matches('/'));
        Self {
            http,
            endpoint,
            tenant,
        }
    }

    pub async fn run(self, mut rx: mpsc::Receiver<Fanout>) {
        while let Some(item) = rx.recv().await {
            let Fanout::Metrics(batch) = item else {
                continue; // mask is metrics-only; ignore anything else defensively
            };
            if let Err(e) = self.ship(&batch).await {
                warn!(error = %e, "mimir sink push failed; dropping batch");
            }
        }
        info!("mimir sink worker exiting (queue closed)");
    }

    async fn ship(&self, batch: &MetricsBatch) -> anyhow::Result<()> {
        let req = to_write_request(batch);
        if req.timeseries.is_empty() {
            return Ok(()); // nothing mappable (e.g. samples with no dict entry)
        }
        let body = encode_snappy(&req);
        let mut builder = self
            .http
            .post(&self.endpoint)
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .body(body);
        if let Some(tenant) = &self.tenant {
            builder = builder.header("X-Scope-OrgID", tenant);
        }
        let resp = builder.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "mimir responded {status}: {}",
                body.chars().take(400).collect::<String>()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promwrite::{map_remote_write, sample_request};
    use scry_proto::{
        constants::METRIC_TYPE_UNKNOWN,
        fingerprint::fingerprint,
        generated::{MetricSample, MetricsBatch, SeriesDictEntry},
        LabelPair,
    };

    fn lp(k: &str, v: &str) -> LabelPair {
        LabelPair {
            key: k.into(),
            value: v.into(),
        }
    }

    #[test]
    fn builds_one_timeseries_per_series_with_dict_labels_and_ms() {
        let labels = vec![lp("__name__", "up"), lp("job", "x")];
        let fp = fingerprint(&labels);
        let batch = MetricsBatch {
            series: vec![SeriesDictEntry {
                fingerprint: fp,
                metric_type: METRIC_TYPE_UNKNOWN,
                labels: labels.clone(),
            }],
            samples: vec![
                MetricSample {
                    fingerprint: fp,
                    ts_unix_nano: 1_700_000_000_000_000_000,
                    value: 1.0,
                },
                MetricSample {
                    fingerprint: fp,
                    ts_unix_nano: 1_700_000_001_000_000_000,
                    value: 2.0,
                },
            ],
        };

        let req = to_write_request(&batch);
        assert_eq!(req.timeseries.len(), 1);
        let ts = &req.timeseries[0];
        // Labels come from the series dict.
        assert_eq!(ts.labels.len(), 2);
        assert_eq!(ts.labels[0].name, "__name__");
        assert_eq!(ts.labels[0].value, "up");
        // Both samples grouped under the one fingerprint, ns → ms.
        assert_eq!(ts.samples.len(), 2);
        assert_eq!(ts.samples[0].timestamp, 1_700_000_000_000);
        assert_eq!(ts.samples[0].value, 1.0);
        assert_eq!(ts.samples[1].timestamp, 1_700_000_001_000);
    }

    #[test]
    fn drops_samples_with_unknown_fingerprint() {
        let labels = vec![lp("__name__", "up")];
        let fp = fingerprint(&labels);
        let batch = MetricsBatch {
            series: vec![SeriesDictEntry {
                fingerprint: fp,
                metric_type: METRIC_TYPE_UNKNOWN,
                labels,
            }],
            samples: vec![
                MetricSample {
                    fingerprint: fp,
                    ts_unix_nano: 1_000_000,
                    value: 1.0,
                },
                MetricSample {
                    fingerprint: 0xdead_beef, // no dict entry
                    ts_unix_nano: 2_000_000,
                    value: 9.0,
                },
            ],
        };
        let req = to_write_request(&batch);
        assert_eq!(req.timeseries.len(), 1);
        assert_eq!(req.timeseries[0].samples.len(), 1);
        assert_eq!(req.timeseries[0].samples[0].timestamp, 1);
    }

    #[test]
    fn roundtrips_through_remote_write_decode() {
        // A ms-aligned remote-write request → scry batch → back to remote-write
        // reproduces the same series and sample counts and values.
        let original = sample_request(3, 4);
        let batch = map_remote_write(original.clone());
        let rebuilt = to_write_request(&batch);

        assert_eq!(rebuilt.timeseries.len(), original.timeseries.len());
        let total_samples: usize = rebuilt.timeseries.iter().map(|t| t.samples.len()).sum();
        let original_samples: usize = original.timeseries.iter().map(|t| t.samples.len()).sum();
        assert_eq!(total_samples, original_samples);

        // Match rebuilt series back to originals by fingerprint and compare the
        // sample sets (order within a series is preserved; series order is too,
        // since map_remote_write and to_write_request both keep first-seen order).
        for (orig, rebuilt) in original.timeseries.iter().zip(rebuilt.timeseries.iter()) {
            assert_eq!(rebuilt.samples.len(), orig.samples.len());
            for (a, b) in orig.samples.iter().zip(rebuilt.samples.iter()) {
                assert_eq!(a.timestamp, b.timestamp);
                assert_eq!(a.value, b.value);
            }
        }
    }
}
