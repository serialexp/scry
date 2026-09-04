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

use prost::Message;
use scry_proto::generated::{
    MetricCountV2, MetricCountV2Value, MetricDescriptorV2, MetricExemplarV2, MetricNumberV2,
    MetricNumberV2Value, MetricPointV2Value, MetricsBatch, MetricsBatchV2, SparseBucketsV2,
};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::prometheus_proto::{v1, v2, REMOTE_WRITE_V1_CONTENT_TYPE};
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

fn millis(ns: u64) -> i64 {
    (ns / 1_000_000) as i64
}

fn number(value: &MetricNumberV2) -> f64 {
    match &value.value {
        MetricNumberV2Value::IntegerValueV2(v) => v.value as f64,
        MetricNumberV2Value::DoubleValueV2(v) => v.value,
    }
}

fn labels(descriptor: &MetricDescriptorV2, attrs: &[scry_proto::LabelPair]) -> Vec<v1::Label> {
    std::iter::once(v1::Label {
        name: "__name__".into(),
        value: descriptor.name.clone(),
    })
    .chain(
        descriptor
            .resource_attrs
            .iter()
            .chain(descriptor.scope_attrs.iter())
            .chain(attrs)
            .map(|l| v1::Label {
                name: l.key.clone(),
                value: l.value.clone(),
            }),
    )
    .collect()
}

fn exemplars(values: &[MetricExemplarV2]) -> Vec<v1::Exemplar> {
    values
        .iter()
        .map(|e| {
            let mut labels: Vec<_> = e
                .filtered_attrs
                .iter()
                .map(|l| v1::Label {
                    name: l.key.clone(),
                    value: l.value.clone(),
                })
                .collect();
            if e.trace_id.iter().any(|b| *b != 0) {
                labels.push(v1::Label {
                    name: "trace_id".into(),
                    value: e.trace_id.iter().map(|b| format!("{b:02x}")).collect(),
                });
                labels.push(v1::Label {
                    name: "span_id".into(),
                    value: e.span_id.iter().map(|b| format!("{b:02x}")).collect(),
                });
            }
            v1::Exemplar {
                labels,
                value: number(&e.number),
                timestamp: millis(e.ts_unix_nano),
            }
        })
        .collect()
}

fn sparse<T>(
    b: &SparseBucketsV2,
    make_span: impl Fn(i32, u32) -> T,
) -> (Vec<T>, Vec<i64>, Vec<f64>) {
    let mut indices = Vec::with_capacity(b.deltas.len());
    let mut index = i64::from(b.offset);
    for delta in &b.deltas {
        index += i64::from(*delta);
        // Canonical validation has already guaranteed this range.
        indices.push(index as i32);
    }
    let mut spans = Vec::new();
    let mut previous_end = 0i32;
    let mut pos = 0;
    while pos < indices.len() {
        let start = indices[pos];
        let mut end = start;
        while pos + 1 < indices.len() && indices[pos + 1] == end + 1 {
            pos += 1;
            end += 1;
        }
        spans.push(make_span(start - previous_end, (end - start + 1) as u32));
        previous_end = end + 1;
        pos += 1;
    }
    let all_int = b
        .counts
        .iter()
        .all(|c| matches!(c.value, MetricCountV2Value::IntegerCountV2(_)));
    if all_int {
        let mut previous = 0i128;
        let deltas = b
            .counts
            .iter()
            .map(|c| {
                let MetricCountV2Value::IntegerCountV2(v) = &c.value else {
                    unreachable!()
                };
                let current = i128::from(v.value);
                let difference = current - previous;
                let delta = i64::try_from(difference).unwrap_or_else(|_| {
                    // Prometheus's integer-delta wire cannot represent this
                    // discontinuity; validation at the caller reports it.
                    if difference.is_negative() {
                        i64::MIN
                    } else {
                        i64::MAX
                    }
                });
                previous = current;
                delta
            })
            .collect();
        (spans, deltas, vec![])
    } else {
        let counts = b
            .counts
            .iter()
            .map(|c| match &c.value {
                MetricCountV2Value::IntegerCountV2(v) => v.value as f64,
                MetricCountV2Value::FloatCountV2(v) => v.value,
            })
            .collect();
        (spans, vec![], counts)
    }
}

fn count_v1(c: &MetricCountV2) -> v1::histogram::Count {
    match &c.value {
        MetricCountV2Value::IntegerCountV2(v) => v1::histogram::Count::CountInt(v.value),
        MetricCountV2Value::FloatCountV2(v) => v1::histogram::Count::CountFloat(v.value),
    }
}
fn zero_v1(c: &MetricCountV2) -> v1::histogram::ZeroCount {
    match &c.value {
        MetricCountV2Value::IntegerCountV2(v) => v1::histogram::ZeroCount::ZeroCountInt(v.value),
        MetricCountV2Value::FloatCountV2(v) => v1::histogram::ZeroCount::ZeroCountFloat(v.value),
    }
}

/// Losslessly maps canonical structured metrics to Remote Write 1.0 where the
/// protocol permits it. Explicit histograms and summaries have no native RW
/// representation and are rejected rather than silently dropped.
pub fn structured_to_v1(batch: &MetricsBatchV2) -> anyhow::Result<v1::WriteRequest> {
    scry_proto::metrics_v2::validate(batch)?;
    let descriptors: HashMap<_, _> = batch.descriptors.iter().map(|d| (d.id, d)).collect();
    let metadata = batch
        .descriptors
        .iter()
        .map(|d| v1::MetricMetadata {
            r#type: match d.metric_kind {
                1 => v1::MetricType::Gauge,
                2 => v1::MetricType::Counter,
                4 => v1::MetricType::Histogram,
                _ => v1::MetricType::Unknown,
            } as i32,
            metric_family_name: d.name.clone(),
            help: d.description.clone(),
            unit: d.unit.clone(),
        })
        .collect();
    let mut timeseries = Vec::with_capacity(batch.points.len());
    for point in &batch.points {
        let (id, attrs, ex, sample, histogram) = match &point.value {
            MetricPointV2Value::ScalarPointV2(p) => (
                p.descriptor_id,
                &p.attributes,
                &p.exemplars,
                Some(v1::Sample {
                    value: number(&p.number),
                    timestamp: millis(p.ts_unix_nano),
                }),
                None,
            ),
            MetricPointV2Value::ExponentialHistogramPointV2(p) => {
                if !p.custom_bounds.is_empty() {
                    anyhow::bail!("custom-bound histograms require Prometheus remote write v2");
                }
                let (negative_spans, negative_deltas, negative_counts) =
                    sparse(&p.negative, |offset, length| v1::BucketSpan {
                        offset,
                        length,
                    });
                let (positive_spans, positive_deltas, positive_counts) =
                    sparse(&p.positive, |offset, length| v1::BucketSpan {
                        offset,
                        length,
                    });
                let h = v1::Histogram {
                    count: Some(count_v1(&p.count)),
                    sum: if p.has_sum == 1 { p.sum } else { f64::NAN },
                    schema: p.scale,
                    zero_threshold: p.zero_threshold,
                    zero_count: Some(zero_v1(&p.zero_count)),
                    negative_spans,
                    negative_deltas,
                    negative_counts,
                    positive_spans,
                    positive_deltas,
                    positive_counts,
                    reset_hint: i32::from(p.reset_hint),
                    timestamp: millis(p.ts_unix_nano),
                    custom_values: p.custom_bounds.clone(),
                };
                (p.descriptor_id, &p.attributes, &p.exemplars, None, Some(h))
            }
            MetricPointV2Value::HistogramPointV2(_) => {
                anyhow::bail!("explicit histograms are not supported by Prometheus remote write")
            }
            MetricPointV2Value::SummaryPointV2(_) => {
                anyhow::bail!("summaries are not supported by Prometheus remote write")
            }
        };
        let descriptor = descriptors[&id];
        timeseries.push(v1::TimeSeries {
            labels: labels(descriptor, attrs),
            samples: sample.into_iter().collect(),
            exemplars: exemplars(ex),
            histograms: histogram.into_iter().collect(),
        });
    }
    Ok(v1::WriteRequest {
        timeseries,
        metadata,
    })
}

/// Structured metrics encoder for Remote Write 2.0.
pub fn structured_to_v2(batch: &MetricsBatchV2) -> anyhow::Result<v2::Request> {
    let v1 = structured_to_v1(batch)?;
    let mut symbols = vec![String::new()];
    let mut intern = |s: &str| -> u32 {
        if let Some(i) = symbols.iter().position(|x| x == s) {
            i as u32
        } else {
            symbols.push(s.into());
            (symbols.len() - 1) as u32
        }
    };
    let meta: HashMap<_, _> = batch
        .descriptors
        .iter()
        .map(|d| (d.name.as_str(), d))
        .collect();
    let timeseries = v1
        .timeseries
        .into_iter()
        .map(|ts| {
            let name = ts
                .labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.as_str())
                .unwrap_or("");
            let d = meta[name];
            let labels_refs = ts
                .labels
                .iter()
                .flat_map(|l| [intern(&l.name), intern(&l.value)])
                .collect();
            let samples = ts
                .samples
                .into_iter()
                .map(|s| v2::Sample {
                    value: s.value,
                    timestamp: s.timestamp,
                    start_timestamp: 0,
                })
                .collect();
            let exemplars = ts
                .exemplars
                .into_iter()
                .map(|e| v2::Exemplar {
                    labels_refs: e
                        .labels
                        .iter()
                        .flat_map(|l| [intern(&l.name), intern(&l.value)])
                        .collect(),
                    value: e.value,
                    timestamp: e.timestamp,
                })
                .collect();
            let histograms = ts
                .histograms
                .into_iter()
                .map(|h| v2::Histogram {
                    count: h.count.map(|c| match c {
                        v1::histogram::Count::CountInt(x) => v2::histogram::Count::CountInt(x),
                        v1::histogram::Count::CountFloat(x) => v2::histogram::Count::CountFloat(x),
                    }),
                    sum: h.sum,
                    schema: h.schema,
                    zero_threshold: h.zero_threshold,
                    zero_count: h.zero_count.map(|c| match c {
                        v1::histogram::ZeroCount::ZeroCountInt(x) => {
                            v2::histogram::ZeroCount::ZeroCountInt(x)
                        }
                        v1::histogram::ZeroCount::ZeroCountFloat(x) => {
                            v2::histogram::ZeroCount::ZeroCountFloat(x)
                        }
                    }),
                    negative_spans: h
                        .negative_spans
                        .into_iter()
                        .map(|s| v2::BucketSpan {
                            offset: s.offset,
                            length: s.length,
                        })
                        .collect(),
                    negative_deltas: h.negative_deltas,
                    negative_counts: h.negative_counts,
                    positive_spans: h
                        .positive_spans
                        .into_iter()
                        .map(|s| v2::BucketSpan {
                            offset: s.offset,
                            length: s.length,
                        })
                        .collect(),
                    positive_deltas: h.positive_deltas,
                    positive_counts: h.positive_counts,
                    reset_hint: h.reset_hint,
                    timestamp: h.timestamp,
                    custom_values: h.custom_values,
                    start_timestamp: 0,
                })
                .collect();
            let metadata = Some(v2::Metadata {
                r#type: match d.metric_kind {
                    1 => v2::MetricType::Gauge,
                    2 => v2::MetricType::Counter,
                    4 => v2::MetricType::Histogram,
                    _ => v2::MetricType::Unspecified,
                } as i32,
                help_ref: intern(&d.description),
                unit_ref: intern(&d.unit),
            });
            v2::TimeSeries {
                labels_refs,
                samples,
                histograms,
                exemplars,
                metadata,
            }
        })
        .collect();
    // Restore v2 start timestamps, which v1 cannot carry.
    let mut request = v2::Request {
        symbols,
        timeseries,
    };
    for (out, point) in request.timeseries.iter_mut().zip(&batch.points) {
        match &point.value {
            MetricPointV2Value::ScalarPointV2(p) => {
                out.samples[0].start_timestamp = millis(p.start_unix_nano)
            }
            MetricPointV2Value::ExponentialHistogramPointV2(p) => {
                out.histograms[0].start_timestamp = millis(p.start_unix_nano)
            }
            _ => {}
        }
    }
    Ok(request)
}

/// Worker that ships fanned-out metric batches to a Mimir distributor's
/// remote-write endpoint.
pub struct MimirSink {
    http: reqwest::Client,
    endpoint: String,
    tenant: Option<String>,
    reporter: crate::metrics::SinkReporter,
}

impl MimirSink {
    /// `base` is the Mimir base URL (e.g. `http://mimir:9009`); the distributor
    /// remote-write path is appended. `tenant`, when set, is sent as the
    /// `X-Scope-OrgID` header for multi-tenant Mimir.
    pub fn new(
        http: reqwest::Client,
        base: &str,
        tenant: Option<String>,
        reporter: crate::metrics::SinkReporter,
    ) -> Self {
        let endpoint = format!("{}/api/v1/push", base.trim_end_matches('/'));
        Self {
            http,
            endpoint,
            tenant,
            reporter,
        }
    }

    pub async fn run(self, mut rx: mpsc::Receiver<Fanout>) {
        while let Some(item) = rx.recv().await {
            let signal = crate::metrics::GatewaySignal::Metrics;
            if let Fanout::StructuredMetrics(batch) = item {
                match structured_to_v1(&batch) {
                    Ok(req) if req.timeseries.is_empty() => self.reporter.skipped_empty(signal),
                    Ok(req) => {
                        self.reporter.attempt(signal);
                        if let Err(e) = self.ship_structured(&req).await {
                            self.reporter.attempt_failed(signal);
                            self.reporter.failed(signal);
                            warn!(error = %e, "mimir sink push failed; dropping batch");
                        } else {
                            self.reporter.delivered(signal);
                        }
                    }
                    Err(e) => {
                        self.reporter.failed(signal);
                        warn!(error = %e, "mimir structured encoding failed; dropping batch");
                    }
                }
                continue;
            }
            let Fanout::Metrics(batch) = item else {
                continue;
            };
            if to_write_request(&batch).timeseries.is_empty() {
                self.reporter.skipped_empty(signal);
                continue;
            }
            self.reporter.attempt(signal);
            if let Err(e) = self.ship(&batch).await {
                self.reporter.attempt_failed(signal);
                self.reporter.failed(signal);
                warn!(error = %e, "mimir sink push failed; dropping batch");
            } else {
                self.reporter.delivered(signal);
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

    async fn ship_structured(&self, req: &v1::WriteRequest) -> anyhow::Result<()> {
        let body = snap::raw::Encoder::new().compress_vec(&req.encode_to_vec())?;
        let mut builder = self
            .http
            .post(&self.endpoint)
            .header("Content-Type", REMOTE_WRITE_V1_CONTENT_TYPE)
            .header("Content-Encoding", "snappy")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .body(body);
        if let Some(tenant) = &self.tenant {
            builder = builder.header("X-Scope-OrgID", tenant);
        }
        let resp = builder.send().await?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!(
                "mimir responded {status}: {}",
                resp.text()
                    .await
                    .unwrap_or_default()
                    .chars()
                    .take(400)
                    .collect::<String>()
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
