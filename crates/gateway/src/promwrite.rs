//! Prometheus remote-write ingest: `POST /api/v1/write` (and the Mimir/Cortex
//! alias `/api/v1/push`).
//!
//! Accepts **remote-write v1**: a snappy-compressed `prometheus.WriteRequest`
//! protobuf (`Content-Encoding: snappy`, `Content-Type: application/x-protobuf`).
//! Each `TimeSeries` becomes a scry series (labels → `LabelPair`s, xxh3-64
//! fingerprint) and each `Sample` a `MetricSample`. This maps 1:1 onto scry's
//! sample-based `MetricsBatch`: classic histograms/summaries arrive already
//! exploded into `_bucket`/`_sum`/`_count` series, exactly the shape the wire
//! format expects.
//!
//! Out of scope (v0): remote-write **v2** (symbol-table protobuf), native
//! histograms (`TimeSeries.histograms`), exemplars, and per-series type
//! metadata — v1's sample stream carries no type, so every series lands as
//! `METRIC_TYPE_UNKNOWN`. The v1 message tags we don't consume are simply
//! ignored by prost on decode.

use std::collections::HashMap;

use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
};
use prost::Message as _;
use scry_proto::{
    constants::METRIC_TYPE_UNKNOWN,
    fingerprint::fingerprint,
    generated::{MetricSample, MetricsBatch, SeriesDictEntry},
    LabelPair,
};

use crate::sink::AppState;

// ── Prometheus remote-write v1 wire types (hand-written prost) ──────────
// The full proto is large; we declare only the subset we consume. prost
// ignores unrecognised field tags on decode, so a v1 WriteRequest carrying
// exemplars/metadata/native-histograms decodes fine — those fields are dropped.

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// Handle one Prometheus remote-write push.
pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    let raw = if wants_snappy(&headers) {
        snap::raw::Decoder::new()
            .decompress_vec(&body)
            .map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("snappy decompress failed: {e}"),
                )
            })?
    } else {
        body.to_vec()
    };

    let req = WriteRequest::decode(raw.as_slice()).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("remote-write protobuf decode failed: {e}"),
        )
    })?;

    let batch = map_remote_write(req);

    // Best-effort fan-out to every configured sink (see crate::sink / D-041).
    state.offer_metrics(batch);

    // Prometheus treats any 2xx as success; 204 is the conventional reply.
    Ok(StatusCode::NO_CONTENT)
}

/// Remote-write bodies are snappy-compressed by protocol. Treat the body as
/// snappy unless a `Content-Encoding` header explicitly says otherwise.
fn wants_snappy(headers: &HeaderMap) -> bool {
    match headers.get(header::CONTENT_ENCODING) {
        Some(v) => v
            .to_str()
            .map(|s| s.to_ascii_lowercase().contains("snappy"))
            .unwrap_or(true),
        None => true,
    }
}

/// Pure mapping: a remote-write `WriteRequest` → scry `MetricsBatch`.
///
/// Series are deduplicated by fingerprint within the request. Sample
/// timestamps are unix **milliseconds** on the wire and converted to ns;
/// non-positive timestamps are dropped. Every series lands as
/// `METRIC_TYPE_UNKNOWN` (v1 carries no per-series type).
pub fn map_remote_write(req: WriteRequest) -> MetricsBatch {
    let mut series: Vec<SeriesDictEntry> = Vec::new();
    let mut samples: Vec<MetricSample> = Vec::new();
    let mut seen: HashMap<u64, ()> = HashMap::new();

    for ts in req.timeseries {
        if ts.samples.is_empty() || ts.labels.is_empty() {
            continue;
        }
        let labels: Vec<LabelPair> = ts
            .labels
            .into_iter()
            .map(|l| LabelPair {
                key: l.name,
                value: l.value,
            })
            .collect();
        let fp = fingerprint(&labels);
        if seen.insert(fp, ()).is_none() {
            series.push(SeriesDictEntry {
                fingerprint: fp,
                metric_type: METRIC_TYPE_UNKNOWN,
                labels,
            });
        }
        for s in ts.samples {
            if s.timestamp <= 0 {
                continue;
            }
            samples.push(MetricSample {
                fingerprint: fp,
                ts_unix_nano: (s.timestamp as u64).saturating_mul(1_000_000),
                value: s.value,
            });
        }
    }

    MetricsBatch { series, samples }
}

/// Build a sample remote-write request with `n_series` series, each carrying
/// `n_samples` samples. Used by the probe binary and the mapping tests.
pub fn sample_request(n_series: usize, n_samples: usize) -> WriteRequest {
    let base_ms = 1_700_000_000_000i64; // unix ms
    let mut timeseries = Vec::with_capacity(n_series);
    for s in 0..n_series {
        let labels = vec![
            Label {
                name: "__name__".into(),
                value: format!("scry_demo_metric_{s}"),
            },
            Label {
                name: "job".into(),
                value: "smoke".into(),
            },
            Label {
                name: "instance".into(),
                value: format!("inst-{s}"),
            },
        ];
        let samples = (0..n_samples)
            .map(|i| Sample {
                value: (s * 1000 + i) as f64,
                timestamp: base_ms + (i as i64) * 1_000,
            })
            .collect();
        timeseries.push(TimeSeries { labels, samples });
    }
    WriteRequest { timeseries }
}

/// Encode a `WriteRequest` to the on-the-wire body: protobuf, then snappy
/// (raw block format), as Prometheus remote-write sends it.
pub fn encode_snappy(req: &WriteRequest) -> Vec<u8> {
    let proto = req.encode_to_vec();
    snap::raw::Encoder::new()
        .compress_vec(&proto)
        .expect("snappy compress is infallible on Vec input")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_request_to_metrics_batch() {
        let batch = map_remote_write(sample_request(3, 4));
        assert_eq!(batch.series.len(), 3);
        assert_eq!(batch.samples.len(), 12);

        // Series carry the labels verbatim and a matching xxh3-64 fingerprint.
        let s0 = &batch.series[0];
        assert_eq!(s0.metric_type, METRIC_TYPE_UNKNOWN);
        assert_eq!(s0.fingerprint, fingerprint(&s0.labels));
        assert_eq!(
            s0.labels
                .iter()
                .find(|l| l.key == "__name__")
                .map(|l| l.value.as_str()),
            Some("scry_demo_metric_0")
        );

        // ms → ns conversion on the first sample.
        let first = &batch.samples[0];
        assert_eq!(first.ts_unix_nano, 1_700_000_000_000 * 1_000_000);
        // Every sample references a declared series fingerprint.
        let fps: std::collections::HashSet<u64> =
            batch.series.iter().map(|s| s.fingerprint).collect();
        assert!(batch.samples.iter().all(|s| fps.contains(&s.fingerprint)));
    }

    #[test]
    fn snappy_roundtrips_through_decode() {
        let req = sample_request(2, 2);
        let body = encode_snappy(&req);
        let raw = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
        let decoded = WriteRequest::decode(raw.as_slice()).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn drops_empty_and_nonpositive() {
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![],
                    samples: vec![Sample {
                        value: 1.0,
                        timestamp: 1,
                    }],
                },
                TimeSeries {
                    labels: vec![Label {
                        name: "__name__".into(),
                        value: "x".into(),
                    }],
                    samples: vec![
                        Sample {
                            value: 1.0,
                            timestamp: 0,
                        },
                        Sample {
                            value: 2.0,
                            timestamp: 5,
                        },
                    ],
                },
            ],
        };
        let batch = map_remote_write(req);
        // First series dropped (no labels); second keeps only the positive ts.
        assert_eq!(batch.series.len(), 1);
        assert_eq!(batch.samples.len(), 1);
        assert_eq!(batch.samples[0].ts_unix_nano, 5 * 1_000_000);
    }
}
