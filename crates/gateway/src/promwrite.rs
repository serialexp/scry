//! Prometheus Remote Write 1.0 and 2.0 ingestion.
//!
//! Wire messages are pinned in [`crate::prometheus_proto`].  The mapping in this
//! module is deliberately independent of the gateway sinks: both revisions are
//! first converted to the canonical structured-metrics v2 representation.

use std::collections::HashMap;

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{header, HeaderMap, HeaderValue, Response, StatusCode},
};
use prost::Message as _;
use scry_proto::{
    generated::{
        DoubleValueV2Input, ExponentialHistogramPointV2Input, FloatCountV2Input,
        IntegerCountV2Input, MetricCountV2, MetricDescriptorV2, MetricExemplarV2, MetricNumberV2,
        MetricPointV2, MetricPointV2Value, MetricsBatch, MetricsBatchV2, ScalarPointV2Input,
        SeriesDictEntry, SparseBucketsV2,
    },
    metrics_v2::{MetricKind, ResetHint, Temporality},
    LabelPair,
};

use crate::{
    prometheus_proto::{v1, v2},
    sink::AppState,
};

// Minimal legacy v1 surface retained because the Mimir sink and probe construct
// these with struct literals. Inbound decoding itself uses prometheus_proto.
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
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

const MAX_BODY: usize = crate::otlp_common::MAX_OTLP_BODY_BYTES;
const WRITTEN_SAMPLES: &str = "x-prometheus-remote-write-samples-written";
const WRITTEN_HISTOGRAMS: &str = "x-prometheus-remote-write-histograms-written";
const WRITTEN_EXEMPLARS: &str = "x-prometheus-remote-write-exemplars-written";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingError(pub String);
impl std::fmt::Display for MappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for MappingError {}

type HttpError = (StatusCode, String);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Revision {
    V1,
    V2,
}

/// Handle a Remote Write push.  Structured aggregate data is not acknowledged
/// until `AppState` grows a v2 metrics fanout path; returning 503 lets senders retry.
pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, HttpError> {
    let revision = dispatch(&headers)?;
    let raw = decompress(&headers, &body)?;
    let (batch, samples, histograms, exemplars) = match revision {
        Revision::V1 => {
            let request = v1::WriteRequest::decode(raw.as_slice()).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("remote-write v1 protobuf decode failed: {e}"),
                )
            })?;
            let counts = counts_v1(&request);
            (
                map_v1(request).map_err(bad_mapping)?,
                counts.0,
                counts.1,
                counts.2,
            )
        }
        Revision::V2 => {
            let request = v2::Request::decode(raw.as_slice()).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("remote-write v2 protobuf decode failed: {e}"),
                )
            })?;
            let counts = counts_v2(&request);
            (
                map_v2(request).map_err(bad_mapping)?,
                counts.0,
                counts.1,
                counts.2,
            )
        }
    };

    if let Some(metrics) = state.metrics() {
        metrics.inbound_accepted(crate::metrics::Inbound::PromRemoteWriteHttp);
    }
    state.offer_structured_metrics(batch);

    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    if revision == Revision::V2 {
        for (name, value) in [
            (WRITTEN_SAMPLES, samples),
            (WRITTEN_HISTOGRAMS, histograms),
            (WRITTEN_EXEMPLARS, exemplars),
        ] {
            response.headers_mut().insert(
                name,
                HeaderValue::from_str(&value.to_string()).expect("decimal header"),
            );
        }
    }
    Ok(response)
}

fn bad_mapping(e: MappingError) -> HttpError {
    (StatusCode::BAD_REQUEST, e.to_string())
}

/// Select the protobuf solely from an exact `proto` media-type parameter.
/// The historical parameter-less media type is accepted as v1 only with the
/// legacy `X-Prometheus-Remote-Write-Version: 0.1.0` header.
fn dispatch(headers: &HeaderMap) -> Result<Revision, HttpError> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "missing remote-write Content-Type".into(),
            )
        })?;
    let mut parts = ct.split(';').map(str::trim);
    if !parts
        .next()
        .is_some_and(|v| v.eq_ignore_ascii_case("application/x-protobuf"))
    {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/x-protobuf".into(),
        ));
    }
    let mut proto = None;
    for part in parts {
        let Some((key, value)) = part.split_once('=') else {
            return Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "malformed Content-Type parameter".into(),
            ));
        };
        if key.trim().eq_ignore_ascii_case("proto") {
            if proto.replace(value.trim().trim_matches('"')).is_some() {
                return Err((
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "duplicate proto parameter".into(),
                ));
            }
        } else {
            return Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                format!("unknown Content-Type parameter {key}"),
            ));
        }
    }
    let version = headers
        .get("x-prometheus-remote-write-version")
        .and_then(|v| v.to_str().ok());
    match proto {
        Some("prometheus.WriteRequest") if version.is_none() || version == Some("0.1.0") => {
            Ok(Revision::V1)
        }
        Some("io.prometheus.write.v2.Request") if version != Some("0.1.0") => Ok(Revision::V2),
        None if version == Some("0.1.0") => Ok(Revision::V1),
        Some("prometheus.WriteRequest") | Some("io.prometheus.write.v2.Request") => Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "remote-write version header and proto disagree".into(),
        )),
        Some(other) => Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unknown remote-write proto {other}"),
        )),
        None => Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "ambiguous parameter-less remote-write Content-Type".into(),
        )),
    }
}

fn decompress(headers: &HeaderMap, body: &[u8]) -> Result<Vec<u8>, HttpError> {
    if body.len() > MAX_BODY {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "compressed remote-write request exceeds 32 MiB".into(),
        ));
    }
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok());
    if !encoding.is_some_and(|v| v.trim().eq_ignore_ascii_case("snappy")) {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Encoding must be snappy".into(),
        ));
    }
    let expanded = snap::raw::decompress_len(body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("snappy header decode failed: {e}"),
        )
    })?;
    if expanded > MAX_BODY {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "expanded remote-write request exceeds 32 MiB".into(),
        ));
    }
    snap::raw::Decoder::new().decompress_vec(body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("snappy decompress failed: {e}"),
        )
    })
}

/// Pure Remote Write 1.0 to canonical metrics-v2 mapping.
pub fn map_v1(req: v1::WriteRequest) -> Result<MetricsBatchV2, MappingError> {
    let metadata: HashMap<_, _> = req
        .metadata
        .into_iter()
        .map(|m| (m.metric_family_name.clone(), m))
        .collect();
    let mut out = MetricsBatchV2 {
        magic: scry_proto::constants::METRICS_BATCH_V2_MAGIC,
        descriptors: vec![],
        points: vec![],
    };
    for ts in req.timeseries {
        if ts.labels.is_empty() || (ts.samples.is_empty() && ts.histograms.is_empty()) {
            continue;
        }
        let (name, attrs) = split_labels(ts.labels)?;
        let meta = metadata.get(&name);
        let inferred_type = if !ts.histograms.is_empty() {
            Some(3)
        } else {
            None
        };
        let descriptor = descriptor(
            out.descriptors.len() as u32 + 1,
            name,
            attrs,
            meta.map(|m| m.r#type).or(inferred_type),
            meta.map(|m| m.help.as_str()).unwrap_or(""),
            meta.map(|m| m.unit.as_str()).unwrap_or(""),
        );
        let id = descriptor.id;
        out.descriptors.push(descriptor);
        let exemplars = map_exemplars_v1(ts.exemplars)?;
        append_samples(
            &mut out.points,
            id,
            ts.samples.into_iter().map(|s| (s.value, s.timestamp, 0)),
            exemplars.clone(),
        )?;
        for histogram in ts.histograms {
            out.points
                .push(histogram_point_v1(id, histogram, exemplars.clone())?);
        }
    }
    Ok(out)
}

/// Pure Remote Write 2.0 to canonical metrics-v2 mapping.
pub fn map_v2(req: v2::Request) -> Result<MetricsBatchV2, MappingError> {
    let symbols = req.symbols;
    let mut out = MetricsBatchV2 {
        magic: scry_proto::constants::METRICS_BATCH_V2_MAGIC,
        descriptors: vec![],
        points: vec![],
    };
    for ts in req.timeseries {
        let labels = refs_to_labels(&symbols, &ts.labels_refs)?;
        if labels.is_empty() || (ts.samples.is_empty() && ts.histograms.is_empty()) {
            continue;
        }
        let (name, attrs) = split_labels(labels)?;
        let meta = ts.metadata;
        let (kind, help, unit) = if let Some(m) = meta {
            (
                Some(m.r#type),
                symbol(&symbols, m.help_ref)?,
                symbol(&symbols, m.unit_ref)?,
            )
        } else {
            (None, "", "")
        };
        let descriptor = descriptor(
            out.descriptors.len() as u32 + 1,
            name,
            attrs,
            kind,
            help,
            unit,
        );
        let id = descriptor.id;
        out.descriptors.push(descriptor);
        let exemplars = map_exemplars_v2(&symbols, ts.exemplars)?;
        append_samples(
            &mut out.points,
            id,
            ts.samples
                .into_iter()
                .map(|s| (s.value, s.timestamp, s.start_timestamp)),
            exemplars.clone(),
        )?;
        for histogram in ts.histograms {
            out.points
                .push(histogram_point_v2(id, histogram, exemplars.clone())?);
        }
    }
    Ok(out)
}

fn descriptor(
    id: u32,
    name: String,
    attrs: Vec<LabelPair>,
    ty: Option<i32>,
    description: &str,
    unit: &str,
) -> MetricDescriptorV2 {
    let (metric_kind, temporality, monotonic) = match ty.unwrap_or(0) {
        1 => (MetricKind::Sum as u8, Temporality::Cumulative as u8, 1),
        3 => (
            MetricKind::ExponentialHistogram as u8,
            Temporality::Cumulative as u8,
            0,
        ),
        4 => (
            MetricKind::ExponentialHistogram as u8,
            Temporality::Unspecified as u8,
            0,
        ),
        _ => (MetricKind::Gauge as u8, Temporality::Unspecified as u8, 0),
    };
    MetricDescriptorV2 {
        id,
        name,
        description: description.into(),
        unit: unit.into(),
        metric_kind,
        temporality,
        monotonic,
        resource_attrs: attrs,
        scope_name: String::new(),
        scope_version: String::new(),
        scope_attrs: vec![],
    }
}

fn split_labels(labels: Vec<v1::Label>) -> Result<(String, Vec<LabelPair>), MappingError> {
    let mut name = None;
    let mut attrs = Vec::new();
    for label in labels {
        if label.name == "__name__" {
            if name.replace(label.value).is_some() {
                return err("duplicate __name__ label");
            }
        } else {
            attrs.push(LabelPair {
                key: label.name,
                value: label.value,
            });
        }
    }
    let name = name
        .filter(|v| !v.is_empty())
        .ok_or_else(|| MappingError("series has no __name__ label".into()))?;
    attrs.sort_by(|a, b| a.key.cmp(&b.key).then(a.value.cmp(&b.value)));
    Ok((name, attrs))
}

fn refs_to_labels(symbols: &[String], refs: &[u32]) -> Result<Vec<v1::Label>, MappingError> {
    if refs.len() % 2 != 0 {
        return err("label symbol references must be name/value pairs");
    }
    refs.chunks_exact(2)
        .map(|pair| {
            Ok(v1::Label {
                name: symbol(symbols, pair[0])?.into(),
                value: symbol(symbols, pair[1])?.into(),
            })
        })
        .collect()
}
fn symbol(symbols: &[String], reference: u32) -> Result<&str, MappingError> {
    symbols
        .get(reference as usize)
        .map(String::as_str)
        .ok_or_else(|| MappingError(format!("symbol reference {reference} is out of range")))
}
fn ms(value: i64) -> Result<u64, MappingError> {
    let value = u64::try_from(value).map_err(|_| MappingError("negative timestamp".into()))?;
    value
        .checked_mul(1_000_000)
        .ok_or_else(|| MappingError("timestamp overflows nanoseconds".into()))
}
fn append_samples<I: Iterator<Item = (f64, i64, i64)>>(
    points: &mut Vec<MetricPointV2>,
    id: u32,
    samples: I,
    exemplars: Vec<MetricExemplarV2>,
) -> Result<(), MappingError> {
    for (value, timestamp, start) in samples {
        points.push(MetricPointV2 {
            value: MetricPointV2Value::ScalarPointV2(
                ScalarPointV2Input {
                    descriptor_id: id,
                    start_unix_nano: if start == 0 { 0 } else { ms(start)? },
                    ts_unix_nano: ms(timestamp)?,
                    flags: 0,
                    attributes: vec![],
                    exemplars: exemplars.clone(),
                    number: number(value),
                }
                .into(),
            ),
        });
    }
    Ok(())
}
fn number(value: f64) -> MetricNumberV2 {
    MetricNumberV2 {
        value: scry_proto::generated::MetricNumberV2Value::DoubleValueV2(
            DoubleValueV2Input { value }.into(),
        ),
    }
}
fn int_count(value: u64) -> MetricCountV2 {
    MetricCountV2 {
        value: scry_proto::generated::MetricCountV2Value::IntegerCountV2(
            IntegerCountV2Input { value }.into(),
        ),
    }
}
fn float_count(value: f64) -> MetricCountV2 {
    MetricCountV2 {
        value: scry_proto::generated::MetricCountV2Value::FloatCountV2(
            FloatCountV2Input { value }.into(),
        ),
    }
}

fn map_exemplars_v1(xs: Vec<v1::Exemplar>) -> Result<Vec<MetricExemplarV2>, MappingError> {
    xs.into_iter()
        .map(|x| {
            exemplar(
                x.labels
                    .into_iter()
                    .map(|l| LabelPair {
                        key: l.name,
                        value: l.value,
                    })
                    .collect(),
                x.value,
                x.timestamp,
            )
        })
        .collect()
}
fn map_exemplars_v2(
    symbols: &[String],
    xs: Vec<v2::Exemplar>,
) -> Result<Vec<MetricExemplarV2>, MappingError> {
    xs.into_iter()
        .map(|x| {
            exemplar(
                refs_to_labels(symbols, &x.labels_refs)?
                    .into_iter()
                    .map(|l| LabelPair {
                        key: l.name,
                        value: l.value,
                    })
                    .collect(),
                x.value,
                x.timestamp,
            )
        })
        .collect()
}
fn exemplar(
    mut labels: Vec<LabelPair>,
    value: f64,
    timestamp: i64,
) -> Result<MetricExemplarV2, MappingError> {
    let mut trace_id = vec![];
    let mut span_id = vec![];
    labels.retain(|l| match l.key.as_str() {
        "trace_id" => {
            trace_id = decode_hex(&l.value).unwrap_or_default();
            false
        }
        "span_id" => {
            span_id = decode_hex(&l.value).unwrap_or_default();
            false
        }
        _ => true,
    });
    Ok(MetricExemplarV2 {
        ts_unix_nano: ms(timestamp)?,
        number: number(value),
        filtered_attrs: labels,
        trace_id,
        span_id,
    })
}
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

fn histogram_point_v1(
    id: u32,
    h: v1::Histogram,
    exemplars: Vec<MetricExemplarV2>,
) -> Result<MetricPointV2, MappingError> {
    histogram_point(
        id,
        h.count.map(|c| match c {
            v1::histogram::Count::CountInt(v) => Count::Int(v),
            v1::histogram::Count::CountFloat(v) => Count::Float(v),
        }),
        h.sum,
        h.schema,
        h.zero_threshold,
        h.zero_count.map(|c| match c {
            v1::histogram::ZeroCount::ZeroCountInt(v) => Count::Int(v),
            v1::histogram::ZeroCount::ZeroCountFloat(v) => Count::Float(v),
        }),
        h.negative_spans
            .into_iter()
            .map(|s| (s.offset, s.length))
            .collect(),
        h.negative_deltas,
        h.negative_counts,
        h.positive_spans
            .into_iter()
            .map(|s| (s.offset, s.length))
            .collect(),
        h.positive_deltas,
        h.positive_counts,
        h.reset_hint,
        h.timestamp,
        0,
        h.custom_values,
        exemplars,
    )
}
fn histogram_point_v2(
    id: u32,
    h: v2::Histogram,
    exemplars: Vec<MetricExemplarV2>,
) -> Result<MetricPointV2, MappingError> {
    histogram_point(
        id,
        h.count.map(|c| match c {
            v2::histogram::Count::CountInt(v) => Count::Int(v),
            v2::histogram::Count::CountFloat(v) => Count::Float(v),
        }),
        h.sum,
        h.schema,
        h.zero_threshold,
        h.zero_count.map(|c| match c {
            v2::histogram::ZeroCount::ZeroCountInt(v) => Count::Int(v),
            v2::histogram::ZeroCount::ZeroCountFloat(v) => Count::Float(v),
        }),
        h.negative_spans
            .into_iter()
            .map(|s| (s.offset, s.length))
            .collect(),
        h.negative_deltas,
        h.negative_counts,
        h.positive_spans
            .into_iter()
            .map(|s| (s.offset, s.length))
            .collect(),
        h.positive_deltas,
        h.positive_counts,
        h.reset_hint,
        h.timestamp,
        h.start_timestamp,
        h.custom_values,
        exemplars,
    )
}
#[derive(Clone, Copy)]
enum Count {
    Int(u64),
    Float(f64),
}
#[allow(clippy::too_many_arguments)]
fn histogram_point(
    id: u32,
    count: Option<Count>,
    sum: f64,
    schema: i32,
    zero_threshold: f64,
    zero_count: Option<Count>,
    nsp: Vec<(i32, u32)>,
    nd: Vec<i64>,
    nc: Vec<f64>,
    psp: Vec<(i32, u32)>,
    pd: Vec<i64>,
    pc: Vec<f64>,
    reset: i32,
    timestamp: i64,
    start: i64,
    custom: Vec<f64>,
    exemplars: Vec<MetricExemplarV2>,
) -> Result<MetricPointV2, MappingError> {
    let count = count.ok_or_else(|| MappingError("histogram has no count".into()))?;
    let integer = matches!(count, Count::Int(_));
    let zero_count =
        zero_count.ok_or_else(|| MappingError("histogram has no zero count".into()))?;
    if integer != matches!(zero_count, Count::Int(_)) {
        return err("mixed integer/float histogram counts");
    }
    let negative = sparse(nsp, nd, nc, integer)?;
    let positive = sparse(psp, pd, pc, integer)?;
    let point = ExponentialHistogramPointV2Input {
        descriptor_id: id,
        start_unix_nano: if start == 0 { 0 } else { ms(start)? },
        ts_unix_nano: ms(timestamp)?,
        flags: 0,
        attributes: vec![],
        exemplars,
        count: match count {
            Count::Int(v) => int_count(v),
            Count::Float(v) => float_count(v),
        },
        has_sum: 1,
        sum,
        has_min: 0,
        min: 0.0,
        has_max: 0,
        max: 0.0,
        scale: schema,
        zero_threshold,
        zero_count: match zero_count {
            Count::Int(v) => int_count(v),
            Count::Float(v) => float_count(v),
        },
        positive,
        negative,
        custom_bounds: custom,
        reset_hint: u8::try_from(reset)
            .ok()
            .filter(|v| *v <= ResetHint::Gauge as u8)
            .ok_or_else(|| MappingError("invalid histogram reset hint".into()))?,
    };
    Ok(MetricPointV2 {
        value: MetricPointV2Value::ExponentialHistogramPointV2(point.into()),
    })
}
fn sparse(
    spans: Vec<(i32, u32)>,
    deltas: Vec<i64>,
    counts: Vec<f64>,
    integer: bool,
) -> Result<SparseBucketsV2, MappingError> {
    let expected = spans
        .iter()
        .try_fold(0usize, |n, s| n.checked_add(s.1 as usize))
        .ok_or_else(|| MappingError("bucket span length overflow".into()))?;
    if (integer && deltas.len() != expected) || (!integer && counts.len() != expected) {
        return err("bucket span/count lengths differ");
    }
    if integer && !counts.is_empty() || !integer && !deltas.is_empty() {
        return err("mixed integer/float bucket encoding");
    }
    let mut indices = Vec::with_capacity(expected);
    let mut previous_end = 0i32;
    for (i, (offset, length)) in spans.into_iter().enumerate() {
        let start = if i == 0 {
            offset
        } else {
            previous_end
                .checked_add(offset)
                .ok_or_else(|| MappingError("bucket span offset overflow".into()))?
        };
        for j in 0..length {
            indices.push(
                start
                    .checked_add(
                        i32::try_from(j)
                            .map_err(|_| MappingError("bucket span too long".into()))?,
                    )
                    .ok_or_else(|| MappingError("bucket index overflow".into()))?,
            );
        }
        previous_end = start
            .checked_add(
                i32::try_from(length).map_err(|_| MappingError("bucket span too long".into()))?,
            )
            .ok_or_else(|| MappingError("bucket span end overflow".into()))?;
    }
    let offset = indices.first().copied().unwrap_or(0);
    let mut gaps = Vec::with_capacity(indices.len().saturating_sub(1));
    for pair in indices.windows(2) {
        gaps.push(
            pair[1]
                .checked_sub(pair[0])
                .ok_or_else(|| MappingError("bucket index delta overflow".into()))?,
        );
    }
    let values = if integer {
        let mut current = 0i64;
        deltas
            .into_iter()
            .map(|d| {
                current = current
                    .checked_add(d)
                    .ok_or_else(|| MappingError("bucket count delta overflow".into()))?;
                Ok(int_count(u64::try_from(current).map_err(|_| {
                    MappingError("negative bucket count".into())
                })?))
            })
            .collect::<Result<_, MappingError>>()?
    } else {
        counts.into_iter().map(float_count).collect()
    };
    Ok(SparseBucketsV2 {
        offset,
        deltas: gaps,
        counts: values,
    })
}
fn err<T>(message: &str) -> Result<T, MappingError> {
    Err(MappingError(message.into()))
}

fn counts_v1(r: &v1::WriteRequest) -> (usize, usize, usize) {
    r.timeseries.iter().fold((0, 0, 0), |a, t| {
        (
            a.0 + t.samples.len(),
            a.1 + t.histograms.len(),
            a.2 + t.exemplars.len(),
        )
    })
}
fn counts_v2(r: &v2::Request) -> (usize, usize, usize) {
    r.timeseries.iter().fold((0, 0, 0), |a, t| {
        (
            a.0 + t.samples.len(),
            a.1 + t.histograms.len(),
            a.2 + t.exemplars.len(),
        )
    })
}

fn scalar_legacy(batch: &MetricsBatchV2) -> MetricsBatch {
    use scry_proto::{fingerprint::fingerprint, generated::MetricSample};
    let mut series = vec![];
    let mut samples = vec![];
    let mut fps = HashMap::new();
    for d in &batch.descriptors {
        let mut labels = d.resource_attrs.clone();
        labels.push(LabelPair {
            key: "__name__".into(),
            value: d.name.clone(),
        });
        let fp = fingerprint(&labels);
        fps.insert(d.id, fp);
        series.push(SeriesDictEntry {
            fingerprint: fp,
            metric_type: 0,
            labels,
        });
    }
    for p in &batch.points {
        if let MetricPointV2Value::ScalarPointV2(v) = &p.value {
            if let Some(fp) = fps.get(&v.descriptor_id) {
                if let scry_proto::generated::MetricNumberV2Value::DoubleValueV2(n) =
                    &v.number.value
                {
                    samples.push(MetricSample {
                        fingerprint: *fp,
                        ts_unix_nano: v.ts_unix_nano,
                        value: n.value,
                    });
                }
            }
        }
    }
    MetricsBatch { series, samples }
}

/// Legacy pure scalar mapper retained for callers that have not migrated.
pub fn map_remote_write(req: WriteRequest) -> MetricsBatch {
    let req = v1::WriteRequest {
        timeseries: req
            .timeseries
            .into_iter()
            .map(|ts| v1::TimeSeries {
                labels: ts
                    .labels
                    .into_iter()
                    .map(|l| v1::Label {
                        name: l.name,
                        value: l.value,
                    })
                    .collect(),
                samples: ts
                    .samples
                    .into_iter()
                    .map(|s| v1::Sample {
                        value: s.value,
                        timestamp: s.timestamp,
                    })
                    .collect(),
                exemplars: vec![],
                histograms: vec![],
            })
            .collect(),
        metadata: vec![],
    };
    map_v1(req)
        .map(|b| scalar_legacy(&b))
        .unwrap_or(MetricsBatch {
            series: vec![],
            samples: vec![],
        })
}

pub fn sample_request(n_series: usize, n_samples: usize) -> WriteRequest {
    let mut timeseries = vec![];
    for s in 0..n_series {
        timeseries.push(TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: format!("scry_demo_metric_{s}"),
                },
                Label {
                    name: "job".into(),
                    value: "smoke".into(),
                },
            ],
            samples: (0..n_samples)
                .map(|i| Sample {
                    value: (s * 1000 + i) as f64,
                    timestamp: 1_700_000_000_000 + i as i64 * 1000,
                })
                .collect(),
        });
    }
    WriteRequest { timeseries }
}
pub fn encode_snappy(req: &WriteRequest) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy compression")
}

#[cfg(test)]
mod structured_tests {
    use super::*;
    use crate::prometheus_proto::{v1, v2};

    #[test]
    fn maps_v1_histogram_only_series_without_dropping_it() {
        let request = v1::WriteRequest {
            timeseries: vec![v1::TimeSeries {
                labels: vec![v1::Label {
                    name: "__name__".into(),
                    value: "latency".into(),
                }],
                samples: vec![],
                exemplars: vec![],
                histograms: vec![v1::Histogram {
                    count: Some(v1::histogram::Count::CountInt(3)),
                    sum: 6.0,
                    schema: 0,
                    zero_threshold: 0.0,
                    zero_count: Some(v1::histogram::ZeroCount::ZeroCountInt(0)),
                    negative_spans: vec![],
                    negative_deltas: vec![],
                    negative_counts: vec![],
                    positive_spans: vec![v1::BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    positive_deltas: vec![1, 1],
                    positive_counts: vec![],
                    reset_hint: v1::ResetHint::No as i32,
                    timestamp: 1_700_000_000_000,
                    custom_values: vec![],
                }],
            }],
            metadata: vec![],
        };
        let mapped = map_v1(request).unwrap();
        assert_eq!(mapped.points.len(), 1);
        assert_eq!(
            mapped.descriptors[0].metric_kind,
            MetricKind::ExponentialHistogram as u8
        );
        assert!(matches!(
            mapped.points[0].value,
            MetricPointV2Value::ExponentialHistogramPointV2(_)
        ));
    }

    #[test]
    fn maps_v2_symbol_metadata_and_start_timestamp() {
        let request = v2::Request {
            symbols: vec![
                "".into(),
                "__name__".into(),
                "requests".into(),
                "help".into(),
            ],
            timeseries: vec![v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![v2::Sample {
                    value: 2.0,
                    timestamp: 20,
                    start_timestamp: 10,
                }],
                histograms: vec![],
                exemplars: vec![],
                metadata: Some(v2::Metadata {
                    r#type: v2::MetricType::Counter as i32,
                    help_ref: 3,
                    unit_ref: 0,
                }),
            }],
        };
        let mapped = map_v2(request).unwrap();
        assert_eq!(mapped.descriptors[0].description, "help");
        let MetricPointV2Value::ScalarPointV2(point) = &mapped.points[0].value else {
            panic!()
        };
        assert_eq!(point.start_unix_nano, 10_000_000);
    }
}
