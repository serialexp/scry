//! OTLP metrics ingestion and lossless structured-metrics staging.

use std::collections::HashMap;

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::{
        ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    },
    metrics::v1::{
        exemplar, exponential_histogram_data_point, metric, number_data_point,
        AggregationTemporality, Exemplar, NumberDataPoint,
    },
};
use scry_proto::{
    constants::{METRIC_TYPE_COUNTER, METRIC_TYPE_GAUGE, METRIC_TYPE_UNKNOWN},
    fingerprint::fingerprint,
    generated::{
        DoubleValueV2Input, ExponentialHistogramPointV2Input, HistogramPointV2Input,
        IntegerCountV2Input, IntegerValueV2Input, MetricCountV2, MetricCountV2Value,
        MetricDescriptorV2, MetricExemplarV2, MetricNumberV2, MetricNumberV2Value, MetricPointV2,
        MetricPointV2Value, MetricSample, MetricsBatch, MetricsBatchV2, QuantileValueV2,
        ScalarPointV2Input, SeriesDictEntry, SparseBucketsV2, SummaryPointV2Input,
    },
    metrics_v2::{self, MetricKind, ResetHint, Temporality},
    LabelPair,
};

use crate::{
    otlp_common::{decode_request, encode_response, insert_if_absent, kv_to_labels},
    sink::AppState,
};

#[derive(Debug)]
pub struct MetricsMapping {
    pub batch: MetricsBatch,
    pub rejected: u64,
}

/// Pure, lossless OTLP to structured-metrics conversion result.
#[derive(Debug)]
pub struct MetricsMappingV2 {
    pub batch: MetricsBatchV2,
    pub rejected: u64,
    pub rejected_details: Vec<String>,
}

pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let (request, encoding) = decode_request::<ExportMetricsServiceRequest>(&headers, body)
        .inspect_err(|_| {
            if let Some(metrics) = state.metrics() {
                metrics.inbound_rejected(crate::metrics::Inbound::OtlpHttp);
            }
        })?;
    if let Some(metrics) = state.metrics() {
        metrics.inbound_accepted(crate::metrics::Inbound::OtlpHttp);
    }
    let response = accept(&state, request);
    Ok(encode_response(&response, encoding))
}

pub fn accept(
    state: &AppState,
    request: ExportMetricsServiceRequest,
) -> ExportMetricsServiceResponse {
    let mapped = map_metrics_v2(request);
    state.offer_structured_metrics(mapped.batch);
    ExportMetricsServiceResponse {
        partial_success: (mapped.rejected != 0).then(|| ExportMetricsPartialSuccess {
            rejected_data_points: mapped.rejected.min(i64::MAX as u64) as i64,
            error_message: format!(
                "invalid metric points were rejected: {}",
                mapped.rejected_details.join("; ")
            ),
        }),
    }
}

pub fn map_metrics(request: ExportMetricsServiceRequest) -> MetricsMapping {
    let mut series = Vec::new();
    let mut samples = Vec::new();
    let mut by_fingerprint = HashMap::<u64, usize>::new();
    let mut rejected = 0u64;

    for resource_metrics in request.resource_metrics {
        let resource_labels = resource_metrics
            .resource
            .map(|resource| kv_to_labels(&resource.attributes))
            .unwrap_or_default();
        for scope_metrics in resource_metrics.scope_metrics {
            let scope = scope_metrics.scope;
            for metric in scope_metrics.metrics {
                let name = metric.name;
                let (metric_type, points) = match metric.data {
                    Some(metric::Data::Gauge(gauge)) => (METRIC_TYPE_GAUGE, gauge.data_points),
                    Some(metric::Data::Sum(sum))
                        if sum.aggregation_temporality
                            == AggregationTemporality::Cumulative as i32 =>
                    {
                        (
                            if sum.is_monotonic {
                                METRIC_TYPE_COUNTER
                            } else {
                                METRIC_TYPE_UNKNOWN
                            },
                            sum.data_points,
                        )
                    }
                    Some(metric::Data::Sum(sum)) => {
                        rejected += sum.data_points.len() as u64;
                        continue;
                    }
                    Some(metric::Data::Histogram(value)) => {
                        rejected += value.data_points.len() as u64;
                        continue;
                    }
                    Some(metric::Data::ExponentialHistogram(value)) => {
                        rejected += value.data_points.len() as u64;
                        continue;
                    }
                    Some(metric::Data::Summary(value)) => {
                        rejected += value.data_points.len() as u64;
                        continue;
                    }
                    None => {
                        rejected += 1;
                        continue;
                    }
                };
                for point in points {
                    let Some(value) = scalar_value(&point) else {
                        rejected += 1;
                        continue;
                    };
                    if point.time_unix_nano == 0 {
                        rejected += 1;
                        continue;
                    }
                    let mut labels = resource_labels.clone();
                    if let Some(scope) = &scope {
                        insert_if_absent(&mut labels, "otel.scope.name", &scope.name);
                        insert_if_absent(&mut labels, "otel.scope.version", &scope.version);
                    }
                    merge_point_labels(&mut labels, &point.attributes);
                    labels.retain(|label| label.key != "__name__");
                    labels.push(LabelPair {
                        key: "__name__".into(),
                        value: name.clone(),
                    });
                    labels.sort_by(|a, b| a.key.cmp(&b.key).then(a.value.cmp(&b.value)));
                    labels.dedup_by(|right, left| right.key == left.key);
                    let fp = fingerprint(&labels);
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        by_fingerprint.entry(fp)
                    {
                        entry.insert(series.len());
                        series.push(SeriesDictEntry {
                            fingerprint: fp,
                            metric_type,
                            labels,
                        });
                    }
                    samples.push(MetricSample {
                        fingerprint: fp,
                        ts_unix_nano: point.time_unix_nano,
                        value,
                    });
                }
            }
        }
    }
    MetricsMapping {
        batch: MetricsBatch { series, samples },
        rejected,
    }
}

/// Convert every OTLP metric shape without coercing integer values to doubles.
///
/// Invalid data points are omitted independently, so one malformed point does not
/// prevent valid points in the same export from reaching the structured-metrics sink.
pub fn map_metrics_v2(request: ExportMetricsServiceRequest) -> MetricsMappingV2 {
    let mut descriptors = Vec::new();
    let mut points = Vec::new();
    let mut rejected = 0;
    let mut rejected_details = Vec::new();

    for resource in request.resource_metrics {
        let resource_attrs = resource
            .resource
            .map(|r| kv_to_labels(&r.attributes))
            .unwrap_or_default();
        for scoped in resource.scope_metrics {
            let (scope_name, scope_version, scope_attrs) = scoped
                .scope
                .map(|s| (s.name, s.version, kv_to_labels(&s.attributes)))
                .unwrap_or_default();
            for metric in scoped.metrics {
                let id = descriptors.len() as u32;
                let (kind, temporality, monotonic) = match &metric.data {
                    Some(metric::Data::Gauge(_)) => {
                        (MetricKind::Gauge, Temporality::Unspecified, false)
                    }
                    Some(metric::Data::Sum(v)) => (
                        MetricKind::Sum,
                        temporality(v.aggregation_temporality),
                        v.is_monotonic,
                    ),
                    Some(metric::Data::Histogram(v)) => (
                        MetricKind::Histogram,
                        temporality(v.aggregation_temporality),
                        false,
                    ),
                    Some(metric::Data::ExponentialHistogram(v)) => (
                        MetricKind::ExponentialHistogram,
                        temporality(v.aggregation_temporality),
                        false,
                    ),
                    Some(metric::Data::Summary(_)) => {
                        (MetricKind::Summary, Temporality::Unspecified, false)
                    }
                    // An empty metric contains no rejected data points. OTLP partial
                    // success counts points, not malformed metric envelopes.
                    None => continue,
                };
                let point_count = match &metric.data {
                    Some(metric::Data::Gauge(v)) => v.data_points.len(),
                    Some(metric::Data::Sum(v)) => v.data_points.len(),
                    Some(metric::Data::Histogram(v)) => v.data_points.len(),
                    Some(metric::Data::ExponentialHistogram(v)) => v.data_points.len(),
                    Some(metric::Data::Summary(v)) => v.data_points.len(),
                    None => 0,
                };
                let descriptor = MetricDescriptorV2 {
                    id,
                    name: metric.name.clone(),
                    description: metric.description,
                    unit: metric.unit,
                    metric_kind: kind as u8,
                    temporality: temporality as u8,
                    monotonic: monotonic as u8,
                    resource_attrs: resource_attrs.clone(),
                    scope_name: scope_name.clone(),
                    scope_version: scope_version.clone(),
                    scope_attrs: scope_attrs.clone(),
                };
                if let Err(error) = metrics_v2::validate(&MetricsBatchV2 {
                    magic: scry_proto::constants::METRICS_BATCH_V2_MAGIC,
                    descriptors: vec![descriptor.clone()],
                    points: Vec::new(),
                }) {
                    rejected += point_count as u64;
                    rejected_details.push(format!(
                        "metric {:?} has an invalid descriptor: {error}",
                        metric.name
                    ));
                    continue;
                }
                let mapped: Vec<Result<MetricPointV2, String>> = match metric.data.unwrap() {
                    metric::Data::Gauge(v) => v
                        .data_points
                        .into_iter()
                        .map(|p| scalar_point(id, p))
                        .collect(),
                    metric::Data::Sum(v) => v
                        .data_points
                        .into_iter()
                        .map(|p| scalar_point(id, p))
                        .collect(),
                    metric::Data::Histogram(v) => v
                        .data_points
                        .into_iter()
                        .map(|p| {
                            let (has_sum, sum) = optional_f64(p.sum);
                            let (has_min, min) = optional_f64(p.min);
                            let (has_max, max) = optional_f64(p.max);
                            Ok(MetricPointV2 {
                                value: MetricPointV2Value::HistogramPointV2(
                                    HistogramPointV2Input {
                                        descriptor_id: id,
                                        start_unix_nano: p.start_time_unix_nano,
                                        ts_unix_nano: p.time_unix_nano,
                                        flags: p.flags,
                                        attributes: kv_to_labels(&p.attributes),
                                        exemplars: map_exemplars(p.exemplars)?,
                                        count: p.count,
                                        has_sum,
                                        sum,
                                        has_min,
                                        min,
                                        has_max,
                                        max,
                                        explicit_bounds: p.explicit_bounds,
                                        bucket_counts: p.bucket_counts,
                                    }
                                    .into(),
                                ),
                            })
                        })
                        .collect(),
                    metric::Data::ExponentialHistogram(v) => v
                        .data_points
                        .into_iter()
                        .map(|p| {
                            let (has_sum, sum) = optional_f64(p.sum);
                            let (has_min, min) = optional_f64(p.min);
                            let (has_max, max) = optional_f64(p.max);
                            Ok(MetricPointV2 {
                                value: MetricPointV2Value::ExponentialHistogramPointV2(
                                    ExponentialHistogramPointV2Input {
                                        descriptor_id: id,
                                        start_unix_nano: p.start_time_unix_nano,
                                        ts_unix_nano: p.time_unix_nano,
                                        flags: p.flags,
                                        attributes: kv_to_labels(&p.attributes),
                                        exemplars: map_exemplars(p.exemplars)?,
                                        count: integer_count(p.count),
                                        has_sum,
                                        sum,
                                        has_min,
                                        min,
                                        has_max,
                                        max,
                                        scale: p.scale,
                                        zero_threshold: p.zero_threshold,
                                        zero_count: integer_count(p.zero_count),
                                        positive: sparse(p.positive),
                                        negative: sparse(p.negative),
                                        custom_bounds: Vec::new(),
                                        reset_hint: ResetHint::Unknown as u8,
                                    }
                                    .into(),
                                ),
                            })
                        })
                        .collect(),
                    metric::Data::Summary(v) => v
                        .data_points
                        .into_iter()
                        .map(|p| {
                            Ok(MetricPointV2 {
                                value: MetricPointV2Value::SummaryPointV2(
                                    SummaryPointV2Input {
                                        descriptor_id: id,
                                        start_unix_nano: p.start_time_unix_nano,
                                        ts_unix_nano: p.time_unix_nano,
                                        flags: p.flags,
                                        attributes: kv_to_labels(&p.attributes),
                                        exemplars: Vec::new(),
                                        count: p.count,
                                        sum: p.sum,
                                        quantiles: p
                                            .quantile_values
                                            .into_iter()
                                            .map(|q| QuantileValueV2 {
                                                quantile: q.quantile,
                                                value: q.value,
                                            })
                                            .collect(),
                                    }
                                    .into(),
                                ),
                            })
                        })
                        .collect(),
                };
                for result in mapped {
                    match result {
                        Ok(point)
                            if point_time(&point) != 0
                                && metrics_v2::validate(&MetricsBatchV2 {
                                    magic: scry_proto::constants::METRICS_BATCH_V2_MAGIC,
                                    descriptors: vec![descriptor.clone()],
                                    points: vec![point.clone()],
                                })
                                .is_ok() =>
                        {
                            points.push(point)
                        }
                        Ok(_) => {
                            rejected += 1;
                            rejected_details
                                .push(format!("metric {:?} has an invalid point", metric.name));
                        }
                        Err(detail) => {
                            rejected += 1;
                            rejected_details.push(format!("metric {:?}: {detail}", metric.name));
                        }
                    }
                }
                descriptors.push(descriptor);
            }
        }
    }
    MetricsMappingV2 {
        batch: MetricsBatchV2 {
            magic: scry_proto::constants::METRICS_BATCH_V2_MAGIC,
            descriptors,
            points,
        },
        rejected,
        rejected_details,
    }
}

fn temporality(value: i32) -> Temporality {
    match AggregationTemporality::try_from(value) {
        Ok(AggregationTemporality::Delta) => Temporality::Delta,
        Ok(AggregationTemporality::Cumulative) => Temporality::Cumulative,
        _ => Temporality::Unspecified,
    }
}

fn optional_f64(value: Option<f64>) -> (u8, f64) {
    value.map_or((0, 0.0), |v| (1, v))
}

fn number(value: number_data_point::Value) -> MetricNumberV2 {
    let value = match value {
        number_data_point::Value::AsInt(value) => {
            MetricNumberV2Value::IntegerValueV2(IntegerValueV2Input { value }.into())
        }
        number_data_point::Value::AsDouble(value) => {
            MetricNumberV2Value::DoubleValueV2(DoubleValueV2Input { value }.into())
        }
    };
    MetricNumberV2 { value }
}

fn integer_count(value: u64) -> MetricCountV2 {
    MetricCountV2 {
        value: MetricCountV2Value::IntegerCountV2(IntegerCountV2Input { value }.into()),
    }
}

fn scalar_point(id: u32, point: NumberDataPoint) -> Result<MetricPointV2, String> {
    let value = point
        .value
        .ok_or_else(|| "scalar point has no value".to_string())?;
    Ok(MetricPointV2 {
        value: MetricPointV2Value::ScalarPointV2(
            ScalarPointV2Input {
                descriptor_id: id,
                start_unix_nano: point.start_time_unix_nano,
                ts_unix_nano: point.time_unix_nano,
                flags: point.flags,
                attributes: kv_to_labels(&point.attributes),
                exemplars: map_exemplars(point.exemplars)?,
                number: number(value),
            }
            .into(),
        ),
    })
}

fn map_exemplars(values: Vec<Exemplar>) -> Result<Vec<MetricExemplarV2>, String> {
    values
        .into_iter()
        .map(|e| {
            let value = e.value.ok_or_else(|| "exemplar has no value".to_string())?;
            let value = match value {
                exemplar::Value::AsInt(v) => {
                    MetricNumberV2Value::IntegerValueV2(IntegerValueV2Input { value: v }.into())
                }
                exemplar::Value::AsDouble(v) => {
                    MetricNumberV2Value::DoubleValueV2(DoubleValueV2Input { value: v }.into())
                }
            };
            Ok(MetricExemplarV2 {
                ts_unix_nano: e.time_unix_nano,
                number: MetricNumberV2 { value },
                filtered_attrs: kv_to_labels(&e.filtered_attributes),
                trace_id: e.trace_id,
                span_id: e.span_id,
            })
        })
        .collect()
}

fn sparse(value: Option<exponential_histogram_data_point::Buckets>) -> SparseBucketsV2 {
    let Some(value) = value else {
        return SparseBucketsV2 {
            offset: 0,
            deltas: Vec::new(),
            counts: Vec::new(),
        };
    };
    let len = value.bucket_counts.len();
    SparseBucketsV2 {
        offset: value.offset,
        deltas: (0..len).map(|i| if i == 0 { 0 } else { 1 }).collect(),
        counts: value.bucket_counts.into_iter().map(integer_count).collect(),
    }
}

fn point_time(point: &MetricPointV2) -> u64 {
    match &point.value {
        MetricPointV2Value::ScalarPointV2(v) => v.ts_unix_nano,
        MetricPointV2Value::HistogramPointV2(v) => v.ts_unix_nano,
        MetricPointV2Value::ExponentialHistogramPointV2(v) => v.ts_unix_nano,
        MetricPointV2Value::SummaryPointV2(v) => v.ts_unix_nano,
    }
}

fn merge_point_labels(
    labels: &mut Vec<LabelPair>,
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) {
    for point in kv_to_labels(attrs) {
        if let Some(existing) = labels.iter_mut().find(|label| label.key == point.key) {
            *existing = point;
        } else {
            labels.push(point);
        }
    }
}

fn scalar_value(point: &NumberDataPoint) -> Option<f64> {
    match point.value {
        Some(number_data_point::Value::AsDouble(value)) => Some(value),
        Some(number_data_point::Value::AsInt(value)) => Some(value as f64),
        None => None,
    }
}

pub fn sample_request(points: usize) -> ExportMetricsServiceRequest {
    use opentelemetry_proto::tonic::{
        common::v1::{any_value::Value, AnyValue, InstrumentationScope, KeyValue},
        metrics::v1::{Gauge, Metric, ResourceMetrics, ScopeMetrics},
        resource::v1::Resource,
    };
    let attr = |key: &str, value: &str| KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..Default::default()
    };
    let mut data_points = Vec::with_capacity(points);
    for index in 0..points {
        data_points.push(NumberDataPoint {
            attributes: vec![attr("route", "/smoke")],
            time_unix_nano: 1_700_200_000_000_000_000 + index as u64,
            value: Some(number_data_point::Value::AsDouble(index as f64 + 0.5)),
            ..Default::default()
        });
    }
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![attr("service.name", "otlp-metrics")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "probe".into(),
                    version: "1".into(),
                    ..Default::default()
                }),
                metrics: vec![Metric {
                    name: "smoke.gauge".into(),
                    data: Some(metric::Data::Gauge(Gauge { data_points })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        common::v1::{any_value, AnyValue, KeyValue},
        metrics::v1::{
            summary_data_point, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
            Histogram, HistogramDataPoint, Metric, Sum, Summary, SummaryDataPoint,
        },
    };

    fn attr(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
            ..Default::default()
        }
    }

    fn only_metric(request: &mut ExportMetricsServiceRequest, metric: Metric) {
        request.resource_metrics[0].scope_metrics[0].metrics = vec![metric];
    }

    #[test]
    fn legacy_mapper_maps_scalar_points_and_rejects_histograms() {
        let mut request = sample_request(2);
        request.resource_metrics[0].scope_metrics[0]
            .metrics
            .push(Metric {
                name: "hist".into(),
                data: Some(metric::Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint::default()],
                    ..Default::default()
                })),
                ..Default::default()
            });
        let mapped = map_metrics(request);
        assert_eq!(mapped.batch.samples.len(), 2);
        assert_eq!(mapped.rejected, 1);
        assert_eq!(mapped.batch.series[0].metric_type, METRIC_TYPE_GAUGE);
    }

    #[test]
    fn v2_preserves_descriptor_scalars_attributes_and_exemplars() {
        let mut request = sample_request(0);
        let exemplar = Exemplar {
            filtered_attributes: vec![attr("filtered", "yes")],
            time_unix_nano: 19,
            value: Some(exemplar::Value::AsInt(i64::MAX)),
            span_id: vec![2; 8],
            trace_id: vec![3; 16],
        };
        only_metric(
            &mut request,
            Metric {
                name: "requests".into(),
                description: "request count".into(),
                unit: "{request}".into(),
                data: Some(metric::Data::Sum(Sum {
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    is_monotonic: true,
                    data_points: vec![
                        NumberDataPoint {
                            attributes: vec![attr("method", "GET")],
                            start_time_unix_nano: 10,
                            time_unix_nano: 20,
                            flags: 1,
                            exemplars: vec![exemplar],
                            value: Some(number_data_point::Value::AsInt(9_007_199_254_740_993)),
                        },
                        NumberDataPoint {
                            start_time_unix_nano: 10,
                            time_unix_nano: 21,
                            value: Some(number_data_point::Value::AsDouble(1.25)),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                })),
                ..Default::default()
            },
        );
        let mapped = map_metrics_v2(request);
        assert_eq!(mapped.rejected, 0, "{:?}", mapped.rejected_details);
        assert!(metrics_v2::validate(&mapped.batch).is_ok());
        let descriptor = &mapped.batch.descriptors[0];
        assert_eq!(descriptor.name, "requests");
        assert_eq!(descriptor.description, "request count");
        assert_eq!(descriptor.unit, "{request}");
        assert_eq!(descriptor.metric_kind, MetricKind::Sum as u8);
        assert_eq!(descriptor.temporality, Temporality::Cumulative as u8);
        assert_eq!(descriptor.monotonic, 1);
        assert_eq!(descriptor.resource_attrs[0].key, "service.name");
        assert_eq!(descriptor.scope_name, "probe");
        assert_eq!(descriptor.scope_version, "1");
        let MetricPointV2Value::ScalarPointV2(first) = &mapped.batch.points[0].value else {
            panic!("not scalar")
        };
        assert_eq!(first.start_unix_nano, 10);
        assert_eq!(first.ts_unix_nano, 20);
        assert_eq!(first.flags, 1);
        assert_eq!(first.attributes[0].value, "GET");
        assert!(matches!(
            first.number.value,
            MetricNumberV2Value::IntegerValueV2(ref v) if v.value == 9_007_199_254_740_993
        ));
        assert!(matches!(
            first.exemplars[0].number.value,
            MetricNumberV2Value::IntegerValueV2(ref v) if v.value == i64::MAX
        ));
        assert_eq!(first.exemplars[0].trace_id, vec![3; 16]);
        let MetricPointV2Value::ScalarPointV2(second) = &mapped.batch.points[1].value else {
            panic!("not scalar")
        };
        assert!(matches!(
            second.number.value,
            MetricNumberV2Value::DoubleValueV2(ref v) if v.value == 1.25
        ));
    }

    #[test]
    fn v2_preserves_histogram_optional_fields_and_buckets() {
        let mut request = sample_request(0);
        only_metric(
            &mut request,
            Metric {
                name: "latency".into(),
                data: Some(metric::Data::Histogram(Histogram {
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                    data_points: vec![HistogramDataPoint {
                        attributes: vec![attr("route", "/")],
                        start_time_unix_nano: 5,
                        time_unix_nano: 9,
                        count: 6,
                        sum: Some(12.5),
                        bucket_counts: vec![1, 2, 3],
                        explicit_bounds: vec![1.0, 2.0],
                        exemplars: vec![Exemplar {
                            time_unix_nano: 8,
                            value: Some(exemplar::Value::AsDouble(1.5)),
                            ..Default::default()
                        }],
                        flags: 7,
                        min: None,
                        max: Some(4.0),
                    }],
                })),
                ..Default::default()
            },
        );
        let mapped = map_metrics_v2(request);
        assert_eq!(mapped.rejected, 0, "{:?}", mapped.rejected_details);
        let descriptor = &mapped.batch.descriptors[0];
        assert_eq!(descriptor.metric_kind, MetricKind::Histogram as u8);
        assert_eq!(descriptor.temporality, Temporality::Delta as u8);
        let MetricPointV2Value::HistogramPointV2(point) = &mapped.batch.points[0].value else {
            panic!("not histogram")
        };
        assert_eq!((point.has_sum, point.sum), (1, 12.5));
        assert_eq!((point.has_min, point.min), (0, 0.0));
        assert_eq!((point.has_max, point.max), (1, 4.0));
        assert_eq!(point.explicit_bounds, vec![1.0, 2.0]);
        assert_eq!(point.bucket_counts, vec![1, 2, 3]);
        assert!(matches!(
            point.exemplars[0].number.value,
            MetricNumberV2Value::DoubleValueV2(ref v) if v.value == 1.5
        ));
    }

    #[test]
    fn v2_preserves_exponential_histogram_sparse_buckets() {
        let mut request = sample_request(0);
        only_metric(
            &mut request,
            Metric {
                name: "size".into(),
                data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    data_points: vec![ExponentialHistogramDataPoint {
                        start_time_unix_nano: 1,
                        time_unix_nano: 10,
                        count: 10,
                        sum: Some(22.0),
                        scale: -2,
                        zero_count: 4,
                        positive: Some(exponential_histogram_data_point::Buckets {
                            offset: -3,
                            bucket_counts: vec![1, 2],
                        }),
                        negative: Some(exponential_histogram_data_point::Buckets {
                            offset: 7,
                            bucket_counts: vec![3],
                        }),
                        flags: 2,
                        exemplars: vec![],
                        min: Some(-4.0),
                        max: None,
                        zero_threshold: 0.01,
                        attributes: vec![],
                    }],
                })),
                ..Default::default()
            },
        );
        let mapped = map_metrics_v2(request);
        assert_eq!(mapped.rejected, 0, "{:?}", mapped.rejected_details);
        let MetricPointV2Value::ExponentialHistogramPointV2(point) = &mapped.batch.points[0].value
        else {
            panic!("not exponential histogram")
        };
        assert_eq!(point.scale, -2);
        assert_eq!(point.zero_threshold, 0.01);
        assert_eq!(point.positive.offset, -3);
        assert_eq!(point.positive.deltas, vec![0, 1]);
        assert_eq!(point.negative.offset, 7);
        assert_eq!(point.negative.deltas, vec![0]);
        assert_eq!(point.reset_hint, ResetHint::Unknown as u8);
        assert_eq!((point.has_min, point.min), (1, -4.0));
        assert_eq!((point.has_max, point.max), (0, 0.0));
    }

    #[test]
    fn v2_preserves_summary_quantiles() {
        let mut request = sample_request(0);
        only_metric(
            &mut request,
            Metric {
                name: "rpc.summary".into(),
                data: Some(metric::Data::Summary(Summary {
                    data_points: vec![SummaryDataPoint {
                        start_time_unix_nano: 1,
                        time_unix_nano: 2,
                        count: 3,
                        sum: 9.0,
                        quantile_values: vec![
                            summary_data_point::ValueAtQuantile {
                                quantile: 0.5,
                                value: 2.0,
                            },
                            summary_data_point::ValueAtQuantile {
                                quantile: 0.99,
                                value: 5.0,
                            },
                        ],
                        flags: 4,
                        attributes: vec![attr("rpc", "call")],
                    }],
                })),
                ..Default::default()
            },
        );
        let mapped = map_metrics_v2(request);
        assert_eq!(mapped.rejected, 0, "{:?}", mapped.rejected_details);
        let MetricPointV2Value::SummaryPointV2(point) = &mapped.batch.points[0].value else {
            panic!("not summary")
        };
        assert_eq!(point.count, 3);
        assert_eq!(point.sum, 9.0);
        assert_eq!(point.quantiles[1].quantile, 0.99);
        assert_eq!(point.quantiles[1].value, 5.0);
        assert!(point.exemplars.is_empty());
    }

    #[test]
    fn v2_rejects_only_invalid_points() {
        let mut request = sample_request(2);
        let points = match request.resource_metrics[0].scope_metrics[0].metrics[0]
            .data
            .as_mut()
            .unwrap()
        {
            metric::Data::Gauge(Gauge { data_points }) => data_points,
            _ => unreachable!(),
        };
        points[0].time_unix_nano = 0;
        points[1].value = Some(number_data_point::Value::AsDouble(f64::NAN));
        points.push(NumberDataPoint {
            time_unix_nano: 3,
            value: Some(number_data_point::Value::AsInt(7)),
            ..Default::default()
        });
        let mapped = map_metrics_v2(request);
        assert_eq!(mapped.rejected, 1, "NaN is a valid Prometheus/OTLP scalar");
        assert_eq!(mapped.batch.points.len(), 2);
        assert!(metrics_v2::validate(&mapped.batch).is_ok());
    }
}
