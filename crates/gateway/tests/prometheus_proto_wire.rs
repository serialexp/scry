//! Compatibility fixtures for the pinned Prometheus Remote Write schemas.

use prost::Message;
use scry_gateway::prometheus_proto::{self, v1, v2};

#[test]
fn content_types_are_the_canonical_schema_discriminators() {
    assert_eq!(
        prometheus_proto::REMOTE_WRITE_V1_CONTENT_TYPE,
        "application/x-protobuf;proto=prometheus.WriteRequest"
    );
    assert_eq!(
        prometheus_proto::REMOTE_WRITE_V2_CONTENT_TYPE,
        "application/x-protobuf;proto=io.prometheus.write.v2.Request"
    );
}

#[test]
fn rw1_decodes_upstream_tag_fixture_and_roundtrips() {
    // Hand-authored protobuf fixture: WriteRequest.metadata (tag 3), containing
    // type=COUNTER (1), family (2), help (4), and unit (5). In particular this
    // protects the intentional metadata gap at tag 3.
    const WIRE: &[u8] = &[
        0x1a, 0x0b, // WriteRequest.metadata
        0x08, 0x01, // MetricMetadata.type
        0x12, 0x01, b'x', // metric_family_name
        0x22, 0x01, b'h', // help (tag 4)
        0x2a, 0x01, b's', // unit (tag 5)
    ];
    let request = v1::WriteRequest::decode(WIRE).unwrap();
    assert_eq!(request.metadata.len(), 1);
    let metadata = &request.metadata[0];
    assert_eq!(metadata.r#type, v1::MetricType::Counter as i32);
    assert_eq!(metadata.metric_family_name, "x");
    assert_eq!(metadata.help, "h");
    assert_eq!(metadata.unit, "s");
    assert_eq!(request.encode_to_vec(), WIRE);
}

#[test]
fn rw2_decodes_upstream_tag_fixture_and_roundtrips() {
    // Request.symbols is tag 4 and Request.timeseries is tag 5. The nested
    // series uses labels_refs=1 and metadata=5; metadata itself intentionally
    // has no tag 2 (help_ref=3, unit_ref=4).
    const WIRE: &[u8] = &[
        0x22, 0x00, // symbols[0] = ""
        0x22, 0x01, b'x', // symbols[1] = "x"
        0x2a, 0x0a, // timeseries
        0x0a, 0x02, 0x01, 0x01, // labels_refs
        0x2a, 0x04, // metadata
        0x08, 0x01, // type=COUNTER
        0x18, 0x01, // help_ref (tag 3)
    ];
    let request = v2::Request::decode(WIRE).unwrap();
    assert_eq!(request.symbols, ["", "x"]);
    let series = &request.timeseries[0];
    assert_eq!(series.labels_refs, [1, 1]);
    let metadata = series.metadata.as_ref().unwrap();
    assert_eq!(metadata.r#type, v2::MetricType::Counter as i32);
    assert_eq!(metadata.help_ref, 1);
    assert_eq!(metadata.unit_ref, 0);
    assert_eq!(request.encode_to_vec(), WIRE);
}

#[test]
fn rw1_all_write_payload_fields_roundtrip() {
    let histogram = v1::Histogram {
        count: Some(v1::histogram::Count::CountInt(7)),
        sum: 8.5,
        schema: -2,
        zero_threshold: 0.001,
        zero_count: Some(v1::histogram::ZeroCount::ZeroCountFloat(1.5)),
        negative_spans: vec![v1::BucketSpan {
            offset: -3,
            length: 2,
        }],
        negative_deltas: vec![1, -2],
        negative_counts: vec![1.25],
        positive_spans: vec![v1::BucketSpan {
            offset: 1,
            length: 3,
        }],
        positive_deltas: vec![2, -1],
        positive_counts: vec![2.5],
        reset_hint: v1::ResetHint::Gauge as i32,
        timestamp: 1_700_000_000_000,
        custom_values: vec![0.5, 1.0],
    };
    let request = v1::WriteRequest {
        timeseries: vec![v1::TimeSeries {
            labels: vec![v1::Label {
                name: "__name__".into(),
                value: "requests".into(),
            }],
            samples: vec![v1::Sample {
                value: 3.5,
                timestamp: 11,
            }],
            exemplars: vec![v1::Exemplar {
                labels: vec![v1::Label {
                    name: "trace_id".into(),
                    value: "abc".into(),
                }],
                value: 3.5,
                timestamp: 10,
            }],
            histograms: vec![histogram],
        }],
        metadata: vec![v1::MetricMetadata {
            r#type: v1::MetricType::Histogram as i32,
            metric_family_name: "requests".into(),
            help: "request distribution".into(),
            unit: "seconds".into(),
        }],
    };
    let wire = request.encode_to_vec();
    assert_eq!(v1::WriteRequest::decode(wire.as_slice()).unwrap(), request);
}

#[test]
fn rw2_all_write_payload_fields_roundtrip() {
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
                value: 4.5,
                timestamp: 20,
                start_timestamp: 10,
            }],
            histograms: vec![v2::Histogram {
                count: Some(v2::histogram::Count::CountFloat(4.5)),
                sum: 9.0,
                schema: -53,
                zero_threshold: 0.0,
                zero_count: Some(v2::histogram::ZeroCount::ZeroCountInt(0)),
                negative_spans: vec![],
                negative_deltas: vec![],
                negative_counts: vec![],
                positive_spans: vec![v2::BucketSpan {
                    offset: 0,
                    length: 2,
                }],
                positive_deltas: vec![2, 1],
                positive_counts: vec![2.0, 3.0],
                reset_hint: v2::ResetHint::Yes as i32,
                timestamp: 20,
                custom_values: vec![1.0, 2.0],
                start_timestamp: 10,
            }],
            exemplars: vec![v2::Exemplar {
                labels_refs: vec![1, 2],
                value: 2.0,
                timestamp: 19,
            }],
            metadata: Some(v2::Metadata {
                r#type: v2::MetricType::Histogram as i32,
                help_ref: 3,
                unit_ref: 0,
            }),
        }],
    };
    let wire = request.encode_to_vec();
    assert_eq!(v2::Request::decode(wire.as_slice()).unwrap(), request);
}
