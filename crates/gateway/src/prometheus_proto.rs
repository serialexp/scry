//! Pinned Prometheus Remote Write protobuf wire definitions.
//!
//! These hand-maintained `prost` types mirror the write-related messages from
//! Prometheus commit `d141c42f7c53caf25e294a5488cf5f5c3f358d1a` (2025-03-06).
//! The exact upstream sources and license are vendored in `../proto/vendor/`.
//! Keeping the types here avoids a build-time `protoc`/gogoproto dependency.

/// Remote Write 1.0 protobuf content type.
pub const REMOTE_WRITE_V1_CONTENT_TYPE: &str =
    "application/x-protobuf;proto=prometheus.WriteRequest";
/// Remote Write 2.0 protobuf content type.
pub const REMOTE_WRITE_V2_CONTENT_TYPE: &str =
    "application/x-protobuf;proto=io.prometheus.write.v2.Request";

/// Prometheus Remote Write 1.0 (`prometheus.WriteRequest`).
pub mod v1 {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct WriteRequest {
        #[prost(message, repeated, tag = "1")]
        pub timeseries: Vec<TimeSeries>,
        // Field 2 is reserved upstream.
        #[prost(message, repeated, tag = "3")]
        pub metadata: Vec<MetricMetadata>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TimeSeries {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub exemplars: Vec<Exemplar>,
        #[prost(message, repeated, tag = "4")]
        pub histograms: Vec<Histogram>,
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

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Exemplar {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct MetricMetadata {
        #[prost(enumeration = "MetricType", tag = "1")]
        pub r#type: i32,
        #[prost(string, tag = "2")]
        pub metric_family_name: String,
        // Field 3 is intentionally absent upstream.
        #[prost(string, tag = "4")]
        pub help: String,
        #[prost(string, tag = "5")]
        pub unit: String,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum MetricType {
        Unknown = 0,
        Counter = 1,
        Gauge = 2,
        Histogram = 3,
        Gaugehistogram = 4,
        Summary = 5,
        Info = 6,
        Stateset = 7,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Histogram {
        #[prost(oneof = "histogram::Count", tags = "1, 2")]
        pub count: Option<histogram::Count>,
        #[prost(double, tag = "3")]
        pub sum: f64,
        #[prost(sint32, tag = "4")]
        pub schema: i32,
        #[prost(double, tag = "5")]
        pub zero_threshold: f64,
        #[prost(oneof = "histogram::ZeroCount", tags = "6, 7")]
        pub zero_count: Option<histogram::ZeroCount>,
        #[prost(message, repeated, tag = "8")]
        pub negative_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, packed = "true", tag = "9")]
        pub negative_deltas: Vec<i64>,
        #[prost(double, repeated, packed = "true", tag = "10")]
        pub negative_counts: Vec<f64>,
        #[prost(message, repeated, tag = "11")]
        pub positive_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, packed = "true", tag = "12")]
        pub positive_deltas: Vec<i64>,
        #[prost(double, repeated, packed = "true", tag = "13")]
        pub positive_counts: Vec<f64>,
        #[prost(enumeration = "ResetHint", tag = "14")]
        pub reset_hint: i32,
        #[prost(int64, tag = "15")]
        pub timestamp: i64,
        #[prost(double, repeated, packed = "true", tag = "16")]
        pub custom_values: Vec<f64>,
    }

    pub mod histogram {
        #[derive(Clone, Copy, PartialEq, ::prost::Oneof)]
        pub enum Count {
            #[prost(uint64, tag = "1")]
            CountInt(u64),
            #[prost(double, tag = "2")]
            CountFloat(f64),
        }
        #[derive(Clone, Copy, PartialEq, ::prost::Oneof)]
        pub enum ZeroCount {
            #[prost(uint64, tag = "6")]
            ZeroCountInt(u64),
            #[prost(double, tag = "7")]
            ZeroCountFloat(f64),
        }
    }

    #[derive(Clone, Copy, PartialEq, ::prost::Message)]
    pub struct BucketSpan {
        #[prost(sint32, tag = "1")]
        pub offset: i32,
        #[prost(uint32, tag = "2")]
        pub length: u32,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ResetHint {
        Unknown = 0,
        Yes = 1,
        No = 2,
        Gauge = 3,
    }
}

/// Prometheus Remote Write 2.0 (`io.prometheus.write.v2.Request`).
pub mod v2 {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Request {
        // Fields 1 through 3 are reserved upstream.
        #[prost(string, repeated, tag = "4")]
        pub symbols: Vec<String>,
        #[prost(message, repeated, tag = "5")]
        pub timeseries: Vec<TimeSeries>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TimeSeries {
        #[prost(uint32, repeated, packed = "true", tag = "1")]
        pub labels_refs: Vec<u32>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub histograms: Vec<Histogram>,
        #[prost(message, repeated, tag = "4")]
        pub exemplars: Vec<Exemplar>,
        #[prost(message, optional, tag = "5")]
        pub metadata: Option<Metadata>,
        // Field 6 is reserved upstream.
    }

    #[derive(Clone, Copy, PartialEq, ::prost::Message)]
    pub struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
        #[prost(int64, tag = "3")]
        pub start_timestamp: i64,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Exemplar {
        #[prost(uint32, repeated, packed = "true", tag = "1")]
        pub labels_refs: Vec<u32>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }

    #[derive(Clone, Copy, PartialEq, ::prost::Message)]
    pub struct Metadata {
        #[prost(enumeration = "MetricType", tag = "1")]
        pub r#type: i32,
        // Field 2 is intentionally absent upstream.
        #[prost(uint32, tag = "3")]
        pub help_ref: u32,
        #[prost(uint32, tag = "4")]
        pub unit_ref: u32,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum MetricType {
        Unspecified = 0,
        Counter = 1,
        Gauge = 2,
        Histogram = 3,
        Gaugehistogram = 4,
        Summary = 5,
        Info = 6,
        Stateset = 7,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Histogram {
        #[prost(oneof = "histogram::Count", tags = "1, 2")]
        pub count: Option<histogram::Count>,
        #[prost(double, tag = "3")]
        pub sum: f64,
        #[prost(sint32, tag = "4")]
        pub schema: i32,
        #[prost(double, tag = "5")]
        pub zero_threshold: f64,
        #[prost(oneof = "histogram::ZeroCount", tags = "6, 7")]
        pub zero_count: Option<histogram::ZeroCount>,
        #[prost(message, repeated, tag = "8")]
        pub negative_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, packed = "true", tag = "9")]
        pub negative_deltas: Vec<i64>,
        #[prost(double, repeated, packed = "true", tag = "10")]
        pub negative_counts: Vec<f64>,
        #[prost(message, repeated, tag = "11")]
        pub positive_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, packed = "true", tag = "12")]
        pub positive_deltas: Vec<i64>,
        #[prost(double, repeated, packed = "true", tag = "13")]
        pub positive_counts: Vec<f64>,
        #[prost(enumeration = "ResetHint", tag = "14")]
        pub reset_hint: i32,
        #[prost(int64, tag = "15")]
        pub timestamp: i64,
        #[prost(double, repeated, packed = "true", tag = "16")]
        pub custom_values: Vec<f64>,
        #[prost(int64, tag = "17")]
        pub start_timestamp: i64,
    }

    pub mod histogram {
        #[derive(Clone, Copy, PartialEq, ::prost::Oneof)]
        pub enum Count {
            #[prost(uint64, tag = "1")]
            CountInt(u64),
            #[prost(double, tag = "2")]
            CountFloat(f64),
        }
        #[derive(Clone, Copy, PartialEq, ::prost::Oneof)]
        pub enum ZeroCount {
            #[prost(uint64, tag = "6")]
            ZeroCountInt(u64),
            #[prost(double, tag = "7")]
            ZeroCountFloat(f64),
        }
    }

    #[derive(Clone, Copy, PartialEq, ::prost::Message)]
    pub struct BucketSpan {
        #[prost(sint32, tag = "1")]
        pub offset: i32,
        #[prost(uint32, tag = "2")]
        pub length: u32,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ResetHint {
        Unspecified = 0,
        Yes = 1,
        No = 2,
        Gauge = 3,
    }
}
