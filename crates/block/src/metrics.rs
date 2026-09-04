//! Block builder for metrics samples — the v0.2 first-real-signal.
//!
//! Per `ARCHITECTURE.md § Metrics`, a metrics block consists of three
//! objects in the bucket:
//!
//! - `<block>.parquet` — `(series_fingerprint, ts_unix_nano, value)`
//!   sorted by `(series_fingerprint, ts)`. The intra-block sort makes
//!   parquet row-group min/max stats on the fingerprint column an
//!   aggressive pruning lever once a query has resolved its target
//!   fingerprint set.
//! - `<block>.postings.parquet` — `(label_name, label_value,
//!   series_fingerprints LIST<u64>)` sorted by `(label_name,
//!   label_value)`. This is the inverted index that turns a
//!   `metric{service="api", env="prod"}` predicate into a small
//!   fingerprint set without scanning the main parquet.
//! - `<block>.meta.json` — the catalog's source of truth for block
//!   existence; carries `has_postings`/`postings_size_bytes` plus the
//!   per-block `series_types` map (since the canonical postings schema
//!   has nowhere to encode counter-vs-gauge intent).
//!
//! Wire input is `MetricsBatch { series: Vec<SeriesDictEntry>, samples:
//! Vec<MetricSample> }` (see `scry_proto::generated`). Each batch
//! re-sends whatever portion of its series dictionary the agent
//! considered active; we dedup by fingerprint server-side. The hot
//! ingest path is per-sample (3 × u64 + 1 × f64 = 24 bytes); series
//! ingestion is amortised across many samples.
//!
//! ## CSR layout
//!
//! Hot-path sample storage uses three parallel `Vec`s instead of
//! `Vec<MetricSample>` so the data lives in column-shaped memory —
//! matches Arrow's internal layout, which lets `from_iter_values` walk
//! each column as a single contiguous memcpy at parquet-encode time.
//! Same lesson as `crates/block/src/dummy.rs`; see CLAUDE.md
//! § Performance.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Float64Array, Int32Array, Int64Array, ListArray,
    StringArray, StructArray, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use scry_proto::generated::{
    LabelPair, MetricCountV2, MetricCountV2Value, MetricDescriptorV2, MetricExemplarV2,
    MetricNumberV2, MetricNumberV2Value, MetricPointV2, MetricPointV2Value, SparseBucketsV2,
};
use scry_proto::streaming::MetricsAppender;
use scry_proto::streaming_v2::MetricsV2Appender;
use uuid::Uuid;

use crate::{block_path, BlockBuilder, BlockBuilderConfig, BlockMeta, EncodedBlock};

const SIGNAL: &str = "metrics";
const SCHEMA_VERSION: u32 = 3;

#[derive(Clone)]
struct MetricRow {
    fingerprint: u64,
    ts: u64,
    value: Option<f64>,
    descriptor_id: Option<u32>,
    descriptor: Option<MetricDescriptorV2>,
    point: Option<MetricPointV2>,
}

fn fields(items: Vec<Field>) -> Fields {
    items.into()
}

fn attr_item_field() -> Arc<Field> {
    Arc::new(Field::new(
        "item",
        DataType::Struct(fields(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        false,
    ))
}

fn attrs_type() -> DataType {
    DataType::List(attr_item_field())
}

fn number_fields(integer_type: DataType) -> Fields {
    fields(vec![
        Field::new("kind", DataType::UInt8, false),
        Field::new("integer", integer_type, true),
        Field::new("float", DataType::Float64, true),
    ])
}

fn number_type() -> DataType {
    DataType::Struct(number_fields(DataType::Int64))
}

fn count_type() -> DataType {
    DataType::Struct(number_fields(DataType::UInt64))
}

fn exemplar_item_field() -> Arc<Field> {
    Arc::new(Field::new(
        "item",
        DataType::Struct(fields(vec![
            Field::new("ts_unix_nano", DataType::UInt64, false),
            Field::new("number", number_type(), false),
            Field::new("filtered_attrs", attrs_type(), false),
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("span_id", DataType::FixedSizeBinary(8), false),
        ])),
        false,
    ))
}

fn sparse_type() -> DataType {
    DataType::Struct(fields(vec![
        Field::new("offset", DataType::Int32, false),
        Field::new(
            "deltas",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            false,
        ),
        Field::new(
            "counts",
            DataType::List(Arc::new(Field::new("item", count_type(), false))),
            false,
        ),
    ]))
}

fn point_fields() -> Fields {
    let floats = || DataType::List(Arc::new(Field::new("item", DataType::Float64, false)));
    let uints = || DataType::List(Arc::new(Field::new("item", DataType::UInt64, false)));
    let histogram = DataType::Struct(fields(vec![
        Field::new("count", DataType::UInt64, false),
        Field::new("has_sum", DataType::UInt8, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("has_min", DataType::UInt8, false),
        Field::new("min", DataType::Float64, false),
        Field::new("has_max", DataType::UInt8, false),
        Field::new("max", DataType::Float64, false),
        Field::new("explicit_bounds", floats(), false),
        Field::new("bucket_counts", uints(), false),
    ]));
    let exponential = DataType::Struct(fields(vec![
        Field::new("count", count_type(), false),
        Field::new("has_sum", DataType::UInt8, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("has_min", DataType::UInt8, false),
        Field::new("min", DataType::Float64, false),
        Field::new("has_max", DataType::UInt8, false),
        Field::new("max", DataType::Float64, false),
        Field::new("scale", DataType::Int32, false),
        Field::new("zero_threshold", DataType::Float64, false),
        Field::new("zero_count", count_type(), false),
        Field::new("positive", sparse_type(), false),
        Field::new("negative", sparse_type(), false),
        Field::new("custom_bounds", floats(), false),
        Field::new("reset_hint", DataType::UInt8, false),
    ]));
    let summary = DataType::Struct(fields(vec![
        Field::new("count", DataType::UInt64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new(
            "quantiles",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(fields(vec![
                    Field::new("quantile", DataType::Float64, false),
                    Field::new("value", DataType::Float64, false),
                ])),
                false,
            ))),
            false,
        ),
    ]));
    fields(vec![
        Field::new("kind", DataType::UInt8, false),
        Field::new("start_unix_nano", DataType::UInt64, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("attributes", attrs_type(), false),
        Field::new("exemplars", DataType::List(exemplar_item_field()), false),
        Field::new("scalar", number_type(), true),
        Field::new("histogram", histogram, true),
        Field::new("exponential_histogram", exponential, true),
        Field::new("summary", summary, true),
    ])
}

fn descriptor_fields() -> Fields {
    fields(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
        Field::new("metric_kind", DataType::UInt8, false),
        Field::new("temporality", DataType::UInt8, false),
        Field::new("monotonic", DataType::UInt8, false),
        Field::new("resource_attrs", attrs_type(), false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
        Field::new("scope_attrs", attrs_type(), false),
    ])
}

fn validity(valid: impl IntoIterator<Item = bool>) -> Option<NullBuffer> {
    Some(NullBuffer::new(BooleanBuffer::from_iter(valid)))
}

fn primitive_list<T, A>(rows: &[Option<&[T]>], field: Arc<Field>, values: A) -> ArrayRef
where
    A: arrow::array::Array + 'static,
{
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0_i32);
    let mut running = 0_i32;
    for row in rows {
        running += row.map_or(0, |v| v.len()) as i32;
        offsets.push(running);
    }
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        validity(rows.iter().map(Option::is_some)),
    ))
}

fn build_attrs(rows: &[Option<&[LabelPair]>]) -> ArrayRef {
    let flattened = rows.iter().flat_map(|row| row.unwrap_or_default());
    let (keys, values): (Vec<_>, Vec<_>) = flattened
        .map(|p| (p.key.as_str(), p.value.as_str()))
        .unzip();
    let entries = StructArray::new(
        match attr_item_field().data_type() {
            DataType::Struct(f) => f.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(StringArray::from(values)),
        ],
        None,
    );
    primitive_list(rows, attr_item_field(), entries)
}

fn build_numbers(rows: &[Option<&MetricNumberV2>]) -> ArrayRef {
    let mut kinds = Vec::with_capacity(rows.len());
    let mut integers = Vec::with_capacity(rows.len());
    let mut floats = Vec::with_capacity(rows.len());
    for row in rows {
        match row.map(|n| &n.value) {
            Some(MetricNumberV2Value::IntegerValueV2(v)) => {
                kinds.push(1);
                integers.push(Some(v.value));
                floats.push(None);
            }
            Some(MetricNumberV2Value::DoubleValueV2(v)) => {
                kinds.push(2);
                integers.push(None);
                floats.push(Some(v.value));
            }
            None => {
                kinds.push(0);
                integers.push(None);
                floats.push(None);
            }
        }
    }
    Arc::new(StructArray::new(
        number_fields(DataType::Int64),
        vec![
            Arc::new(UInt8Array::from(kinds)),
            Arc::new(Int64Array::from(integers)),
            Arc::new(Float64Array::from(floats)),
        ],
        validity(rows.iter().map(Option::is_some)),
    ))
}

fn build_counts(rows: &[Option<&MetricCountV2>]) -> ArrayRef {
    let mut kinds = Vec::with_capacity(rows.len());
    let mut integers = Vec::with_capacity(rows.len());
    let mut floats = Vec::with_capacity(rows.len());
    for row in rows {
        match row.map(|n| &n.value) {
            Some(MetricCountV2Value::IntegerCountV2(v)) => {
                kinds.push(1);
                integers.push(Some(v.value));
                floats.push(None);
            }
            Some(MetricCountV2Value::FloatCountV2(v)) => {
                kinds.push(2);
                integers.push(None);
                floats.push(Some(v.value));
            }
            None => {
                kinds.push(0);
                integers.push(None);
                floats.push(None);
            }
        }
    }
    Arc::new(StructArray::new(
        number_fields(DataType::UInt64),
        vec![
            Arc::new(UInt8Array::from(kinds)),
            Arc::new(UInt64Array::from(integers)),
            Arc::new(Float64Array::from(floats)),
        ],
        validity(rows.iter().map(Option::is_some)),
    ))
}

fn build_exemplars(rows: &[Option<&[MetricExemplarV2]>]) -> Result<ArrayRef> {
    let flat: Vec<&MetricExemplarV2> = rows
        .iter()
        .flat_map(|row| row.unwrap_or_default())
        .collect();
    let numbers: Vec<_> = flat.iter().map(|e| Some(&e.number)).collect();
    let attrs: Vec<_> = flat
        .iter()
        .map(|e| Some(e.filtered_attrs.as_slice()))
        .collect();
    let mut trace_ids = FixedSizeBinaryBuilder::with_capacity(flat.len(), 16);
    let mut span_ids = FixedSizeBinaryBuilder::with_capacity(flat.len(), 8);
    for exemplar in &flat {
        trace_ids.append_value(&exemplar.trace_id)?;
        span_ids.append_value(&exemplar.span_id)?;
    }
    let entries = StructArray::new(
        match exemplar_item_field().data_type() {
            DataType::Struct(f) => f.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(UInt64Array::from_iter_values(
                flat.iter().map(|e| e.ts_unix_nano),
            )),
            build_numbers(&numbers),
            build_attrs(&attrs),
            Arc::new(trace_ids.finish()),
            Arc::new(span_ids.finish()),
        ],
        None,
    );
    Ok(primitive_list(rows, exemplar_item_field(), entries))
}

fn list_f64(rows: &[Option<&[f64]>]) -> ArrayRef {
    primitive_list(
        rows,
        Arc::new(Field::new("item", DataType::Float64, false)),
        Float64Array::from_iter_values(rows.iter().flat_map(|v| v.unwrap_or_default()).copied()),
    )
}

fn list_u64(rows: &[Option<&[u64]>]) -> ArrayRef {
    primitive_list(
        rows,
        Arc::new(Field::new("item", DataType::UInt64, false)),
        UInt64Array::from_iter_values(rows.iter().flat_map(|v| v.unwrap_or_default()).copied()),
    )
}

fn list_i32(rows: &[Option<&[i32]>]) -> ArrayRef {
    primitive_list(
        rows,
        Arc::new(Field::new("item", DataType::Int32, false)),
        Int32Array::from_iter_values(rows.iter().flat_map(|v| v.unwrap_or_default()).copied()),
    )
}

fn build_count_lists(rows: &[Option<&[MetricCountV2]>]) -> ArrayRef {
    let flattened: Vec<_> = rows
        .iter()
        .flat_map(|v| v.unwrap_or_default())
        .map(Some)
        .collect();
    primitive_list(
        rows,
        Arc::new(Field::new("item", count_type(), false)),
        build_counts(&flattened),
    )
}

fn build_sparse(rows: &[Option<&SparseBucketsV2>]) -> ArrayRef {
    let deltas: Vec<_> = rows
        .iter()
        .map(|v| v.map(|v| v.deltas.as_slice()))
        .collect();
    let counts: Vec<_> = rows
        .iter()
        .map(|v| v.map(|v| v.counts.as_slice()))
        .collect();
    Arc::new(StructArray::new(
        match sparse_type() {
            DataType::Struct(f) => f,
            _ => unreachable!(),
        },
        vec![
            Arc::new(Int32Array::from_iter(
                rows.iter().map(|v| v.map(|v| v.offset)),
            )),
            list_i32(&deltas),
            build_count_lists(&counts),
        ],
        validity(rows.iter().map(Option::is_some)),
    ))
}

fn build_descriptors(rows: &[Option<&MetricDescriptorV2>]) -> ArrayRef {
    let resource: Vec<_> = rows
        .iter()
        .map(|d| d.map(|d| d.resource_attrs.as_slice()))
        .collect();
    let scope: Vec<_> = rows
        .iter()
        .map(|d| d.map(|d| d.scope_attrs.as_slice()))
        .collect();
    Arc::new(StructArray::new(
        descriptor_fields(),
        vec![
            Arc::new(StringArray::from_iter(
                rows.iter().map(|d| d.map(|d| d.name.as_str())),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|d| d.map(|d| d.description.as_str())),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|d| d.map(|d| d.unit.as_str())),
            )),
            Arc::new(UInt8Array::from_iter(
                rows.iter().map(|d| d.map(|d| d.metric_kind)),
            )),
            Arc::new(UInt8Array::from_iter(
                rows.iter().map(|d| d.map(|d| d.temporality)),
            )),
            Arc::new(UInt8Array::from_iter(
                rows.iter().map(|d| d.map(|d| d.monotonic)),
            )),
            build_attrs(&resource),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|d| d.map(|d| d.scope_name.as_str())),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|d| d.map(|d| d.scope_version.as_str())),
            )),
            build_attrs(&scope),
        ],
        validity(rows.iter().map(Option::is_some)),
    ))
}

fn build_points(rows: &[Option<&MetricPointV2>]) -> Result<ArrayRef> {
    use scry_proto::generated::{ExponentialHistogramPointV2, HistogramPointV2, SummaryPointV2};
    let mut kinds = Vec::with_capacity(rows.len());
    let mut starts = Vec::with_capacity(rows.len());
    let mut flags = Vec::with_capacity(rows.len());
    let mut attrs = Vec::with_capacity(rows.len());
    let mut exemplars = Vec::with_capacity(rows.len());
    let mut scalars = Vec::with_capacity(rows.len());
    let mut histograms: Vec<Option<&HistogramPointV2>> = Vec::with_capacity(rows.len());
    let mut exponentials: Vec<Option<&ExponentialHistogramPointV2>> =
        Vec::with_capacity(rows.len());
    let mut summaries: Vec<Option<&SummaryPointV2>> = Vec::with_capacity(rows.len());
    for row in rows {
        match row.map(|p| &p.value) {
            Some(MetricPointV2Value::ScalarPointV2(p)) => {
                kinds.push(1);
                starts.push(p.start_unix_nano);
                flags.push(p.flags);
                attrs.push(Some(p.attributes.as_slice()));
                exemplars.push(Some(p.exemplars.as_slice()));
                scalars.push(Some(&p.number));
                histograms.push(None);
                exponentials.push(None);
                summaries.push(None);
            }
            Some(MetricPointV2Value::HistogramPointV2(p)) => {
                kinds.push(2);
                starts.push(p.start_unix_nano);
                flags.push(p.flags);
                attrs.push(Some(p.attributes.as_slice()));
                exemplars.push(Some(p.exemplars.as_slice()));
                scalars.push(None);
                histograms.push(Some(p));
                exponentials.push(None);
                summaries.push(None);
            }
            Some(MetricPointV2Value::ExponentialHistogramPointV2(p)) => {
                kinds.push(3);
                starts.push(p.start_unix_nano);
                flags.push(p.flags);
                attrs.push(Some(p.attributes.as_slice()));
                exemplars.push(Some(p.exemplars.as_slice()));
                scalars.push(None);
                histograms.push(None);
                exponentials.push(Some(p));
                summaries.push(None);
            }
            Some(MetricPointV2Value::SummaryPointV2(p)) => {
                kinds.push(4);
                starts.push(p.start_unix_nano);
                flags.push(p.flags);
                attrs.push(Some(p.attributes.as_slice()));
                exemplars.push(Some(p.exemplars.as_slice()));
                scalars.push(None);
                histograms.push(None);
                exponentials.push(None);
                summaries.push(Some(p));
            }
            None => {
                kinds.push(0);
                starts.push(0);
                flags.push(0);
                attrs.push(None);
                exemplars.push(None);
                scalars.push(None);
                histograms.push(None);
                exponentials.push(None);
                summaries.push(None);
            }
        }
    }
    let hist_bounds: Vec<_> = histograms
        .iter()
        .map(|p| p.map(|p| p.explicit_bounds.as_slice()))
        .collect();
    let hist_buckets: Vec<_> = histograms
        .iter()
        .map(|p| p.map(|p| p.bucket_counts.as_slice()))
        .collect();
    let histogram = StructArray::new(
        match &point_fields()[6].data_type() {
            DataType::Struct(f) => f.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(UInt64Array::from_iter(
                histograms.iter().map(|p| p.map(|p| p.count)),
            )),
            Arc::new(UInt8Array::from_iter(
                histograms.iter().map(|p| p.map(|p| p.has_sum)),
            )),
            Arc::new(Float64Array::from_iter(
                histograms.iter().map(|p| p.map(|p| p.sum)),
            )),
            Arc::new(UInt8Array::from_iter(
                histograms.iter().map(|p| p.map(|p| p.has_min)),
            )),
            Arc::new(Float64Array::from_iter(
                histograms.iter().map(|p| p.map(|p| p.min)),
            )),
            Arc::new(UInt8Array::from_iter(
                histograms.iter().map(|p| p.map(|p| p.has_max)),
            )),
            Arc::new(Float64Array::from_iter(
                histograms.iter().map(|p| p.map(|p| p.max)),
            )),
            list_f64(&hist_bounds),
            list_u64(&hist_buckets),
        ],
        validity(histograms.iter().map(Option::is_some)),
    );
    let exp_counts: Vec<_> = exponentials.iter().map(|p| p.map(|p| &p.count)).collect();
    let zero_counts: Vec<_> = exponentials
        .iter()
        .map(|p| p.map(|p| &p.zero_count))
        .collect();
    let positives: Vec<_> = exponentials
        .iter()
        .map(|p| p.map(|p| &p.positive))
        .collect();
    let negatives: Vec<_> = exponentials
        .iter()
        .map(|p| p.map(|p| &p.negative))
        .collect();
    let custom: Vec<_> = exponentials
        .iter()
        .map(|p| p.map(|p| p.custom_bounds.as_slice()))
        .collect();
    let exponential = StructArray::new(
        match &point_fields()[7].data_type() {
            DataType::Struct(f) => f.clone(),
            _ => unreachable!(),
        },
        vec![
            build_counts(&exp_counts),
            Arc::new(UInt8Array::from_iter(
                exponentials.iter().map(|p| p.map(|p| p.has_sum)),
            )),
            Arc::new(Float64Array::from_iter(
                exponentials.iter().map(|p| p.map(|p| p.sum)),
            )),
            Arc::new(UInt8Array::from_iter(
                exponentials.iter().map(|p| p.map(|p| p.has_min)),
            )),
            Arc::new(Float64Array::from_iter(
                exponentials.iter().map(|p| p.map(|p| p.min)),
            )),
            Arc::new(UInt8Array::from_iter(
                exponentials.iter().map(|p| p.map(|p| p.has_max)),
            )),
            Arc::new(Float64Array::from_iter(
                exponentials.iter().map(|p| p.map(|p| p.max)),
            )),
            Arc::new(Int32Array::from_iter(
                exponentials.iter().map(|p| p.map(|p| p.scale)),
            )),
            Arc::new(Float64Array::from_iter(
                exponentials.iter().map(|p| p.map(|p| p.zero_threshold)),
            )),
            build_counts(&zero_counts),
            build_sparse(&positives),
            build_sparse(&negatives),
            list_f64(&custom),
            Arc::new(UInt8Array::from_iter(
                exponentials.iter().map(|p| p.map(|p| p.reset_hint)),
            )),
        ],
        validity(exponentials.iter().map(Option::is_some)),
    );
    let quantile_rows: Vec<Option<&[scry_proto::generated::QuantileValueV2]>> = summaries
        .iter()
        .map(|p| p.map(|p| p.quantiles.as_slice()))
        .collect();
    let flat_quantiles: Vec<_> = quantile_rows
        .iter()
        .flat_map(|v| v.unwrap_or_default())
        .collect();
    let quantile_struct = StructArray::new(
        fields(vec![
            Field::new("quantile", DataType::Float64, false),
            Field::new("value", DataType::Float64, false),
        ]),
        vec![
            Arc::new(Float64Array::from_iter_values(
                flat_quantiles.iter().map(|q| q.quantile),
            )),
            Arc::new(Float64Array::from_iter_values(
                flat_quantiles.iter().map(|q| q.value),
            )),
        ],
        None,
    );
    let quantiles = primitive_list(
        &quantile_rows,
        Arc::new(Field::new(
            "item",
            quantile_struct.data_type().clone(),
            false,
        )),
        quantile_struct,
    );
    let summary = StructArray::new(
        match &point_fields()[8].data_type() {
            DataType::Struct(f) => f.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(UInt64Array::from_iter(
                summaries.iter().map(|p| p.map(|p| p.count)),
            )),
            Arc::new(Float64Array::from_iter(
                summaries.iter().map(|p| p.map(|p| p.sum)),
            )),
            quantiles,
        ],
        validity(summaries.iter().map(Option::is_some)),
    );
    Ok(Arc::new(StructArray::new(
        point_fields(),
        vec![
            Arc::new(UInt8Array::from(kinds)),
            Arc::new(UInt64Array::from(starts)),
            Arc::new(UInt32Array::from(flags)),
            build_attrs(&attrs),
            build_exemplars(&exemplars)?,
            build_numbers(&scalars),
            Arc::new(histogram),
            Arc::new(exponential),
            Arc::new(summary),
        ],
        validity(rows.iter().map(Option::is_some)),
    )))
}

/// One unique series accumulated for this block. Owned labels because
/// we dedup by fingerprint and the wire payload is dropped after
/// decode — we have to copy the bytes somewhere if we want them at
/// finish time, and the postings build later needs them as
/// `&str` for Arrow `StringArray::from_iter_values`.
struct OwnedSeries {
    fingerprint: u64,
    metric_type: u8,
    labels: Vec<(String, String)>,
}

/// In-memory metrics block under construction.
pub struct MetricsBlockBuilder {
    writer_id: Uuid,
    cfg: BlockBuilderConfig,
    // Per-sample column-shaped storage (hot path).
    fingerprints: Vec<u64>,
    ts: Vec<u64>,
    values: Vec<f64>,
    // Per-series dedup. `series_seen` cheaply rejects duplicates;
    // `series_dict` keeps them in insertion order (mostly for stable
    // postings output during tests — order doesn't matter to query
    // correctness).
    series_seen: HashSet<u64>,
    series_dict: Vec<OwnedSeries>,
    descriptors: HashMap<u32, MetricDescriptorV2>,
    v2_points: Vec<MetricRow>,
    bytes_est: u64,
    ts_min: u64,
    ts_max: u64,
}

impl MetricsBlockBuilder {
    /// Metrics parquet v3. Legacy samples populate the first three columns;
    /// v2 samples additionally carry lossless, portable nested Arrow values.
    pub fn main_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("series_fingerprint", DataType::UInt64, false),
            Field::new("ts_unix_nano", DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
            Field::new("descriptor_id", DataType::UInt32, true),
            Field::new("descriptor", DataType::Struct(descriptor_fields()), true),
            Field::new("point", DataType::Struct(point_fields()), true),
        ]))
    }

    pub fn postings_schema() -> SchemaRef {
        crate::postings::postings_schema()
    }

    pub fn row_count(&self) -> u64 {
        (self.fingerprints.len() + self.v2_points.len()) as u64
    }
}

impl BlockBuilder for MetricsBlockBuilder {
    const SIGNAL: &'static str = SIGNAL;

    fn new(writer_id: Uuid, cfg: BlockBuilderConfig) -> Self {
        Self {
            writer_id,
            cfg,
            fingerprints: Vec::with_capacity(4096),
            ts: Vec::with_capacity(4096),
            values: Vec::with_capacity(4096),
            series_seen: HashSet::with_capacity(256),
            series_dict: Vec::with_capacity(256),
            descriptors: HashMap::new(),
            v2_points: Vec::new(),
            bytes_est: 0,
            ts_min: u64::MAX,
            ts_max: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.fingerprints.is_empty() && self.v2_points.is_empty()
    }

    fn should_close(&self) -> bool {
        self.row_count() >= self.cfg.max_rows || self.bytes_est >= self.cfg.target_bytes
    }

    fn merge(&mut self, other: &mut Self) {
        // Sample columns: a bulk move each — `append` drains `other`'s
        // vec and keeps its capacity for reuse.
        self.fingerprints.append(&mut other.fingerprints);
        self.ts.append(&mut other.ts);
        self.values.append(&mut other.values);
        self.v2_points.append(&mut other.v2_points);
        self.descriptors.extend(other.descriptors.drain());

        // Series dictionary: dedup against the *shared* builder's
        // `series_seen` so cross-batch dedup scope matches decoding
        // straight into the shared builder. A fingerprint already
        // accumulated here is dropped (labels are assumed identical for
        // a given fingerprint — same trust as `observe_series`).
        for s in other.series_dict.drain(..) {
            if self.series_seen.insert(s.fingerprint) {
                self.series_dict.push(s);
            }
        }
        other.series_seen.clear();

        self.bytes_est += other.bytes_est;
        self.ts_min = self.ts_min.min(other.ts_min);
        self.ts_max = self.ts_max.max(other.ts_max);

        other.bytes_est = 0;
        other.ts_min = u64::MAX;
        other.ts_max = 0;
    }

    fn reset(&mut self) {
        self.fingerprints.clear();
        self.ts.clear();
        self.values.clear();
        self.series_seen.clear();
        self.series_dict.clear();
        self.descriptors.clear();
        self.v2_points.clear();
        self.bytes_est = 0;
        self.ts_min = u64::MAX;
        self.ts_max = 0;
    }

    fn set_compression_level(&mut self, level: i32) {
        self.cfg.compression_level = level;
    }

    fn set_wal_seg_max(&mut self, seg: u64) {
        self.cfg.wal_seg_max = Some(seg);
    }

    fn set_wal_shard(&mut self, shard: u32) {
        self.cfg.wal_shard = Some(shard);
    }

    fn finish_and_upload(
        self,
        store: &dyn ObjectStore,
    ) -> impl std::future::Future<Output = Result<Option<BlockMeta>>> + Send {
        self.finish_and_upload_impl(store)
    }
}

impl MetricsAppender for MetricsBlockBuilder {
    fn observe_series(
        &mut self,
        fingerprint: u64,
        metric_type: u8,
        labels: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        if !self.series_seen.insert(fingerprint) {
            // Already accumulated this series in an earlier batch.
            // Wire spec assumes the labels are identical across
            // batches for a given fingerprint (hash is over the
            // labels); we trust that without re-verifying.
            return;
        }
        // Convert wire bytes to UTF-8 Strings. Label keys/values are
        // strings on the wire (per `LabelPair`'s encode); if the agent
        // sent invalid UTF-8 we substitute U+FFFD rather than failing
        // the whole block. A misbehaving agent shouldn't poison
        // ingest.
        let owned: Vec<(String, String)> = labels
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(&k).into_owned(),
                    String::from_utf8_lossy(&v).into_owned(),
                )
            })
            .collect();
        self.series_dict.push(OwnedSeries {
            fingerprint,
            metric_type,
            labels: owned,
        });
    }

    fn append_sample(&mut self, fingerprint: u64, ts_unix_nano: u64, value: f64) {
        self.ts_min = self.ts_min.min(ts_unix_nano);
        self.ts_max = self.ts_max.max(ts_unix_nano);
        // Each sample is 24 bytes on disk after parquet encoding
        // (3 × 8). Real compressed size is much smaller, but the
        // estimate is for "stop accumulating" pacing, not exact
        // accounting.
        self.bytes_est += 24;
        self.fingerprints.push(fingerprint);
        self.ts.push(ts_unix_nano);
        self.values.push(value);
    }
}

impl MetricsV2Appender for MetricsBlockBuilder {
    fn descriptor(&mut self, descriptor: &MetricDescriptorV2) -> std::result::Result<(), String> {
        if self.descriptors.contains_key(&descriptor.id) {
            return Err(format!("duplicate metric descriptor {}", descriptor.id));
        }
        self.descriptors.insert(descriptor.id, descriptor.clone());
        Ok(())
    }

    fn point(&mut self, point: &MetricPointV2) -> std::result::Result<(), String> {
        let (id, ts, attrs, number) = match &point.value {
            MetricPointV2Value::ScalarPointV2(p) => (
                p.descriptor_id,
                p.ts_unix_nano,
                &p.attributes,
                Some(&p.number.value),
            ),
            MetricPointV2Value::HistogramPointV2(p) => {
                (p.descriptor_id, p.ts_unix_nano, &p.attributes, None)
            }
            MetricPointV2Value::ExponentialHistogramPointV2(p) => {
                (p.descriptor_id, p.ts_unix_nano, &p.attributes, None)
            }
            MetricPointV2Value::SummaryPointV2(p) => {
                (p.descriptor_id, p.ts_unix_nano, &p.attributes, None)
            }
        };
        let descriptor = self
            .descriptors
            .get(&id)
            .ok_or_else(|| format!("unknown metric descriptor {id}"))?;
        // Storage and live tail must identify a structured series identically.
        let (labels, fingerprint) = scry_proto::metrics_v2::canonical_series(descriptor, attrs);
        if self.series_seen.insert(fingerprint) {
            self.series_dict.push(OwnedSeries {
                fingerprint,
                metric_type: descriptor.metric_kind,
                labels: labels.into_iter().map(|p| (p.key, p.value)).collect(),
            });
        }
        let value = match number {
            Some(MetricNumberV2Value::IntegerValueV2(v)) => Some(v.value as f64),
            Some(MetricNumberV2Value::DoubleValueV2(v)) => Some(v.value),
            None => None,
        };
        self.ts_min = self.ts_min.min(ts);
        self.ts_max = self.ts_max.max(ts);
        self.bytes_est += 256;
        self.v2_points.push(MetricRow {
            fingerprint,
            ts,
            value,
            descriptor_id: Some(id),
            descriptor: Some(descriptor.clone()),
            point: Some(point.clone()),
        });
        Ok(())
    }
}

impl MetricsBlockBuilder {
    /// Body of [`BlockBuilder::finish_and_upload`]. Split out for the
    /// `mut self` rebinding ergonomic — see `dummy.rs` for the same
    /// pattern.
    async fn finish_and_upload_impl(self, store: &dyn ObjectStore) -> Result<Option<BlockMeta>> {
        if self.is_empty() {
            return Ok(None);
        }
        // Offload the CPU-heavy encode (sort + Arrow build + zstd +
        // postings) onto the blocking pool so it doesn't monopolise an
        // async worker thread; the PUTs run back here on the async side.
        let enc = tokio::task::spawn_blocking(move || self.encode())
            .await
            .context("join metrics encode task")??;
        crate::put_block_objects(store, enc.puts).await?;
        let meta = enc.meta;
        tracing::info!(
            block_uuid = %meta.uuid,
            row_count = meta.row_count,
            series_count = meta.series_types.as_ref().map_or(0, |v| v.len()),
            byte_size = meta.byte_size,
            postings_size = meta.postings_size_bytes.unwrap_or(0),
            ts_min = meta.ts_min_unix_nano,
            ts_max = meta.ts_max_unix_nano,
            "metrics block uploaded"
        );
        Ok(Some(meta))
    }

    /// Encode buffered samples into the main + postings parquet and the
    /// JSON sidecar. Pure CPU, no I/O — runs on the blocking pool via
    /// `spawn_blocking`. The async `finish_and_upload_impl` performs the
    /// PUTs.
    fn encode(mut self) -> Result<EncodedBlock> {
        let n = self.fingerprints.len() + self.v2_points.len();

        // ── Main parquet ───────────────────────────────────────────
        //
        // Sort the rows ascending by (fingerprint, ts). The intra-block
        // sort is what makes the postings index pay off at query time:
        // with sorted rows, parquet's row-group min/max stats on the
        // fingerprint column let queriers skip most groups once they've
        // resolved the fingerprint set from postings.
        //
        // We sort one *contiguous* `(fp, ts, value)` row array rather
        // than a `Vec<u32>` permutation that indexes back into three
        // separate 8 MB columns. The permutation form costs ~4 random
        // loads per comparison (fp/ts of both sides, across two arrays)
        // and three more gather passes to build the columns — all
        // cache-missing at n≈1M. Packing the row keeps every comparison
        // *and* the column build on sequential memory; sequential
        // bandwidth dwarfs random access, so the 24 MB temp pays for
        // itself many times over. (Per-block allocation, not per-record
        // — see CLAUDE.md § Performance.)
        //
        // `sort_unstable_by`: rows sharing an identical (fp, ts) are
        // interchangeable to every reader, so we skip the stable sort's
        // O(n) scratch buffer and take the faster algorithm.
        debug_assert_eq!(self.fingerprints.len(), self.ts.len());
        debug_assert_eq!(self.fingerprints.len(), self.values.len());
        let mut rows: Vec<MetricRow> = std::mem::take(&mut self.v2_points);
        rows.extend(
            self.fingerprints
                .drain(..)
                .zip(self.ts.drain(..))
                .zip(self.values.drain(..))
                .map(|((fingerprint, ts), value)| MetricRow {
                    fingerprint,
                    ts,
                    value: Some(value),
                    descriptor_id: None,
                    descriptor: None,
                    point: None,
                }),
        );
        rows.sort_unstable_by_key(|row| (row.fingerprint, row.ts));

        let main_schema = Self::main_schema();
        let fp_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|r| r.fingerprint),
        ));
        let ts_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.ts)));
        let val_arr: ArrayRef = Arc::new(Float64Array::from(
            rows.iter().map(|r| r.value).collect::<Vec<_>>(),
        ));
        let descriptor_ids: ArrayRef = Arc::new(UInt32Array::from(
            rows.iter().map(|r| r.descriptor_id).collect::<Vec<_>>(),
        ));
        let descriptor_rows: Vec<_> = rows.iter().map(|r| r.descriptor.as_ref()).collect();
        let point_rows: Vec<_> = rows.iter().map(|r| r.point.as_ref()).collect();
        let descriptors = build_descriptors(&descriptor_rows);
        let points = build_points(&point_rows).context("building metric point StructArray")?;

        let main_batch = RecordBatch::try_new(
            main_schema.clone(),
            vec![fp_arr, ts_arr, val_arr, descriptor_ids, descriptors, points],
        )
        .context("constructing metrics main RecordBatch")?;

        let props = self.cfg.main_writer_props()?;
        let mut main_buf: Vec<u8> = Vec::with_capacity(self.bytes_est as usize);
        {
            let mut w = ArrowWriter::try_new(&mut main_buf, main_schema, Some(props.clone()))
                .context("ArrowWriter::try_new (metrics main)")?;
            w.write(&main_batch)
                .context("ArrowWriter::write (metrics main)")?;
            w.close().context("ArrowWriter::close (metrics main)")?;
        }
        let main_bytes = Bytes::from(main_buf);
        let byte_size = main_bytes.len() as u64;

        // ── Postings parquet ───────────────────────────────────────
        //
        // Build the inverted index as HashMap<(name,value), Vec<u64>>
        // (cheap inserts), then sort each fingerprint vec at write
        // time and walk the outer keys in sorted order. BTreeMap would
        // give sortedness for free but is slower per insert; the
        // dominant cost here is the outer hash inserts × N_series ×
        // N_labels_per_series, not the final sort.
        //
        // TODO(v0.3): At the architecture's 60M-active-series ceiling
        // this transiently allocates ~50k (String,String) entries
        // (~2.5 MB). If real workloads ever approach that, intern
        // label names/values into a per-block dictionary and key the
        // postings map by integer IDs instead.
        let postings = self.build_postings();
        let postings_props = self.cfg.postings_writer_props()?;
        let postings_bytes = crate::postings::encode_postings(&postings, &postings_props)?;
        let postings_size = postings_bytes.len() as u64;

        // ── Sidecar JSON ───────────────────────────────────────────
        let block_uuid = Uuid::now_v7();
        let series_types: Vec<(u64, u8)> = self
            .series_dict
            .iter()
            .map(|s| (s.fingerprint, s.metric_type))
            .collect();
        // `all_fingerprints` is the signal-agnostic view of
        // `series_types`. Cheap to derive (one u64 per series) and
        // lets `scry_query::postings::resolve_fingerprints` handle
        // empty-matcher queries without a metrics-specific branch.
        let all_fingerprints: Vec<u64> = series_types.iter().map(|(fp, _)| *fp).collect();
        let meta = BlockMeta {
            uuid: block_uuid,
            signal: SIGNAL.to_string(),
            writer_id: self.writer_id,
            ts_min_unix_nano: self.ts_min,
            ts_max_unix_nano: self.ts_max,
            row_count: n as u64,
            byte_size,
            schema_version: SCHEMA_VERSION,
            level: 0,
            compacted_from: Vec::new(),
            producer_version: env!("CARGO_PKG_VERSION").to_string(),
            label_fingerprint_bloom: None,
            has_postings: true,
            postings_size_bytes: Some(postings_size),
            series_types: Some(series_types),
            all_fingerprints: Some(all_fingerprints),
            has_body_bloom: false,
            body_bloom_size_bytes: None,
            wal_seg_max: self.cfg.wal_seg_max,
            wal_shard: self.cfg.wal_shard,
        };
        let meta_bytes =
            Bytes::from(serde_json::to_vec_pretty(&meta).context("serialising metrics BlockMeta")?);

        // ── Upload order: main → postings → meta ───────────────────
        //
        // Same ordering invariant as dummy: the meta.json sidecar is
        // the "block exists" signal for catalog reconcile. If we
        // crash after main+postings but before meta, the orphaned
        // parquets stay until retention; if we crash after main but
        // before postings, reconcile sees no meta and skips them.
        // The only durable persistence ordering that matters is
        // "meta last."
        let main_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "parquet",
        ));
        let postings_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "postings.parquet",
        ));
        let meta_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "meta.json",
        ));

        Ok(EncodedBlock {
            meta,
            puts: vec![
                (main_path, main_bytes),
                (postings_path, postings_bytes),
                (meta_path, meta_bytes),
            ],
        })
    }

    /// Walk the series dictionary, building
    /// `Vec<((name, value), sorted fingerprints)>` keyed in lexicographic
    /// `(name, value)` order for the postings parquet.
    fn build_postings(&self) -> Vec<((String, String), Vec<u64>)> {
        use std::collections::HashMap;
        let mut inv: HashMap<(String, String), Vec<u64>> = HashMap::new();
        for series in &self.series_dict {
            for (k, v) in &series.labels {
                inv.entry((k.clone(), v.clone()))
                    .or_default()
                    .push(series.fingerprint);
            }
        }
        let mut entries: Vec<((String, String), Vec<u64>)> = inv.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, fps) in entries.iter_mut() {
            fps.sort_unstable();
            fps.dedup();
        }
        entries
    }
}
