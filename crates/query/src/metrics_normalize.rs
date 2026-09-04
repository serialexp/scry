//! Physical metrics schema versions and conversion to the current nested v3 schema.
use anyhow::Result;
use arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryBuilder, Float64Array, Int32Array, Int64Array,
    ListArray, StringArray, StructArray, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use scry_proto::generated::{
    LabelPair, MetricCountV2, MetricCountV2Value, MetricDescriptorV2, MetricExemplarV2,
    MetricNumberV2, MetricNumberV2Value, MetricPointV2, MetricPointV2Value, SparseBucketsV2,
};
use std::{any::Any, sync::Arc};

pub(crate) fn physical_schema(version: u32) -> DfResult<SchemaRef> {
    match version {
        1 => Ok(Arc::new(Schema::new(vec![
            Field::new("series_fingerprint", DataType::UInt64, false),
            Field::new("ts_unix_nano", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ]))),
        2 => Ok(Arc::new(Schema::new(vec![
            Field::new("series_fingerprint", DataType::UInt64, false),
            Field::new("ts_unix_nano", DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
            Field::new("descriptor_id", DataType::UInt32, true),
            Field::new("metric_kind", DataType::UInt8, true),
            Field::new("scalar_i64", DataType::Int64, true),
            Field::new("scalar_f64", DataType::Float64, true),
            Field::new("descriptor", DataType::Binary, true),
            Field::new("point", DataType::Binary, true),
        ]))),
        3 => Ok(scry_block::MetricsBlockBuilder::main_schema()),
        _ => Err(DataFusionError::Plan(format!(
            "unsupported metrics block schema version {version}"
        ))),
    }
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
pub fn metric_point_array(rows: &[Option<&MetricPointV2>]) -> Result<ArrayRef> {
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

fn metric_descriptor_array(rows: &[Option<&MetricDescriptorV2>]) -> ArrayRef {
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

fn normalize_batch(batch: &RecordBatch, version: u32, schema: &SchemaRef) -> DfResult<RecordBatch> {
    if version == 3 {
        return Ok(batch.clone());
    }
    let n = batch.num_rows();
    let mut columns = batch.columns()[..3].to_vec();
    if version == 1 {
        columns.push(arrow::array::new_null_array(schema.field(3).data_type(), n));
        columns.push(arrow::array::new_null_array(schema.field(4).data_type(), n));
        columns.push(arrow::array::new_null_array(schema.field(5).data_type(), n));
    } else {
        columns.push(batch.column(3).clone());
        let descriptors = batch
            .column(7)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("metrics v2 descriptor is not Binary".into())
            })?;
        let points = batch
            .column(8)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Execution("metrics v2 point is not Binary".into()))?;
        let decoded_descriptors: Vec<Option<MetricDescriptorV2>> = (0..n)
            .map(|i| {
                if descriptors.is_null(i) {
                    Ok(None)
                } else {
                    MetricDescriptorV2::decode(descriptors.value(i)).map(Some)
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                DataFusionError::Execution(format!("invalid metrics v2 descriptor: {e}"))
            })?;
        let decoded_points: Vec<Option<MetricPointV2>> = (0..n)
            .map(|i| {
                if points.is_null(i) {
                    Ok(None)
                } else {
                    MetricPointV2::decode(points.value(i)).map(Some)
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::Execution(format!("invalid metrics v2 point: {e}")))?;
        columns.push(metric_descriptor_array(
            &decoded_descriptors
                .iter()
                .map(Option::as_ref)
                .collect::<Vec<_>>(),
        ));
        columns.push(
            metric_point_array(
                &decoded_points
                    .iter()
                    .map(Option::as_ref)
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| {
                DataFusionError::Execution(format!("normalizing metrics v2 point: {e:#}"))
            })?,
        );
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(Into::into)
}

pub(crate) struct MetricsNormalizeExec {
    input: Arc<dyn ExecutionPlan>,
    version: u32,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}
impl MetricsNormalizeExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, version: u32) -> Self {
        let schema = scry_block::MetricsBlockBuilder::main_schema();
        let child = input.properties();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            child.partitioning.clone(),
            child.emission_type,
            child.boundedness,
        ));
        Self {
            input,
            version,
            schema,
            props,
        }
    }
}
impl std::fmt::Debug for MetricsNormalizeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsNormalizeExec")
            .field("version", &self.version)
            .finish()
    }
}
impl DisplayAs for MetricsNormalizeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetricsNormalizeExec: v{} -> v3", self.version)
    }
}
impl ExecutionPlan for MetricsNormalizeExec {
    fn name(&self) -> &str {
        "MetricsNormalizeExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "MetricsNormalizeExec expects one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(children.remove(0), self.version)))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let version = self.version;
        let stream = self
            .input
            .execute(partition, context)?
            .map(move |b| normalize_batch(&b?, version, &schema));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
