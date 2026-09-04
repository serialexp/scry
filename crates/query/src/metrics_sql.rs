//! Native SQL accessors for the nested metrics v3 Arrow columns.
//!
//! These deliberately inspect Arrow structs instead of decoding the legacy v2
//! binary representation. Consequently a v1/v2 row (whose normalized `point`
//! or `descriptor` is null) naturally produces null.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, ListArray, StructArray, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    create_udf, ColumnarValue, ScalarFunctionImplementation, Volatility,
};
use datafusion::prelude::SessionContext;

use crate::metrics_normalize::{descriptor_fields, point_fields};

const NO_RECORDED_VALUE: u32 = scry_proto::metrics_v2::FLAG_NO_RECORDED_VALUE;

fn malformed(what: &str) -> DataFusionError {
    DataFusionError::Execution(format!("malformed structured metric array: {what}"))
}

fn structs<'a>(a: &'a ArrayRef, what: &str) -> Result<&'a StructArray> {
    a.as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| malformed(what))
}
fn child<'a, T: Array + 'static>(s: &'a StructArray, name: &str) -> Result<&'a T> {
    s.column_by_name(name)
        .ok_or_else(|| malformed(&format!("missing {name}")))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| malformed(&format!("bad type for {name}")))
}
fn arrays(args: &[ColumnarValue], n: usize) -> Result<Vec<ArrayRef>> {
    if args.len() != n {
        return Err(DataFusionError::Execution(format!(
            "expected {n} arguments"
        )));
    }
    ColumnarValue::values_to_arrays(args)
}
fn point_valid(p: &StructArray, flags: &UInt32Array, row: usize) -> bool {
    !p.is_null(row) && !flags.is_null(row) && flags.value(row) & NO_RECORDED_VALUE == 0
}

fn unary_point<T, F>(args: &[ColumnarValue], f: F) -> Result<ColumnarValue>
where
    T: arrow::array::ArrowPrimitiveType,
    arrow::array::PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    F: FnOnce(&StructArray, &UInt8Array, &UInt32Array) -> Result<Vec<Option<T::Native>>>,
{
    let a = arrays(args, 1)?;
    let p = structs(&a[0], "point must be a struct")?;
    let kind = child::<UInt8Array>(p, "kind")?;
    let flags = child::<UInt32Array>(p, "flags")?;
    Ok(ColumnarValue::Array(Arc::new(
        arrow::array::PrimitiveArray::<T>::from(f(p, kind, flags)?),
    )))
}

fn metric_number_arm<T, F>(args: &[ColumnarValue], expected: u8, values: F) -> Result<ColumnarValue>
where
    T: arrow::array::ArrowPrimitiveType,
    arrow::array::PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    F: Fn(&StructArray) -> Result<&arrow::array::PrimitiveArray<T>>,
{
    unary_point::<T, _>(args, |p, kind, flags| {
        let scalar = child::<StructArray>(p, "scalar")?;
        let number_kind = child::<UInt8Array>(scalar, "kind")?;
        let values = values(scalar)?;
        Ok((0..p.len())
            .map(|row| {
                (point_valid(p, flags, row)
                    && kind.value(row) == 1
                    && !scalar.is_null(row)
                    && number_kind.value(row) == expected
                    && !values.is_null(row))
                .then(|| values.value(row))
            })
            .collect())
    })
}

fn metric_integer(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    metric_number_arm::<arrow::datatypes::Int64Type, _>(args, 1, |scalar| {
        child::<arrow::array::Int64Array>(scalar, "integer")
    })
}

fn metric_float(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    metric_number_arm::<arrow::datatypes::Float64Type, _>(args, 2, |scalar| {
        child::<Float64Array>(scalar, "float")
    })
}

/// Compatibility projection for callers that explicitly accept i64-to-f64
/// conversion. Prefer `metric_integer` or `metric_float` for exact access.
fn metric_number(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    unary_point::<arrow::datatypes::Float64Type, _>(args, |p, kind, flags| {
        let scalar = child::<StructArray>(p, "scalar")?;
        let nk = child::<UInt8Array>(scalar, "kind")?;
        let ints = child::<arrow::array::Int64Array>(scalar, "integer")?;
        let floats = child::<Float64Array>(scalar, "float")?;
        (0..p.len())
            .map(|i| {
                if !point_valid(p, flags, i) || kind.value(i) != 1 || scalar.is_null(i) {
                    return Ok(None);
                }
                match nk.value(i) {
                    1 if !ints.is_null(i) => Ok(Some(ints.value(i) as f64)),
                    2 if !floats.is_null(i) => Ok(Some(floats.value(i))),
                    1 | 2 => Err(malformed("scalar number arm is null")),
                    _ => Err(malformed("invalid scalar number kind")),
                }
            })
            .collect()
    })
}

fn metric_count(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    unary_point::<arrow::datatypes::UInt64Type, _>(args, |p, kind, flags| {
        let hist = child::<StructArray>(p, "histogram")?;
        let exp = child::<StructArray>(p, "exponential_histogram")?;
        let summary = child::<StructArray>(p, "summary")?;
        let hc = child::<UInt64Array>(hist, "count")?;
        let ec = child::<StructArray>(exp, "count")?;
        let eck = child::<UInt8Array>(ec, "kind")?;
        let ecu = child::<UInt64Array>(ec, "integer")?;
        let sumc = child::<UInt64Array>(summary, "count")?;
        (0..p.len())
            .map(|i| {
                if !point_valid(p, flags, i) {
                    return Ok(None);
                }
                match kind.value(i) {
                    2 if !hist.is_null(i) => Ok(Some(hc.value(i))),
                    3 if !exp.is_null(i) && eck.value(i) == 1 && !ecu.is_null(i) => {
                        Ok(Some(ecu.value(i)))
                    }
                    4 if !summary.is_null(i) => Ok(Some(sumc.value(i))),
                    1..=4 => Ok(None),
                    _ => Err(malformed("invalid point kind")),
                }
            })
            .collect()
    })
}

fn metric_sum(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    unary_point::<arrow::datatypes::Float64Type, _>(args, |p, kind, flags| {
        let hist = child::<StructArray>(p, "histogram")?;
        let exp = child::<StructArray>(p, "exponential_histogram")?;
        let summary = child::<StructArray>(p, "summary")?;
        let hh = child::<UInt8Array>(hist, "has_sum")?;
        let hs = child::<Float64Array>(hist, "sum")?;
        let eh = child::<UInt8Array>(exp, "has_sum")?;
        let es = child::<Float64Array>(exp, "sum")?;
        let ss = child::<Float64Array>(summary, "sum")?;
        (0..p.len())
            .map(|i| {
                if !point_valid(p, flags, i) {
                    return Ok(None);
                }
                match kind.value(i) {
                    2 if !hist.is_null(i) => Ok((hh.value(i) == 1).then(|| hs.value(i))),
                    3 if !exp.is_null(i) => Ok((eh.value(i) == 1).then(|| es.value(i))),
                    4 if !summary.is_null(i) => Ok(Some(ss.value(i))),
                    1..=4 => Ok(None),
                    _ => Err(malformed("invalid point kind")),
                }
            })
            .collect()
    })
}

fn point_u64(args: &[ColumnarValue], name: &str) -> Result<ColumnarValue> {
    unary_point::<arrow::datatypes::UInt64Type, _>(args, |p, _, flags| {
        let v = child::<UInt64Array>(p, name)?;
        Ok((0..p.len())
            .map(|i| point_valid(p, flags, i).then(|| v.value(i)))
            .collect())
    })
}
fn point_flags(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    unary_point::<arrow::datatypes::UInt32Type, _>(args, |p, _, flags| {
        Ok((0..p.len())
            .map(|i| (!p.is_null(i)).then(|| flags.value(i)))
            .collect())
    })
}
fn descriptor_u8(args: &[ColumnarValue], name: &str) -> Result<ColumnarValue> {
    let a = arrays(args, 1)?;
    let d = structs(&a[0], "descriptor must be a struct")?;
    let v = child::<UInt8Array>(d, name)?;
    Ok(ColumnarValue::Array(Arc::new(UInt8Array::from(
        (0..d.len())
            .map(|i| (!d.is_null(i)).then(|| v.value(i)))
            .collect::<Vec<_>>(),
    ))))
}
fn metric_quantile(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let a = arrays(args, 2)?;
    let p = structs(&a[0], "point must be a struct")?;
    let requested = a[1]
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| malformed("quantile must be float64"))?;
    let kind = child::<UInt8Array>(p, "kind")?;
    let flags = child::<UInt32Array>(p, "flags")?;
    let summary = child::<StructArray>(p, "summary")?;
    let lists = child::<ListArray>(summary, "quantiles")?;
    let values = structs(lists.values(), "quantile items must be structs")?;
    let qs = child::<Float64Array>(values, "quantile")?;
    let vs = child::<Float64Array>(values, "value")?;
    let out = (0..p.len())
        .map(|i| {
            if requested.is_null(i)
                || !point_valid(p, flags, i)
                || kind.value(i) != 4
                || summary.is_null(i)
            {
                return None;
            }
            let want = requested.value(i);
            let range = lists.value_offsets()[i] as usize..lists.value_offsets()[i + 1] as usize;
            range
                .into_iter()
                .find(|&j| qs.value(j).to_bits() == want.to_bits())
                .map(|j| vs.value(j))
        })
        .collect::<Vec<_>>();
    Ok(ColumnarValue::Array(Arc::new(Float64Array::from(out))))
}

fn udf(
    name: &str,
    inputs: Vec<DataType>,
    output: DataType,
    f: impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync + 'static,
) -> datafusion::logical_expr::ScalarUDF {
    create_udf(
        name,
        inputs,
        output,
        Volatility::Immutable,
        Arc::new(f) as ScalarFunctionImplementation,
    )
}

/// Register all structured-metric accessors on a query context.
pub fn register_metrics_udfs(ctx: &SessionContext) {
    let point = DataType::Struct(point_fields());
    let descriptor = DataType::Struct(descriptor_fields());
    ctx.register_udf(udf(
        "metric_integer",
        vec![point.clone()],
        DataType::Int64,
        metric_integer,
    ));
    ctx.register_udf(udf(
        "metric_float",
        vec![point.clone()],
        DataType::Float64,
        metric_float,
    ));
    ctx.register_udf(udf(
        "metric_number",
        vec![point.clone()],
        DataType::Float64,
        metric_number,
    ));
    ctx.register_udf(udf(
        "metric_count",
        vec![point.clone()],
        DataType::UInt64,
        metric_count,
    ));
    ctx.register_udf(udf(
        "metric_sum",
        vec![point.clone()],
        DataType::Float64,
        metric_sum,
    ));
    ctx.register_udf(udf(
        "metric_start_time",
        vec![point.clone()],
        DataType::UInt64,
        |a| point_u64(a, "start_unix_nano"),
    ));
    ctx.register_udf(udf(
        "metric_flags",
        vec![point.clone()],
        DataType::UInt32,
        point_flags,
    ));
    ctx.register_udf(udf(
        "metric_kind",
        vec![descriptor.clone()],
        DataType::UInt8,
        |a| descriptor_u8(a, "metric_kind"),
    ));
    ctx.register_udf(udf(
        "metric_temporality",
        vec![descriptor.clone()],
        DataType::UInt8,
        |a| descriptor_u8(a, "temporality"),
    ));
    ctx.register_udf(udf(
        "metric_monotonic",
        vec![descriptor],
        DataType::UInt8,
        |a| descriptor_u8(a, "monotonic"),
    ));
    ctx.register_udf(udf(
        "metric_quantile",
        vec![point, DataType::Float64],
        DataType::Float64,
        metric_quantile,
    ));
}
