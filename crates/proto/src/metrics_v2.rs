//! Canonical structured-metric semantics for ingest protocol v2.
//!
//! The wire currently uses one tagged point array rather than the approved
//! separate typed arrays. This keeps the initial wire compatible; validation
//! enforces the same descriptor/point kind separation.

use crate::generated::{
    MetricCountV2, MetricCountV2Value, MetricDescriptorV2, MetricExemplarV2, MetricPointV2,
    MetricPointV2Value, MetricsBatchV2,
};
use std::collections::HashMap;
use thiserror::Error;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Gauge = 1,
    Sum = 2,
    Histogram = 3,
    ExponentialHistogram = 4,
    Summary = 5,
}
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Temporality {
    Unspecified = 0,
    Delta = 1,
    Cumulative = 2,
}
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetHint {
    Unknown = 0,
    Yes = 1,
    No = 2,
    Gauge = 3,
}

pub const FLAG_NO_RECORDED_VALUE: u32 = 1;

#[derive(Debug, Error, PartialEq)]
pub enum ValidationError {
    #[error("duplicate descriptor id {0}")]
    DuplicateDescriptor(u32),
    #[error("point references unknown descriptor id {0}")]
    UnknownDescriptor(u32),
    #[error("descriptor {0} has invalid kind/temporality/monotonic metadata")]
    BadDescriptor(u32),
    #[error("point kind does not match descriptor {0}")]
    KindMismatch(u32),
    #[error("histogram bucket shape/count is inconsistent")]
    HistogramShape,
    #[error("bounds/quantiles must be finite and strictly increasing")]
    Unsorted,
    #[error("invalid numeric value")]
    BadNumber,
    #[error("optional presence bit is not zero or one")]
    BadPresence,
    #[error("sparse bucket delta/count lengths or indices are invalid")]
    SparseShape,
    #[error("invalid exponential histogram reset hint")]
    BadExponential,
    #[error("invalid point or exemplar timestamp")]
    BadTimestamp,
    #[error("invalid exemplar trace/span identifiers")]
    BadExemplarId,
    #[error("a length cannot be represented by its wire prefix")]
    LengthOverflow,
}

pub fn validate(batch: &MetricsBatchV2) -> Result<(), ValidationError> {
    if batch.magic != crate::constants::METRICS_BATCH_V2_MAGIC {
        return Err(ValidationError::BadDescriptor(0));
    }
    validate_lengths(batch)?;
    let mut descriptors = HashMap::with_capacity(batch.descriptors.len());
    for d in &batch.descriptors {
        if descriptors.insert(d.id, d).is_some() {
            return Err(ValidationError::DuplicateDescriptor(d.id));
        }
        validate_descriptor(d)?;
    }
    for p in &batch.points {
        validate_point(p, &descriptors)?;
    }
    Ok(())
}

/// Validate all length-prefixed fields before generated encoders perform `as` casts.
pub fn validate_for_encode(batch: &MetricsBatchV2) -> Result<(), ValidationError> {
    validate(batch)
}

fn validate_descriptor(d: &MetricDescriptorV2) -> Result<(), ValidationError> {
    if !(1..=5).contains(&d.metric_kind)
        || d.temporality > 2
        || d.monotonic > 1
        || (d.metric_kind != MetricKind::Sum as u8 && d.monotonic != 0)
    {
        Err(ValidationError::BadDescriptor(d.id))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_point<'a>(
    p: &MetricPointV2,
    descriptors: &HashMap<u32, &'a MetricDescriptorV2>,
) -> Result<(), ValidationError> {
    let (id, start, ts, attrs, exemplars, expected) = match &p.value {
        MetricPointV2Value::ScalarPointV2(v) => {
            // IEEE-754 NaN/Inf (including Prometheus stale NaN) are data, not
            // malformed input. Preserve their bits through the canonical f64.
            (
                v.descriptor_id,
                v.start_unix_nano,
                v.ts_unix_nano,
                &v.attributes,
                &v.exemplars,
                0,
            )
        }
        MetricPointV2Value::HistogramPointV2(v) => {
            presence(v.has_sum, v.sum)?;
            presence(v.has_min, v.min)?;
            presence(v.has_max, v.max)?;
            if v.bucket_counts.len()
                != v.explicit_bounds
                    .len()
                    .checked_add(1)
                    .ok_or(ValidationError::HistogramShape)?
                || v.bucket_counts
                    .iter()
                    .try_fold(0u64, |a, x| a.checked_add(*x))
                    .ok_or(ValidationError::HistogramShape)?
                    != v.count
            {
                return Err(ValidationError::HistogramShape);
            }
            increasing(&v.explicit_bounds)?;
            (
                v.descriptor_id,
                v.start_unix_nano,
                v.ts_unix_nano,
                &v.attributes,
                &v.exemplars,
                MetricKind::Histogram as u8,
            )
        }
        MetricPointV2Value::ExponentialHistogramPointV2(v) => {
            presence(v.has_sum, v.sum)?;
            presence(v.has_min, v.min)?;
            presence(v.has_max, v.max)?;
            if v.reset_hint > 3 || !v.zero_threshold.is_finite() || v.zero_threshold < 0.0 {
                return Err(ValidationError::BadExponential);
            }
            finite_count(&v.count)?;
            finite_count(&v.zero_count)?;
            let expected_count = count_value(&v.count)?;
            let mut total = count_value(&v.zero_count)?;
            for b in [&v.positive, &v.negative] {
                if b.deltas.len() != b.counts.len() {
                    return Err(ValidationError::SparseShape);
                }
                let mut index = i64::from(b.offset);
                for (delta, c) in b.deltas.iter().zip(&b.counts) {
                    index = index
                        .checked_add(i64::from(*delta))
                        .ok_or(ValidationError::SparseShape)?;
                    if index < i64::from(i32::MIN) || index > i64::from(i32::MAX) {
                        return Err(ValidationError::SparseShape);
                    }
                    total.add(count_value(c)?)?;
                }
            }
            if !total.matches(&expected_count) {
                return Err(ValidationError::HistogramShape);
            }
            increasing(&v.custom_bounds)?;
            (
                v.descriptor_id,
                v.start_unix_nano,
                v.ts_unix_nano,
                &v.attributes,
                &v.exemplars,
                MetricKind::ExponentialHistogram as u8,
            )
        }
        MetricPointV2Value::SummaryPointV2(v) => {
            if !v.sum.is_finite() {
                return Err(ValidationError::BadNumber);
            }
            let mut last = -1.0;
            for q in &v.quantiles {
                if !q.quantile.is_finite()
                    || !(0.0..=1.0).contains(&q.quantile)
                    || q.quantile <= last
                    || !q.value.is_finite()
                {
                    return Err(ValidationError::Unsorted);
                }
                last = q.quantile;
            }
            (
                v.descriptor_id,
                v.start_unix_nano,
                v.ts_unix_nano,
                &v.attributes,
                &v.exemplars,
                MetricKind::Summary as u8,
            )
        }
    };
    let d = descriptors
        .get(&id)
        .ok_or(ValidationError::UnknownDescriptor(id))?;
    if expected == 0 {
        if d.metric_kind != MetricKind::Gauge as u8 && d.metric_kind != MetricKind::Sum as u8 {
            return Err(ValidationError::KindMismatch(id));
        }
    } else if d.metric_kind != expected {
        return Err(ValidationError::KindMismatch(id));
    }
    // OTLP strongly recommends a start time for cumulative streams but permits
    // zero (absent), and remote-write v1 has no start timestamp at all.
    if ts == 0 || (start != 0 && start > ts) {
        return Err(ValidationError::BadTimestamp);
    }
    let _ = attrs;
    for e in exemplars {
        validate_exemplar(e, ts)?;
    }
    Ok(())
}

fn presence(bit: u8, value: f64) -> Result<(), ValidationError> {
    if bit > 1 {
        Err(ValidationError::BadPresence)
    } else if bit == 1 && !value.is_finite() {
        Err(ValidationError::BadNumber)
    } else {
        Ok(())
    }
}
fn validate_exemplar(e: &MetricExemplarV2, point_ts: u64) -> Result<(), ValidationError> {
    if e.ts_unix_nano == 0 || e.ts_unix_nano > point_ts {
        return Err(ValidationError::BadTimestamp);
    }
    let trace = e.trace_id.iter().any(|x| *x != 0);
    let span = e.span_id.iter().any(|x| *x != 0);
    if trace != span {
        Err(ValidationError::BadExemplarId)
    } else {
        Ok(())
    }
}
fn increasing(xs: &[f64]) -> Result<(), ValidationError> {
    if xs.iter().any(|x| !x.is_finite()) || xs.windows(2).any(|w| w[0] >= w[1]) {
        Err(ValidationError::Unsorted)
    } else {
        Ok(())
    }
}
fn finite_count(v: &MetricCountV2) -> Result<(), ValidationError> {
    count_value(v).map(|_| ())
}
enum CountValue {
    Integer(u64),
    Float(f64),
}

impl CountValue {
    fn add(&mut self, other: Self) -> Result<(), ValidationError> {
        match (self, other) {
            (Self::Integer(total), Self::Integer(value)) => {
                *total = total
                    .checked_add(value)
                    .ok_or(ValidationError::HistogramShape)?;
                Ok(())
            }
            (Self::Float(total), Self::Float(value)) => {
                *total += value;
                if total.is_finite() {
                    Ok(())
                } else {
                    Err(ValidationError::HistogramShape)
                }
            }
            _ => Err(ValidationError::HistogramShape),
        }
    }

    fn matches(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => left == right,
            (Self::Float(left), Self::Float(right)) => {
                let tolerance = f64::EPSILON * left.abs().max(right.abs()).max(1.0) * 8.0;
                (left - right).abs() <= tolerance
            }
            _ => false,
        }
    }
}

fn count_value(v: &MetricCountV2) -> Result<CountValue, ValidationError> {
    match &v.value {
        MetricCountV2Value::IntegerCountV2(x) => Ok(CountValue::Integer(x.value)),
        MetricCountV2Value::FloatCountV2(x) if x.value.is_finite() && x.value >= 0.0 => {
            Ok(CountValue::Float(x.value))
        }
        _ => Err(ValidationError::BadNumber),
    }
}
fn validate_lengths(batch: &MetricsBatchV2) -> Result<(), ValidationError> {
    if batch.descriptors.len() > u32::MAX as usize || batch.points.len() > u32::MAX as usize {
        return Err(ValidationError::LengthOverflow);
    }
    let labels = |xs: &Vec<crate::generated::LabelPair>| -> Result<(), ValidationError> {
        if xs.len() > u16::MAX as usize
            || xs
                .iter()
                .any(|x| x.key.len() > u16::MAX as usize || x.value.len() > u16::MAX as usize)
        {
            Err(ValidationError::LengthOverflow)
        } else {
            Ok(())
        }
    };
    for d in &batch.descriptors {
        if d.name.len() > u16::MAX as usize
            || d.description.len() > u16::MAX as usize
            || d.unit.len() > u16::MAX as usize
            || d.scope_name.len() > u8::MAX as usize
            || d.scope_version.len() > u8::MAX as usize
        {
            return Err(ValidationError::LengthOverflow);
        }
        labels(&d.resource_attrs)?;
        labels(&d.scope_attrs)?;
    }
    for p in &batch.points {
        let (a, e) = match &p.value {
            MetricPointV2Value::ScalarPointV2(v) => (&v.attributes, &v.exemplars),
            MetricPointV2Value::HistogramPointV2(v) => (&v.attributes, &v.exemplars),
            MetricPointV2Value::ExponentialHistogramPointV2(v) => (&v.attributes, &v.exemplars),
            MetricPointV2Value::SummaryPointV2(v) => (&v.attributes, &v.exemplars),
        };
        labels(a)?;
        if e.len() > u16::MAX as usize {
            return Err(ValidationError::LengthOverflow);
        }
        for x in e {
            labels(&x.filtered_attrs)?
        }
    }
    Ok(())
}
