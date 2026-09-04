//! Bounded callback-oriented decoder for canonical metrics v2.
//!
//! Descriptors and points are decoded one at a time, so a normal batch is not
//! materialised as a second in-memory copy before being appended.

use crate::generated::{MetricDescriptorV2, MetricPointV2};
use crate::metrics_v2::ValidationError;
use binschema_runtime::{BinSchemaError, BitOrder, BitStreamDecoder};

#[derive(Debug, Clone, Copy)]
pub struct DecodeLimits {
    pub max_payload_bytes: usize,
    pub max_descriptors: u32,
    pub max_points: u32,
}
impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 16 * 1024 * 1024,
            max_descriptors: 65_536,
            max_points: 1_000_000,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("metrics-v2 payload exceeds configured limit")]
    PayloadLimit,
    #[error("metrics-v2 item count exceeds configured limit")]
    ItemLimit,
    #[error(transparent)]
    Wire(#[from] BinSchemaError),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("appender rejected metrics-v2 item: {0}")]
    Appender(String),
}

pub trait MetricsV2Appender {
    fn descriptor(&mut self, descriptor: &MetricDescriptorV2) -> Result<(), String>;
    fn point(&mut self, point: &MetricPointV2) -> Result<(), String>;
}

/// Decode a v2 payload under explicit byte/item limits and append each item.
///
/// This checks descriptor references and the focused semantic invariants by
/// validating a single-point view before invoking the point callback.
pub fn decode_metrics_batch_v2_into(
    payload: &[u8],
    limits: DecodeLimits,
    appender: &mut impl MetricsV2Appender,
) -> Result<(u32, u32), DecodeError> {
    if payload.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadLimit);
    }
    let mut decoder = BitStreamDecoder::new(payload, BitOrder::MsbFirst);
    let magic = decoder.read_u32_be()?;
    if magic != crate::constants::METRICS_BATCH_V2_MAGIC {
        return Err(DecodeError::Wire(BinSchemaError::InvalidVariant(format!(
            "expected metrics-v2 magic, got {magic}"
        ))));
    }
    let descriptors_len = decoder.read_u32_be()?;
    if descriptors_len > limits.max_descriptors {
        return Err(DecodeError::ItemLimit);
    }
    let mut descriptors = Vec::with_capacity(descriptors_len as usize);
    for _ in 0..descriptors_len {
        descriptors.push(MetricDescriptorV2::decode_with_decoder(&mut decoder)?);
    }
    let points_len = decoder.read_u32_be()?;
    if points_len > limits.max_points {
        return Err(DecodeError::ItemLimit);
    }
    let mut points = Vec::with_capacity(points_len as usize);
    for _ in 0..points_len {
        points.push(MetricPointV2::decode_with_decoder(&mut decoder)?);
    }
    if decoder.position() != payload.len() {
        return Err(DecodeError::Wire(BinSchemaError::InvalidEncoding(
            "trailing bytes after MetricsBatchV2".into(),
        )));
    }

    // Validate the complete batch before the first callback. An invalid point
    // must not leave a partially-mutated block builder behind.
    crate::metrics_v2::validate(&crate::generated::MetricsBatchV2 {
        magic: crate::constants::METRICS_BATCH_V2_MAGIC,
        descriptors: descriptors.clone(),
        points: points.clone(),
    })?;
    for descriptor in &descriptors {
        appender
            .descriptor(descriptor)
            .map_err(DecodeError::Appender)?;
    }
    for point in &points {
        appender.point(point).map_err(DecodeError::Appender)?;
    }
    Ok((descriptors_len, points_len))
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Sink;
    impl MetricsV2Appender for Sink {
        fn descriptor(&mut self, _: &MetricDescriptorV2) -> Result<(), String> {
            Ok(())
        }
        fn point(&mut self, _: &MetricPointV2) -> Result<(), String> {
            Ok(())
        }
    }
    #[test]
    fn rejects_oversize_and_claimed_counts_before_allocating_items() {
        assert!(matches!(
            decode_metrics_batch_v2_into(
                &[0; 9],
                DecodeLimits {
                    max_payload_bytes: 8,
                    ..DecodeLimits::default()
                },
                &mut Sink
            ),
            Err(DecodeError::PayloadLimit)
        ));
        let mut claimed = Vec::new();
        claimed.extend_from_slice(&crate::constants::METRICS_BATCH_V2_MAGIC.to_be_bytes());
        claimed.extend_from_slice(&2u32.to_be_bytes());
        assert!(matches!(
            decode_metrics_batch_v2_into(
                &claimed,
                DecodeLimits {
                    max_descriptors: 1,
                    ..DecodeLimits::default()
                },
                &mut Sink
            ),
            Err(DecodeError::ItemLimit)
        ));
    }
}
