//! Decode-function adapters between `scry_proto::streaming` (which
//! returns the proto crate's `BinSchemaError`) and the pipeline's
//! `DecodeFn` shape (`fn(&[u8], &mut B) -> anyhow::Result<usize>`).
//!
//! Lives in `scry-server` rather than `scry-proto` because the binding
//! between a specific wire decoder and a specific [`scry_block`]
//! builder is a server-layer concern. The proto crate stays unaware of
//! the block crate.
//!
//! Each adapter is a `fn` item (not a closure) so it coerces directly
//! to the pipeline's `DecodeFn<B>` function-pointer type.

use anyhow::Result;
use scry_block::{DummyBlockBuilder, LogsBlockBuilder, MetricsBlockBuilder};
use scry_proto::streaming;

/// Adapter for `decode_dummy_batch_into`, wired to [`DummyBlockBuilder`].
pub fn dummy(payload: &[u8], builder: &mut DummyBlockBuilder) -> Result<usize> {
    streaming::decode_dummy_batch_into(payload, builder)
        .map_err(|e| anyhow::anyhow!("DummyBatch: {e}"))
}

/// Adapter for `decode_metrics_batch_into`, wired to
/// [`MetricsBlockBuilder`]. The streaming decoder returns
/// `(series_count, sample_count)`; the pipeline only cares about
/// samples (series are a dictionary, not records), so we discard the
/// series count here. The handler's connection-summary counter does
/// the same thing.
pub fn metrics(payload: &[u8], builder: &mut MetricsBlockBuilder) -> Result<usize> {
    streaming::decode_metrics_batch_into(payload, builder)
        .map(|(_series, samples)| samples)
        .map_err(|e| anyhow::anyhow!("MetricsBatch: {e}"))
}

/// Adapter for `decode_logs_batch_into`, wired to
/// [`LogsBlockBuilder`]. The streaming decoder returns the total
/// entry count (streams are a dictionary, not records); the
/// pipeline records that directly.
pub fn logs(payload: &[u8], builder: &mut LogsBlockBuilder) -> Result<usize> {
    streaming::decode_logs_batch_into(payload, builder)
        .map_err(|e| anyhow::anyhow!("LogsBatch: {e}"))
}
