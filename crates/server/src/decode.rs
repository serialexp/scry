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
use scry_block::{
    DummyBlockBuilder, LogsBlockBuilder, MetricsBlockBuilder, ProfilesBlockBuilder,
    TracesBlockBuilder,
};
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
    let is_v2 = payload
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_be_bytes)
        == Some(scry_proto::constants::METRICS_BATCH_V2_MAGIC);
    if is_v2 {
        scry_proto::streaming_v2::decode_metrics_batch_v2_into(
            payload,
            scry_proto::streaming_v2::DecodeLimits::default(),
            builder,
        )
        .map(|(_descriptors, points)| points as usize)
        .map_err(|e| anyhow::anyhow!("MetricsBatchV2: {e}"))
    } else {
        streaming::decode_metrics_batch_into(payload, builder)
            .map(|(_series, samples)| samples)
            .map_err(|e| anyhow::anyhow!("MetricsBatch: {e}"))
    }
}

/// Adapter for both logs wire formats, wired to [`LogsBlockBuilder`].
///
/// Format recognition deliberately lives here rather than in live-session
/// feature gating: WAL recovery must always be able to replay a v2 frame that
/// was accepted while the feature was enabled, even if a later process restart
/// omits the flag. Live ingest checks mutual capability negotiation before this
/// decoder can run.
pub fn logs(payload: &[u8], builder: &mut LogsBlockBuilder) -> Result<usize> {
    if is_logs_v2(payload) {
        scry_proto::decode_logs_batch_v2_into(
            payload,
            scry_proto::LogsV2DecodeLimits::default(),
            builder,
        )
        .map(|records| records as usize)
        .map_err(|e| anyhow::anyhow!("LogsBatchV2: {e}"))
    } else {
        streaming::decode_logs_batch_into(payload, builder)
            .map_err(|e| anyhow::anyhow!("LogsBatch: {e}"))
    }
}

/// Cheap envelope discriminator. A magic-v2 payload is never allowed to fall
/// through to the legacy decoder.
pub fn is_logs_v2(payload: &[u8]) -> bool {
    payload
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_be_bytes)
        == Some(scry_proto::constants::LOGS_BATCH_V2_MAGIC)
}

/// Adapter for `decode_traces_batch_into`, wired to
/// [`TracesBlockBuilder`]. The streaming decoder resolves each span's
/// resource/scope dictionary entries and returns the span count (spans
/// are the records; resources/scopes are dictionaries).
pub fn traces(payload: &[u8], builder: &mut TracesBlockBuilder) -> Result<usize> {
    streaming::decode_traces_batch_into(payload, builder)
        .map_err(|e| anyhow::anyhow!("TracesBatch: {e}"))
}

/// Adapter for `decode_profiles_batch_into`, wired to
/// [`ProfilesBlockBuilder`]. Returns the blob count.
pub fn profiles(payload: &[u8], builder: &mut ProfilesBlockBuilder) -> Result<usize> {
    streaming::decode_profiles_batch_into(payload, builder)
        .map_err(|e| anyhow::anyhow!("ProfilesBatch: {e}"))
}
