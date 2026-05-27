//! Block sidecar metadata.
//!
//! Serialised as `<block_uuid>.meta.json` next to the parquet. The
//! catalog reads these on bucket reconciliation and never has to open
//! the parquet itself to know what's inside. Fields the v0.1 dummy
//! record doesn't populate (label fingerprint bloom, per-column
//! min/max) stay in the schema as `Option` so we don't migrate the
//! sidecar format when real signals land.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    pub uuid: Uuid,
    pub signal: String,
    pub writer_id: Uuid,
    pub ts_min_unix_nano: u64,
    pub ts_max_unix_nano: u64,
    pub row_count: u64,
    /// On-disk size of the parquet payload after compression.
    pub byte_size: u64,
    pub schema_version: u32,
    /// Producer software version string (cargo pkg version of the
    /// writer crate). Lets operators correlate a block to a release.
    pub producer_version: String,

    /// Coarse-prune bloom over the labels present in the block.
    /// `None` in v0.1 — the dummy record has no labels. Populated by
    /// real signals starting in v0.2.
    pub label_fingerprint_bloom: Option<Vec<u8>>,

    /// Whether this block has a sibling `.postings.parquet` file.
    /// True for metrics blocks (the inverted index that drives
    /// cardinality-aware query pruning per ARCHITECTURE.md § Metrics
    /// § Per-block postings index), false for every other signal.
    #[serde(default)]
    pub has_postings: bool,

    /// On-disk size of the postings sidecar parquet, if present.
    /// `None` when `has_postings` is false. Carried in the sidecar so
    /// the catalog can surface "how much postings overhead does this
    /// block carry" without opening the parquet itself.
    #[serde(default)]
    pub postings_size_bytes: Option<u64>,

    /// Per-series metric type (counter / gauge / …), keyed by
    /// fingerprint. The wire's `SeriesDictEntry.metric_type` has
    /// nowhere to land in the canonical postings schema (which is
    /// purely label → fingerprints), so we keep it here in the
    /// sidecar JSON. Cheap (~9 bytes × N_series) and avoids
    /// committing to a parquet schema we'd regret. `None` for
    /// non-metrics signals.
    #[serde(default)]
    pub series_types: Option<Vec<(u64, u8)>>,
}
