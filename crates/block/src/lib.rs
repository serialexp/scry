//! Block builder + reader for scry's storage layer.
//!
//! A *block* is an immutable parquet file in object storage plus a
//! small JSON sidecar with metadata used to prune queries without
//! opening the parquet itself. Layout follows
//! `docs/ARCHITECTURE.md § Storage layer § Block layout`:
//!
//! ```text
//! <signal>/<yyyy>/<mm>/<dd>/<writer_id>/<block_uuid>.parquet
//! <signal>/<yyyy>/<mm>/<dd>/<writer_id>/<block_uuid>.meta.json
//! ```
//!
//! In v0.1 the only record shape is [`scry_proto::DummyRecord`] and
//! the only signal is `dummy/`. Signal-specific builders come back
//! when real signals do.

mod dummy;
mod meta;

pub use dummy::DummyBlockBuilder;
pub use meta::BlockMeta;

use uuid::Uuid;

/// Block close triggers. Defaults match `ARCHITECTURE.md § The block
/// builder`. v0.1 doesn't need a `max_block_age` because the spewer
/// drives close-on-shutdown; that fires on graceful flush.
#[derive(Debug, Clone, Copy)]
pub struct BlockBuilderConfig {
    pub max_rows: u64,
    pub target_bytes: u64,
}

impl Default for BlockBuilderConfig {
    fn default() -> Self {
        Self {
            max_rows: 1_000_000,
            target_bytes: 128 * 1024 * 1024, // 128 MiB before compression
        }
    }
}

/// Build the canonical object-storage path for a block, given its
/// signal prefix, the `ts_min` of its records, the writer UUID, and
/// the block UUID. `kind` is the file suffix (`"parquet"`,
/// `"meta.json"`, or `"postings.parquet"` once metrics land).
pub fn block_path(
    signal: &str,
    ts_min_unix_nano: u64,
    writer_id: Uuid,
    block_uuid: Uuid,
    kind: &str,
) -> String {
    let secs = (ts_min_unix_nano / 1_000_000_000) as i64;
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
        .unwrap_or_else(chrono::Utc::now);
    format!(
        "{signal}/{}/{}/{}/{}/{}.{}",
        dt.format("%Y"),
        dt.format("%m"),
        dt.format("%d"),
        writer_id,
        block_uuid,
        kind,
    )
}
