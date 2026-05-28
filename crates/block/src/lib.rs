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
pub mod logs;
mod meta;
pub mod metrics;

pub use dummy::DummyBlockBuilder;
pub use logs::LogsBlockBuilder;
pub use meta::BlockMeta;
pub use metrics::MetricsBlockBuilder;

use anyhow::Result;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use std::future::Future;
use uuid::Uuid;

/// The CPU-bound product of encoding a block, handed back from the
/// blocking encode task to the async upload path.
///
/// Each builder's `finish_and_upload` does its sort + Arrow build +
/// zstd compression inside `tokio::task::spawn_blocking` (so the
/// CPU-heavy encode doesn't monopolise an async worker thread), then
/// performs the object-store PUTs back on the async side. This struct
/// is what crosses that boundary: the catalog `meta` plus the already
/// encoded `(path, body)` pairs to upload **in order** — the `meta.json`
/// sidecar MUST be the last entry, since it's the "block exists" signal
/// for catalog reconcile.
pub(crate) struct EncodedBlock {
    pub(crate) meta: BlockMeta,
    pub(crate) puts: Vec<(Path, Bytes)>,
}

/// A signal-specific in-memory block builder. The pipeline machinery in
/// `scry-server` is generic over this trait so the WAL → builder →
/// upload → catalog plumbing is written once per process, not once per
/// signal.
///
/// The trait is intentionally small: the pipeline only needs to know
/// "is there anything to upload yet," "should I close now," and "give
/// me the upload future + the meta to insert into the catalog." Decode
/// is *not* on this trait — each signal has its own wire shape and its
/// own appender trait (see [`scry_proto::streaming`]), and the pipeline
/// receives the decode function as a closure / fn-pointer at call sites
/// so the trait stays object-safe-shaped (even though we use it as a
/// generic bound, not a dyn).
pub trait BlockBuilder: Send + 'static {
    /// WAL subdirectory name for this signal. Must match the value
    /// passed to `WalConfig::new(dir, signal)` so replay finds the
    /// right segments.
    const SIGNAL: &'static str;

    fn new(writer_id: Uuid, cfg: BlockBuilderConfig) -> Self;
    fn is_empty(&self) -> bool;
    fn should_close(&self) -> bool;

    /// Drain `other`'s buffered records into `self`, leaving `other`
    /// empty (capacity retained) and reusable for the next batch.
    ///
    /// This is the heart of the decode-out-of-lock ingest path: a
    /// connection decodes a batch into a *private* scratch builder with
    /// no lock held (the CPU-heavy bit-level decode runs in parallel
    /// across connections), then takes the pipeline lock only long
    /// enough to `merge` that scratch into the shared builder. The merge
    /// is a handful of column `Vec` appends plus a dictionary dedup —
    /// orders of magnitude cheaper than the decode it replaces under the
    /// lock.
    ///
    /// Any per-block dedup state (e.g. the series/stream "seen" set)
    /// accumulates against `self`, so the dedup scope is identical to
    /// decoding straight into the shared builder. After this returns
    /// `other` is in the same empty state `new` produces.
    fn merge(&mut self, other: &mut Self);

    /// Empty all buffers in place (capacity retained), returning to the
    /// just-constructed state. Used to discard a scratch builder after a
    /// batch that failed to commit — a decode error (partial prefix
    /// absorbed) or a WAL-append failure (records decoded but never
    /// merged) — so the connection's next batch decodes into a clean
    /// scratch. The happy path never calls this: a successful `merge`
    /// already leaves the scratch empty.
    fn reset(&mut self);
    /// Consume the builder, encode to parquet (+ any sidecars), upload
    /// to object storage, and return a `BlockMeta` ready for catalog
    /// insertion. Returns `Ok(None)` if the builder turned out to be
    /// empty after a race with rotation (vanishingly rare; the pipeline
    /// checks `is_empty()` before calling this).
    fn finish_and_upload(
        self,
        store: &dyn ObjectStore,
    ) -> impl Future<Output = Result<Option<BlockMeta>>> + Send;
}

/// Block close triggers. Defaults match `ARCHITECTURE.md § The block
/// builder`. v0.1 doesn't need a `max_block_age` because the spewer
/// drives close-on-shutdown; that fires on graceful flush.
#[derive(Debug, Clone, Copy)]
pub struct BlockBuilderConfig {
    pub max_rows: u64,
    pub target_bytes: u64,
    /// Parquet `set_max_row_group_row_count` for the main data file.
    /// Production default is 1M rows, which yields a handful of
    /// groups per ~128 MiB block — small enough to make row-group
    /// pruning a sharp filter, large enough to keep per-group
    /// overhead negligible. Tests that need to exercise row-group
    /// pruning at small data sizes override this.
    pub row_group_size: usize,
}

impl Default for BlockBuilderConfig {
    fn default() -> Self {
        Self {
            max_rows: 1_000_000,
            target_bytes: 128 * 1024 * 1024, // 128 MiB before compression
            row_group_size: 1024 * 1024,     // ~1M rows
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
