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

pub mod bloom;
mod dummy;
pub mod events;
pub mod fence;
pub mod logs;
mod meta;
pub mod metrics;
pub mod postings;
pub mod profiles;
pub mod traces;

pub use bloom::{BodyBloom, BodyBloomBuilder};
pub use dummy::DummyBlockBuilder;
pub use events::{BlockEvent, BlockEventSink, Envelope, NoopSink};
pub use fence::{AlwaysValid, Fence};
pub use logs::LogsBlockBuilder;
pub use meta::{
    compacted_ancestor_closure, BlockMeta, MAX_COMPACTED_ANCESTORS, MAX_COMPACTED_ANCESTRY_BYTES,
};
pub use metrics::MetricsBlockBuilder;
pub use profiles::ProfilesBlockBuilder;
pub use traces::TracesBlockBuilder;

use anyhow::Result;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
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

    /// Override the ZSTD level this builder will encode with, replacing
    /// whatever `BlockBuilderConfig::compression_level` it was built with.
    ///
    /// The encode reads the level lazily (at `finish_and_upload` time, via
    /// `cfg.main_writer_props()`), so the server can decide the level *at
    /// block-close time* — when it actually knows whether uploads or CPU
    /// are the current bottleneck — rather than being locked to the level
    /// chosen when the builder was first constructed. See the adaptive
    /// compression policy in `scry-server` (`--compression auto`).
    fn set_compression_level(&mut self, level: i32);

    /// Stamp the highest WAL segment this block durably contains, read
    /// into the `BlockMeta` at `finish_and_upload` time. The pipeline
    /// calls this at block-close from the `SegmentId` it just sealed
    /// (`rotate()` return), right beside `set_compression_level` — same
    /// close-time-override idiom. The per-writer dedup watermark for the
    /// merged history+live query (D-054). Default no-op so a builder that
    /// never needs a watermark (or a signal that never live-tails) can
    /// ignore it.
    fn set_wal_seg_max(&mut self, _seg: u64) {}

    /// Stamp the ingest shard index whose WAL wrote this block, read into
    /// the `BlockMeta` at `finish_and_upload` time. Paired with
    /// `set_wal_seg_max`: the `(writer_id, signal, wal_shard)` triple is the
    /// WAL instance whose segments `wal_seg_max` counts, and the catalog
    /// `wal_watermarks` high-water is keyed on it (D-054). The pipeline
    /// passes its own `shard_index` here at block-close. Default no-op.
    fn set_wal_shard(&mut self, _shard: u32) {}

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

    /// ZSTD level for the parquet column data. Only two values are
    /// supported as deployment settings: `3` (default, dense) and `1`
    /// (fast). The encode is CPU-bound on zstd, so this is the dial
    /// between ingest throughput and stored size — when the box is
    /// CPU-bound, `1` buys ~1.5× encode throughput for ~+31% storage;
    /// when uploads to object storage are the wall, `3` keeps bytes
    /// down. Measured 2026-05. Other zstd levels are accepted by
    /// parquet but untested here.
    pub compression_level: i32,

    /// N-gram width for the logs body bloom sidecar (the v0.7 full-text
    /// skip index). Trigrams (`3`) are the default: small enough that
    /// short search terms still produce at least one gram, wide enough
    /// that the false-positive rate on real searches stays low. Patterns
    /// shorter than this can't be grammed, so the bloom can't prune them
    /// (the query scans every candidate block instead — still correct).
    /// Only the logs builder consumes this today.
    pub bloom_ngram: usize,

    /// Target false-positive rate for the logs body bloom. The builder
    /// sizes each block's filter (`m` bits, `k` probes) optimally for its
    /// exact distinct-gram count to hit this rate — possible because the
    /// block is sealed from the complete set of bodies. `0.01` (1%) keeps
    /// the sidecar near ~2% of body bytes. False positives only cost a
    /// wasted scan; there are never false negatives.
    pub bloom_target_fpr: f64,

    /// Highest WAL segment this block durably contains, stamped into the
    /// `BlockMeta` at encode time. Set by the pipeline at block-close via
    /// [`BlockBuilder::set_wal_seg_max`] from the `SegmentId` it just
    /// sealed — the same close-time-override idiom as `compression_level`.
    /// `None` until set (and for blocks that never carry a watermark, e.g.
    /// compaction output). The merged history+live query's per-writer
    /// dedup watermark (D-054).
    pub wal_seg_max: Option<u64>,

    /// Ingest shard index whose WAL wrote this block, stamped into the
    /// `BlockMeta` at encode time via [`BlockBuilder::set_wal_shard`].
    /// Paired with `wal_seg_max` to identify the WAL instance the watermark
    /// counts within (D-054). `None` until set (and for compaction output).
    pub wal_shard: Option<u32>,
}

impl Default for BlockBuilderConfig {
    fn default() -> Self {
        Self {
            max_rows: 1_000_000,
            target_bytes: 128 * 1024 * 1024, // 128 MiB before compression
            row_group_size: 1024 * 1024,     // ~1M rows
            compression_level: 3,            // dense by default
            bloom_ngram: 3,                  // trigrams
            bloom_target_fpr: 0.01,          // 1%
            wal_seg_max: None,               // stamped at block-close
            wal_shard: None,                 // stamped at block-close
        }
    }
}

impl BlockBuilderConfig {
    /// `WriterProperties` for a block's **main columnar data file**:
    /// the configured ZSTD level, the configured row-group size, and
    /// the dictionary **disabled**.
    ///
    /// Dictionary encoding is turned off deliberately. Our main files
    /// are numeric columns (fingerprint / ts / value, plus severities)
    /// where the dictionary is *redundant* with zstd — on the
    /// low-cardinality fingerprint column zstd already collapses the
    /// repetition downstream, and on the high-cardinality ts/value
    /// columns parquet hashes every value, finds the dictionary
    /// useless, and falls back to PLAIN, so the dictionary pass is pure
    /// wasted CPU. Disabling it measured ~1.09× faster *and* ~6%
    /// smaller (2026-05).
    pub fn main_writer_props(&self) -> Result<WriterProperties> {
        Ok(WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(
                self.compression_level,
            )?))
            .set_max_row_group_row_count(Some(self.row_group_size))
            .set_dictionary_enabled(false)
            .build())
    }

    /// `WriterProperties` for a block's **postings index file**: same
    /// ZSTD level and row-group size, but the dictionary **left on**.
    ///
    /// Unlike the main file, postings are `label_name` / `label_value`
    /// string columns — low-cardinality repeated text where the
    /// dictionary genuinely helps and zstd does *not* fully subsume it.
    /// So the dictionary-off reasoning from `main_writer_props` does not
    /// apply here; we keep parquet's default dictionary behaviour.
    pub fn postings_writer_props(&self) -> Result<WriterProperties> {
        Ok(WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(
                self.compression_level,
            )?))
            .set_max_row_group_row_count(Some(self.row_group_size))
            .build())
    }
}

/// Build the canonical object-storage path for a block, given its
/// signal prefix, the `ts_min` of its records, the writer UUID, and
/// the block UUID. `kind` is the file suffix (`"parquet"`,
/// `"meta.json"`, `"postings.parquet"` for metrics/logs, or
/// `"body.bloom"` for the logs full-text skip sidecar).
pub fn block_path(
    signal: &str,
    ts_min_unix_nano: u64,
    writer_id: Uuid,
    block_uuid: Uuid,
    kind: &str,
) -> String {
    let secs = (ts_min_unix_nano / 1_000_000_000) as i64;
    let dt =
        chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0).unwrap_or_else(chrono::Utc::now);
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

/// Attempts per object in [`put_block_objects`] — the initial PUT plus
/// two retries.
const PUT_ATTEMPTS: u32 = 3;

/// Backoff before the *n*-th retry (1-based): 250 ms, then 1 s. Bounded on
/// purpose — see [`put_block_objects`].
fn put_backoff(retry: u32) -> std::time::Duration {
    std::time::Duration::from_millis(250 * 4u64.pow(retry.saturating_sub(1)))
}

/// Upload an encoded block's objects **in order**, retrying each one a
/// bounded number of times before giving up.
///
/// Every builder's `finish_and_upload` funnels through here, so the retry
/// policy is defined once rather than five times. Ordering is preserved
/// exactly: `meta.json` is the last entry of [`EncodedBlock::puts`] and is
/// the commit point that makes a block visible to reconciliation, so it
/// must not be written before the data objects it describes.
///
/// Retries are **per object**, not per block: a failure re-PUTs only the
/// object that failed, leaving already-landed objects alone. PUTs are
/// idempotent here — a block's paths are unique to its UUID and the bytes
/// are fixed — so a retry can only rewrite identical content.
///
/// The attempt budget is deliberately small. The caller (`scry-server`'s
/// `run_upload`) holds an upload-semaphore permit for the whole call, and
/// that pool is what bounds ingest: retrying for too long would
/// backpressure ingest rather than absorb a blip. When the budget is
/// exhausted the final error is returned unchanged, and the caller's WAL
/// segments for this block stay on disk for replay.
pub async fn put_block_objects(store: &dyn ObjectStore, puts: Vec<(Path, Bytes)>) -> Result<()> {
    use anyhow::Context as _;
    use object_store::ObjectStoreExt as _;

    for (path, bytes) in puts {
        let mut attempt = 1;
        loop {
            match store.put(&path, bytes.clone().into()).await {
                Ok(_) => break,
                Err(e) if attempt < PUT_ATTEMPTS => {
                    let backoff = put_backoff(attempt);
                    tracing::warn!(
                        %path,
                        attempt,
                        attempts = PUT_ATTEMPTS,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %e,
                        "block object PUT failed; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!("upload {path} (gave up after {PUT_ATTEMPTS} attempts)")
                    });
                }
            }
        }
    }
    Ok(())
}

/// Delete every object that makes up a block — main parquet, meta.json,
/// and whichever sidecars its flags say it carries. Used by both
/// compaction (reaping merged-away inputs) and retention (reaping
/// expired blocks). Tolerant of already-absent objects (a retried pass,
/// or a partially-deleted block) so deletion is idempotent.
pub async fn delete_block_objects(store: &dyn ObjectStore, meta: &BlockMeta) -> Result<()> {
    use anyhow::Context as _;
    use object_store::ObjectStoreExt as _;

    // The sidecar is the commit marker seen by reconciliation, so remove it
    // last. A crash during a partial reap therefore leaves a discoverable block
    // whose remaining deletion can be retried idempotently.
    let mut kinds: Vec<&str> = vec!["parquet"];
    if meta.has_postings {
        kinds.push("postings.parquet");
    }
    if meta.has_body_bloom {
        kinds.push("body.bloom");
    }
    kinds.push("meta.json");
    for kind in kinds {
        let path = Path::from(block_path(
            &meta.signal,
            meta.ts_min_unix_nano,
            meta.writer_id,
            meta.uuid,
            kind,
        ));
        match store.delete(&path).await {
            Ok(()) => {}
            Err(object_store::Error::NotFound { .. }) => {
                tracing::debug!(%path, "object already absent during delete");
            }
            Err(e) => return Err(e).with_context(|| format!("delete {path}")),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::schema::types::ColumnPath;

    /// The main columnar file encodes at the configured ZSTD level with
    /// the dictionary **off** (numeric columns — zstd subsumes it; see
    /// `main_writer_props`). This is the property the adaptive dial relies
    /// on: whatever level the server sets via `set_compression_level`
    /// flows straight into these props at encode time.
    #[test]
    fn main_writer_props_track_level_and_disable_dictionary() {
        let col = ColumnPath::from("any");
        for level in [1, 3, 7] {
            let cfg = BlockBuilderConfig {
                compression_level: level,
                ..Default::default()
            };
            let props = cfg.main_writer_props().unwrap();
            assert_eq!(
                props.compression(&col),
                Compression::ZSTD(ZstdLevel::try_new(level).unwrap()),
                "main file should use the configured ZSTD level {level}"
            );
            assert!(
                !props.dictionary_enabled(&col),
                "main file dictionary must stay off"
            );
        }
    }

    /// The postings index keeps the dictionary **on** (low-cardinality
    /// label strings benefit; zstd does not fully subsume it there), but
    /// tracks the same configured ZSTD level as the main file.
    #[test]
    fn postings_writer_props_track_level_and_keep_dictionary() {
        let col = ColumnPath::from("label_value");
        let cfg = BlockBuilderConfig {
            compression_level: 3,
            ..Default::default()
        };
        let props = cfg.postings_writer_props().unwrap();
        assert_eq!(
            props.compression(&col),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap())
        );
        assert!(
            props.dictionary_enabled(&col),
            "postings dictionary must stay on"
        );
    }

    /// An `ObjectStore` whose `put` fails the first `fail_first` attempts
    /// (across all objects) with a retryable-looking error, then delegates
    /// to `InMemory`. `attempts` counts every PUT that reached the store.
    struct FlakyStore {
        inner: object_store::memory::InMemory,
        fail_first: u64,
        attempts: std::sync::Arc<std::sync::atomic::AtomicU64>,
    }

    impl FlakyStore {
        fn new(fail_first: u64) -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                fail_first,
                attempts: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }
        }
    }

    impl std::fmt::Display for FlakyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FlakyStore")
        }
    }
    impl std::fmt::Debug for FlakyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FlakyStore").finish()
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FlakyStore {
        async fn put_opts(
            &self,
            l: &Path,
            p: object_store::PutPayload,
            o: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let n = self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < self.fail_first {
                return Err(object_store::Error::Generic {
                    store: "FlakyStore",
                    source: "injected transient failure".into(),
                });
            }
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(
            &self,
            l: &Path,
            o: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(l, o).await
        }
        fn delete_stream(
            &self,
            l: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(
            &self,
            p: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(p)
        }
        fn list_with_offset(
            &self,
            p: Option<&Path>,
            o: &Path,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list_with_offset(p, o)
        }
        async fn list_with_delimiter(
            &self,
            p: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(
            &self,
            f: &Path,
            t: &Path,
            o: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
        async fn rename_opts(
            &self,
            f: &Path,
            t: &Path,
            o: object_store::RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }

    fn two_puts() -> Vec<(Path, Bytes)> {
        vec![
            (Path::from("blk/main.parquet"), Bytes::from_static(b"main")),
            (Path::from("blk/meta.json"), Bytes::from_static(b"meta")),
        ]
    }

    /// A transient object-store failure must not sink the block: the PUT
    /// is retried within the attempt budget and the block still commits.
    /// Without this, one blip meant the records waited for a restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_retries_transient_failures_then_succeeds() {
        // Fail the first two attempts — inside the 3-attempt budget.
        let store = FlakyStore::new(2);
        let attempts = store.attempts.clone();

        put_block_objects(&store, two_puts())
            .await
            .expect("should succeed within the attempt budget");

        // 2 failed attempts on the first object + its success + the
        // second object's single attempt.
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            4,
            "retries are per object; the second object must not be re-PUT"
        );
        // Both objects actually landed.
        use object_store::ObjectStoreExt as _;
        assert!(store.get(&Path::from("blk/main.parquet")).await.is_ok());
        assert!(store.get(&Path::from("blk/meta.json")).await.is_ok());
    }

    /// A permanently-failing store must give up after the attempt budget
    /// and surface the error, so the caller keeps its WAL segments for
    /// replay rather than believing the block landed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_gives_up_after_the_attempt_budget() {
        let store = FlakyStore::new(u64::MAX);
        let attempts = store.attempts.clone();

        let err = put_block_objects(&store, two_puts())
            .await
            .expect_err("a permanently failing store must surface an error");

        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            PUT_ATTEMPTS as u64,
            "should stop at the attempt budget rather than retry forever"
        );
        assert!(
            format!("{err:#}").contains("gave up after"),
            "error should say the budget was exhausted, got: {err:#}"
        );
    }

    /// `meta.json` is the commit point for catalog reconciliation, so it
    /// must never be written before the data objects it describes — not
    /// even when an earlier object needed retries.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_preserves_order_across_retries() {
        let store = FlakyStore::new(1);
        put_block_objects(&store, two_puts()).await.unwrap();

        // The main object exists; since PUTs are sequential and the meta
        // only goes after it returns Ok, meta can't have preceded it.
        use object_store::ObjectStoreExt as _;
        let main = store.get(&Path::from("blk/main.parquet")).await;
        assert!(main.is_ok(), "data object must be present once meta is");
    }
}
