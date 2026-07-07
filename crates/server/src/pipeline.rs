//! Generic, process-scoped durability pipeline for one signal.
//!
//! Owns the per-writer WAL, the active block builder, the destination
//! object store, and the optional online catalog. Shared across
//! sessions via `Arc<Mutex<_>>`. Per `ARCHITECTURE.md § The WAL` the
//! WAL is per-writer (per-signal) — every connection that lands on
//! this process funnels signal-X ingest through the same pipeline.
//!
//! ## Generic over the block builder
//!
//! The same WAL → builder → upload → catalog machinery applies to every
//! signal that follows the v0.1 shape. The only signal-specific bits
//! are (a) the builder type, (b) the WAL signal subdirectory name, and
//! (c) the wire decoder that turns batch payloads into builder appends.
//! [`Pipeline`] is generic over a [`BlockBuilder`] for (a) and (b), and
//! [`Pipeline::ingest`] takes a decode-function pointer for (c). Each
//! signal gets its own concrete `Pipeline<B>` instance; the rest of the
//! file is signal-agnostic.
//!
//! ## Decode out of lock
//!
//! The CPU-heavy part of ingest is the bit-level binschema decode that
//! turns a batch payload into builder appends. It used to run *inside*
//! `ingest()` under the pipeline mutex, serialising every connection's
//! decode for a signal onto a single core. Now each connection decodes
//! into its own private scratch builder with **no lock held** (see
//! [`Pipeline::new_scratch`] / [`Pipeline::decode_fn`]), so decode scales
//! across connections, and then takes the lock only to
//! [`Pipeline::ingest_decoded`] — a WAL append plus a cheap column merge
//! ([`BlockBuilder::merge`]). The serialized critical section shrinks
//! from a full decode to memcpy-grade work.
//!
//! ## Background upload
//!
//! The slow part of closing a block (parquet encode + S3 PUT) used to
//! run inline inside `ingest()`, pinning the pipeline mutex and blocking
//! every subsequent inbound batch on every connection. That made the
//! server ack-bound on upload latency rather than on its own ingest
//! throughput.
//!
//! Now: when the builder hits `should_close`, `spawn_upload` first
//! acquires a permit from the upload semaphore *while the pipeline mutex
//! is still held by the caller*. Only then is the WAL rotated and the
//! full builder swapped out for a fresh one (both fast — fsync +
//! `mem::replace`) and the encode + PUT spawned as a tokio task that
//! owns the permit. The CPU-heavy encode (sort + Arrow build + zstd)
//! itself runs on the blocking pool inside `BlockBuilder::finish_and_upload`
//! (`tokio::task::spawn_blocking`), so compressing a block doesn't
//! monopolise an async worker that should be servicing ingest decode.
//! When the upload finishes the task releases the now-uploaded WAL
//! segments and inserts the catalog row. The WAL lock is held only to
//! *validate + snapshot* the release (`prepare_release`, microseconds);
//! the actual `unlink`s (`release_segments`) run lock-free, so a block
//! release never stalls the foreground append path — which takes the WAL
//! lock under the pipeline mutex on every batch.
//!
//! The semaphore is normally *shared across all signals* (one pool sized
//! to the host's physical core count — see [`Pipeline::with_upload_sem`]
//! and [`MAX_INFLIGHT_UPLOADS`]), because encode is CPU-bound and CPU is
//! a global resource: a single hot signal should be able to use every
//! core, and the total number of blocks encoding at once must not exceed
//! what the cores can absorb.
//!
//! Acquiring the permit *before* spawning is what bounds memory: if
//! `MAX_INFLIGHT_UPLOADS` uploads are already running, the permit wait
//! happens under the pipeline mutex, so the connection's `ingest()`
//! (and every other connection's ingest for this signal) blocks until a
//! slot frees. A bucket slower than ingest throttles the agents via
//! delayed BatchAcks rather than letting finished builders accumulate
//! unbounded in RAM. The permit is taken before the WAL lock, never
//! while holding it — the in-flight upload that frees a permit needs
//! the WAL lock to `mark_uploaded`, so the reverse order would
//! deadlock.
//!
//! The WAL and catalog therefore live behind `Arc<Mutex<…>>` so the
//! background task can share them with the ingest path. Lock contention
//! is negligible: append/rotate take microseconds, segment release holds
//! the WAL lock only for a validate+snapshot (the `unlink`s run
//! lock-free), and `insert_block` is a single SQLite INSERT.

use anyhow::{Context, Result};
use object_store::ObjectStore;
use scry_block::{BlockBuilder, BlockBuilderConfig, BlockEvent, BlockEventSink};
use scry_catalog::Catalog;
use scry_wal::{SegmentId, Wal, WalConfig};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinSet,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::stats::UploadStats;

/// Default number of block encode+upload tasks in flight concurrently,
/// used when no shared semaphore is supplied via
/// [`Pipeline::with_upload_sem`]. Two is a conservative fallback (one
/// block uploading while the next finishes filling).
///
/// In production (scry-ingestd) this default is overridden: all signals
/// share a single semaphore sized to the host's *physical* core count,
/// because the dominant cost of an upload is the parquet encode (sort +
/// Arrow build + zstd), which is CPU-bound and runs on the blocking
/// pool. A shared pool lets one hot signal use every core while still
/// bounding concurrent in-flight blocks (and therefore RAM).
pub const MAX_INFLIGHT_UPLOADS: usize = 2;

/// Number of independent ingest shards per signal. Each connection is
/// pinned to one shard (by session id), so the per-signal ingest mutex
/// is no longer a single global contention point — connections spread
/// across `INGEST_SHARDS` independent `(WAL + builder + mutex)` pipelines.
///
/// Sized at 8 against a 12-physical/24-logical-core host: with the
/// lock-free decode landing ~50–79M rec/s and a single ingest mutex
/// walling at ~8–10M rec/s, ~8× independent shards lifts the ingest
/// ceiling at/above the decode ceiling, so the lock stops being the
/// binding constraint (the parquet encode sort becomes the next lever).
/// The shared upload semaphore still bounds total in-flight encode+PUT
/// across *all* shards and signals, so more shards doesn't mean more
/// concurrent encodes — just finer-grained, less-contended ingest.
pub const INGEST_SHARDS: usize = 8;

/// Decoder function type: takes a (decompressed) batch payload + the
/// active block builder, walks the wire format streaming-style, calls
/// the builder's signal-specific appender for each record, and returns
/// the record count for ack accounting.
///
/// Concrete instances live in `scry_proto::streaming` —
/// `decode_dummy_batch_into` and (after this commit)
/// `decode_metrics_batch_into`. Pulled in as a fn pointer rather than
/// a generic so the pipeline stays decoder-agnostic and binaries can
/// wire up whichever decoder matches their builder.
pub type DecodeFn<B> = fn(&[u8], &mut B) -> anyhow::Result<usize>;

/// ZSTD level for the "dense" end of the adaptive dial: smallest blocks,
/// slowest encode. Picked when uploads are the bottleneck (spend CPU to
/// shrink bytes the bucket has to swallow) *or* when the box has CPU
/// headroom (we can afford to compress hard). Matches `--compression dense`.
const ADAPTIVE_DENSE_LEVEL: i32 = 3;

/// ZSTD level for the "fast" end: ~1.5× encode throughput for ~+31%
/// bytes. Picked only when uploads have slack *and* the host is CPU-busy
/// — i.e. encode is the active constraint and storage can absorb the
/// larger blocks. Matches `--compression fast`.
const ADAPTIVE_FAST_LEVEL: i32 = 1;

/// 1-minute load average **per physical core** at or above which the host
/// counts as "busy" for the adaptive decision. 0.85 trips us into fast
/// encode a little before full saturation, so we shed encode cost *as* the
/// box fills rather than only once it's pinned.
const ADAPTIVE_LOAD_BUSY_PER_CORE: f64 = 0.85;

/// Host physical core count, detected once. Used to normalise the system
/// load average for the adaptive-compression decision (the upload pool is
/// also sized to physical cores, so a per-physical-core ratio of ~1.0 is
/// "encode pool full").
fn physical_cores() -> usize {
    use std::sync::OnceLock;
    static CORES: OnceLock<usize> = OnceLock::new();
    *CORES.get_or_init(|| num_cpus::get_physical().max(1))
}

/// The pure adaptive-compression policy, factored out of
/// [`Pipeline::adaptive_level`] so it can be unit-tested without a live
/// semaphore or `/proc`.
///
/// * `upload_saturated` — the upload pool is full (no free permits): the
///   bucket is the wall, so compress as hard as possible.
/// * otherwise look at `load_per_core` (1-min load average ÷ physical
///   cores): busy → fast encode (CPU is the wall, save it); comfortable
///   or unknown → dense.
fn decide_adaptive_level(upload_saturated: bool, load_per_core: Option<f64>) -> i32 {
    if upload_saturated {
        return ADAPTIVE_DENSE_LEVEL;
    }
    match load_per_core {
        Some(lpc) if lpc >= ADAPTIVE_LOAD_BUSY_PER_CORE => ADAPTIVE_FAST_LEVEL,
        _ => ADAPTIVE_DENSE_LEVEL,
    }
}

pub struct Pipeline<B: BlockBuilder> {
    /// Shared with the upload task so it can call `mark_uploaded` after
    /// a successful upload without funnelling back through the ingest
    /// path.
    wal: Arc<Mutex<Wal>>,
    builder: B,
    store: Arc<dyn ObjectStore>,
    /// Optional online catalog. Updates here are best-effort: a failed
    /// insert (sqlite locked, disk full, etc.) is logged but does not
    /// fail the ingest path, because the bucket is the source of
    /// truth and a future `scry-list --reconcile` would re-derive the
    /// row anyway.
    catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
    writer_uuid: Uuid,
    cfg: BlockBuilderConfig,
    decode: DecodeFn<B>,
    /// Pending upload tasks. Each entry is a spawned task that owns the
    /// old builder + a semaphore permit. `flush()` drains this on
    /// shutdown; routine ingest only `try_join_next`s to reap finished
    /// ones so the set doesn't grow unboundedly during a long run.
    in_flight: JoinSet<()>,
    /// Bounds concurrent uploads so a slow bucket can't let blocks pile
    /// up in RAM. `spawn_upload` acquires a permit *before* spawning the
    /// upload task, while the caller holds the pipeline mutex — so when
    /// permits are exhausted the ingest path blocks here and the agents
    /// see backpressure, rather than builders accumulating. See the
    /// module docs and `spawn_upload`.
    upload_sem: Arc<Semaphore>,
    /// Optional live gauges for the stats endpoint. `None` means "not
    /// observed" — every update is a no-op, so tests and the
    /// no-stats-server path pay nothing.
    upload_stats: Option<Arc<UploadStats>>,
    /// When `true`, pick each closing block's ZSTD level from current load
    /// at `spawn_upload` time (see [`Pipeline::adaptive_level`]) instead of
    /// using the static `cfg.compression_level`. Set via
    /// [`Pipeline::with_adaptive_compression`] (`--compression auto`).
    adaptive_compression: bool,
    /// Optional block-lifecycle event sink. When set (multi-instance mode),
    /// every successful block upload emits a [`BlockEvent::Created`] so peers
    /// converge their catalogs via pub/sub. `None` (single-instance / tests)
    /// means uploads emit nothing — convergence isn't needed. Set via
    /// [`Pipeline::set_event_sink`] / [`ShardedPipeline::with_event_sink`].
    event_sink: Option<Arc<dyn BlockEventSink>>,
    /// When the current (open) block first received a record, or `None`
    /// when the builder is empty. Drives the time-based flush
    /// ([`Pipeline::flush_if_aged`]): a low-volume or idle signal would
    /// otherwise never cross the size-based `should_close` threshold, so
    /// its records would sit in RAM (and re-replay from the WAL on every
    /// restart) and never become queryable. Invariant: `Some` iff the
    /// builder is non-empty. Set on the empty→non-empty transition in the
    /// ingest paths (and after WAL replay), cleared in `spawn_upload`.
    block_started_at: Option<Instant>,
    /// This pipeline's ingest shard index (`0..INGEST_SHARDS`), stamped
    /// into every block's `wal_shard` meta at upload so the D-054 dedup can
    /// key its WAL high-water on `(writer_id, signal, shard)`. Set by
    /// [`ShardedPipeline`] from its construction loop; `0` for a standalone
    /// [`Pipeline`] (the sole shard).
    shard_index: u32,
}

impl<B: BlockBuilder> Pipeline<B> {
    /// Open the WAL (signal subdir = `B::SIGNAL`), replay any leftover
    /// records into a fresh builder, and return a pipeline ready to
    /// ingest. The replayed records are *not* re-acked to the agent
    /// (agents will resend any in-flight batches they hadn't yet seen
    /// an ack for, and dedup is a v0.3 concern), but they are durable
    /// and will be uploaded in the next flush.
    pub async fn open(
        wal_dir: PathBuf,
        store: Arc<dyn ObjectStore>,
        catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
        writer_uuid: Uuid,
        decode: DecodeFn<B>,
    ) -> Result<Self> {
        Self::open_with_config(
            wal_dir,
            store,
            catalog,
            writer_uuid,
            decode,
            BlockBuilderConfig::default(),
        )
        .await
    }

    /// Same as [`Pipeline::open`] but with an explicit
    /// [`BlockBuilderConfig`]. `open` delegates here with the default;
    /// tests use this to force small blocks (e.g. `max_rows = 1`) so a
    /// few `ingest` calls exercise the full rotate → upload path.
    pub async fn open_with_config(
        wal_dir: PathBuf,
        store: Arc<dyn ObjectStore>,
        catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
        writer_uuid: Uuid,
        decode: DecodeFn<B>,
        cfg: BlockBuilderConfig,
    ) -> Result<Self> {
        let wal = Wal::open(WalConfig::new(wal_dir, B::SIGNAL))
            .await
            .with_context(|| format!("opening {} WAL", B::SIGNAL))?;

        let mut builder = B::new(writer_uuid, cfg);
        let mut replayed_records = 0u64;
        let mut replayed_frames = 0u64;
        for frame in wal.replay().context("scanning WAL for replay")? {
            let payload = frame.context("reading WAL frame")?;
            let n = (decode)(&payload, &mut builder)
                .with_context(|| format!("WAL replay: decode {} batch", B::SIGNAL))?;
            replayed_records += n as u64;
            replayed_frames += 1;
        }
        if replayed_records > 0 {
            info!(
                signal = B::SIGNAL,
                replayed_records,
                replayed_frames,
                "WAL replay complete; records merged into next block"
            );
        }

        // Records replayed from the WAL populate the builder before any
        // live ingest. Stamp the block's start now so the time-based flush
        // will seal it even if this signal then goes idle (the exact case
        // that left 22.7M replayed-but-never-uploaded rows stuck in RAM).
        let block_started_at = (!builder.is_empty()).then(Instant::now);

        Ok(Self {
            wal: Arc::new(Mutex::new(wal)),
            builder,
            store,
            catalog,
            writer_uuid,
            cfg,
            decode,
            in_flight: JoinSet::new(),
            upload_sem: Arc::new(Semaphore::new(MAX_INFLIGHT_UPLOADS)),
            upload_stats: None,
            adaptive_compression: false,
            event_sink: None,
            block_started_at,
            shard_index: 0,
        })
    }

    /// Set this pipeline's ingest shard index, stamped into every block's
    /// `wal_shard` meta (D-054). Builder-style so `open`/`open_with_config`
    /// keep their signatures; [`ShardedPipeline`] calls it per shard, and a
    /// standalone pipeline keeps the `0` default.
    pub fn with_shard_index(mut self, shard: u32) -> Self {
        self.shard_index = shard;
        self
    }

    /// This pipeline's ingest shard index. The logs decode path reads it in
    /// phase 2 to stamp the WAL segment tag's shard onto the live ring
    /// records before pushing them (D-054).
    pub fn shard_index(&self) -> u32 {
        self.shard_index
    }

    /// This pipeline's writer UUID — the WAL-instance discriminator that,
    /// with signal + shard, keys the dedup high-water. Read by the live ring
    /// feed / live-query endpoint.
    pub fn writer_uuid(&self) -> Uuid {
        self.writer_uuid
    }

    /// Attach live upload gauges for the stats endpoint. Builder-style
    /// so `open` keeps its signature and call sites that don't observe
    /// uploads stay unchanged.
    pub fn with_upload_stats(mut self, stats: Arc<UploadStats>) -> Self {
        self.upload_stats = Some(stats);
        self
    }

    /// Replace this pipeline's private upload semaphore with a shared
    /// one. All signals in a process should pass the *same* `Arc`, sized
    /// to the host's physical core count, so the concurrent encode+upload
    /// cap is a single global pool rather than one pool per signal —
    /// encode is CPU-bound, and CPU is a shared resource. Builder-style
    /// so `open` keeps its signature and tests that don't share a pool
    /// stay on the [`MAX_INFLIGHT_UPLOADS`] default.
    pub fn with_upload_sem(mut self, sem: Arc<Semaphore>) -> Self {
        self.upload_sem = sem;
        self
    }

    /// Enable adaptive per-block compression (`--compression auto`).
    /// Builder-style so `open` keeps its signature; off by default, so the
    /// static-level path is byte-for-byte unchanged.
    pub fn with_adaptive_compression(mut self, on: bool) -> Self {
        self.adaptive_compression = on;
        self
    }

    /// Attach a block-lifecycle event sink (multi-instance convergence). Every
    /// successful upload thereafter emits a [`BlockEvent::Created`]. Off by
    /// default, so the single-instance path is byte-for-byte unchanged.
    pub fn set_event_sink(&mut self, sink: Arc<dyn BlockEventSink>) {
        self.event_sink = Some(sink);
    }

    /// Choose the ZSTD level for the block about to be encoded, from the
    /// live load picture. Called from [`Pipeline::spawn_upload`] *after*
    /// the upload permit has been acquired, so `available_permits() == 0`
    /// means this block took the last slot — the upload pool is full and
    /// the bucket is the bottleneck. See [`decide_adaptive_level`].
    fn adaptive_level(&self) -> i32 {
        let upload_saturated = self.upload_sem.available_permits() == 0;
        let load_per_core =
            crate::stats::load_avg_1m().map(|load1| load1 / physical_cores() as f64);
        decide_adaptive_level(upload_saturated, load_per_core)
    }

    /// Append a single batch payload (already zstd-decoded; the
    /// binschema-encoded form is what hits the WAL). On replay we
    /// decode the same bytes back into records, so this is the unit
    /// of crash-recovery atomicity. Auto-spawns a background upload
    /// task if the builder hits its close threshold.
    /// A fresh, empty scratch builder for a connection to decode a batch
    /// into *without holding the pipeline lock*. Same writer + config as
    /// the shared builder. See [`Pipeline::ingest_decoded`] and the
    /// module docs for the decode-out-of-lock ingest path.
    pub fn new_scratch(&self) -> B {
        B::new(self.writer_uuid, self.cfg)
    }

    /// This signal's decode function pointer (`Copy`), so a connection
    /// can decode straight into its scratch builder without re-deriving
    /// the decoder choice. Grab it once per connection alongside
    /// [`Pipeline::new_scratch`].
    pub fn decode_fn(&self) -> DecodeFn<B> {
        self.decode
    }

    /// Commit a batch that the caller already decoded into `scratch`
    /// (lock-free). WAL-first, exactly like [`Pipeline::ingest`]: append
    /// the raw payload, then merge the scratch builder's columns into the
    /// shared builder. Auto-spawns a background upload if the merge tips
    /// the builder past its close threshold.
    ///
    /// On WAL-append failure we return the error *before* merging, so the
    /// shared builder is untouched and the agent's retry re-decodes the
    /// batch cleanly. The caller is responsible for `reset`-ing `scratch`
    /// on any error so the connection's next batch decodes into a clean
    /// buffer. (A successful merge already leaves `scratch` empty.)
    ///
    /// Returns the [`SegmentId`] the batch's frame was appended to — the
    /// per-shard WAL segment tag the D-054 live path stamps onto the ring
    /// records it collected during decode, so the merged history+live query
    /// can dedup them against the catalog high-water (`kept iff seg > H`).
    /// The record count for the ack comes from the decode the caller already
    /// performed, so this returns the segment rather than a count.
    pub async fn ingest_decoded(&mut self, payload: &[u8], scratch: &mut B) -> Result<SegmentId> {
        // Order matters: WAL first, builder second — same invariant as
        // `ingest`. If the WAL append fails we never merge the scratch
        // records into the shared builder; the agent sees the BatchAck
        // failure and retries, and the caller resets the scratch.
        //
        // Capture the segment the frame lands in *before* the append: an
        // append that tips the segment past its cap rotates internally
        // *after* writing the frame, so the frame is in the pre-append
        // current segment, not `current_segment()` afterwards. Same lock
        // acquisition so no other holder can rotate between the two.
        let seg = {
            let mut wal = self.wal.lock().await;
            let seg = wal.current_segment();
            wal.append(payload).await.context("WAL append")?;
            seg
        };

        self.builder.merge(scratch);
        self.mark_block_started();

        if self.builder.should_close() {
            self.spawn_upload().await?;
        }
        // Reap any finished upload tasks so the JoinSet doesn't grow
        // unbounded. Non-blocking.
        self.reap_finished();
        Ok(seg)
    }

    pub async fn ingest(&mut self, payload: &[u8]) -> Result<u64> {
        // Order matters: WAL first, builder second. If the WAL append
        // fails we never put the records into the in-memory builder
        // — the agent will see the resulting BatchAck failure and
        // retry. If decode fails partway through, the builder has
        // absorbed a prefix of the batch's records *and* the WAL has
        // the whole payload — on next start, replay re-applies the
        // full batch from the WAL, so the partial absorption here
        // is overwritten by a clean re-decode. Net effect: a decode
        // failure just gets a retry from the agent; no duplicate or
        // missing records.
        self.wal
            .lock()
            .await
            .append(payload)
            .await
            .context("WAL append")?;

        let n = (self.decode)(payload, &mut self.builder)? as u64;
        self.mark_block_started();

        if self.builder.should_close() {
            self.spawn_upload().await?;
        }
        // Reap any finished upload tasks so the JoinSet doesn't grow
        // for the lifetime of the process. Non-blocking — we don't
        // wait for in-flight work here.
        self.reap_finished();
        Ok(n)
    }

    /// Rotate the WAL, swap in a fresh builder, and hand the full one
    /// to a background task for upload. The synchronous portion (WAL
    /// rotate + builder swap) is fast; the slow parquet encode + S3
    /// PUT runs entirely in the spawned task with the pipeline mutex
    /// released as soon as the caller returns from `ingest`.
    async fn spawn_upload(&mut self) -> Result<()> {
        if self.builder.is_empty() {
            return Ok(());
        }

        // ── Backpressure point ──────────────────────────────────────
        // Acquire an upload permit *before* draining the builder, while
        // our caller still holds the pipeline mutex. If
        // MAX_INFLIGHT_UPLOADS uploads are already running we await
        // right here — and because the caller holds the pipeline mutex
        // across this await, every other connection's `ingest()` for
        // this signal blocks behind it too. A saturated bucket
        // therefore *stalls ingest* (which propagates back to agents as
        // delayed BatchAcks) instead of letting finished builders pile
        // up unbounded in RAM. The owned permit moves into the spawned
        // task and is released when the upload completes, freeing the
        // slot for the next block.
        //
        // We acquire before touching the WAL lock, so a blocked
        // acquisition never holds the WAL lock — the in-flight upload
        // that will free a permit needs that lock for `mark_uploaded`,
        // so holding it here would deadlock.
        let permit = match self.upload_sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                let wait_start = Instant::now();
                if let Some(s) = self.upload_stats.as_ref() {
                    s.begin_wait();
                }
                let p = self
                    .upload_sem
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("upload semaphore is never closed");
                if let Some(s) = self.upload_stats.as_ref() {
                    s.end_wait(wait_start.elapsed().as_nanos() as u64);
                }
                p
            }
        };

        // Rotate the WAL *before* we drain the builder. Everything we
        // are about to upload is contained in (current segment & all
        // earlier sealed-but-not-uploaded segments). After rotation,
        // any subsequent appends go into a fresh segment that does
        // not participate in this block.
        let sealed = self
            .wal
            .lock()
            .await
            .rotate()
            .await
            .context("WAL rotate on spawn_upload")?;

        let new_builder = B::new(self.writer_uuid, self.cfg);
        let mut old_builder = std::mem::replace(&mut self.builder, new_builder);
        // The fresh builder is empty: the next record starts a new block.
        self.block_started_at = None;

        // Adaptive compression: decide *this* block's ZSTD level from the
        // current load now that we hold a permit and know the upload pool's
        // occupancy. The encode reads the level lazily, so overriding it
        // here (before the builder moves into the upload task) is what makes
        // the level reflect close-time load rather than open-time config.
        if self.adaptive_compression {
            old_builder.set_compression_level(self.adaptive_level());
        }

        // D-054 dedup watermark: the block we're about to encode durably
        // contains every record up to and including the segment `rotate()`
        // just sealed, for this shard's WAL. Stamp both into the meta so the
        // catalog can advance `H(writer, signal, shard)` on insert — the
        // exact seam the merged history+live query dedups against. Same
        // close-time-override idiom as `set_compression_level`.
        old_builder.set_wal_seg_max(sealed.0);
        old_builder.set_wal_shard(self.shard_index);

        let store = self.store.clone();
        let wal = self.wal.clone();
        let catalog = self.catalog.clone();
        let stats = self.upload_stats.clone();
        let event_sink = self.event_sink.clone();
        if let Some(s) = stats.as_ref() {
            s.start_inflight();
        }
        self.in_flight.spawn(async move {
            // Hold the permit for the upload's lifetime; dropping it
            // when the task ends frees a slot for the next block.
            let _permit = permit;
            run_upload::<B>(
                old_builder,
                sealed,
                store,
                wal,
                catalog,
                stats.as_ref(),
                event_sink.as_ref(),
            )
            .await;
            if let Some(s) = stats.as_ref() {
                s.finish_inflight();
            }
        });
        Ok(())
    }

    /// Reap completed upload tasks from the JoinSet. Non-blocking;
    /// only drains tasks that have already finished. Errors are logged
    /// (the panic / join error itself; per-upload errors are already
    /// logged inside `run_upload` before the task ends).
    fn reap_finished(&mut self) {
        while let Some(joined) = self.in_flight.try_join_next() {
            if let Err(e) = joined {
                warn!(error = %e, "upload task join error");
            }
        }
    }

    /// Stamp the open block's start time on the empty→non-empty
    /// transition. Idempotent while a block stays open (only the first
    /// record after a fresh builder sets it); cleared by `spawn_upload`.
    fn mark_block_started(&mut self) {
        if self.block_started_at.is_none() && !self.builder.is_empty() {
            self.block_started_at = Some(Instant::now());
        }
    }

    /// Seal + upload the open block if it has been accumulating for at
    /// least `max_age`, regardless of size. This is what makes a
    /// low-volume or idle signal queryable: without it a block only
    /// closes on the size-based `should_close` threshold (1M rows / 128
    /// MiB), so a trickle of data never seals and never lands in the
    /// bucket. Returns `true` if a block was sealed. Empty builders and
    /// not-yet-aged blocks are no-ops. Called on an interval by the
    /// server's per-signal flush ticker while holding the shard lock.
    pub async fn flush_if_aged(&mut self, max_age: Duration) -> Result<bool> {
        if self.builder.is_empty() {
            return Ok(false);
        }
        // `None` while non-empty shouldn't happen (the ingest paths and
        // replay both stamp it), but treat it as "seal now" rather than
        // leaking the block forever.
        let aged = self
            .block_started_at
            .map(|t| t.elapsed() >= max_age)
            .unwrap_or(true);
        if !aged {
            return Ok(false);
        }
        self.spawn_upload().await?;
        self.reap_finished();
        Ok(true)
    }

    /// Drain everything: rotate any remaining records into a final
    /// upload task and await all in-flight tasks. Called on graceful
    /// shutdown so we don't leave records sitting in the active block
    /// — the WAL still has them for replay, but the bucket is the
    /// source of truth so we'd rather close cleanly.
    pub async fn flush(&mut self) -> Result<()> {
        if !self.builder.is_empty() {
            self.spawn_upload().await?;
        }
        let mut errors = 0u64;
        while let Some(joined) = self.in_flight.join_next().await {
            if let Err(e) = joined {
                warn!(error = %e, "upload task join error during flush");
                errors += 1;
            }
        }
        if errors > 0 {
            anyhow::bail!("{errors} upload task(s) failed during flush");
        }
        Ok(())
    }
}

/// `INGEST_SHARDS` independent [`Pipeline`] instances for one signal.
///
/// Each connection is pinned to a single shard (chosen by its session
/// id — see [`ShardedPipeline::shard_for`]), so all of a connection's
/// batches for the signal funnel through one shard's `(WAL + builder +
/// mutex)` — preserving per-connection WAL ordering and replay — while
/// *different* connections land on different shards. That turns the
/// single per-signal ingest mutex (the measured throughput wall) into N
/// independent locks, each seeing only `connections / N`-way contention.
///
/// Shards are fully independent on the durability side: each owns a
/// distinct WAL subtree (`<wal_dir>/shard-NN/<signal>/`) so segment
/// files never collide and replay is per-shard. They *share* the object
/// store, the catalog, the writer identity, and — crucially — the global
/// upload semaphore, so the total number of concurrent encode+PUT tasks
/// across every shard and every signal stays bounded by the host's core
/// count (encode is CPU-bound; CPU is the shared resource).
///
/// Cheap to clone (one `Arc` bump): the accept loop clones it per
/// connection.
pub struct ShardedPipeline<B: BlockBuilder> {
    shards: Arc<Vec<Arc<Mutex<Pipeline<B>>>>>,
}

impl<B: BlockBuilder> Clone for ShardedPipeline<B> {
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
        }
    }
}

impl<B: BlockBuilder> ShardedPipeline<B> {
    /// Open `n` independent pipeline shards for one signal with the
    /// default [`BlockBuilderConfig`]. Mirrors [`Pipeline::open`]; see
    /// [`ShardedPipeline::open_with_config`] for the details and to force
    /// a non-default block size (tests).
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        n: usize,
        wal_dir: PathBuf,
        store: Arc<dyn ObjectStore>,
        catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
        writer_uuid: Uuid,
        decode: DecodeFn<B>,
        upload_sem: Arc<Semaphore>,
        upload_stats: Option<Arc<UploadStats>>,
    ) -> Result<Self> {
        Self::open_with_config(
            n,
            wal_dir,
            store,
            catalog,
            writer_uuid,
            decode,
            BlockBuilderConfig::default(),
            upload_sem,
            upload_stats,
            false,
        )
        .await
    }

    /// Open `n` independent pipeline shards for one signal. Each shard
    /// `k` opens its WAL under `<wal_dir>/shard-<k>/` (so the usual
    /// `<…>/<signal>/` subdir lands at `<wal_dir>/shard-<k>/<signal>/`
    /// and never collides with a sibling shard), replaying any leftover
    /// records from its own subtree. All shards share `store`, `catalog`,
    /// `writer_uuid`, the global `upload_sem`, and — when observed — the
    /// per-signal `upload_stats` gauge (so the stats endpoint sees the
    /// signal's totals aggregated across shards). `adaptive_compression`
    /// (from `--compression auto`) is applied to every shard so each picks
    /// its closing block's ZSTD level from live load.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_with_config(
        n: usize,
        wal_dir: PathBuf,
        store: Arc<dyn ObjectStore>,
        catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
        writer_uuid: Uuid,
        decode: DecodeFn<B>,
        cfg: BlockBuilderConfig,
        upload_sem: Arc<Semaphore>,
        upload_stats: Option<Arc<UploadStats>>,
        adaptive_compression: bool,
    ) -> Result<Self> {
        assert!(n >= 1, "ShardedPipeline needs at least one shard");
        let mut shards = Vec::with_capacity(n);
        for k in 0..n {
            let shard_wal = wal_dir.join(format!("shard-{k:02}"));
            let mut pipe = Pipeline::<B>::open_with_config(
                shard_wal,
                store.clone(),
                catalog.clone(),
                writer_uuid,
                decode,
                cfg,
            )
            .await
            .with_context(|| format!("opening {} shard {k}", B::SIGNAL))?
            .with_upload_sem(upload_sem.clone())
            .with_adaptive_compression(adaptive_compression)
            .with_shard_index(k as u32);
            if let Some(s) = upload_stats.as_ref() {
                pipe = pipe.with_upload_stats(s.clone());
            }
            shards.push(Arc::new(Mutex::new(pipe)));
        }
        Ok(Self {
            shards: Arc::new(shards),
        })
    }

    /// Attach a block-lifecycle event sink to every shard (multi-instance
    /// convergence). Call once at startup, before serving. Async because the
    /// shards are behind `tokio::sync::Mutex`; there's no contention yet, so
    /// this is just N uncontended locks. Off by default (single-instance).
    pub async fn with_event_sink(self, sink: Arc<dyn BlockEventSink>) -> Self {
        for shard in self.shards.iter() {
            shard.lock().await.set_event_sink(sink.clone());
        }
        self
    }

    /// The shard a connection's batches for this signal go to. Pinned by
    /// session id so a connection always hits the same shard (stable WAL
    /// ordering); modulo `N` spreads connections evenly across shards.
    pub fn shard_for(&self, session_id: u64) -> &Arc<Mutex<Pipeline<B>>> {
        &self.shards[session_id as usize % self.shards.len()]
    }

    /// All shards, for lifecycle operations that must touch every one
    /// (e.g. the final `flush` on shutdown).
    pub fn shards(&self) -> &[Arc<Mutex<Pipeline<B>>>] {
        &self.shards
    }
}

/// The body of an upload task: encode + PUT, then catch up the WAL and
/// catalog. Per-step errors are logged here; the task itself never
/// returns an error (so a single failed block doesn't poison the
/// JoinSet), but a failed upload leaves the sealed WAL segment in
/// place for the next process start to replay.
#[allow(clippy::too_many_arguments)]
async fn run_upload<B: BlockBuilder>(
    builder: B,
    sealed: SegmentId,
    store: Arc<dyn ObjectStore>,
    wal: Arc<Mutex<Wal>>,
    catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
    stats: Option<&Arc<UploadStats>>,
    event_sink: Option<&Arc<dyn BlockEventSink>>,
) {
    let upload_start = Instant::now();
    let upload_result = builder.finish_and_upload(store.as_ref()).await;
    let upload_nanos = upload_start.elapsed().as_nanos() as u64;
    match upload_result {
        Ok(Some(meta)) => {
            if let Some(s) = stats {
                s.record_success(meta.byte_size, upload_nanos);
            }
            // WAL release: the sealed segments through `sealed` have
            // been uploaded; safe to delete. We hold the WAL lock only
            // to validate + snapshot the signal dir (a comparison + a
            // PathBuf clone, microseconds), then drop it and do the slow
            // part — the directory scan and the unlinks — with *no* lock
            // held. The foreground ingest path takes the WAL lock under
            // the pipeline mutex on every batch, so unlinking under the
            // lock would periodically stall all ingest for this signal.
            let release = wal.lock().await.prepare_release(sealed);
            match release {
                Ok(dir) => {
                    if let Err(e) = Wal::release_segments(&dir, sealed).await {
                        warn!(
                            signal = B::SIGNAL,
                            sealed_seq = sealed.0,
                            error = %e,
                            "WAL release_segments after block upload"
                        );
                    }
                }
                Err(e) => warn!(
                    signal = B::SIGNAL,
                    sealed_seq = sealed.0,
                    error = %e,
                    "WAL prepare_release after block upload"
                ),
            }
            // Catalog update is best-effort by design: the bucket is
            // the source of truth, and reconcile_from_bucket can
            // always re-derive a missing row. We don't want a
            // transient sqlite hiccup to fail the ingest path.
            if let Some(cat) = catalog.as_ref() {
                match cat
                    .lock()
                    .expect("catalog mutex poisoned")
                    .insert_block(&meta)
                {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::debug!(block_uuid = %meta.uuid, "catalog row already present");
                    }
                    Err(e) => {
                        warn!(
                            signal = B::SIGNAL,
                            block_uuid = %meta.uuid,
                            error = %e,
                            "catalog insert failed; bucket has the data — recover via scry-list --reconcile"
                        );
                    }
                }
            }
            // Multi-instance convergence: announce the new block so peers
            // insert it into their catalogs without waiting for a poll / full
            // walk. Emitted on every successful upload regardless of the local
            // catalog insert result — the bucket has the data and peers need
            // to know. `emit` is non-blocking and never fails at the call
            // site (drop-on-full); polling backstops a dropped publish. No-op
            // when no sink is attached (single-instance).
            if let Some(sink) = event_sink {
                sink.emit(BlockEvent::Created { meta: meta.clone() });
            }
            info!(
                signal = B::SIGNAL,
                block_uuid = %meta.uuid,
                row_count = meta.row_count,
                byte_size = meta.byte_size,
                "block uploaded; WAL segments through {} released",
                sealed.0,
            );
        }
        Ok(None) => {
            // Builder was empty — vanishingly unlikely since
            // spawn_upload checks above, but possible if someone
            // called flush() under tight races. Leave the sealed WAL
            // segment in place; replay will pick it up next time.
            warn!(
                signal = B::SIGNAL,
                "upload produced no block; WAL segment retained for replay"
            );
        }
        Err(e) => {
            if let Some(s) = stats {
                s.record_failure();
            }
            // The upload failed. The sealed WAL segment is *not*
            // marked uploaded, so a future flush (or next-start
            // replay) will retry. We don't propagate the error from
            // the task — failing the task would also be invisible
            // because the JoinSet entry just records a returned unit.
            // Logging here is the recovery signal.
            warn!(
                signal = B::SIGNAL,
                sealed_seq = sealed.0,
                error = ?e,
                "block upload failed; WAL segment retained for replay"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        memory::InMemory, path::Path, CopyOptions, GetOptions, GetResult, ListResult,
        MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
        PutResult, RenameOptions, Result as OsResult,
    };
    use scry_block::{BlockBuilderConfig, DummyBlockBuilder};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The adaptive-compression policy, exhaustively. When the upload pool
    /// is full we pick DENSE regardless of CPU (the bucket is the wall).
    /// When the pool has slack the CPU decides: busy → FAST (encode is the
    /// wall, save CPU), comfortable → DENSE (we can afford to compress
    /// hard), and an unreadable load average → DENSE (safe default).
    #[test]
    fn adaptive_level_policy() {
        // Saturated → dense, whatever the load reads (even busy / unknown).
        assert_eq!(decide_adaptive_level(true, None), ADAPTIVE_DENSE_LEVEL);
        assert_eq!(decide_adaptive_level(true, Some(5.0)), ADAPTIVE_DENSE_LEVEL);
        assert_eq!(decide_adaptive_level(true, Some(0.1)), ADAPTIVE_DENSE_LEVEL);

        // Slack + busy CPU → fast. Test right at and above the threshold.
        assert_eq!(
            decide_adaptive_level(false, Some(ADAPTIVE_LOAD_BUSY_PER_CORE)),
            ADAPTIVE_FAST_LEVEL
        );
        assert_eq!(decide_adaptive_level(false, Some(2.0)), ADAPTIVE_FAST_LEVEL);

        // Slack + comfortable CPU → dense.
        assert_eq!(
            decide_adaptive_level(false, Some(ADAPTIVE_LOAD_BUSY_PER_CORE - 0.01)),
            ADAPTIVE_DENSE_LEVEL
        );
        assert_eq!(
            decide_adaptive_level(false, Some(0.0)),
            ADAPTIVE_DENSE_LEVEL
        );

        // Slack + unreadable load → dense (safe default).
        assert_eq!(decide_adaptive_level(false, None), ADAPTIVE_DENSE_LEVEL);
    }

    /// An `ObjectStore` wrapping `InMemory` whose writes block on a
    /// caller-controlled gate. Starting the gate at zero permits makes
    /// every `put` hang until the test calls `add_permits`, which lets
    /// us hold uploads "in flight" indefinitely and observe whether
    /// ingest backpressures. Everything except `put_opts` delegates
    /// straight to `InMemory`.
    struct GateStore {
        inner: InMemory,
        gate: Arc<Semaphore>,
        puts: Arc<AtomicU64>,
    }

    impl GateStore {
        fn new(gate: Arc<Semaphore>) -> Self {
            Self {
                inner: InMemory::new(),
                gate,
                puts: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl std::fmt::Display for GateStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "GateStore")
        }
    }
    impl std::fmt::Debug for GateStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("GateStore").finish()
        }
    }

    #[async_trait]
    impl ObjectStore for GateStore {
        async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> OsResult<PutResult> {
            // Block until the test opens the gate. The permit is
            // released immediately; we only use it as a one-way valve.
            let _g = self.gate.acquire().await.expect("gate semaphore closed");
            self.puts.fetch_add(1, Ordering::Relaxed);
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &Path, o: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(l, o).await
        }
        fn delete_stream(
            &self,
            l: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(p)
        }
        fn list_with_offset(
            &self,
            p: Option<&Path>,
            o: &Path,
        ) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list_with_offset(p, o)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(f, t, o).await
        }
        async fn rename_opts(&self, f: &Path, t: &Path, o: RenameOptions) -> OsResult<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }

    /// Decode stand-in: appends exactly one dummy record per batch so
    /// `max_rows = 1` closes a block on every `ingest`. Ignores the
    /// payload (the WAL stores it; we never replay in this test).
    fn append_one(_payload: &[u8], b: &mut DummyBlockBuilder) -> anyhow::Result<usize> {
        use scry_proto::streaming::DummyAppender;
        b.append_raw(1_000_000_000, b"k", b"v");
        Ok(1)
    }

    /// Regression test for the unbounded-RSS bug: a bucket slower than
    /// ingest must make `ingest` *block* once `CAP` uploads are
    /// outstanding, rather than spawning unbounded upload tasks that
    /// each pin a finished block in memory. Uses an explicit shared
    /// upload semaphore of `CAP` permits via `with_upload_sem`.
    ///
    /// With the gate held shut, the upload tasks never finish, so only
    /// `CAP` permits are ever handed out. The `CAP + 1`-th `ingest` must
    /// therefore stall at the permit acquisition — we assert progress
    /// plateaus at exactly `CAP`. Opening the gate then lets everything
    /// drain and all blocks upload.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_backpressures_when_uploads_stall() {
        // Explicit small shared pool, independent of the production
        // default — the test asserts ingest stalls once `CAP` uploads
        // are outstanding.
        const CAP: usize = 2;
        const N: u64 = 6; // > CAP

        let tmp = tempfile::tempdir().unwrap();
        let gate = Arc::new(Semaphore::new(0)); // shut
        let store = GateStore::new(gate.clone());
        let puts = store.puts.clone();
        let store: Arc<dyn ObjectStore> = Arc::new(store);

        let stats = Arc::new(UploadStats::default());
        let upload_sem = Arc::new(Semaphore::new(CAP));
        // every ingest closes a block
        let cfg = BlockBuilderConfig {
            max_rows: 1,
            ..Default::default()
        };

        let pipeline = Pipeline::<DummyBlockBuilder>::open_with_config(
            tmp.path().to_path_buf(),
            store,
            None,
            Uuid::now_v7(),
            append_one,
            cfg,
        )
        .await
        .unwrap()
        .with_upload_stats(stats.clone())
        .with_upload_sem(upload_sem.clone());
        let pipeline = Arc::new(Mutex::new(pipeline));

        // Drive N ingests from a separate task, counting how many
        // actually complete.
        let progress = Arc::new(AtomicU64::new(0));
        let driver = {
            let pipeline = pipeline.clone();
            let progress = progress.clone();
            tokio::spawn(async move {
                for _ in 0..N {
                    pipeline.lock().await.ingest(b"x").await.unwrap();
                    progress.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        // Let it run into the wall. With uploads stuck, progress must
        // plateau at MAX_INFLIGHT_UPLOADS (the rest of ingest is blocked
        // acquiring a permit).
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            progress.load(Ordering::Relaxed),
            CAP as u64,
            "ingest should stall once CAP uploads are outstanding"
        );
        // Nothing has uploaded yet (gate shut), so at most CAP blocks
        // are pinned in memory.
        assert_eq!(puts.load(Ordering::Relaxed), 0);

        // Open the gate; ingest should now run to completion.
        gate.add_permits(10_000);
        tokio::time::timeout(std::time::Duration::from_secs(10), driver)
            .await
            .expect("driver did not finish after gate opened")
            .unwrap();
        assert_eq!(progress.load(Ordering::Relaxed), N);

        // Drain remaining uploads and confirm every block landed
        // (2 puts each: parquet + meta.json).
        pipeline.lock().await.flush().await.unwrap();
        assert_eq!(puts.load(Ordering::Relaxed), N * 2);
    }

    /// The decode-out-of-lock path: a caller decodes a batch into a
    /// private scratch builder (here via the `append_one` stand-in),
    /// then commits it with `ingest_decoded`. The shared builder must
    /// close + upload on the same threshold as `ingest`, and the scratch
    /// must come back empty after each merge so it's reusable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_decoded_merges_and_closes() {
        const N: u64 = 4;

        let tmp = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // every committed batch closes a block
        let cfg = BlockBuilderConfig {
            max_rows: 1,
            ..Default::default()
        };

        let mut pipeline = Pipeline::<DummyBlockBuilder>::open_with_config(
            tmp.path().to_path_buf(),
            store.clone(),
            None,
            Uuid::now_v7(),
            append_one,
            cfg,
        )
        .await
        .unwrap();

        let decode_fn = pipeline.decode_fn();
        let mut scratch = pipeline.new_scratch();

        for _ in 0..N {
            // Phase 1: decode into the private scratch (lock-free in prod).
            let n = decode_fn(b"x", &mut scratch).unwrap();
            assert_eq!(n, 1);
            assert!(!scratch.is_empty(), "scratch holds the decoded record");
            // Phase 2: commit. Merge drains scratch back to empty.
            pipeline.ingest_decoded(b"x", &mut scratch).await.unwrap();
            assert!(scratch.is_empty(), "merge drains scratch for reuse");
        }

        // Flush and confirm every block landed (2 puts each).
        pipeline.flush().await.unwrap();
        use futures::StreamExt;
        let mut listing = store.list(None);
        let mut objs = 0u64;
        while listing.next().await.is_some() {
            objs += 1;
        }
        assert_eq!(objs, N * 2, "each closed block uploads parquet + meta.json");
    }

    /// The time-based flush seals an open block that is below the
    /// size-based `should_close` threshold — the case a low-volume or
    /// idle signal would otherwise never upload. `max_age = 0` makes any
    /// non-empty block immediately "aged".
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_if_aged_seals_below_size_threshold() {
        let tmp = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // A high threshold so size-based sealing never fires in this test.
        let cfg = BlockBuilderConfig {
            max_rows: 1_000_000,
            ..Default::default()
        };

        let mut pipeline = Pipeline::<DummyBlockBuilder>::open_with_config(
            tmp.path().to_path_buf(),
            store.clone(),
            None,
            Uuid::now_v7(),
            append_one,
            cfg,
        )
        .await
        .unwrap();

        // An empty builder is a no-op regardless of age.
        assert!(
            !pipeline
                .flush_if_aged(std::time::Duration::ZERO)
                .await
                .unwrap(),
            "empty builder never seals"
        );

        // One record — well under max_rows, so `should_close` stays false.
        pipeline.ingest(b"x").await.unwrap();

        // A large max_age means the block isn't old enough yet.
        assert!(
            !pipeline
                .flush_if_aged(std::time::Duration::from_secs(3600))
                .await
                .unwrap(),
            "a fresh block younger than max_age is not sealed"
        );

        // max_age = 0 ⇒ aged ⇒ seal it even though it's tiny.
        assert!(
            pipeline
                .flush_if_aged(std::time::Duration::ZERO)
                .await
                .unwrap(),
            "an aged non-empty block seals regardless of size"
        );
        pipeline.flush().await.unwrap();

        use futures::StreamExt;
        let mut listing = store.list(None);
        let mut objs = 0u64;
        while listing.next().await.is_some() {
            objs += 1;
        }
        assert_eq!(objs, 2, "the time-based flush uploaded parquet + meta.json");

        // After sealing, the builder is empty again — another aged flush
        // is a no-op (no duplicate block).
        assert!(
            !pipeline
                .flush_if_aged(std::time::Duration::ZERO)
                .await
                .unwrap(),
            "no block to seal after a flush"
        );
    }
}
