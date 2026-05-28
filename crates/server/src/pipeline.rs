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
//! When the upload finishes the task briefly re-acquires the WAL and
//! catalog locks to call `mark_uploaded` and `insert_block`, then drops
//! the permit.
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
//! is negligible: append/rotate take microseconds, mark_uploaded is a
//! handful of `unlink` syscalls, and `insert_block` is a single SQLite
//! INSERT.

use anyhow::{Context, Result};
use object_store::ObjectStore;
use scry_block::{BlockBuilder, BlockBuilderConfig};
use scry_catalog::Catalog;
use scry_wal::{SegmentId, Wal, WalConfig};
use std::{path::PathBuf, sync::Arc, time::Instant};
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
/// In production (noise-sink) this default is overridden: all signals
/// share a single semaphore sized to the host's *physical* core count,
/// because the dominant cost of an upload is the parquet encode (sort +
/// Arrow build + zstd), which is CPU-bound and runs on the blocking
/// pool. A shared pool lets one hot signal use every core while still
/// bounding concurrent in-flight blocks (and therefore RAM).
pub const MAX_INFLIGHT_UPLOADS: usize = 2;

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
    catalog: Option<Arc<Mutex<Catalog>>>,
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
        catalog: Option<Arc<Mutex<Catalog>>>,
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
        catalog: Option<Arc<Mutex<Catalog>>>,
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
        })
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
    /// The record count for the ack comes from the decode the caller
    /// already performed, so this returns `()` rather than a count.
    pub async fn ingest_decoded(&mut self, payload: &[u8], scratch: &mut B) -> Result<()> {
        // Order matters: WAL first, builder second — same invariant as
        // `ingest`. If the WAL append fails we never merge the scratch
        // records into the shared builder; the agent sees the BatchAck
        // failure and retries, and the caller resets the scratch.
        self.wal
            .lock()
            .await
            .append(payload)
            .await
            .context("WAL append")?;

        self.builder.merge(scratch);

        if self.builder.should_close() {
            self.spawn_upload().await?;
        }
        // Reap any finished upload tasks so the JoinSet doesn't grow
        // unbounded. Non-blocking.
        self.reap_finished();
        Ok(())
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
        let old_builder = std::mem::replace(&mut self.builder, new_builder);

        let store = self.store.clone();
        let wal = self.wal.clone();
        let catalog = self.catalog.clone();
        let stats = self.upload_stats.clone();
        if let Some(s) = stats.as_ref() {
            s.start_inflight();
        }
        self.in_flight.spawn(async move {
            // Hold the permit for the upload's lifetime; dropping it
            // when the task ends frees a slot for the next block.
            let _permit = permit;
            run_upload::<B>(old_builder, sealed, store, wal, catalog, stats.as_ref()).await;
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

/// The body of an upload task: encode + PUT, then catch up the WAL and
/// catalog. Per-step errors are logged here; the task itself never
/// returns an error (so a single failed block doesn't poison the
/// JoinSet), but a failed upload leaves the sealed WAL segment in
/// place for the next process start to replay.
async fn run_upload<B: BlockBuilder>(
    builder: B,
    sealed: SegmentId,
    store: Arc<dyn ObjectStore>,
    wal: Arc<Mutex<Wal>>,
    catalog: Option<Arc<Mutex<Catalog>>>,
    stats: Option<&Arc<UploadStats>>,
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
            // been uploaded; safe to delete. We re-acquire the lock
            // briefly here; the ingest path's `append` will contend
            // with us for a few microseconds.
            if let Err(e) = wal.lock().await.mark_uploaded(sealed).await {
                warn!(
                    signal = B::SIGNAL,
                    sealed_seq = sealed.0,
                    error = %e,
                    "WAL mark_uploaded after block upload"
                );
            }
            // Catalog update is best-effort by design: the bucket is
            // the source of truth, and reconcile_from_bucket can
            // always re-derive a missing row. We don't want a
            // transient sqlite hiccup to fail the ingest path.
            if let Some(cat) = catalog.as_ref() {
                match cat.lock().await.insert_block(&meta) {
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
            warn!(signal = B::SIGNAL, "upload produced no block; WAL segment retained for replay");
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
                error = %e,
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
        async fn put_opts(
            &self,
            l: &Path,
            p: PutPayload,
            o: PutOptions,
        ) -> OsResult<PutResult> {
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
        let cfg = BlockBuilderConfig { max_rows: 1, ..Default::default() };

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
        let cfg = BlockBuilderConfig { max_rows: 1, ..Default::default() };

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
}
