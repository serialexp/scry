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
//! ## Background upload
//!
//! The slow part of closing a block (parquet encode + S3 PUT, ~3 s for
//! a 46 MiB block on Garage) used to run inline inside `ingest()`,
//! pinning the pipeline mutex and blocking every subsequent inbound
//! batch on every connection. That made the server ack-bound on upload
//! latency rather than on its own ingest throughput.
//!
//! Now: when the builder hits `should_close`, the WAL is rotated and
//! the full builder is swapped out for a fresh one synchronously (both
//! are fast — fsync + `mem::replace`), then the slow upload is spawned
//! as a tokio task. The task acquires a permit from a small semaphore
//! (`MAX_INFLIGHT_UPLOADS`) so we never pile up unbounded blocks under
//! a slow bucket; when the upload finishes it briefly re-acquires the
//! WAL and catalog locks to call `mark_uploaded` and `insert_block`.
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
use std::{path::PathBuf, sync::Arc};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinSet,
};
use tracing::{info, warn};
use uuid::Uuid;

/// Maximum number of block uploads in flight concurrently. Two gives
/// us one block actively uploading while the next one finishes filling,
/// without unbounded growth under a slow bucket. Hardcoded for v0.2;
/// promote to `BlockBuilderConfig` (or a dedicated `IngestConfig`) if
/// a real workload ever justifies tuning it.
const MAX_INFLIGHT_UPLOADS: usize = 2;

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
    /// up in RAM. When the permit count is exhausted, the next spawn
    /// awaits the permit, which transitively backpressures the ingest
    /// path through the pipeline mutex held by the caller.
    upload_sem: Arc<Semaphore>,
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
        let wal = Wal::open(WalConfig::new(wal_dir, B::SIGNAL))
            .await
            .with_context(|| format!("opening {} WAL", B::SIGNAL))?;

        let cfg = BlockBuilderConfig::default();
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
        })
    }

    /// Append a single batch payload (already zstd-decoded; the
    /// binschema-encoded form is what hits the WAL). On replay we
    /// decode the same bytes back into records, so this is the unit
    /// of crash-recovery atomicity. Auto-spawns a background upload
    /// task if the builder hits its close threshold.
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
        let sem = self.upload_sem.clone();
        self.in_flight.spawn(async move {
            // Acquire the permit *inside* the task. If
            // MAX_INFLIGHT_UPLOADS are already in flight, we wait
            // here without holding the pipeline mutex. Owned variant
            // so the permit's lifetime is tied to the task, not to a
            // borrow of the semaphore.
            let _permit = sem
                .acquire_owned()
                .await
                .expect("upload semaphore is never closed");
            run_upload::<B>(old_builder, sealed, store, wal, catalog).await;
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
) {
    match builder.finish_and_upload(store.as_ref()).await {
        Ok(Some(meta)) => {
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
