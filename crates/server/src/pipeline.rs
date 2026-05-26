//! Process-scoped Dummy durability pipeline.
//!
//! Owns the per-writer WAL, the active block builder, the destination
//! object store, and the optional online catalog. Shared across
//! sessions via `Arc<Mutex<_>>`. Per `ARCHITECTURE.md § The WAL` the
//! WAL is per-writer, not per-session — every connection that lands on
//! this process funnels Dummy ingest through the same pipeline.

use anyhow::{Context, Result};
use object_store::ObjectStore;
use scry_block::{BlockBuilderConfig, DummyBlockBuilder};
use scry_catalog::Catalog;
use scry_proto::streaming::decode_dummy_batch_into;
use scry_wal::{Wal, WalConfig};
use std::{path::PathBuf, sync::Arc};
use tracing::{info, warn};
use uuid::Uuid;

pub struct DummyPipeline {
    wal: Wal,
    builder: DummyBlockBuilder,
    store: Arc<dyn ObjectStore>,
    /// Optional online catalog. Updates here are best-effort: a failed
    /// insert (sqlite locked, disk full, etc.) is logged but does not
    /// fail the ingest path, because the bucket is the source of
    /// truth and a future `scry-list --reconcile` would re-derive the
    /// row anyway.
    catalog: Option<Catalog>,
    writer_uuid: Uuid,
    cfg: BlockBuilderConfig,
}

impl DummyPipeline {
    /// Open the WAL, replay any leftover records into a fresh builder,
    /// and return a pipeline ready to ingest. The replayed records
    /// are *not* re-acked to the agent (agents will resend any
    /// in-flight batches they hadn't yet seen an ack for, and dedup
    /// is a v0.2 concern), but they are durable and will be uploaded
    /// in the next flush.
    pub async fn open(
        wal_dir: PathBuf,
        store: Arc<dyn ObjectStore>,
        catalog: Option<Catalog>,
        writer_uuid: Uuid,
    ) -> Result<Self> {
        let wal = Wal::open(WalConfig::new(wal_dir, "dummy"))
            .await
            .context("opening Dummy WAL")?;

        let cfg = BlockBuilderConfig::default();
        let mut builder = DummyBlockBuilder::new(writer_uuid, cfg);
        let mut replayed_records = 0u64;
        let mut replayed_frames = 0u64;
        for frame in wal.replay().context("scanning WAL for replay")? {
            let payload = frame.context("reading WAL frame")?;
            let n = decode_dummy_batch_into(&payload, &mut builder)
                .map_err(|e| anyhow::anyhow!("WAL replay: decode DummyBatch: {e}"))?;
            replayed_records += n as u64;
            replayed_frames += 1;
        }
        if replayed_records > 0 {
            info!(
                replayed_records,
                replayed_frames, "WAL replay complete; records merged into next block"
            );
        }

        Ok(Self {
            wal,
            builder,
            store,
            catalog,
            writer_uuid,
            cfg,
        })
    }

    /// Append a single DummyBatch payload (already zstd-decoded; the
    /// binschema-encoded form is what hits the WAL). On replay we
    /// decode the same bytes back into records, so this is the unit
    /// of crash-recovery atomicity. Auto-flushes if the builder hits
    /// its close threshold.
    ///
    /// Decode is streaming: we never materialise a `DummyBatch` /
    /// `Vec<DummyRecord>` / per-record `String` + `Vec<u8>`. See
    /// [`scry_proto::streaming`] for the rationale.
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
        self.wal.append(payload).await.context("WAL append")?;

        let n = decode_dummy_batch_into(payload, &mut self.builder)
            .map_err(|e| anyhow::anyhow!("DummyBatch: {e}"))?
            as u64;

        if self.builder.should_close() {
            self.flush().await?;
        }
        Ok(n)
    }

    /// Seal the active WAL segment, upload the active block, and
    /// delete the uploaded WAL segments. A no-op if the builder is
    /// empty (no segment to seal, nothing to upload).
    pub async fn flush(&mut self) -> Result<()> {
        if self.builder.is_empty() {
            return Ok(());
        }
        // Rotate the WAL *before* we drain the builder. Everything we
        // are about to upload is contained in (current segment & all
        // earlier sealed-but-not-uploaded segments). After rotation,
        // any subsequent appends go into a fresh segment that does
        // not participate in this block.
        let sealed = self.wal.rotate().await.context("WAL rotate on flush")?;

        let new_builder = DummyBlockBuilder::new(self.writer_uuid, self.cfg);
        let old_builder = std::mem::replace(&mut self.builder, new_builder);
        match old_builder.finish_and_upload(self.store.as_ref()).await {
            Ok(Some(meta)) => {
                self.wal
                    .mark_uploaded(sealed)
                    .await
                    .context("WAL mark_uploaded after block upload")?;
                // Catalog update is best-effort by design: the bucket
                // is the source of truth, and reconcile_from_bucket
                // can always re-derive a missing row. We don't want a
                // transient sqlite hiccup to fail the ingest path.
                if let Some(cat) = self.catalog.as_ref() {
                    match cat.insert_block(&meta) {
                        Ok(true) => {}
                        Ok(false) => {
                            tracing::debug!(block_uuid = %meta.uuid, "catalog row already present");
                        }
                        Err(e) => {
                            warn!(
                                block_uuid = %meta.uuid,
                                error = %e,
                                "catalog insert failed; bucket has the data — recover via scry-list --reconcile"
                            );
                        }
                    }
                }
                info!(
                    block_uuid = %meta.uuid,
                    row_count = meta.row_count,
                    byte_size = meta.byte_size,
                    "dummy block uploaded; WAL segments through {} released",
                    sealed.0,
                );
            }
            Ok(None) => {
                // Builder was empty after rotation — vanishingly
                // unlikely since we checked above, but possible if
                // someone called flush() under tight races. Leave the
                // sealed WAL segment in place; replay will pick it up
                // next time.
                warn!("flush() produced no block; WAL segment retained for replay");
            }
            Err(e) => {
                // The upload failed. The sealed WAL segment is *not*
                // marked uploaded, so a future flush (or next-start
                // replay) will retry. Returning the error lets the
                // caller decide whether the ingest path should fail
                // the batch.
                return Err(e.context("dummy block upload"));
            }
        }
        Ok(())
    }
}
