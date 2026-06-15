//! Compaction pass driver: plan → merge → supersede → (grace) → delete.
//!
//! [`compact_once`] runs a single pass over the catalog's live blocks,
//! delegating each planned merge to [`compact_partition`], which executes the
//! full `ARCHITECTURE.md § Compaction § Per-merge sequence` lifecycle:
//!
//! 1. Merge the inputs into one block at the next level (uploaded, meta
//!    last — and the meta PUT is the **commit-point fence**, see
//!    [`merge_blocks`]).
//! 2. Insert the merged block into the catalog; emit `Created`.
//! 3. Mark the inputs `superseded_by = merged` — **at this point queries
//!    read the merged block, not the inputs** (the query path filters
//!    `superseded_by IS NULL`); emit `Superseded`.
//! 4. Wait the configured grace period (default 0 single-instance).
//! 5. Delete the input objects from the bucket.
//! 6. Drop the input catalog rows; emit `Deleted`.
//!
//! The catalog is derived state, so the bucket truth (step 5) is removed
//! before the catalog rows (step 6). If a merge fails partway, the
//! immutable + content-addressed design means the worst case is an
//! orphaned merged block that the next pass treats as just another input
//! at its level — correctness is never at risk.
//!
//! ## Fencing (multi-instance)
//!
//! [`compact_partition`] takes a [`Fence`] (the "do I still hold the lease?"
//! re-check) and consults it before every irreversible step: inside the merge
//! right before the `meta.json` commit, before `mark_superseded`, and again
//! after the grace window before the deletes. A lost lease aborts the
//! partition cleanly — inputs survive, the rightful holder re-merges. The
//! single-instance path ([`compact_once`]) passes [`AlwaysValid`] + a
//! [`NoopSink`], so its behaviour is byte-for-byte what it was before v0.9.

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use scry_block::{
    delete_block_objects, AlwaysValid, BlockBuilderConfig, BlockEvent, BlockEventSink, Fence,
    NoopSink,
};
use scry_catalog::{Catalog, CatalogHandle};
use uuid::Uuid;

use crate::merge::merge_blocks;
use crate::policy::{plan_merges, CompactConfig, PlannedMerge};

/// Outcome of one [`compact_once`] pass.
#[derive(Debug, Clone, Default)]
pub struct CompactReport {
    /// Number of merges executed (committed; fence-aborted merges don't count).
    pub merges: usize,
    /// Total input blocks consumed across all merges.
    pub blocks_in: usize,
    /// Merged blocks produced (one per merge).
    pub blocks_out: usize,
    /// On-disk bytes of the merged main parquets produced.
    pub bytes_out: u64,
    /// Partitions abandoned because the lease was lost mid-merge (the
    /// commit-point fence, or a fence check before a destructive step).
    pub aborted: usize,
}

/// Run a single compaction pass over a privately-owned catalog. Returns a
/// report; an empty report (`merges == 0`) means no partition had enough
/// blocks to compact.
///
/// This is the **single-instance** entry point: it plans every eligible
/// partition and runs each through [`compact_partition`] with an
/// [`AlwaysValid`] fence (there is no lease to lose with one actor) and a
/// [`NoopSink`] (no peers to notify). Signature and behaviour are unchanged
/// from v0.8.
pub async fn compact_once(
    store: Arc<dyn ObjectStore>,
    catalog: &Catalog,
    bucket: &str,
    cfg: &CompactConfig,
    block_cfg: &BlockBuilderConfig,
) -> Result<CompactReport> {
    let live = catalog.list_blocks().context("list live blocks")?;
    let plans = plan_merges(&live, cfg);
    let mut report = CompactReport::default();

    // One compactor identity for this pass — block paths are
    // content-addressed under it (`<signal>/.../<writer_id>/<uuid>`).
    let writer_id = Uuid::now_v7();
    let fence = AlwaysValid;
    let sink = NoopSink;

    for plan in plans {
        let outcome = compact_partition(
            &plan,
            store.clone(),
            catalog,
            bucket,
            writer_id,
            cfg,
            block_cfg,
            &fence,
            &sink,
        )
        .await
        .with_context(|| format!("compacting {} {} partition", plan.signal, plan.date))?;
        report.absorb(&outcome, plan.inputs.len());
    }

    Ok(report)
}

/// Outcome of compacting one partition. The merged-block bytes/inputs count
/// is folded into the pass-level [`CompactReport`] by [`CompactReport::absorb`].
#[derive(Debug, Clone)]
pub enum PartitionOutcome {
    /// The merge committed: this is the merged block's `byte_size`.
    Merged { bytes_out: u64 },
    /// The lease was lost before the merge committed (or before a destructive
    /// step). Inputs are intact; nothing was superseded or deleted.
    Aborted,
}

impl CompactReport {
    fn absorb(&mut self, outcome: &PartitionOutcome, inputs: usize) {
        match outcome {
            PartitionOutcome::Merged { bytes_out } => {
                self.merges += 1;
                self.blocks_in += inputs;
                self.blocks_out += 1;
                self.bytes_out += bytes_out;
            }
            PartitionOutcome::Aborted => {
                self.aborted += 1;
            }
        }
    }
}

/// Execute the full per-merge lifecycle for one planned partition.
///
/// Generic over [`CatalogHandle`] so the same routine serves the
/// single-instance path (`&Catalog`) and the multi-instance daemon
/// (`&Mutex<Catalog>` shared with the convergence consumer). The catalog lock
/// is acquired only for the individual synchronous SQLite calls (`with(...)`),
/// **never** across the merge or the object DELETEs.
///
/// `fence` is consulted before each irreversible step (and inside the merge,
/// before the `meta.json` commit); a lost lease returns
/// [`PartitionOutcome::Aborted`] with inputs intact. `sink` receives a
/// `Created` / `Superseded` / `Deleted` event at each lifecycle point so peers
/// converge promptly (a [`NoopSink`] for single-instance).
#[allow(clippy::too_many_arguments)]
pub async fn compact_partition<C: CatalogHandle>(
    plan: &PlannedMerge,
    store: Arc<dyn ObjectStore>,
    catalog: &C,
    bucket: &str,
    writer_id: Uuid,
    cfg: &CompactConfig,
    block_cfg: &BlockBuilderConfig,
    fence: &dyn Fence,
    sink: &dyn BlockEventSink,
) -> Result<PartitionOutcome> {
    let input_uuids: Vec<Uuid> = plan.inputs.iter().map(|e| e.meta.uuid).collect();
    tracing::info!(
        signal = %plan.signal,
        date = %plan.date,
        input_level = plan.input_level,
        output_level = plan.output_level(),
        inputs = plan.inputs.len(),
        "compacting partition"
    );

    // Cheap early bail: if we already don't hold the lease, don't even start
    // the (expensive) merge.
    if fence.check().is_err() {
        tracing::warn!(signal = %plan.signal, date = %plan.date, "lease lost before merge; skipping partition");
        return Ok(PartitionOutcome::Aborted);
    }

    // 1. Merge → upload data objects → fenced meta.json commit. `None` means
    //    the fence tripped before the commit; the inputs are untouched.
    let merged = match merge_blocks(
        store.clone(),
        bucket,
        &plan.signal,
        &plan.inputs,
        plan.output_level(),
        writer_id,
        block_cfg,
        fence,
    )
    .await
    .with_context(|| format!("merging {} {} blocks", plan.inputs.len(), plan.signal))?
    {
        Some(m) => m,
        None => return Ok(PartitionOutcome::Aborted),
    };

    // 2. Re-check the fence before mutating any catalog state. The merge may
    //    have run for minutes; if the lease was lost we must not publish the
    //    merged block or supersede the inputs. The merged objects committed in
    //    step 1 become a bucket-level leak (reclaimed by a future full
    //    walk / orphan-GC) — but the local catalog stays clean (just the live
    //    inputs) and the inputs are left for the rightful holder to re-merge.
    if fence.check().is_err() {
        tracing::warn!(signal = %plan.signal, date = %plan.date, "lease lost after merge commit; leaving inputs live");
        return Ok(PartitionOutcome::Aborted);
    }

    // 3. Insert the merged block and supersede the inputs back-to-back (two
    //    quick synchronous catalog calls) so queries switch from the inputs to
    //    the merged block atomically from a reader's view. Announce both to
    //    peers.
    catalog
        .with(|c| c.insert_block(&merged))
        .context("insert merged block")?;
    sink.emit(BlockEvent::Created {
        meta: merged.clone(),
    });
    catalog
        .with(|c| c.mark_superseded(&input_uuids, merged.uuid))
        .context("mark inputs superseded")?;
    sink.emit(BlockEvent::Superseded {
        inputs: input_uuids.clone(),
        by: merged.uuid,
        by_meta: merged.clone(),
    });

    // 4. Grace period (single-instance default is 0).
    if !cfg.grace.is_zero() {
        tokio::time::sleep(cfg.grace).await;
    }

    // 5. Delete the input objects from the bucket. Fence once more — the grace
    //    window may have been long enough to lose the lease.
    if fence.check().is_err() {
        tracing::warn!(signal = %plan.signal, date = %plan.date, "lease lost before input delete; inputs remain superseded but not reaped");
        return Ok(PartitionOutcome::Aborted);
    }
    for input in &plan.inputs {
        delete_block_objects(store.as_ref(), &input.meta)
            .await
            .context("delete superseded input objects")?;
    }

    // 6. Drop the input catalog rows; announce the deletion to peers.
    catalog
        .with(|c| c.delete_blocks(&input_uuids))
        .context("delete superseded input rows")?;
    sink.emit(BlockEvent::Deleted {
        signal: plan.signal.clone(),
        uuids: input_uuids,
    });

    Ok(PartitionOutcome::Merged {
        bytes_out: merged.byte_size,
    })
}
