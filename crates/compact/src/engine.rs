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
use futures::StreamExt;
use object_store::ObjectStore;
use scry_block::{
    delete_block_objects, AlwaysValid, BlockBuilderConfig, BlockEvent, BlockEventSink, Fence,
    NoopSink,
};
use scry_catalog::{Catalog, CatalogHandle, PendingReap};
use uuid::Uuid;

use crate::merge::merge_blocks;
use crate::policy::{plan_merges, CompactConfig, OversizedPartition, PlannedMerge};

/// Surface partitions the planner declined. These are operator-actionable
/// (raise `--compact-max-level`, restore the previous `--compact-fanout`, or
/// accept the partition as terminal), and silently never compacting would be
/// worse than a line per pass.
pub fn warn_oversized(oversized: &[OversizedPartition]) {
    for p in oversized {
        tracing::warn!(
            signal = %p.signal,
            date = %p.date,
            input_level = p.input_level,
            projected_ancestors = p.projected_ancestors,
            limit = scry_block::MAX_COMPACTED_ANCESTORS,
            "partition cannot compact: merged block's ancestry would exceed the sidecar limit \
             (blocks were likely built with a smaller --compact-fanout); skipping it"
        );
    }
}

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
    /// Partitions abandoned because the lease was lost before commit.
    pub aborted: usize,
    /// Previously-superseded input blocks physically reaped this pass.
    pub reaped: usize,
    /// Pending inputs whose object deletion failed and will be retried.
    pub reap_failed: usize,
    /// Partitions whose merge/reconciliation failed while the pass continued.
    pub partition_failed: usize,
    /// Eligible partitions currently leased by another maintenance instance.
    pub lease_held: usize,
    /// Eligible partitions skipped because the lease backend was unavailable.
    pub lease_unavailable: usize,
    /// Eligible partitions declined because the merged output's ancestor
    /// closure would exceed the sidecar cap (usually a `--compact-fanout`
    /// changed between runs). These never make progress until the operator
    /// intervenes, so the pass reports them rather than failing.
    pub oversized: usize,
}

/// Run a single compaction pass over a privately-owned catalog. Returns a
/// report; an empty report (`merges == 0`) means no partition had enough
/// blocks to compact.
///
/// This is the **single-instance** entry point: it plans every eligible
/// partition and runs each through [`compact_partition`] with an
/// [`AlwaysValid`] fence (there is no lease to lose with one actor) and a
/// [`NoopSink`] (no peers to notify).
///
/// Takes `Arc<Mutex<Catalog>>` rather than `&Catalog` so that when
/// `parallelism > 1` the merge I/O for multiple partitions can overlap while
/// the catalog ops (milliseconds) are serialized. The caller wraps its catalog
/// once and passes it in.
pub async fn compact_once(
    store: Arc<dyn ObjectStore>,
    catalog: &Arc<std::sync::Mutex<Catalog>>,
    bucket: &str,
    cfg: &CompactConfig,
    block_cfg: &BlockBuilderConfig,
) -> Result<CompactReport> {
    cfg.validate().context("invalid compaction policy")?;
    let live = catalog
        .with(|c| c.list_blocks())
        .context("list live blocks")?;
    let plan = plan_merges(&live, cfg);
    let plans = plan.merges;
    let mut report = CompactReport {
        oversized: plan.oversized.len(),
        ..Default::default()
    };
    warn_oversized(&plan.oversized);
    let now = now_unix_nano();
    let pending = catalog
        .with(|c| c.list_pending_reaps(now))
        .context("list pending compaction reaps")?;
    reap_pending(store.clone(), catalog, &pending, &NoopSink, &mut report).await;

    // One compactor identity for this pass — block paths are
    // content-addressed under it (`<signal>/.../<writer_id>/<uuid>`).
    let writer_id = Uuid::now_v7();

    let parallelism = cfg.parallelism.max(1);
    let results: Vec<_> = futures::stream::iter(plans.into_iter().map(|plan| {
        let store = store.clone();
        let catalog = catalog.clone();
        let bucket = bucket.to_string();
        let cfg = cfg.clone();
        let block_cfg = block_cfg.clone();
        async move {
            let inputs = plan.inputs.len();
            let label = format!("{} {}", plan.signal, plan.date);
            let outcome = compact_partition(
                &plan,
                store,
                &catalog,
                &bucket,
                writer_id,
                &cfg,
                &block_cfg,
                &AlwaysValid,
                &NoopSink,
            )
            .await
            .with_context(|| format!("compacting {label} partition"))?;
            Ok::<_, anyhow::Error>((outcome, inputs))
        }
    }))
    .buffer_unordered(parallelism)
    .collect()
    .await;

    for result in results {
        let (outcome, inputs) = result?;
        report.absorb(&outcome, inputs);
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
    pub fn absorb(&mut self, outcome: &PartitionOutcome, inputs: usize) {
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

    // meta.json is the logical commit point. Once it exists, catalog
    // publication is mandatory idempotent completion work even if lease renewal
    // failed immediately afterwards; a later holder will reconcile the same
    // committed output before planning.
    let eligible_at = now_unix_nano().saturating_add(cfg.grace.as_nanos() as u64);
    catalog
        .with(|c| c.apply_compaction(&merged, &input_uuids, eligible_at))
        .context("atomically apply committed compaction")?;
    sink.emit(BlockEvent::Created {
        meta: merged.clone(),
    });
    sink.emit(BlockEvent::Superseded {
        inputs: input_uuids,
        by: merged.uuid,
        by_meta: merged.clone(),
        reap_eligible_at_unix_nano: eligible_at,
    });

    // Grace is an eligibility timestamp, never an inline sleep. If immediate
    // cleanup is allowed and the fence remains valid, try it now; otherwise the
    // durable pending rows are retried by a later maintenance pass.
    if cfg.grace.is_zero() && fence.check().is_ok() {
        let pending = catalog
            .with(|c| c.list_pending_reaps(now_unix_nano()))
            .context("list newly pending compaction reaps")?;
        let mut report = CompactReport::default();
        reap_pending(store, catalog, &pending, sink, &mut report).await;
    }

    Ok(PartitionOutcome::Merged {
        bytes_out: merged.byte_size,
    })
}

/// Retry physically deleting superseded inputs whose eligibility deadline has
/// passed. Each input is independent; failures remain durable pending work and
/// do not prevent other inputs from progressing.
pub async fn reap_pending<C: CatalogHandle>(
    store: Arc<dyn ObjectStore>,
    catalog: &C,
    pending: &[PendingReap],
    sink: &dyn BlockEventSink,
    report: &mut CompactReport,
) {
    for reap in pending {
        match delete_block_objects(store.as_ref(), &reap.entry.meta).await {
            Ok(()) => {
                let uuid = reap.entry.meta.uuid;
                match catalog.with(|c| c.delete_blocks(&[uuid])) {
                    Ok(()) => {
                        report.reaped += 1;
                        sink.emit(BlockEvent::Deleted {
                            signal: reap.entry.meta.signal.clone(),
                            uuids: vec![uuid],
                        });
                    }
                    Err(e) => {
                        report.reap_failed += 1;
                        tracing::warn!(%uuid, error = %e, "deleted compacted objects but catalog cleanup failed; retrying later");
                    }
                }
            }
            Err(e) => {
                report.reap_failed += 1;
                tracing::warn!(
                    uuid = %reap.entry.meta.uuid,
                    error = %e,
                    "compaction input reap failed; durable pending row will retry"
                );
            }
        }
    }
}

fn now_unix_nano() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
