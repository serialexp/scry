//! The lease-guarded maintenance loop: compaction + retention across N
//! instances sharing one bucket.
//!
//! Each pass plans work from the local catalog, then for every unit of
//! destructive work tries to acquire a lease ([`LeaseProvider`]). Only the
//! holder acts; peers that lose the race skip that unit this pass. The
//! acquired guard's [`Fence`](scry_block::Fence) is threaded into the engine
//! ([`compact_partition`] / [`retain_planned`]) so a lease lost mid-operation
//! aborts before any irreversible step — see the commit-point fence in
//! `scry-compact`.
//!
//! Lease granularity (per the v0.9 plan):
//! - **compaction** — one lease per `(signal, date, input_level)` partition,
//!   so independent partitions compact concurrently across instances;
//! - **retention** — one global lease (`lease/retention`), since a
//!   retention pass spans all signals and is cheap.
//!
//! Lease keys here are **logical**: this crate knows nothing about Valkey, so
//! it names a lease and lets the provider decide where that lives. The Valkey
//! provider prefixes them with the deployment namespace
//! (`scry_valkey::Keyspace`), which is what keeps two deployments sharing one
//! Valkey from contending for each other's leases.
//!
//! `try_acquire` returning `Err` (backend unreachable) pauses that unit: no
//! lease ⇒ no destructive work. The functions here are the unit-testable
//! cores; the daemon drives them on a timer (Phase 6).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use object_store::ObjectStore;
use scry_block::{BlockBuilderConfig, BlockEvent, BlockEventSink};
use scry_catalog::CatalogHandle;
use scry_compact::{
    compact_partition, plan_merges, reap_pending, CompactConfig, CompactReport, PartitionOutcome,
};
use scry_retention::{
    plan_reaping, reap_pending_deletions, retain_planned, RetentionConfig, RetentionReport,
};
use uuid::Uuid;

use crate::lease::{LeaseGuard, LeaseProvider};
use crate::poll::reconcile_partition;

/// Logical lease key for a compaction partition.
fn compaction_lease_key(signal: &str, date: &str, input_level: u32) -> String {
    format!("lease/compact/{signal}/{date}/{input_level}")
}

/// The single global retention lease's logical key.
pub const RETENTION_LEASE_KEY: &str = "lease/retention";

/// Re-emit a `SoftDeleted` for every block that is hidden but not yet reaped.
///
/// A staging announcement is one-shot, and the thing consuming it — the Valkey
/// staged-deletions registry — holds entries under a TTL sized for a reap that
/// happens on schedule. This is what keeps that TTL honest when the reap
/// stalls, so an entry lapses only once the block is actually gone.
///
/// Best-effort and non-fatal: a catalog read failure here must not stop the
/// pass from doing its real work, and the sink itself is drop-on-full.
/// Grouped per `(signal, deadline pair)`: the signal is the pub/sub channel
/// selector, and the two timestamps have to stay paired because a receiver
/// derives the grace *duration* from their difference.
fn announce_outstanding_deletions<C: CatalogHandle>(catalog: &C, sink: &dyn BlockEventSink) {
    let staged = match catalog.with(|c| c.list_staged_deletions()) {
        Ok(s) if !s.is_empty() => s,
        Ok(_) => return,
        Err(e) => {
            tracing::warn!(error = %e, "listing outstanding deletions failed; the staged-deletions registry may expire while objects remain");
            return;
        }
    };

    let mut by_key: BTreeMap<(String, u64, u64), Vec<Uuid>> = BTreeMap::new();
    for (uuid, signal, deleted_at, eligible_at) in staged {
        by_key
            .entry((signal, deleted_at, eligible_at))
            .or_default()
            .push(uuid);
    }
    let groups = by_key.len();
    let blocks: usize = by_key.values().map(|v| v.len()).sum();
    for ((signal, deleted_at, eligible_at), uuids) in by_key {
        sink.emit(BlockEvent::SoftDeleted {
            signal,
            uuids,
            deleted_at_unix_nano: deleted_at,
            delete_eligible_at_unix_nano: eligible_at,
        });
    }
    tracing::debug!(blocks, groups, "re-announced outstanding staged deletions");
}

/// Run one lease-guarded compaction pass. Plans every eligible partition;
/// for each, tries to acquire its lease and (if won) runs the full merge
/// lifecycle under the lease's fence, emitting events through `sink`.
/// Partitions held by a peer — or whose lease backend is unreachable — are
/// skipped this pass.
#[allow(clippy::too_many_arguments)]
pub async fn run_compaction_pass<L, C>(
    provider: &L,
    store: Arc<dyn ObjectStore>,
    catalog: &C,
    bucket: &str,
    cfg: &CompactConfig,
    block_cfg: &BlockBuilderConfig,
    sink: &dyn BlockEventSink,
    lease_ttl: Duration,
) -> Result<CompactReport>
where
    L: LeaseProvider,
    C: CatalogHandle,
{
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
    scry_compact::warn_oversized(&plan.oversized);

    // Reaping is durable catalog work, independent of whether this pass finds a
    // new merge. Partition leases still fence new logical commits; cleanup is
    // idempotent and only touches inputs already marked non-live.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let pending = catalog
        .with(|c| c.list_pending_reaps(now))
        .context("list pending compaction reaps")?;
    reap_pending(store.clone(), catalog, &pending, sink, &mut report).await;

    let parallelism = cfg.parallelism.max(1);
    let results: Vec<PartitionResult> = futures::stream::iter(plans.into_iter().map(|plan| {
        let store = store.clone();
        let bucket = bucket.to_string();
        let cfg = cfg.clone();
        let block_cfg = block_cfg.clone();
        async move {
            let key = compaction_lease_key(&plan.signal, &plan.date, plan.input_level);
            let guard = match provider.try_acquire(&key, lease_ttl).await {
                Ok(Some(g)) => g,
                Ok(None) => {
                    tracing::debug!(%key, "compaction partition held by a peer; skipping");
                    return PartitionResult::LeaseHeld;
                }
                Err(e) => {
                    tracing::warn!(%key, error = %e, "lease backend unreachable; skipping compaction");
                    return PartitionResult::LeaseUnavailable;
                }
            };

            let fence = guard.fence();
            if let Err(error) = reconcile_partition(
                store.as_ref(),
                catalog,
                &bucket,
                &plan.signal,
                &plan.date,
                cfg.grace,
            )
            .await
            {
                guard.release().await;
                tracing::warn!(
                    signal = %plan.signal,
                    date = %plan.date,
                    input_level = plan.input_level,
                    error = %format!("{error:#}"),
                    "compaction partition reconcile failed; continuing pass"
                );
                return PartitionResult::Failed;
            }

            // Revalidate the exact inputs after authoritative reconciliation.
            let still_live = match catalog.with(|c| c.list_blocks()) {
                Ok(l) => l,
                Err(e) => {
                    guard.release().await;
                    tracing::warn!(error = %e, "re-list after partition reconcile failed");
                    return PartitionResult::Failed;
                }
            };
            let live_ids: std::collections::HashSet<_> =
                still_live.iter().map(|entry| entry.meta.uuid).collect();
            if plan
                .inputs
                .iter()
                .any(|input| !live_ids.contains(&input.meta.uuid))
            {
                tracing::info!(
                    signal = %plan.signal,
                    date = %plan.date,
                    input_level = plan.input_level,
                    "compaction plan became stale after authoritative reconcile; skipping"
                );
                guard.release().await;
                return PartitionResult::Stale;
            }

            let writer_id = plan
                .inputs
                .iter()
                .map(|input| input.meta.writer_id)
                .min()
                .unwrap_or_else(Uuid::now_v7);
            let outcome = compact_partition(
                &plan,
                store.clone(),
                catalog,
                &bucket,
                writer_id,
                &cfg,
                &block_cfg,
                fence.as_ref(),
                sink,
            )
            .await;
            guard.release().await;
            match outcome {
                Ok(o) => PartitionResult::Done {
                    outcome: o,
                    inputs: plan.inputs.len(),
                },
                Err(error) => {
                    tracing::warn!(
                        signal = %plan.signal,
                        date = %plan.date,
                        input_level = plan.input_level,
                        error = %format!("{error:#}"),
                        "compaction partition failed; continuing pass"
                    );
                    PartitionResult::Failed
                }
            }
        }
    }))
    .buffer_unordered(parallelism)
    .collect()
    .await;

    for result in results {
        match result {
            PartitionResult::Done { outcome, inputs } => {
                report.absorb(&outcome, inputs);
            }
            PartitionResult::LeaseHeld => report.lease_held += 1,
            PartitionResult::LeaseUnavailable => report.lease_unavailable += 1,
            PartitionResult::Failed => report.partition_failed += 1,
            PartitionResult::Stale => {}
        }
    }

    Ok(report)
}

/// Per-partition result, accumulated into [`CompactReport`] after the
/// concurrent partition stream completes.
enum PartitionResult {
    Done {
        outcome: PartitionOutcome,
        inputs: usize,
    },
    LeaseHeld,
    LeaseUnavailable,
    Failed,
    Stale,
}

/// Run one lease-guarded retention pass. In dry-run (`cfg.apply == false`) it
/// reports candidates and acquires no lease (fully inert). In apply mode it
/// first completes any deletion work whose durable grace deadline has passed
/// (lease-free — those rows are already soft-deleted), then acquires the
/// global retention lease and stages newly-expired blocks under its fence,
/// emitting `Deleted` events through `sink`. If the lease is held by a peer or
/// its backend is unreachable, the pass reports `aborted` and stages nothing —
/// but already-eligible pending work still gets finished.
pub async fn run_retention_pass<L, C>(
    provider: &L,
    store: Arc<dyn ObjectStore>,
    catalog: &C,
    cfg: &RetentionConfig,
    now_unix_nano: u64,
    sink: &dyn BlockEventSink,
    lease_ttl: Duration,
) -> Result<RetentionReport>
where
    L: LeaseProvider,
    C: CatalogHandle,
{
    let live = catalog
        .with(|c| c.list_blocks())
        .context("list live blocks")?;
    let expired = plan_reaping(&live, cfg, now_unix_nano);

    let mut report = RetentionReport {
        scanned: live.len(),
        dry_run: !cfg.apply,
        ..Default::default()
    };
    for e in &expired {
        report.candidates += 1;
        report.bytes_candidates += e.meta.byte_size;
    }

    if !cfg.apply {
        return Ok(report);
    }

    // Re-announce deletion work that is still outstanding, before doing any of
    // it. The `SoftDeleted` a staging pass emitted was a one-shot: peers that
    // were listening got it, and the Valkey staged-deletions registry got an
    // entry whose TTL assumed the reap would happen roughly on schedule. When
    // it does not — a crashed reaper, a lease handed over, a bucket that keeps
    // failing — that entry expires while the objects are still sitting there,
    // and an instance booting afterwards walks the bucket and serves data
    // retention deliberately hid. Re-emitting keeps the registry alive exactly
    // as long as the work is, and costs one event per signal per pass.
    // Idempotent for peers: adopting a deletion they already have is a no-op.
    announce_outstanding_deletions(catalog, sink);

    // Finish deletion work staged by an earlier pass (possibly on another
    // instance) whose grace has elapsed — including anything stranded by a
    // crash or a lost lease mid-grace. Lease-free and idempotent: these
    // rows are already soft-deleted and invisible to queries, the same
    // reasoning that lets compaction reap outside the partition lease.
    let pending = catalog
        .with(|c| c.list_pending_deletions(now_unix_nano))
        .context("list pending deletions")?;
    reap_pending_deletions(store.clone(), catalog, &pending, sink, &mut report).await;

    if expired.is_empty() {
        return Ok(report);
    }

    let guard = match provider.try_acquire(RETENTION_LEASE_KEY, lease_ttl).await {
        Ok(Some(g)) => g,
        Ok(None) => {
            tracing::debug!("retention lease held by a peer; skipping");
            report.aborted = true;
            return Ok(report);
        }
        Err(e) => {
            tracing::warn!(error = %e, "lease backend unreachable; skipping retention");
            report.aborted = true;
            return Ok(report);
        }
    };

    let fence = guard.fence();
    let aborted = retain_planned(
        &expired,
        store,
        catalog,
        cfg,
        now_unix_nano,
        fence.as_ref(),
        sink,
        &mut report,
    )
    .await;
    guard.release().await;
    report.aborted = aborted.context("retain_planned")?;
    Ok(report)
}
