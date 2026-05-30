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
//! - **retention** — one global lease (`scry/lease/retention`), since a
//!   retention pass spans all signals and is cheap.
//!
//! `try_acquire` returning `Err` (backend unreachable) pauses that unit: no
//! lease ⇒ no destructive work. The functions here are the unit-testable
//! cores; the daemon drives them on a timer (Phase 6).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use scry_block::{BlockBuilderConfig, BlockEventSink};
use scry_catalog::CatalogHandle;
use scry_compact::{compact_partition, plan_merges, CompactConfig, CompactReport, PartitionOutcome};
use scry_retention::{plan_reaping, retain_planned, RetentionConfig, RetentionReport};
use uuid::Uuid;

use crate::lease::{LeaseGuard, LeaseProvider};

/// Lease key for a compaction partition.
fn compaction_lease_key(signal: &str, date: &str, input_level: u32) -> String {
    format!("scry/lease/compact/{signal}/{date}/{input_level}")
}

/// The single global retention lease key.
pub const RETENTION_LEASE_KEY: &str = "scry/lease/retention";

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
    let live = catalog.with(|c| c.list_blocks()).context("list live blocks")?;
    let plans = plan_merges(&live, cfg);
    let mut report = CompactReport::default();

    for plan in plans {
        let key = compaction_lease_key(&plan.signal, &plan.date, plan.input_level);
        let guard = match provider.try_acquire(&key, lease_ttl).await {
            Ok(Some(g)) => g,
            Ok(None) => {
                tracing::debug!(%key, "compaction partition held by a peer; skipping");
                continue;
            }
            Err(e) => {
                // Backend unreachable — pause destructive work for this pass.
                tracing::warn!(%key, error = %e, "lease backend unreachable; skipping compaction");
                continue;
            }
        };

        let fence = guard.fence();
        let writer_id = Uuid::now_v7();
        let outcome = compact_partition(
            &plan,
            store.clone(),
            catalog,
            bucket,
            writer_id,
            cfg,
            block_cfg,
            fence.as_ref(),
            sink,
        )
        .await;
        // Release promptly regardless of outcome, then surface any error.
        guard.release().await;
        let outcome =
            outcome.with_context(|| format!("compacting {} {} partition", plan.signal, plan.date))?;

        match outcome {
            PartitionOutcome::Merged { bytes_out } => {
                report.merges += 1;
                report.blocks_in += plan.inputs.len();
                report.blocks_out += 1;
                report.bytes_out += bytes_out;
            }
            PartitionOutcome::Aborted => report.aborted += 1,
        }
    }

    Ok(report)
}

/// Run one lease-guarded retention pass. In dry-run (`cfg.apply == false`) it
/// reports candidates and acquires no lease (fully inert). In apply mode it
/// acquires the global retention lease and runs the reap lifecycle under its
/// fence, emitting `Deleted` events through `sink`. If the lease is held by a
/// peer or its backend is unreachable, the pass reports `aborted` and reaps
/// nothing.
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
    let live = catalog.with(|c| c.list_blocks()).context("list live blocks")?;
    let expired = plan_reaping(&live, cfg, now_unix_nano);

    let mut report = RetentionReport {
        scanned: live.len(),
        dry_run: !cfg.apply,
        ..Default::default()
    };
    for e in &expired {
        let slot = report.by_signal.entry(e.meta.signal.clone()).or_default();
        slot.0 += 1;
        slot.1 += e.meta.byte_size;
        report.reaped += 1;
        report.bytes_reaped += e.meta.byte_size;
    }

    if !cfg.apply || expired.is_empty() {
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
    )
    .await;
    guard.release().await;
    report.aborted = aborted.context("retain_planned")?;
    Ok(report)
}
