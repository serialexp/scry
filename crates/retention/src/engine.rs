//! Retention pass driver: plan → (dry-run report | apply: stage with a
//! durable grace deadline → reap once eligible).
//!
//! [`retain_once`] runs a single pass over the catalog's live blocks. In
//! **dry-run** (the default) it only reports what *would* be reaped and
//! touches nothing. With `apply`, it hands the expired set to
//! [`retain_planned`], which stages the deletion, and reaps whatever has
//! become eligible — mirroring compaction's staged reap tail:
//!
//! 1. `mark_deleted` the expired blocks (queries stop listing them
//!    immediately) *together with* a `delete_eligible_at = now + grace`
//!    deadline. Both are one transaction.
//! 2. Once that deadline passes, [`reap_pending_deletions`] deletes the
//!    objects from the bucket (bucket truth) and then drops the catalog
//!    rows (derived state — removed last).
//!
//! At `grace == 0` step 2 runs immediately in the same pass: there's no
//! live-overlap window for a single reaper, so the objects + rows go
//! straight away, exactly as before.
//!
//! ## Why the grace window is a timestamp, not a sleep
//!
//! Grace used to be an in-process `tokio::time::sleep` between the soft
//! delete and the object deletion. Any interruption during that window —
//! a crash, an ordinary shutdown, or the fence tripping — left the rows
//! soft-deleted forever: `list_blocks` filters `deleted_at IS NULL` so no
//! later pass re-planned them, reconcile's `INSERT OR IGNORE` couldn't
//! resurrect them, and their objects stayed in the bucket invisible to
//! every reaper. Persisting the deadline (compaction's `reap_eligible_at`
//! pattern) makes the pending work durable: whichever instance runs the
//! next pass finishes it.
//!
//! ## Fencing (multi-instance)
//!
//! [`retain_planned`] takes a [`Fence`] — the "do I still hold the
//! retention lease?" re-check — and consults it before staging and again
//! before deleting. A lost lease aborts cleanly, leaving durable pending
//! work rather than stranded rows. [`reap_pending_deletions`] needs no
//! lease: the rows it touches are already soft-deleted and invisible to
//! queries, and every step is idempotent — the same reasoning that lets
//! compaction reap outside the partition lease.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use scry_block::{delete_block_objects, AlwaysValid, BlockEvent, BlockEventSink, Fence, NoopSink};
use scry_catalog::{Catalog, CatalogEntry, CatalogHandle};
use uuid::Uuid;

use crate::policy::{plan_reaping, RetentionConfig};

/// Outcome of one [`retain_once`] pass.
///
/// The counts distinguish *planned*, *staged*, and *actually removed*
/// work, because with a non-zero grace those are three different things
/// within a single pass — a report that conflated them claimed deletions
/// that had not happened.
#[derive(Debug, Clone, Default)]
pub struct RetentionReport {
    /// Live blocks examined this pass.
    pub scanned: usize,
    /// Expired blocks selected by the policy this pass (in dry-run, what
    /// *would* be reaped).
    pub candidates: usize,
    /// On-disk main-parquet bytes of the candidate set.
    pub bytes_candidates: u64,
    /// Candidates soft-deleted this pass and now awaiting their grace
    /// deadline. Zero when grace is zero (they're reaped immediately).
    pub staged: usize,
    /// Blocks whose objects **and** catalog rows were actually removed
    /// this pass. Includes work staged by an earlier pass whose grace has
    /// since elapsed, so this can exceed `candidates`.
    pub reaped: usize,
    /// On-disk main-parquet bytes actually reaped this pass.
    pub bytes_reaped: u64,
    /// Blocks whose object deletion failed; they remain durable pending
    /// work and are retried by a later pass.
    pub reap_failed: usize,
    /// Whether this was a dry-run (nothing was deleted or staged).
    pub dry_run: bool,
    /// Whether the apply aborted because the lease was lost (multi-instance).
    pub aborted: bool,
    /// Per-signal `(count, bytes)` breakdown of the blocks actually reaped.
    pub by_signal: BTreeMap<String, (usize, u64)>,
}

/// Run a single retention pass over a privately-owned catalog. `now_unix_nano`
/// is the reference instant the policy ages blocks against (injected for
/// determinism; the CLI passes `SystemTime::now()`). Returns a report; in
/// dry-run the bucket and catalog are untouched.
///
/// This is the **single-instance** entry point: it reaps any deletion work
/// left pending by an earlier pass, then in `apply` mode delegates to
/// [`retain_planned`] with an [`AlwaysValid`] fence and a [`NoopSink`].
pub async fn retain_once(
    store: Arc<dyn ObjectStore>,
    catalog: &Catalog,
    cfg: &RetentionConfig,
    now_unix_nano: u64,
) -> Result<RetentionReport> {
    let live = catalog.list_blocks().context("list live blocks")?;
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
        for e in &expired {
            tracing::info!(
                signal = %e.meta.signal,
                date = %e.date,
                uuid = %e.meta.uuid,
                ts_max = e.meta.ts_max_unix_nano,
                bytes = e.meta.byte_size,
                "would reap (dry-run)"
            );
        }
        return Ok(report);
    }

    // Finish anything a previous pass staged whose grace has elapsed —
    // including work stranded by a crash mid-grace.
    let pending = catalog
        .list_pending_deletions(now_unix_nano)
        .context("list pending deletions")?;
    reap_pending_deletions(store.clone(), catalog, &pending, &NoopSink, &mut report).await;

    if expired.is_empty() {
        return Ok(report);
    }

    let aborted = retain_planned(
        &expired,
        store,
        catalog,
        cfg,
        now_unix_nano,
        &AlwaysValid,
        &NoopSink,
        &mut report,
    )
    .await?;
    report.aborted = aborted;
    Ok(report)
}

/// Stage the destructive retention lifecycle for an already-planned
/// `expired` set, and — when grace is zero — carry it out immediately.
/// Returns `true` if the pass aborted because the lease was lost.
///
/// Generic over [`CatalogHandle`] so the same routine serves the
/// single-instance CLI (`&Catalog`) and the multi-instance daemon
/// (`&Mutex<Catalog>` shared with the convergence consumer). The catalog
/// lock is held only for the individual synchronous calls, never across
/// the object DELETEs. `sink` receives one `Deleted` event per signal so
/// peers evict the reaped blocks (a [`NoopSink`] for single-instance).
///
/// Caller contract: `expired` is non-empty and `cfg.apply` is true (the
/// dry-run / empty short-circuits live in [`retain_once`]).
#[allow(clippy::too_many_arguments)]
pub async fn retain_planned<C: CatalogHandle>(
    expired: &[CatalogEntry],
    store: Arc<dyn ObjectStore>,
    catalog: &C,
    cfg: &RetentionConfig,
    now_unix_nano: u64,
    fence: &dyn Fence,
    sink: &dyn BlockEventSink,
    report: &mut RetentionReport,
) -> Result<bool> {
    if expired.is_empty() {
        return Ok(false);
    }

    let uuids: Vec<Uuid> = expired.iter().map(|e| e.meta.uuid).collect();

    // Fence before any destructive step — a lost lease means a peer now owns
    // retention; back off without touching the bucket or catalog.
    if fence.check().is_err() {
        tracing::warn!("retention lease lost before reaping; aborting pass");
        return Ok(true);
    }

    // 1. Soft-delete + durable grace deadline, atomically. Queries stop
    //    listing these blocks now; the deadline is what lets any later
    //    pass finish the job if this one doesn't.
    let eligible_at =
        now_unix_nano.saturating_add(cfg.grace.as_nanos().min(u64::MAX as u128) as u64);
    catalog
        .with(|c| c.mark_deleted(&uuids, now_unix_nano, eligible_at))
        .context("stage expired blocks for deletion")?;

    // 1b. Tell peers now, not after the objects are gone. Their catalogs hide
    //     the same rows for the same window, so they stop planning queries
    //     against blocks that are about to be deleted — otherwise every peer
    //     query in the gap between our DELETEs and the `Deleted` event 404s and
    //     has to self-heal. Grouped per signal because that is the pub/sub
    //     channel selector and a pass can span signals.
    let mut staged_by_signal: BTreeMap<String, Vec<Uuid>> = BTreeMap::new();
    for e in expired {
        staged_by_signal
            .entry(e.meta.signal.clone())
            .or_default()
            .push(e.meta.uuid);
    }
    for (signal, uuids) in staged_by_signal {
        sink.emit(BlockEvent::SoftDeleted {
            signal,
            uuids,
            deleted_at_unix_nano: now_unix_nano,
            delete_eligible_at_unix_nano: eligible_at,
        });
    }

    // 2. Grace is an eligibility timestamp, never an inline sleep. If
    //    immediate cleanup is allowed and the fence still holds, do it
    //    now; otherwise the durable pending rows are picked up by a later
    //    pass. Same shape as compaction's staged reap.
    if cfg.grace.is_zero() && fence.check().is_ok() {
        let pending = catalog
            .with(|c| c.list_pending_deletions(now_unix_nano))
            .context("list newly pending deletions")?;
        reap_pending_deletions(store, catalog, &pending, sink, report).await;
    } else {
        report.staged += expired.len();
        tracing::info!(
            staged = expired.len(),
            grace_secs = cfg.grace.as_secs(),
            "expired blocks staged for deletion after their grace window"
        );
    }

    Ok(false)
}

/// Delete the objects and catalog rows of soft-deleted blocks whose grace
/// has elapsed. Each block is independent: a failure leaves that block as
/// durable pending work and does not stop the others.
///
/// Object deletion precedes the row drop because the catalog is derived
/// state — a crash between the two leaves a row whose objects are already
/// gone, which the next pass re-attempts harmlessly (`delete_block_objects`
/// tolerates absent objects).
pub async fn reap_pending_deletions<C: CatalogHandle>(
    store: Arc<dyn ObjectStore>,
    catalog: &C,
    pending: &[CatalogEntry],
    sink: &dyn BlockEventSink,
    report: &mut RetentionReport,
) {
    let mut reaped_by_signal: BTreeMap<String, Vec<Uuid>> = BTreeMap::new();

    for e in pending {
        if let Err(error) = delete_block_objects(store.as_ref(), &e.meta).await {
            report.reap_failed += 1;
            tracing::warn!(
                signal = %e.meta.signal,
                uuid = %e.meta.uuid,
                error = %error,
                "expired block object deletion failed; durable pending row will retry"
            );
            continue;
        }
        match catalog.with(|c| c.delete_blocks(&[e.meta.uuid])) {
            Ok(()) => {
                report.reaped += 1;
                report.bytes_reaped += e.meta.byte_size;
                let slot = report.by_signal.entry(e.meta.signal.clone()).or_default();
                slot.0 += 1;
                slot.1 += e.meta.byte_size;
                reaped_by_signal
                    .entry(e.meta.signal.clone())
                    .or_default()
                    .push(e.meta.uuid);
                tracing::info!(
                    signal = %e.meta.signal,
                    date = %e.date,
                    uuid = %e.meta.uuid,
                    bytes = e.meta.byte_size,
                    "reaped expired block"
                );
            }
            Err(error) => {
                report.reap_failed += 1;
                tracing::warn!(
                    uuid = %e.meta.uuid,
                    error = %error,
                    "deleted expired objects but catalog cleanup failed; retrying later"
                );
            }
        }
    }

    // Announce the deletions to peers, grouped per signal (the pub/sub
    // channel selector). Retention can span signals in one pass.
    for (signal, uuids) in reaped_by_signal {
        sink.emit(BlockEvent::Deleted { signal, uuids });
    }
}
