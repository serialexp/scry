//! Retention pass driver: plan → (dry-run report | apply: supersede-skip
//! → grace → delete objects → drop rows).
//!
//! [`retain_once`] runs a single pass over the catalog's live blocks. In
//! **dry-run** (the default) it only reports what *would* be reaped and
//! touches nothing. With `apply`, it hands the expired set to
//! [`retain_planned`], which executes the delete lifecycle — mirroring
//! compaction's reap tail:
//!
//! 1. (grace > 0 only) `mark_deleted` the expired blocks so queries stop
//!    listing them immediately, then wait the grace window.
//! 2. Delete the expired blocks' objects from the bucket (bucket truth).
//! 3. Drop the expired catalog rows (derived state — removed last).
//!
//! At `grace == 0` step 1 is skipped: there's no live-overlap window for a
//! single reaper, so the objects + rows go straight away.
//!
//! ## Fencing (multi-instance)
//!
//! [`retain_planned`] takes a [`Fence`] — the "do I still hold the retention
//! lease?" re-check — and consults it before `mark_deleted` and again before
//! the deletes. A lost lease aborts cleanly (no objects/rows removed). The
//! standalone `scry-retention` CLI passes [`AlwaysValid`] + [`NoopSink`], so
//! its single-instance behaviour is unchanged from v0.8.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use scry_block::{delete_block_objects, AlwaysValid, BlockEvent, BlockEventSink, Fence, NoopSink};
use scry_catalog::{Catalog, CatalogEntry, CatalogHandle};
use uuid::Uuid;

use crate::policy::{plan_reaping, RetentionConfig};

/// Outcome of one [`retain_once`] pass.
#[derive(Debug, Clone, Default)]
pub struct RetentionReport {
    /// Live blocks examined this pass.
    pub scanned: usize,
    /// Blocks reaped (or, in dry-run, that *would* be reaped).
    pub reaped: usize,
    /// On-disk main-parquet bytes reaped (sum of `byte_size`).
    pub bytes_reaped: u64,
    /// Whether this was a dry-run (nothing was actually deleted).
    pub dry_run: bool,
    /// Whether the apply aborted because the lease was lost (multi-instance).
    pub aborted: bool,
    /// Per-signal `(count, bytes)` breakdown of the reaped set.
    pub by_signal: BTreeMap<String, (usize, u64)>,
}

/// Run a single retention pass over a privately-owned catalog. `now_unix_nano`
/// is the reference instant the policy ages blocks against (injected for
/// determinism; the CLI passes `SystemTime::now()`). Returns a report; in
/// dry-run the bucket and catalog are untouched.
///
/// This is the **single-instance** entry point: in `apply` mode it delegates
/// to [`retain_planned`] with an [`AlwaysValid`] fence and a [`NoopSink`].
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
        let slot = report.by_signal.entry(e.meta.signal.clone()).or_default();
        slot.0 += 1;
        slot.1 += e.meta.byte_size;
        report.reaped += 1;
        report.bytes_reaped += e.meta.byte_size;
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
    )
    .await?;
    report.aborted = aborted;
    Ok(report)
}

/// Execute the destructive retention lifecycle for an already-planned
/// `expired` set. Returns `true` if the pass aborted because the lease was
/// lost (nothing further was deleted).
///
/// Generic over [`CatalogHandle`] so the same routine serves the
/// single-instance CLI (`&Catalog`) and the multi-instance daemon
/// (`&Mutex<Catalog>` shared with the convergence consumer). The catalog lock
/// is held only for the individual synchronous calls, never across the object
/// DELETEs. `sink` receives one `Deleted` event per signal so peers evict the
/// reaped blocks from their catalogs (a [`NoopSink`] for single-instance).
///
/// Caller contract: `expired` is non-empty and `cfg.apply` is true (the
/// dry-run / empty short-circuits live in [`retain_once`]).
pub async fn retain_planned<C: CatalogHandle>(
    expired: &[CatalogEntry],
    store: Arc<dyn ObjectStore>,
    catalog: &C,
    cfg: &RetentionConfig,
    now_unix_nano: u64,
    fence: &dyn Fence,
    sink: &dyn BlockEventSink,
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

    // 1. Grace: soft-delete so queries stop listing the blocks, then wait.
    //    Skipped at grace 0 — a single reaper has no concurrent-read window.
    if !cfg.grace.is_zero() {
        catalog
            .with(|c| c.mark_deleted(&uuids, now_unix_nano))
            .context("mark expired blocks deleted")?;
        tokio::time::sleep(cfg.grace).await;
        // Re-check after the (possibly long) grace window.
        if fence.check().is_err() {
            tracing::warn!("retention lease lost during grace; aborting before object delete");
            return Ok(true);
        }
    }

    // 2. Delete the objects from the bucket (bucket truth before catalog).
    for e in expired {
        delete_block_objects(store.as_ref(), &e.meta)
            .await
            .with_context(|| format!("delete expired {} block {}", e.meta.signal, e.meta.uuid))?;
        tracing::info!(
            signal = %e.meta.signal,
            date = %e.date,
            uuid = %e.meta.uuid,
            bytes = e.meta.byte_size,
            "reaped expired block"
        );
    }

    // 3. Drop the catalog rows.
    catalog
        .with(|c| c.delete_blocks(&uuids))
        .context("drop expired catalog rows")?;

    // 4. Announce the deletions to peers, grouped per signal (the pub/sub
    //    channel selector). Retention can span signals in one pass.
    let mut by_signal: BTreeMap<String, Vec<Uuid>> = BTreeMap::new();
    for e in expired {
        by_signal
            .entry(e.meta.signal.clone())
            .or_default()
            .push(e.meta.uuid);
    }
    for (signal, uuids) in by_signal {
        sink.emit(BlockEvent::Deleted { signal, uuids });
    }

    Ok(false)
}
