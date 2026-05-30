//! Retention pass driver: plan → (dry-run report | apply: supersede-skip
//! → grace → delete objects → drop rows).
//!
//! `retain_once` runs a single pass over the catalog's live blocks. In
//! **dry-run** (the default) it only reports what *would* be reaped and
//! touches nothing. With `apply`, it executes the delete lifecycle —
//! mirroring compaction's reap tail:
//!
//! 1. (grace > 0 only) `mark_deleted` the expired blocks so queries stop
//!    listing them immediately, then wait the grace window.
//! 2. Delete the expired blocks' objects from the bucket (bucket truth).
//! 3. Drop the expired catalog rows (derived state — removed last).
//!
//! At `grace == 0` step 1 is skipped: there's no live-overlap window for a
//! single reaper, so the objects + rows go straight away.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use scry_block::delete_block_objects;
use scry_catalog::Catalog;
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
    /// Per-signal `(count, bytes)` breakdown of the reaped set.
    pub by_signal: BTreeMap<String, (usize, u64)>,
}

/// Run a single retention pass. `now_unix_nano` is the reference instant
/// the policy ages blocks against (injected for determinism; the CLI
/// passes `SystemTime::now()`). Returns a report; in dry-run the bucket
/// and catalog are untouched.
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

    let uuids: Vec<Uuid> = expired.iter().map(|e| e.meta.uuid).collect();

    // 1. Grace: soft-delete so queries stop listing the blocks, then wait.
    //    Skipped at grace 0 — a single reaper has no concurrent-read window.
    if !cfg.grace.is_zero() {
        catalog
            .mark_deleted(&uuids, now_unix_nano)
            .context("mark expired blocks deleted")?;
        tokio::time::sleep(cfg.grace).await;
    }

    // 2. Delete the objects from the bucket (bucket truth before catalog).
    for e in &expired {
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
        .delete_blocks(&uuids)
        .context("drop expired catalog rows")?;

    Ok(report)
}
