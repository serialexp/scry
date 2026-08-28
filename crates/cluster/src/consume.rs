//! The convergence consumer: apply a [`BlockEvent`] to the local catalog.
//!
//! Each instance broadcasts block lifecycle events (`Created` / `Superseded`
//! / `Deleted`) over Valkey pub/sub; peers apply them here to keep their
//! catalogs fresh without waiting for a poll or full walk. Events may be
//! **duplicated, reordered, and self-delivered**, so every apply is
//! idempotent and order-independent:
//!
//! - `Created`  → `insert_block` (`INSERT OR IGNORE`) + advance the block's
//!   poll cursor, so a healthy pub/sub stream keeps cursors at the head and
//!   subsequent polls list nothing.
//! - `Superseded` → insert `by_meta` first (satisfies the
//!   `superseded_by REFERENCES blocks(uuid)` foreign key even if this peer
//!   missed the `Created` for the merged block), then `mark_superseded` the
//!   inputs (a no-op for inputs this peer never saw).
//! - `Deleted`  → `delete_blocks` (DELETE by uuid; absent rows are a no-op).
//!
//! The bucket remains the source of truth; this is a low-latency hint layer.
//! Cursors are a high-water mark and only ever advance, so a `Deleted` never
//! regresses them (a later poll must not "rediscover" the deleted block).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use scry_block::BlockEvent;
use scry_catalog::{date_dir, CatalogHandle};

/// What an [`apply_event`] call did, for metrics/logging. All variants are
/// benign; none indicates an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ApplyOutcome {
    /// Rows newly inserted (0 if the block was already known — the common
    /// case for a self-delivered or duplicate event).
    pub inserted: usize,
    /// Rows transitioned to superseded.
    pub superseded: usize,
    /// Rows hidden from queries pending their retention grace deadline.
    pub soft_deleted: usize,
    /// Rows deleted.
    pub deleted: usize,
}

/// Apply one [`BlockEvent`] to `catalog`, idempotently. Safe to call with
/// duplicated, reordered, or self-originated events.
pub fn apply_event<C: CatalogHandle>(catalog: &C, event: &BlockEvent) -> Result<ApplyOutcome> {
    apply_event_with_grace(catalog, event, Duration::ZERO)
}

pub fn apply_event_with_grace<C: CatalogHandle>(
    catalog: &C,
    event: &BlockEvent,
    local_grace: Duration,
) -> Result<ApplyOutcome> {
    let mut outcome = ApplyOutcome::default();
    match event {
        BlockEvent::Created { meta } => {
            let inserted = catalog
                .with(|c| c.insert_block(meta))
                .context("apply Created: insert_block")?;
            if inserted {
                outcome.inserted = 1;
            }
            // Keep the poll cursor at the head so a healthy pub/sub stream
            // means polls find nothing new. Monotonic — never regresses.
            catalog
                .with(|c| {
                    c.advance_cursor(
                        &meta.signal,
                        meta.writer_id,
                        &date_dir(meta.ts_min_unix_nano),
                        meta.uuid,
                    )
                })
                .context("apply Created: advance_cursor")?;
        }
        BlockEvent::Superseded {
            inputs,
            by: _,
            by_meta,
            reap_eligible_at_unix_nano,
        } => {
            // Apply output + lineage + logical supersession under one catalog
            // lock so a peer cannot list both representations between calls.
            // Legacy events decode eligibility as 0; a receiving node with a
            // configured grace must never interpret that as immediate deletion.
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let local_eligible = now.saturating_add(local_grace.as_nanos() as u64);
            let event_eligible = if *reap_eligible_at_unix_nano == 0 {
                local_eligible
            } else {
                *reap_eligible_at_unix_nano
            };
            let eligible = if local_grace.is_zero() {
                event_eligible
            } else {
                event_eligible.max(local_eligible)
            };
            let inserted = catalog
                .with(|c| c.apply_compaction(by_meta, inputs, eligible))
                .context("apply Superseded atomically")?;
            if inserted {
                outcome.inserted = 1;
            }
            catalog
                .with(|c| {
                    c.advance_cursor(
                        &by_meta.signal,
                        by_meta.writer_id,
                        &date_dir(by_meta.ts_min_unix_nano),
                        by_meta.uuid,
                    )
                })
                .context("apply Superseded: advance_cursor")?;
            // We can't cheaply know how many rows actually transitioned
            // (some inputs may be unknown to this peer); report the intent.
            outcome.superseded = inputs.len();
        }
        BlockEvent::SoftDeleted {
            uuids,
            deleted_at_unix_nano,
            delete_eligible_at_unix_nano,
            ..
        } => {
            // Hide the rows locally for the remainder of the owner's grace
            // window, so this peer stops planning queries against blocks whose
            // objects are about to disappear. Idempotent, and never shortens an
            // existing deadline.
            catalog
                .with(|c| {
                    c.mark_deleted(uuids, *deleted_at_unix_nano, *delete_eligible_at_unix_nano)
                })
                .context("apply SoftDeleted: mark_deleted")?;
            outcome.soft_deleted = uuids.len();
        }
        BlockEvent::Deleted { uuids, .. } => {
            catalog
                .with(|c| c.delete_blocks(uuids))
                .context("apply Deleted: delete_blocks")?;
            outcome.deleted = uuids.len();
        }
    }
    Ok(outcome)
}
