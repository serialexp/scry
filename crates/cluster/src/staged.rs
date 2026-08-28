//! Applying peers' staged deletions to the local catalog.
//!
//! The counterpart to `scry_valkey::staged`, kept here so it is Valkey-agnostic
//! and unit-testable: the caller supplies the `(uuid, delete_eligible_at)`
//! pairs however it obtained them.
//!
//! # Why this is a separate step from the event consumer
//!
//! [`apply_event`](crate::apply_event) handles a `SoftDeleted` that arrives
//! while we are listening and already know the block. Two situations it cannot
//! cover, because in both the information simply never reaches us:
//!
//! - We booted after the staging. Pub/sub is not replayed, and the bucket still
//!   holds the objects (that is what the grace window is for), so seeding from
//!   the bucket inserts the blocks as live with nothing to contradict it.
//! - We never saw the block's `Created`, so the `SoftDeleted` updated no rows,
//!   and a later poll or walk then inserted the block as live.
//!
//! Both are fixed by re-reading the staged set *after* the inserts and applying
//! it — at boot before the listener opens, and after each periodic poll and
//! walk. Applying after the inserts is what makes this work without any memory
//! of events for blocks we did not have: by then the rows exist.

use anyhow::{Context, Result};
use scry_catalog::CatalogHandle;
use std::collections::BTreeMap;
use uuid::Uuid;

/// Hide every block a peer has staged for deletion, using the peer's own
/// reap deadline.
///
/// `staged` is `(block uuid, delete_eligible_at_unix_nano)`. Entries naming
/// blocks this catalog does not have are a no-op — they cost one UPDATE that
/// matches nothing, which is the correct outcome, not an error.
///
/// Returns the number of entries applied (the intent, not the rows changed;
/// `mark_deleted` does not report a count and most calls here are re-applying
/// what is already hidden).
///
/// Idempotent: `mark_deleted` `COALESCE`s the `deleted_at` it already has and
/// takes the `MAX` of the deadlines, so repeating this every poll cycle never
/// re-dates a hidden block or shortens a window a reader is relying on.
pub fn apply_staged_deletions<C: CatalogHandle + ?Sized>(
    catalog: &C,
    staged: &[(Uuid, u64)],
    now_unix_nano: u64,
) -> Result<usize> {
    if staged.is_empty() {
        return Ok(0);
    }
    // Group by deadline so blocks staged in the same retention pass go through
    // as one transaction, instead of one per block.
    let mut by_deadline: BTreeMap<u64, Vec<Uuid>> = BTreeMap::new();
    for (uuid, eligible_at) in staged {
        by_deadline.entry(*eligible_at).or_default().push(*uuid);
    }

    for (eligible_at, uuids) in &by_deadline {
        catalog
            .with(|c| c.mark_deleted(uuids, now_unix_nano, *eligible_at))
            .context("applying peers' staged deletions")?;
    }
    Ok(staged.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use scry_block::BlockMeta;
    use scry_catalog::Catalog;

    const NOW: u64 = 1_700_000_000_000_000_000;

    fn open_catalog() -> (Catalog, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let cat = Catalog::open(&tmp.path().join("c.sqlite"), "b").unwrap();
        (cat, tmp)
    }

    /// A fabricated meta (no bucket objects) — enough to exercise catalog
    /// applies, which is all this module does.
    fn meta(uuid: Uuid) -> BlockMeta {
        BlockMeta {
            uuid,
            signal: "logs".into(),
            writer_id: Uuid::now_v7(),
            ts_min_unix_nano: NOW,
            ts_max_unix_nano: NOW + 1,
            row_count: 10,
            byte_size: 100,
            schema_version: 1,
            level: 0,
            producer_version: String::new(),
            label_fingerprint_bloom: None,
            has_postings: false,
            postings_size_bytes: None,
            series_types: None,
            all_fingerprints: None,
            has_body_bloom: false,
            body_bloom_size_bytes: None,
            wal_seg_max: None,
            wal_shard: None,
            compacted_from: Vec::new(),
        }
    }

    /// The case the whole module exists for: a block inserted by a bucket walk
    /// that a peer had already staged is hidden once the staged set is applied.
    #[test]
    fn hides_a_block_the_walk_inserted_as_live() {
        let (cat, _tmp) = open_catalog();
        let uuid = Uuid::now_v7();
        cat.insert_block(&meta(uuid)).unwrap();
        assert_eq!(cat.list_blocks().unwrap().len(), 1, "live before");

        let applied = apply_staged_deletions(&cat, &[(uuid, NOW + 600_000_000_000)], NOW).unwrap();

        assert_eq!(applied, 1);
        assert!(
            cat.list_blocks().unwrap().is_empty(),
            "a staged block must not be listed for queries"
        );
    }

    /// Entries for blocks we do not have must not error — peers converge at
    /// different rates, and the registry is a shared set, not a per-peer one.
    #[test]
    fn unknown_blocks_are_a_no_op() {
        let (cat, _tmp) = open_catalog();
        let applied = apply_staged_deletions(&cat, &[(Uuid::now_v7(), NOW + 1)], NOW).unwrap();
        assert_eq!(applied, 1, "intent is reported");
        assert_eq!(cat.block_count().unwrap(), 0);
    }

    /// Re-applied every poll cycle, so it must never shorten a grace window a
    /// reader is relying on, nor re-date the hiding.
    #[test]
    fn reapplying_never_shortens_the_window() {
        let (cat, _tmp) = open_catalog();
        let uuid = Uuid::now_v7();
        cat.insert_block(&meta(uuid)).unwrap();

        let far = NOW + 600_000_000_000;
        apply_staged_deletions(&cat, &[(uuid, far)], NOW).unwrap();
        // A second pass carrying an earlier deadline must not win.
        apply_staged_deletions(&cat, &[(uuid, NOW + 1)], NOW + 5).unwrap();

        let pending = cat.list_pending_deletions(far - 1).unwrap();
        assert!(
            pending.is_empty(),
            "block became reapable early: the shorter deadline was taken"
        );
        let pending = cat.list_pending_deletions(far + 1).unwrap();
        assert_eq!(
            pending.len(),
            1,
            "still reapable once the real deadline passes"
        );
    }

    #[test]
    fn empty_input_touches_nothing() {
        let (cat, _tmp) = open_catalog();
        assert_eq!(apply_staged_deletions(&cat, &[], NOW).unwrap(), 0);
    }
}
