//! Applying peers' staged deletions to the local catalog.
//!
//! The counterpart to `scry_valkey::staged`, kept here so it is Valkey-agnostic
//! and unit-testable: the caller supplies the entries however it obtained them.
//!
//! # Why this is a separate step from the event consumer
//!
//! [`apply_event`](crate::apply_event) handles a `SoftDeleted` that arrives
//! while we are listening and already know the block. Three situations it
//! cannot cover, because in each the information never reaches us at a moment
//! when we can act on it:
//!
//! - We booted after the staging. Pub/sub is not replayed, and the bucket still
//!   holds the objects (that is what the grace window is for), so seeding from
//!   the bucket inserts the blocks as live with nothing to contradict it.
//! - We never saw the block's `Created`, so the `SoftDeleted` updated no rows,
//!   and a later poll or walk then inserted the block as live.
//! - A walk fetched a block's sidecar, the block was hard-deleted, the
//!   `Deleted` event arrived and matched nothing, and then the walk inserted
//!   the row it had already fetched.
//!
//! All three are fixed by re-reading the staged set *after* the inserts and
//! applying it — at boot before the listener opens, and at the end of each
//! poll and walk. Applying after the inserts is what makes this work without
//! any memory of events for blocks we did not have: by then the rows exist.
//!
//! # The deadline is re-based on the local clock
//!
//! Reaping pending deletions is deliberately lease-free (see
//! [`crate::maintain`]), so any instance holding a pending row will eventually
//! delete those objects. That makes it unsafe to adopt a peer's *absolute*
//! deadline: an instance whose clock is behind writes a deadline that every
//! peer reads as already past, and the grace window collapses to nothing
//! everywhere at once. So an entry carries the staging instant as well, and we
//! keep only the **duration**, re-based on our own clock.
//!
//! This grants a full grace window from the moment we first hear about the
//! block, which is usually longer than what remains of the original. That is
//! the safe direction: a late reap is idempotent and `NotFound`-tolerant, and
//! the owner reaps on schedule regardless. Short would mean deleting objects
//! out from under readers who were promised the window.

use anyhow::{Context, Result};
use scry_catalog::CatalogHandle;
use std::collections::BTreeMap;
use uuid::Uuid;

/// One entry of the staged-deletions registry.
///
/// Both timestamps come from the **staging instance's** clock and are only ever
/// meaningful relative to each other; see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedDeletion {
    pub uuid: Uuid,
    /// When the staging instance hid the block, by its own clock.
    pub staged_at_unix_nano: u64,
    /// When the staging instance intends the objects to become reapable, by
    /// its own clock.
    pub delete_eligible_at_unix_nano: u64,
}

impl StagedDeletion {
    /// The grace window the staging instance intended, in nanoseconds.
    /// Clock-independent, because both ends came from the same clock.
    ///
    /// Saturating: a deadline at or before the staging instant means "no
    /// grace", which is the legitimate `grace = 0` configuration.
    pub fn grace_nanos(&self) -> u64 {
        self.delete_eligible_at_unix_nano
            .saturating_sub(self.staged_at_unix_nano)
    }
}

/// Hide every block a peer has staged for deletion, granting each the grace
/// window its stager intended, measured from *our* clock.
///
/// Entries naming blocks this catalog does not have are a no-op — one UPDATE
/// that matches nothing, which is the correct outcome, not an error.
///
/// Returns the number of rows **newly** hidden. Re-running this on every poll
/// cycle is the normal case and returns 0 once converged, so a caller can log
/// only when something actually changed.
///
/// Uses [`adopt_peer_deletion`](scry_catalog::Catalog::adopt_peer_deletion)
/// rather than `mark_deleted`: first application wins. `mark_deleted` takes the
/// `MAX` of the deadlines, which is right for the owner but would let this
/// path — which recomputes `now + grace` every cycle — push the deadline
/// forward forever, so the block would never become reapable here.
pub fn apply_staged_deletions<C: CatalogHandle + ?Sized>(
    catalog: &C,
    staged: &[StagedDeletion],
    now_unix_nano: u64,
) -> Result<usize> {
    if staged.is_empty() {
        return Ok(0);
    }
    // Group by intended grace so blocks staged in the same retention pass go
    // through as one transaction rather than one per block.
    let mut by_grace: BTreeMap<u64, Vec<Uuid>> = BTreeMap::new();
    for entry in staged {
        by_grace
            .entry(entry.grace_nanos())
            .or_default()
            .push(entry.uuid);
    }

    let mut hidden = 0usize;
    for (grace, uuids) in &by_grace {
        let eligible_at = now_unix_nano.saturating_add(*grace);
        hidden += catalog
            .with(|c| c.adopt_peer_deletion(uuids, now_unix_nano, eligible_at))
            .context("applying peers' staged deletions")?;
    }
    Ok(hidden)
}

#[cfg(test)]
mod tests {
    use super::*;
    use scry_block::BlockMeta;
    use scry_catalog::Catalog;

    const NOW: u64 = 1_700_000_000_000_000_000;
    const GRACE: u64 = 600_000_000_000; // 600s

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

    fn entry(uuid: Uuid, staged_at: u64, eligible_at: u64) -> StagedDeletion {
        StagedDeletion {
            uuid,
            staged_at_unix_nano: staged_at,
            delete_eligible_at_unix_nano: eligible_at,
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

        let hidden =
            apply_staged_deletions(&cat, &[entry(uuid, NOW, NOW + GRACE)], NOW + 5).unwrap();

        assert_eq!(hidden, 1);
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
        let hidden =
            apply_staged_deletions(&cat, &[entry(Uuid::now_v7(), NOW, NOW + GRACE)], NOW).unwrap();
        assert_eq!(hidden, 0, "nothing was hidden; there was nothing to hide");
        assert_eq!(cat.block_count().unwrap(), 0);
    }

    /// The clock-skew fix. A peer whose clock is an hour behind writes a
    /// deadline that is already in *our* past. Adopting it verbatim would make
    /// the block instantly reapable and destroy the grace window; only the
    /// duration may cross the wire.
    #[test]
    fn a_stagers_slow_clock_cannot_collapse_the_grace_window() {
        let (cat, _tmp) = open_catalog();
        let uuid = Uuid::now_v7();
        cat.insert_block(&meta(uuid)).unwrap();

        // Stager is an hour behind: its "now + 600s" is still 55 minutes ago.
        let hour = 3_600_000_000_000u64;
        let stager_now = NOW - hour;
        apply_staged_deletions(&cat, &[entry(uuid, stager_now, stager_now + GRACE)], NOW).unwrap();

        assert!(
            cat.list_pending_deletions(NOW).unwrap().is_empty(),
            "block became reapable immediately: the peer's absolute deadline was adopted"
        );
        assert_eq!(
            cat.list_pending_deletions(NOW + GRACE + 1).unwrap().len(),
            1,
            "a full grace window from our own clock, then reapable"
        );
    }

    /// Re-applied after every poll and walk, so the deadline must not drift
    /// forward each time — otherwise the block never becomes reapable here.
    #[test]
    fn reapplying_does_not_push_the_deadline_forward() {
        let (cat, _tmp) = open_catalog();
        let uuid = Uuid::now_v7();
        cat.insert_block(&meta(uuid)).unwrap();

        let e = entry(uuid, NOW, NOW + GRACE);
        assert_eq!(apply_staged_deletions(&cat, &[e], NOW).unwrap(), 1);
        // Many cycles later, the same entry is still in the registry.
        for i in 1..5 {
            assert_eq!(
                apply_staged_deletions(&cat, &[e], NOW + i * GRACE).unwrap(),
                0,
                "already hidden; nothing new"
            );
        }

        assert_eq!(
            cat.list_pending_deletions(NOW + GRACE + 1).unwrap().len(),
            1,
            "deadline drifted forward with each re-apply"
        );
    }

    /// grace = 0 is a legitimate configuration (the single-instance default),
    /// and must not underflow into an enormous window.
    #[test]
    fn zero_and_inverted_grace_are_immediately_reapable() {
        let (cat, _tmp) = open_catalog();
        let zero = Uuid::now_v7();
        let inverted = Uuid::now_v7();
        cat.insert_block(&meta(zero)).unwrap();
        cat.insert_block(&meta(inverted)).unwrap();

        apply_staged_deletions(
            &cat,
            &[
                entry(zero, NOW, NOW),
                // A deadline before the staging instant: nonsense, but must
                // saturate to "no grace", not wrap to ~584 years.
                entry(inverted, NOW, NOW - 1),
            ],
            NOW,
        )
        .unwrap();

        assert_eq!(
            cat.list_pending_deletions(NOW).unwrap().len(),
            2,
            "both should be reapable now"
        );
    }

    #[test]
    fn empty_input_touches_nothing() {
        let (cat, _tmp) = open_catalog();
        assert_eq!(apply_staged_deletions(&cat, &[], NOW).unwrap(), 0);
    }
}
