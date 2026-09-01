//! Lease fencing for destructive block operations.
//!
//! Compaction and retention both delete blocks. In a multi-instance
//! deployment a *lease* (held in Valkey — see `scry-valkey`) ensures only
//! one instance does destructive work on a given partition at a time. But a
//! lease can be **lost mid-operation** (a renewal fails, the holder paused
//! past its TTL and a peer took over). The engines must therefore re-check,
//! right before each irreversible step, that they *still* hold the lease —
//! and abort if not.
//!
//! [`Fence`] is that re-check, kept deliberately tiny and **synchronous**:
//! the real implementation (in `scry-valkey`) is an `AtomicBool` load that
//! the lease's background renew task flips to "lost" on renewal failure. No
//! I/O, no allocation, callable in a hot path.
//!
//! The seam lives in `scry-block` — the lowest crate every engine already
//! depends on — so `scry-compact` / `scry-retention` can name `&dyn Fence`
//! without taking a dependency on Valkey. The single-instance path
//! ([`AlwaysValid`]) and tests pass a fence that never fails.

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;

/// A cheap, synchronous "do I still hold the lease?" check, consulted
/// immediately before each irreversible step of a destructive operation
/// (the `meta.json` commit of a merged block, `mark_superseded`, and the
/// object/row deletes).
///
/// [`check`](Fence::check) returns `Err` when the lease has been lost; the
/// caller must then **abort without performing the step**, leaving inputs
/// intact (a half-merged orphan is harmless — the next pass re-merges it).
pub trait Fence: Send + Sync {
    /// `Ok(())` while the lease is provably still held; `Err` if it was
    /// lost (or can no longer be confirmed held within the safety margin).
    /// Must be cheap and non-blocking — implementations are an atomic load.
    fn check(&self) -> Result<()>;
}

/// A [`Fence`] that is always valid — the single-instance path. The
/// standalone `scry-compact` / `scry-retention` CLIs and all engine unit
/// tests use this: with exactly one actor there is no lease to lose.
#[derive(Debug, Clone, Copy, Default)]
pub struct AlwaysValid;

impl Fence for AlwaysValid {
    #[inline]
    fn check(&self) -> Result<()> {
        Ok(())
    }
}

/// Live progress of an in-flight compaction pass.
///
/// Written by [`run_compaction_pass`] / [`compact_once`], read by the status
/// endpoint. Both fields are zero when no pass is running — which is why the
/// type lives here (the lowest common ancestor of `scry-cluster`,
/// `scry-compact`, and `scry-server`) rather than in a metrics crate.
///
/// `planned` is set to the number of eligible partitions at the start of the
/// pass; `completed` is incremented as each partition finishes (merged, skipped
/// because a peer holds the lease, or failed). The status page renders this as
/// "compacting 45 / 211"; zero planned means idle.
pub struct CompactionProgress {
    pub planned: AtomicU64,
    pub completed: AtomicU64,
}

impl CompactionProgress {
    pub fn new() -> Self {
        Self {
            planned: AtomicU64::new(0),
            completed: AtomicU64::new(0),
        }
    }

    /// Mark the start of a new pass with `n` planned partitions.
    pub fn start(&self, n: usize) {
        self.completed.store(0, Ordering::Relaxed);
        self.planned.store(n as u64, Ordering::Release);
    }

    /// Record one partition finished (any outcome).
    pub fn tick(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Clear: no pass in flight.
    pub fn clear(&self) {
        self.planned.store(0, Ordering::Relaxed);
        self.completed.store(0, Ordering::Relaxed);
    }

    /// Snapshot for the status page. `(planned, completed)` — `(0, _)` means idle.
    pub fn snapshot(&self) -> (u64, u64) {
        let planned = self.planned.load(Ordering::Acquire);
        let completed = self.completed.load(Ordering::Relaxed);
        (planned, completed)
    }
}

impl Default for CompactionProgress {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_valid_never_fails() {
        assert!(AlwaysValid.check().is_ok());
        // Usable as a trait object, the way the engines consume it.
        let f: &dyn Fence = &AlwaysValid;
        assert!(f.check().is_ok());
    }
}
