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

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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

/// How often [`run_fenced`] re-checks its fence while the guarded future is
/// pending. A check is an atomic load, so this costs nothing measurable; it
/// bounds how long an irreversible request keeps being driven — and retried
/// — after the lease is known lost.
pub const FENCE_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The lease was lost while a fenced step was in flight; the step's future
/// was dropped before it completed. See [`run_fenced`].
#[derive(Debug)]
pub struct LeaseLost(pub anyhow::Error);

impl std::fmt::Display for LeaseLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "lease lost while a fenced step was in flight: {:#}",
            self.0
        )
    }
}

impl std::error::Error for LeaseLost {}

/// Drive `step` — one irreversible request, such as a commit PUT — only while
/// `fence` holds, re-checking every `poll_every`.
///
/// A single fence check before the request is not enough: an object-store
/// client retries a failing request for minutes (three by default), so a
/// commit that began under the lease can otherwise land long after it was
/// lost and a peer has taken over. Here the fence is re-checked on every
/// tick while `step` is pending; when it fails, `step` is **dropped** —
/// cancelling any in-flight attempt and every retry after it — and
/// [`LeaseLost`] is returned.
///
/// The fence is **not** checked before `step` is first polled. Check it
/// yourself at the last point where aborting is still clean (before any
/// irreversible preparation, such as disarming cleanup of staged data): a
/// loss found there is unambiguous, whereas any loss reported here is not.
///
/// `LeaseLost` does **not** mean the step had no effect. A request whose
/// bytes were already sent can still be applied by the server after the
/// drop, so the caller must treat the outcome as ambiguous and keep whatever
/// a successful step would need (e.g. a commit's staged data). If `step`
/// completes on the same wake-up that the fence fails, its result wins: the
/// step is known to have happened.
pub async fn run_fenced<F: Future>(
    fence: &dyn Fence,
    poll_every: Duration,
    step: F,
) -> std::result::Result<F::Output, LeaseLost> {
    let mut step = std::pin::pin!(step);
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + poll_every, poll_every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            out = &mut step => return Ok(out),
            _ = tick.tick() => fence.check().map_err(LeaseLost)?,
        }
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

    /// A fence that can be tripped from outside.
    #[derive(Default)]
    struct Toggle(std::sync::atomic::AtomicBool);

    impl Fence for Toggle {
        fn check(&self) -> Result<()> {
            if self.0.load(Ordering::Acquire) {
                anyhow::bail!("tripped")
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_fenced_returns_the_step_while_the_fence_holds() {
        let fence = Toggle::default();
        let out = run_fenced(&fence, Duration::from_millis(1), async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            7
        })
        .await
        .unwrap();
        assert_eq!(out, 7);
    }

    #[tokio::test]
    async fn run_fenced_prefers_a_completed_step_over_a_lost_lease() {
        // A step that has completed has happened; reporting it as abandoned
        // would make the caller treat a known commit as ambiguous.
        let fence = Toggle::default();
        fence.0.store(true, Ordering::Release);
        let out = run_fenced(&fence, Duration::from_millis(1), async { 7 })
            .await
            .unwrap();
        assert_eq!(out, 7);
    }

    #[tokio::test]
    async fn run_fenced_reports_a_lease_lost_before_a_pending_step_finishes() {
        let fence = Toggle::default();
        fence.0.store(true, Ordering::Release);
        let err = run_fenced(
            &fence,
            Duration::from_millis(1),
            std::future::pending::<()>(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("tripped"));
    }

    #[tokio::test]
    async fn run_fenced_drops_a_pending_step_when_the_lease_is_lost() {
        let fence = std::sync::Arc::new(Toggle::default());
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct SetOnDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for SetOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let guard = SetOnDrop(dropped.clone());
        let tripper = {
            let fence = fence.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                fence.0.store(true, Ordering::Release);
            })
        };
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            run_fenced(fence.as_ref(), Duration::from_millis(5), async move {
                let _guard = guard;
                std::future::pending::<()>().await
            }),
        )
        .await
        .expect("a lost lease ends the step promptly");
        tripper.await.unwrap();
        assert!(result.is_err(), "the step must not be reported complete");
        assert!(dropped.load(Ordering::Acquire), "the step was cancelled");
    }
}
