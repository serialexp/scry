//! Lease provider abstraction + an in-process implementation.
//!
//! The maintenance loop must do destructive work (compaction, retention)
//! under a lease so exactly one instance acts on a given partition at a time.
//! It is generic over [`LeaseProvider`] — production passes the Valkey-backed
//! provider (`scry-valkey`), tests pass [`LocalLeaseProvider`]. Because the
//! loop uses **static dispatch** (a concrete `L`), native `async fn` in the
//! trait works and no `async-trait` dependency is needed; the spawn site
//! checks `Send` against the concrete provider's futures.
//!
//! A held lease yields a [`Fence`] the engines consult before every
//! irreversible step (`scry_block::Fence`); the provider's renew machinery
//! flips it to "lost" the instant it can no longer prove ownership.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use scry_block::Fence;

/// A handle proving the holder currently owns a lease. Provides the [`Fence`]
/// the engines check mid-operation; [`release`](LeaseGuard::release)
/// relinquishes ownership (also relinquished on drop, best-effort).
pub trait LeaseGuard: Send + Sync {
    /// The fence to hand the engines — `check()` returns `Err` once the lease
    /// is lost (renewal failed, or the guard was released/dropped).
    fn fence(&self) -> Arc<dyn Fence>;

    /// Relinquish the lease promptly rather than waiting for TTL expiry.
    #[allow(async_fn_in_trait)]
    async fn release(self);
}

/// Acquires short-lived exclusive leases identified by string keys.
///
/// `try_acquire` is the single coordination primitive the maintenance loop
/// needs: `Ok(Some(guard))` = acquired (do the work), `Ok(None)` = a peer
/// holds it (skip this partition this pass), `Err` = the lease backend is
/// unreachable (the loop pauses — no lease, no destructive work).
pub trait LeaseProvider: Send + Sync {
    type Guard: LeaseGuard;

    #[allow(async_fn_in_trait)]
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<Option<Self::Guard>>;
}

/// An in-process [`LeaseProvider`]: a shared set of currently-held keys.
/// Exact mutual exclusion within one process — used by the unit/integration
/// tests (two "instances" sharing one provider contend for a partition key,
/// proving single-winner compaction) and usable as a trivial single-process
/// coordinator. Not a substitute for the Valkey provider across processes.
#[derive(Clone, Default)]
pub struct LocalLeaseProvider {
    held: Arc<Mutex<HashSet<String>>>,
}

impl LocalLeaseProvider {
    pub fn new() -> Self {
        Self::default()
    }
}

/// The fence behind a [`LocalGuard`]: valid (`Ok`) while the guard is held,
/// `Err` once released or dropped.
struct LocalFence(AtomicBool);

impl Fence for LocalFence {
    fn check(&self) -> Result<()> {
        if self.0.load(Ordering::SeqCst) {
            Ok(())
        } else {
            anyhow::bail!("local lease released")
        }
    }
}

/// Guard returned by [`LocalLeaseProvider::try_acquire`]. Releasing (or
/// dropping) removes the key from the shared held-set and invalidates the
/// fence.
pub struct LocalGuard {
    key: String,
    held: Arc<Mutex<HashSet<String>>>,
    fence: Arc<LocalFence>,
}

impl LocalGuard {
    fn relinquish(&self) {
        self.fence.0.store(false, Ordering::SeqCst);
        self.held
            .lock()
            .expect("held set poisoned")
            .remove(&self.key);
    }
}

impl LeaseGuard for LocalGuard {
    fn fence(&self) -> Arc<dyn Fence> {
        self.fence.clone()
    }

    async fn release(self) {
        self.relinquish();
    }
}

impl Drop for LocalGuard {
    fn drop(&mut self) {
        self.relinquish();
    }
}

impl LeaseProvider for LocalLeaseProvider {
    type Guard = LocalGuard;

    async fn try_acquire(&self, key: &str, _ttl: Duration) -> Result<Option<LocalGuard>> {
        let mut held = self.held.lock().expect("held set poisoned");
        if held.contains(key) {
            return Ok(None);
        }
        held.insert(key.to_string());
        Ok(Some(LocalGuard {
            key: key.to_string(),
            held: self.held.clone(),
            fence: Arc::new(LocalFence(AtomicBool::new(true))),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_lease_is_mutually_exclusive() {
        let p = LocalLeaseProvider::new();
        let g1 = p.try_acquire("k", Duration::from_secs(1)).await.unwrap();
        assert!(g1.is_some(), "first acquire wins");
        let g1 = g1.unwrap();
        assert!(g1.fence().check().is_ok(), "held lease fence is valid");

        let g2 = p.try_acquire("k", Duration::from_secs(1)).await.unwrap();
        assert!(g2.is_none(), "contended key is refused while held");

        // A different key is independent.
        assert!(p
            .try_acquire("other", Duration::from_secs(1))
            .await
            .unwrap()
            .is_some());

        // Release frees the key and invalidates the fence.
        let fence = g1.fence();
        g1.release().await;
        assert!(fence.check().is_err(), "released lease fence is invalid");
        assert!(p
            .try_acquire("k", Duration::from_secs(1))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn drop_releases_the_lease() {
        let p = LocalLeaseProvider::new();
        {
            let _g = p.try_acquire("k", Duration::from_secs(1)).await.unwrap();
            assert!(p
                .try_acquire("k", Duration::from_secs(1))
                .await
                .unwrap()
                .is_none());
        }
        assert!(
            p.try_acquire("k", Duration::from_secs(1))
                .await
                .unwrap()
                .is_some(),
            "dropping the guard frees the key"
        );
    }
}
