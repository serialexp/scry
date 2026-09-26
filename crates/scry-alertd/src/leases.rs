//! The lease backend alertd runs on, as one concrete (static-dispatch) type.
//!
//! [`LeaseProvider`] uses native `async fn`, so spawned evaluation tasks need
//! a concrete provider whose futures are provably `Send`. [`Leases`] is that
//! type for both modes: the explicit local single-writer provider, or a Valkey
//! provider that may not be connected yet. Clustered alertd serves its API
//! immediately and connects to Valkey in the background; until it does, every
//! acquire fails, so evaluation fails closed (no lease, no commit) while reads
//! and control mutations stay available.

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use anyhow::{bail, Result};
use scry_cluster::{LeaseGuard, LeaseProvider, LocalGuard, LocalLeaseProvider};
use scry_valkey::{ValkeyLease, ValkeyLeaseProvider};

#[derive(Clone)]
pub enum Leases {
    Local(LocalLeaseProvider),
    Valkey(Arc<OnceLock<ValkeyLeaseProvider>>),
}

pub enum Lease {
    Local(LocalGuard),
    Valkey(ValkeyLease),
}

impl Leases {
    pub fn local() -> Self {
        Self::Local(LocalLeaseProvider::new())
    }

    /// A Valkey backend whose provider is installed later through the
    /// returned cell (see [`crate::connect_valkey_in_background`]).
    pub fn valkey_pending() -> (Self, Arc<OnceLock<ValkeyLeaseProvider>>) {
        let cell = Arc::new(OnceLock::new());
        (Self::Valkey(cell.clone()), cell)
    }
}

impl LeaseProvider for Leases {
    type Guard = Lease;

    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<Option<Lease>> {
        match self {
            Self::Local(provider) => Ok(provider.try_acquire(key, ttl).await?.map(Lease::Local)),
            Self::Valkey(cell) => {
                let Some(provider) = cell.get() else {
                    bail!("Valkey lease backend is not connected yet");
                };
                Ok(provider.try_acquire(key, ttl).await?.map(Lease::Valkey))
            }
        }
    }
}

impl LeaseGuard for Lease {
    fn fence(&self) -> Arc<dyn scry_block::Fence> {
        match self {
            Self::Local(guard) => guard.fence(),
            Self::Valkey(guard) => guard.fence(),
        }
    }

    async fn release(self) {
        match self {
            Self::Local(guard) => guard.release().await,
            Self::Valkey(guard) => guard.release().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unconnected_valkey_backend_fails_closed() {
        let (leases, cell) = Leases::valkey_pending();
        assert!(cell.get().is_none());
        assert!(leases
            .try_acquire("lease/alert/eval/x", Duration::from_secs(5))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn local_backend_is_exclusive_until_release() {
        let leases = Leases::local();
        let first = leases
            .try_acquire("k", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert!(leases
            .try_acquire("k", Duration::from_secs(5))
            .await
            .unwrap()
            .is_none());
        first.fence().check().unwrap();
        first.release().await;
        assert!(leases
            .try_acquire("k", Duration::from_secs(5))
            .await
            .unwrap()
            .is_some());
    }
}
