//! The Valkey lease — exact mutual exclusion across instances.
//!
//! Replaces the object-store `If-None-Match` lease of **D-013**, which is
//! unbuildable on Garage (no consensus; Garage's own docs say `if-none-match`
//! cannot implement mutual exclusion between writers). Valkey gives us a real
//! atomic compare-and-set.
//!
//! - **acquire** — `SET key token NX PX ttl`. Won iff the key was unset; the
//!   per-acquisition random `token` lets renew/release be safe compare-and-set
//!   operations (we only ever touch a key we still own).
//! - **renew** — a background task every `ttl/3` runs a Lua
//!   compare-and-`PEXPIRE` (extend only if the value is still our token). The
//!   first failed renew **latches the fence invalid** and stops renewing: the
//!   old holder ceases acting at ~`ttl/3`, strictly before the key's
//!   server-side expiry at `ttl`, so no peer can acquire while we still think
//!   we hold it. Expiry is server-side, so client clock skew is irrelevant.
//! - **release** — a Lua compare-and-`DEL` (only delete a key we still own),
//!   so a slow predecessor can't delete a successor's freshly-acquired lease.
//!
//! The guard hands the engines an [`Arc<dyn Fence>`]; `check()` is an atomic
//! load that returns `Err` the instant the lease is lost.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use fred::prelude::*;
use scry_block::Fence;
use scry_cluster::{LeaseGuard, LeaseProvider};
use uuid::Uuid;

/// Extend the lease iff we still own it. KEYS[1]=key, ARGV[1]=token,
/// ARGV[2]=ttl_ms. Returns 1 if renewed, 0 if the lease is no longer ours.
const RENEW_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
"#;

/// Delete the lease iff we still own it. KEYS[1]=key, ARGV[1]=token.
/// Returns 1 if deleted, 0 if it wasn't ours (already expired / taken over).
const RELEASE_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
"#;

/// The fence behind a held Valkey lease: an atomic flag the renew task flips
/// to `false` the moment the lease can no longer be confirmed held.
struct ValkeyFence(AtomicBool);

impl ValkeyFence {
    fn invalidate(&self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl Fence for ValkeyFence {
    fn check(&self) -> Result<()> {
        if self.0.load(Ordering::SeqCst) {
            Ok(())
        } else {
            anyhow::bail!("valkey lease lost")
        }
    }
}

/// A held Valkey lease. Auto-renews in the background; releasing (or dropping)
/// invalidates the fence, stops the renew task, and best-effort deletes the
/// key.
pub struct ValkeyLease {
    client: Client,
    key: String,
    token: String,
    fence: Arc<ValkeyFence>,
    renew: tokio::task::JoinHandle<()>,
}

impl LeaseGuard for ValkeyLease {
    fn fence(&self) -> Arc<dyn Fence> {
        self.fence.clone()
    }

    async fn release(self) {
        self.fence.invalidate();
        self.renew.abort();
        // Compare-and-DEL: only delete the key if it is still our token.
        let res: Result<i64, Error> = self
            .client
            .eval(RELEASE_LUA, vec![self.key.clone()], vec![self.token.clone()])
            .await;
        if let Err(e) = res {
            tracing::warn!(key = %self.key, error = %e, "lease release DEL failed; will expire via TTL");
        }
    }
}

impl Drop for ValkeyLease {
    fn drop(&mut self) {
        // Can't await a DEL here; invalidate + stop renewing and let the key
        // expire via its TTL. (release() is the graceful path.)
        self.fence.invalidate();
        self.renew.abort();
    }
}

/// A [`LeaseProvider`] backed by Valkey. Clone-cheap (holds a `fred::Client`).
#[derive(Clone)]
pub struct ValkeyLeaseProvider {
    client: Client,
}

impl ValkeyLeaseProvider {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl LeaseProvider for ValkeyLeaseProvider {
    type Guard = ValkeyLease;

    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<Option<ValkeyLease>> {
        let token = Uuid::now_v7().to_string();
        let ttl_ms = ttl.as_millis().max(1) as i64;

        // SET key token NX PX ttl. Null reply ⇒ key already held ⇒ not ours.
        let res: Value = self
            .client
            .set(
                key,
                token.clone(),
                Some(Expiration::PX(ttl_ms)),
                Some(SetOptions::NX),
                false,
            )
            .await
            .with_context(|| format!("SET NX for lease {key}"))?;
        if res.is_null() {
            return Ok(None);
        }

        let fence = Arc::new(ValkeyFence(AtomicBool::new(true)));
        let renew = spawn_renew(
            self.client.clone(),
            key.to_string(),
            token.clone(),
            ttl,
            ttl_ms,
            fence.clone(),
        );

        Ok(Some(ValkeyLease {
            client: self.client.clone(),
            key: key.to_string(),
            token,
            fence,
            renew,
        }))
    }
}

/// Background renewal: every `ttl/3`, extend the lease iff still ours. The
/// first failure (lost, or backend error) latches the fence invalid and ends
/// the task — the holder stops acting well before server-side expiry.
fn spawn_renew(
    client: Client,
    key: String,
    token: String,
    ttl: Duration,
    ttl_ms: i64,
    fence: Arc<ValkeyFence>,
) -> tokio::task::JoinHandle<()> {
    // ttl/3 gives two renewal attempts before expiry under a single blip.
    let period = (ttl / 3).max(Duration::from_millis(50));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            let renewed: Result<i64, Error> = client
                .eval(RENEW_LUA, vec![key.clone()], vec![token.clone(), ttl_ms.to_string()])
                .await;
            match renewed {
                Ok(1) => continue,
                Ok(_) => {
                    tracing::warn!(key = %key, "lease no longer ours on renew; fencing off");
                    fence.invalidate();
                    return;
                }
                Err(e) => {
                    tracing::warn!(key = %key, error = %e, "lease renew failed; fencing off");
                    fence.invalidate();
                    return;
                }
            }
        }
    })
}
