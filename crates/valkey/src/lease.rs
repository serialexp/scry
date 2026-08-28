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
//! The guard hands the engines an [`Arc<dyn Fence>`]; `check()` is two atomic
//! loads and returns `Err` the instant the lease is lost — or can no longer be
//! shown to be held (see below).
//!
//! ## Why a failed renew is not enough
//!
//! Latching on renew *failure* only fences off if the renew actually resolves.
//! Two ways it doesn't: fred is built with the default `PerformanceConfig`,
//! whose `default_command_timeout` is `0` — **disabled** — and it buffers
//! commands across reconnects, so a stalled connection can leave the `eval`
//! pending indefinitely; and the renew task itself can simply not be scheduled
//! (runtime starvation, a blocked worker). Either way the key expires
//! server-side, a peer acquires the same lease, and both holders believe they
//! are the single winner — two merges of the same inputs, a permanent
//! double-count.
//!
//! So the fence has two independent guards, and needs only one to hold:
//!
//! 1. **Fail-fast renew** — every renew `eval` is wrapped in a
//!    `tokio::time::timeout` of one renew period. A hang becomes a failure,
//!    which latches as before.
//! 2. **Wall-clock backstop** — the fence records the instant of the last
//!    renew the server is known to have accepted, and `check()` fails once
//!    that is older than [`Self::backstop`], *regardless of what the renew
//!    task is doing*. This is the guard that survives starvation, because it
//!    does not depend on any future resolving — only on the caller asking.
//!
//! The recorded instant is taken **before** the `eval` is sent, never after
//! the reply. The server processed the `PEXPIRE` at some point after we sent
//! it, so server-side expiry is at least `sent + ttl`: timing from the send
//! under-estimates our remaining validity, which is the safe direction.
//! `Instant` is monotonic, so none of this is exposed to clock skew.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

/// Bound on the best-effort release DEL. Release is awaited on the shutdown /
/// maintenance path, so it must not hang (fred has no default command timeout).
const RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// Delete the lease iff we still own it. KEYS[1]=key, ARGV[1]=token.
/// Returns 1 if deleted, 0 if it wasn't ours (already expired / taken over).
const RELEASE_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
"#;

/// The fence behind a held Valkey lease.
///
/// Two guards, either of which fences off (see the module docs): a latch the
/// renew task flips on a failed/lost/timed-out renew, and a staleness backstop
/// on the last renew the server accepted.
struct ValkeyFence {
    /// Latched to `false` by the renew task (or release/drop). Never returns
    /// to `true` — a lost lease stays lost for this guard's lifetime.
    valid: AtomicBool,
    /// Millis since [`Self::origin`] at which the most recent accepted renew
    /// was *sent*. Seeded at acquire time, since `SET NX` is itself a renew.
    last_ok_millis: AtomicU64,
    /// Monotonic zero point, so the two fields above can be plain integers.
    origin: Instant,
    /// Maximum age of `last_ok_millis` before `check()` fences off. Strictly
    /// less than the lease TTL, so we stop acting before a peer can acquire.
    backstop: Duration,
}

impl ValkeyFence {
    /// `origin` must be an instant at or before the moment the acquiring
    /// `SET NX PX` was sent, so the seeded watermark under-estimates rather
    /// than over-estimates how long we have been confirmed the holder.
    fn new(origin: Instant, backstop: Duration) -> Self {
        Self {
            valid: AtomicBool::new(true),
            last_ok_millis: AtomicU64::new(0),
            origin,
            backstop,
        }
    }

    fn invalidate(&self) {
        self.valid.store(false, Ordering::SeqCst);
    }

    /// Record that the server accepted a renew that was *sent* at `at`.
    /// Monotonic-max, so an out-of-order late reply can never move the
    /// watermark backwards.
    fn mark_renewed(&self, at: Instant) {
        let ms = at.saturating_duration_since(self.origin).as_millis() as u64;
        self.last_ok_millis.fetch_max(ms, Ordering::SeqCst);
    }

    /// The check, factored on an injected `now` so the staleness boundary is
    /// unit-testable without sleeping.
    fn check_at(&self, now_millis: u64) -> Result<()> {
        if !self.valid.load(Ordering::SeqCst) {
            anyhow::bail!("valkey lease lost");
        }
        let last_ok = self.last_ok_millis.load(Ordering::SeqCst);
        let age = now_millis.saturating_sub(last_ok);
        if age >= self.backstop.as_millis() as u64 {
            // Latch, so every later caller agrees and we don't flap if the
            // renew task wakes up late and succeeds against a key a peer now
            // owns.
            self.invalidate();
            anyhow::bail!(
                "valkey lease not confirmed for {age}ms (backstop {}ms); fencing off",
                self.backstop.as_millis()
            );
        }
        Ok(())
    }
}

impl Fence for ValkeyFence {
    fn check(&self) -> Result<()> {
        self.check_at(self.origin.elapsed().as_millis() as u64)
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
        // Bounded for the same reason renew is: this is awaited by the
        // maintenance loop, and an unbounded hang here would stall it. The
        // DEL is best-effort anyway — the key expires via its TTL.
        let res = tokio::time::timeout(
            RELEASE_TIMEOUT,
            self.client.eval::<i64, _, _, _>(
                RELEASE_LUA,
                vec![self.key.clone()],
                vec![self.token.clone()],
            ),
        )
        .await;
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::warn!(key = %self.key, error = %e, "lease release DEL failed; will expire via TTL");
            }
            Err(_) => {
                tracing::warn!(key = %self.key, "lease release DEL timed out; will expire via TTL");
            }
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

        // Timed from *before* the SET: server-side expiry is at least
        // `sent + ttl`, so anchoring the fence here can only under-state our
        // validity. See the module docs.
        let sent = Instant::now();

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

        let fence = Arc::new(ValkeyFence::new(sent, backstop_for(ttl)));
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

/// Renewal cadence: `ttl/3` gives two renewal attempts before expiry under a
/// single blip. Also the per-renew command timeout — a renew that has not
/// answered within a full period is not going to help us.
fn renew_period(ttl: Duration) -> Duration {
    (ttl / 3).max(Duration::from_millis(50))
}

/// How stale the last confirmed renew may get before [`ValkeyFence::check`]
/// fences off on its own.
///
/// `ttl - ttl/3` — two renew periods. Under healthy operation the watermark is
/// at most one period plus a round-trip old, so this never fires spuriously;
/// when it does fire we still have a full `ttl/3` before the key can expire
/// server-side and a peer can acquire it.
fn backstop_for(ttl: Duration) -> Duration {
    ttl.saturating_sub(ttl / 3).max(Duration::from_millis(50))
}

/// Background renewal: every `ttl/3`, extend the lease iff still ours. The
/// first failure — lost, backend error, **or timeout** — latches the fence
/// invalid and ends the task, so the holder stops acting well before
/// server-side expiry. Each success advances the fence's staleness watermark.
fn spawn_renew(
    client: Client,
    key: String,
    token: String,
    ttl: Duration,
    ttl_ms: i64,
    fence: Arc<ValkeyFence>,
) -> tokio::task::JoinHandle<()> {
    let period = renew_period(ttl);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            // Timed from before the send, and bounded: fred's default command
            // timeout is disabled and it buffers across reconnects, so without
            // this a stalled connection would leave the fence valid forever.
            let sent = Instant::now();
            let renewed: Result<Result<i64, Error>, tokio::time::error::Elapsed> =
                tokio::time::timeout(
                    period,
                    client.eval(
                        RENEW_LUA,
                        vec![key.clone()],
                        vec![token.clone(), ttl_ms.to_string()],
                    ),
                )
                .await;
            match renewed {
                Ok(Ok(1)) => {
                    fence.mark_renewed(sent);
                    continue;
                }
                Ok(Ok(_)) => {
                    tracing::warn!(key = %key, "lease no longer ours on renew; fencing off");
                    fence.invalidate();
                    return;
                }
                Ok(Err(e)) => {
                    tracing::warn!(key = %key, error = %e, "lease renew failed; fencing off");
                    fence.invalidate();
                    return;
                }
                Err(_) => {
                    tracing::warn!(
                        key = %key,
                        timeout_ms = period.as_millis(),
                        "lease renew timed out; fencing off"
                    );
                    fence.invalidate();
                    return;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(30);

    fn fence() -> ValkeyFence {
        ValkeyFence::new(Instant::now(), backstop_for(TTL))
    }

    #[test]
    fn backstop_leaves_headroom_before_server_side_expiry() {
        // The whole point: we must stop acting strictly before the key can
        // expire and a peer can take it.
        assert!(backstop_for(TTL) < TTL);
        // And strictly after one renew period, or a single in-flight renew
        // would trip it.
        assert!(backstop_for(TTL) > renew_period(TTL));
    }

    #[test]
    fn a_freshly_acquired_lease_is_valid() {
        assert!(fence().check_at(0).is_ok());
    }

    #[test]
    fn fence_holds_while_renews_keep_landing() {
        let f = fence();
        let period = renew_period(TTL).as_millis() as u64;
        // Simulate the renew task ticking normally for a few TTLs.
        for i in 1..=10u64 {
            let at = i * period;
            assert!(f.check_at(at).is_ok(), "should be valid at {at}ms");
            f.last_ok_millis.fetch_max(at, Ordering::SeqCst);
        }
    }

    #[test]
    fn fence_fails_once_the_last_confirmed_renew_goes_stale() {
        let f = fence();
        let backstop = backstop_for(TTL).as_millis() as u64;
        // One renew lands, then the task stops answering entirely (a hang or
        // a starved runtime — nothing ever calls invalidate()).
        f.mark_renewed(f.origin + Duration::from_millis(1_000));
        assert!(f.check_at(1_000 + backstop - 1).is_ok());
        assert!(
            f.check_at(1_000 + backstop).is_err(),
            "must fence off at the backstop even though no renew ever failed"
        );
    }

    #[test]
    fn a_stale_fence_stays_invalid_even_if_a_renew_lands_late() {
        let f = fence();
        let backstop = backstop_for(TTL).as_millis() as u64;
        assert!(f.check_at(backstop).is_err());
        // A peer may own the lease by now; a late renew must not resurrect us.
        f.mark_renewed(f.origin + Duration::from_millis(backstop + 10));
        assert!(f.check_at(backstop + 10).is_err(), "latched invalid");
    }

    #[test]
    fn an_explicit_invalidate_beats_a_fresh_watermark() {
        let f = fence();
        f.invalidate();
        f.mark_renewed(Instant::now());
        assert!(f.check_at(0).is_err());
    }

    #[test]
    fn watermark_never_moves_backwards() {
        let f = fence();
        f.mark_renewed(f.origin + Duration::from_millis(5_000));
        // An out-of-order/late reply reporting an older send time.
        f.mark_renewed(f.origin + Duration::from_millis(1_000));
        assert_eq!(f.last_ok_millis.load(Ordering::SeqCst), 5_000);
    }

    #[test]
    fn tiny_ttls_still_produce_sane_bounds() {
        // Guard the .max() floors: a sub-millisecond TTL must not yield a
        // zero backstop (which would fence off instantly).
        for ms in [1u64, 10, 100, 1_000] {
            let ttl = Duration::from_millis(ms);
            assert!(backstop_for(ttl) > Duration::ZERO, "ttl={ms}ms");
            assert!(renew_period(ttl) > Duration::ZERO, "ttl={ms}ms");
        }
    }
}
