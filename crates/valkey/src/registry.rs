//! The ingester tail-address registry — how the queryd live-tail front-door
//! (D-053) discovers which ingesters to fan-in from.
//!
//! An ingester that has Valkey configured heartbeats its **advertised
//! tail-serving address** (its ordinary ingest `--listen` endpoint, which
//! already serves `Subscribe`/`TailRecord`) into a per-instance key:
//!
//! ```text
//! SET scry/tail/ingesters/<writer_uuid> "<host:port>" PX <ttl_ms>
//! ```
//!
//! renewed every `ttl/3`. The queryd relay enumerates the live set with one
//! read-only Lua `SCAN` ([`discover_tail_endpoints`]). This mirrors the lease
//! idiom in [`crate::lease`]: **server-side `PX` expiry** (no client clocks),
//! and `eval` for the atomic bits. A crashed ingester's key simply expires —
//! discovery is best-effort, so a briefly-stale entry only costs the relay a
//! failed dial, never correctness.

use std::time::Duration;

use anyhow::{Context, Result};
use fred::prelude::*;
use uuid::Uuid;

/// Key prefix for the per-instance tail-address registry.
pub const TAIL_REGISTRY_PREFIX: &str = "scry/tail/ingesters/";

/// Enumerate live ingester tail addresses. One read-only `EVAL` that `SCAN`s
/// the registry prefix and `GET`s each key, so expired (crashed) instances are
/// naturally absent. Order is unspecified.
///
/// ARGV[1] = match pattern. No `KEYS` — this touches only registry keys the
/// caller owns the namespace of, and is read-only, so `SCAN`-in-script is safe.
const DISCOVER_LUA: &str = r#"
local cursor = "0"
local out = {}
repeat
  local r = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', 100)
  cursor = r[1]
  for _, k in ipairs(r[2]) do
    local v = redis.call('GET', k)
    if v then out[#out + 1] = v end
  end
until cursor == "0"
return out
"#;

/// Delete the registry key iff its value is still our advertised address, so a
/// slow predecessor's deregister can't remove a successor's fresh entry.
/// KEYS[1]=key, ARGV[1]=addr. (Same-uuid restarts advertise the same addr, so
/// the TTL is the real backstop; this just makes graceful shutdown prompt.)
const DEREGISTER_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
"#;

/// The registry key for an instance.
fn key_for(writer_uuid: Uuid) -> String {
    format!("{TAIL_REGISTRY_PREFIX}{writer_uuid}")
}

/// A live registration of this instance's tail address. Auto-renews in the
/// background; [`deregister`](Self::deregister) removes the key promptly, and
/// dropping stops renewing and lets the key expire via its TTL.
pub struct TailRegistration {
    client: Client,
    key: String,
    addr: String,
    renew: tokio::task::JoinHandle<()>,
}

impl TailRegistration {
    /// Register `addr` for `writer_uuid` and start the heartbeat. Performs the
    /// initial `SET` synchronously (so a broken Valkey surfaces immediately),
    /// then renews every `ttl/3`.
    pub async fn spawn(
        client: Client,
        writer_uuid: Uuid,
        addr: String,
        ttl: Duration,
    ) -> Result<Self> {
        let key = key_for(writer_uuid);
        let ttl_ms = ttl.as_millis().max(1) as i64;

        set_key(&client, &key, &addr, ttl_ms)
            .await
            .with_context(|| format!("registering tail address {addr} at {key}"))?;

        let renew = spawn_renew(client.clone(), key.clone(), addr.clone(), ttl, ttl_ms);

        tracing::info!(%addr, %key, ttl_ms, "registered tail address in Valkey");
        Ok(Self {
            client,
            key,
            addr,
            renew,
        })
    }

    /// Stop renewing and remove the key (compare-and-`DEL`). Best-effort — on
    /// error the key simply expires via its TTL.
    pub async fn deregister(self) {
        self.renew.abort();
        let res: Result<i64, Error> = self
            .client
            .eval(
                DEREGISTER_LUA,
                vec![self.key.clone()],
                vec![self.addr.clone()],
            )
            .await;
        if let Err(e) = res {
            tracing::warn!(key = %self.key, error = %e, "tail-registry deregister failed; will expire via TTL");
        }
    }
}

impl Drop for TailRegistration {
    fn drop(&mut self) {
        // Can't await a DEL here; stop renewing and let the key expire via TTL.
        // `deregister()` is the prompt path.
        self.renew.abort();
    }
}

async fn set_key(client: &Client, key: &str, addr: &str, ttl_ms: i64) -> Result<()> {
    let _: Value = client
        .set(key, addr, Some(Expiration::PX(ttl_ms)), None, false)
        .await
        .context("SET tail-registry key")?;
    Ok(())
}

/// Background heartbeat: every `ttl/3`, re-`SET` the key (refreshing the TTL and
/// self-healing if the key was evicted). A transient error is logged and
/// retried on the next tick — a missed beat costs at most one dropped discovery
/// until the next renew, never correctness.
fn spawn_renew(
    client: Client,
    key: String,
    addr: String,
    ttl: Duration,
    ttl_ms: i64,
) -> tokio::task::JoinHandle<()> {
    let period = (ttl / 3).max(Duration::from_millis(50));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            if let Err(e) = set_key(&client, &key, &addr, ttl_ms).await {
                tracing::warn!(key = %key, error = %e, "tail-registry heartbeat failed; retrying next tick");
            }
        }
    })
}

/// Enumerate the live ingester tail addresses currently in the registry.
pub async fn discover_tail_endpoints(client: &Client) -> Result<Vec<String>> {
    let pattern = format!("{TAIL_REGISTRY_PREFIX}*");
    let addrs: Vec<String> = client
        .eval(DISCOVER_LUA, Vec::<String>::new(), vec![pattern])
        .await
        .context("discovering tail endpoints")?;
    Ok(addrs)
}
