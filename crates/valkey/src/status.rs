//! The per-instance status registry — how any daemon's status page (D-057)
//! renders the whole fleet.
//!
//! Every ingest / query instance that has Valkey configured heartbeats its full
//! status snapshot (a JSON [`scry_server::StatusSnapshot`], produced by the
//! daemon) into a per-instance key:
//!
//! ```text
//! SET scry/status/<instance_uuid> "<snapshot-json>" PX <ttl_ms>
//! ```
//!
//! Unlike the tail registry ([`crate::registry`]) — whose *value* (the address)
//! is static and only the TTL is refreshed — the status **value changes every
//! heartbeat**, so the renew task re-invokes a producer closure and re-`SET`s
//! the fresh JSON each tick. A status page enumerates the live set with one
//! read-only Lua `SCAN` ([`discover_status_blobs`]) and marks which snapshot is
//! its own. A crashed instance's key simply expires — the fleet view is
//! best-effort, never a correctness input.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use fred::prelude::*;
use uuid::Uuid;

/// Key prefix for the per-instance status registry.
pub const STATUS_PREFIX: &str = "scry/status/";

/// TTL for a status heartbeat. Chosen short so a dead instance drops off the
/// fleet view promptly; the renew task re-publishes every `ttl/3` (~2s), which
/// also refreshes the live counters, so the page feels current.
pub const STATUS_TTL: Duration = Duration::from_secs(6);

/// One read-only `EVAL` that `SCAN`s the status prefix and `GET`s each key, so
/// expired (crashed) instances are naturally absent. ARGV[1] = match pattern.
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

/// Delete the status key iff its value is still ours (compare-and-`DEL`), so a
/// slow predecessor's deregister can't remove a successor's fresh entry. Since
/// the value churns every heartbeat, we compare against the *last-published*
/// blob. KEYS[1]=key, ARGV[1]=last blob.
const DEREGISTER_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
"#;

/// The registry key for an instance.
fn key_for(instance_uuid: Uuid) -> String {
    format!("{STATUS_PREFIX}{instance_uuid}")
}

/// Produces the current status snapshot as a JSON string. Invoked on every
/// heartbeat, so it must be cheap (a handful of atomic reads + serialise).
pub type StatusProducer = Arc<dyn Fn() -> String + Send + Sync>;

/// A live registration of this instance's status. Auto-refreshes the published
/// snapshot in the background; [`deregister`](Self::deregister) removes the key
/// promptly, and dropping stops refreshing and lets the key expire via its TTL.
pub struct StatusRegistration {
    client: Client,
    key: String,
    /// The most recently published blob, for the compare-and-`DEL` deregister.
    last: Arc<std::sync::Mutex<String>>,
    renew: tokio::task::JoinHandle<()>,
}

impl StatusRegistration {
    /// Register `instance_uuid`'s status and start the heartbeat. Performs the
    /// initial `SET` synchronously (so a broken Valkey surfaces immediately),
    /// then re-publishes `producer()` every `ttl/3`.
    pub async fn spawn(
        client: Client,
        instance_uuid: Uuid,
        ttl: Duration,
        producer: StatusProducer,
    ) -> Result<Self> {
        let key = key_for(instance_uuid);
        let ttl_ms = ttl.as_millis().max(1) as i64;
        let last = Arc::new(std::sync::Mutex::new(String::new()));

        let initial = producer();
        set_key(&client, &key, &initial, ttl_ms)
            .await
            .with_context(|| format!("registering status at {key}"))?;
        *last.lock().unwrap() = initial;

        let renew = spawn_renew(
            client.clone(),
            key.clone(),
            ttl,
            ttl_ms,
            producer,
            last.clone(),
        );

        tracing::info!(%key, ttl_ms, "registered status in Valkey");
        Ok(Self {
            client,
            key,
            last,
            renew,
        })
    }

    /// Stop refreshing and remove the key (compare-and-`DEL`). Best-effort — on
    /// error the key simply expires via its TTL.
    pub async fn deregister(self) {
        self.renew.abort();
        let last = self.last.lock().unwrap().clone();
        let res: Result<i64, Error> = self
            .client
            .eval(DEREGISTER_LUA, vec![self.key.clone()], vec![last])
            .await;
        if let Err(e) = res {
            tracing::warn!(key = %self.key, error = %e, "status deregister failed; will expire via TTL");
        }
    }
}

impl Drop for StatusRegistration {
    fn drop(&mut self) {
        self.renew.abort();
    }
}

async fn set_key(client: &Client, key: &str, blob: &str, ttl_ms: i64) -> Result<()> {
    let _: Value = client
        .set(key, blob, Some(Expiration::PX(ttl_ms)), None, false)
        .await
        .context("SET status-registry key")?;
    Ok(())
}

/// Background heartbeat: every `ttl/3`, re-`SET` the key with a freshly produced
/// snapshot (refreshing both the TTL and the live counters, and self-healing if
/// the key was evicted). A transient error is logged and retried next tick.
fn spawn_renew(
    client: Client,
    key: String,
    ttl: Duration,
    ttl_ms: i64,
    producer: StatusProducer,
    last: Arc<std::sync::Mutex<String>>,
) -> tokio::task::JoinHandle<()> {
    let period = (ttl / 3).max(Duration::from_millis(50));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            let blob = producer();
            match set_key(&client, &key, &blob, ttl_ms).await {
                Ok(()) => *last.lock().unwrap() = blob,
                Err(e) => {
                    tracing::warn!(key = %key, error = %e, "status heartbeat failed; retrying next tick")
                }
            }
        }
    })
}

/// Enumerate the live status snapshots currently in the registry (each a JSON
/// blob). Order is unspecified.
pub async fn discover_status_blobs(client: &Client) -> Result<Vec<String>> {
    let pattern = format!("{STATUS_PREFIX}*");
    let blobs: Vec<String> = client
        .eval(DISCOVER_LUA, Vec::<String>::new(), vec![pattern])
        .await
        .context("discovering status blobs")?;
    Ok(blobs)
}
