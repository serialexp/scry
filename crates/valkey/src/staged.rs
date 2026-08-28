//! The staged-deletions registry — how an instance that was not listening
//! learns which blocks its peers have already hidden.
//!
//! # Why this exists
//!
//! Retention deletes a block in two steps: **stage** it (set `deleted_at` in
//! the catalog, so queries stop listing it) and, once the grace window has
//! passed, **reap** it (DELETE the objects, drop the row). In between, the
//! block is hidden but its objects are still in the bucket — that is the whole
//! point of the window.
//!
//! [`BlockEvent::SoftDeleted`](scry_block::BlockEvent::SoftDeleted) tells peers
//! about the staging so they hide the same rows at the same time. But pub/sub
//! only reaches instances that are *listening at that moment*, and it is never
//! replayed. Two cases slip through:
//!
//! 1. An instance that boots **after** the staging and seeds its catalog by
//!    walking the bucket. It finds the objects — they are still there — and
//!    inserts the blocks as live. Nobody will re-announce the staging.
//! 2. An instance that never saw the block's `Created`, so the `SoftDeleted`
//!    applied to nothing, and a later poll or walk then inserts it live.
//!
//! Neither is recoverable from bucket state, because a staged deletion is
//! deliberately invisible in the bucket. So the staging is also written here,
//! as one key per block:
//!
//! ```text
//! SET scry/deleted/<block_uuid> "<delete_eligible_at_unix_nano>" PX <ttl_ms>
//! ```
//!
//! and read back with one read-only Lua `SCAN` ([`list_staged_deletions`]).
//! A booting instance walks the bucket, then — before it serves anything —
//! reads this set and applies it, so it starts up already knowing what its
//! peers know. Same read after every periodic poll and walk, which is what
//! closes case 2.
//!
//! # Why the TTL is right
//!
//! The grace deadline *is* an expiry, so Valkey's own `PX` does the
//! housekeeping: an entry becomes irrelevant exactly when the objects are
//! reaped, and disappears on its own. There is no cleanup pass and no way to
//! leak entries — a crashed reaper's keys expire like anyone else's.
//!
//! [`unstage_deletions`] removes entries promptly once the objects really are
//! gone (the peer-visible truth is then the absence of the block itself), but
//! it is only an optimisation: a stale entry names a block nobody has, and
//! applying it is a no-op.

use std::time::Duration;

use anyhow::{Context, Result};
use fred::prelude::*;
use scry_catalog::CatalogHandle;
use uuid::Uuid;

/// Key prefix for the staged-deletions registry.
pub const STAGED_DELETION_PREFIX: &str = "scry/deleted/";

/// Slack added to every entry's TTL on top of the remaining grace window.
///
/// The entry must outlive the grace window it describes, not expire exactly at
/// the boundary: the reaper may run late (a missed tick, a lost lease handed to
/// a peer, a slow bucket), and until the objects are actually gone a booting
/// instance still needs to be told to hide them. Erring long is free — the
/// entry names a block that is either still hidden or already deleted, and
/// applying it in the second case is a no-op.
const STAGED_TTL_MARGIN: Duration = Duration::from_secs(300);

/// Enumerate the registry: one read-only `EVAL` that `SCAN`s the prefix and
/// `GET`s each key, returning a flat `[key, value, key, value, …]`. Expired
/// entries are naturally absent.
///
/// ARGV[1] = match pattern. No `KEYS` — read-only, and confined to a prefix
/// this crate owns, so `SCAN`-in-script is safe (same idiom as
/// [`crate::registry`]).
const LIST_LUA: &str = r#"
local cursor = "0"
local out = {}
repeat
  local r = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', 100)
  cursor = r[1]
  for _, k in ipairs(r[2]) do
    local v = redis.call('GET', k)
    if v then
      out[#out + 1] = k
      out[#out + 1] = v
    end
  end
until cursor == "0"
return out
"#;

/// The registry key for a block.
fn key_for(uuid: Uuid) -> String {
    format!("{STAGED_DELETION_PREFIX}{uuid}")
}

/// Parse a `uuid` back out of a registry key. Returns `None` for a key that
/// does not carry the prefix or whose remainder is not a UUID — a foreign key
/// caught by the `SCAN` pattern is skipped, never fatal.
fn uuid_from_key(key: &str) -> Option<Uuid> {
    key.strip_prefix(STAGED_DELETION_PREFIX)?.parse().ok()
}

/// Record that `uuids` have been staged for deletion and will become reapable
/// at `delete_eligible_at_unix_nano`.
///
/// `now_unix_nano` is passed in rather than read here so the TTL is a pure
/// function of the caller's clock — the same clock that produced the deadline.
pub async fn stage_deletions(
    client: &Client,
    uuids: &[Uuid],
    delete_eligible_at_unix_nano: u64,
    now_unix_nano: u64,
) -> Result<()> {
    if uuids.is_empty() {
        return Ok(());
    }
    let remaining_nanos = delete_eligible_at_unix_nano.saturating_sub(now_unix_nano);
    let ttl_ms = (Duration::from_nanos(remaining_nanos) + STAGED_TTL_MARGIN).as_millis();
    // Clamp: fred takes an i64, and a nonsense deadline must not wrap.
    let ttl_ms = ttl_ms.min(i64::MAX as u128) as i64;
    let value = delete_eligible_at_unix_nano.to_string();

    for uuid in uuids {
        let _: Value = client
            .set(
                key_for(*uuid),
                value.as_str(),
                Some(Expiration::PX(ttl_ms)),
                None,
                false,
            )
            .await
            .with_context(|| format!("staging deletion of block {uuid}"))?;
    }
    tracing::debug!(
        blocks = uuids.len(),
        ttl_ms,
        "recorded staged deletions in Valkey"
    );
    Ok(())
}

/// Drop registry entries for blocks whose objects have actually been reaped.
/// Best-effort: on error the entries expire via their TTL.
pub async fn unstage_deletions(client: &Client, uuids: &[Uuid]) -> Result<()> {
    if uuids.is_empty() {
        return Ok(());
    }
    let keys: Vec<String> = uuids.iter().map(|u| key_for(*u)).collect();
    let _: Value = client
        .del(keys)
        .await
        .context("unstaging reaped deletions")?;
    Ok(())
}

/// Read the whole registry: `(block uuid, delete_eligible_at_unix_nano)` for
/// every block a peer has staged and not yet reaped. Order is unspecified.
pub async fn list_staged_deletions(client: &Client) -> Result<Vec<(Uuid, u64)>> {
    let pattern = format!("{STAGED_DELETION_PREFIX}*");
    let flat: Vec<String> = client
        .eval(LIST_LUA, Vec::<String>::new(), vec![pattern])
        .await
        .context("listing staged deletions")?;

    let mut out = Vec::with_capacity(flat.len() / 2);
    for pair in flat.chunks_exact(2) {
        let (Some(uuid), Ok(eligible)) = (uuid_from_key(&pair[0]), pair[1].parse::<u64>()) else {
            tracing::warn!(key = %pair[0], "skipping unparseable staged-deletion entry");
            continue;
        };
        out.push((uuid, eligible));
    }
    Ok(out)
}

/// Read the staged set and apply it to `catalog`: the one call both daemons
/// make, at boot and on a timer.
///
/// Errors are returned rather than logged so the caller can say which of its
/// phases failed; neither failure mode is fatal, because the fallback is the
/// behaviour that existed before this registry (a peer-deleted block is healed
/// at query time by `EvictOnNotFound`).
pub async fn converge_staged_deletions<C: CatalogHandle + ?Sized>(
    client: &Client,
    catalog: &C,
) -> Result<usize> {
    let staged = list_staged_deletions(client).await?;
    if staged.is_empty() {
        return Ok(0);
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    scry_cluster::apply_staged_deletions(catalog, &staged, now)
}

/// Background loop that re-applies the staged set every `interval`.
///
/// Complements [`crate::sink`]'s live `SoftDeleted` publishing rather than
/// duplicating it: the event covers peers that already know the block, this
/// covers the block we learned about *after* the event went past — a poll or
/// walk inserts it as live, and the next tick here hides it again. Cheap
/// enough to run unconditionally: one `SCAN` over a prefix that is empty
/// whenever nothing is staged, and the catalog write is skipped entirely when
/// the set comes back empty.
pub fn spawn_staged_deletion_refresh<C>(
    client: Client,
    catalog: std::sync::Arc<C>,
    interval: Duration,
) -> tokio::task::JoinHandle<()>
where
    C: CatalogHandle + Send + Sync + 'static + ?Sized,
{
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval.max(Duration::from_secs(1)));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await; // consume the immediate first tick; boot already applied
        loop {
            tick.tick().await;
            match converge_staged_deletions(&client, catalog.as_ref()).await {
                Ok(n) if n > 0 => {
                    tracing::debug!(staged = n, "re-applied peers' staged deletions")
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "refreshing peers' staged deletions failed; retrying next tick")
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_round_trips_through_uuid() {
        let uuid = Uuid::now_v7();
        assert_eq!(uuid_from_key(&key_for(uuid)), Some(uuid));
    }

    #[test]
    fn foreign_keys_are_skipped_not_fatal() {
        assert_eq!(uuid_from_key("scry/tail/ingesters/whatever"), None);
        assert_eq!(uuid_from_key("scry/deleted/not-a-uuid"), None);
    }
}
