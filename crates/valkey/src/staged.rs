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
//! replayed. Three cases slip through:
//!
//! 1. An instance that boots **after** the staging and seeds its catalog by
//!    walking the bucket. It finds the objects — they are still there — and
//!    inserts the blocks as live. Nobody will re-announce the staging.
//! 2. An instance that never saw the block's `Created`, so the `SoftDeleted`
//!    applied to nothing, and a later poll or walk then inserts it live.
//! 3. A block whose objects were *hard* deleted between a walk fetching its
//!    sidecar and that walk inserting the row — the `Deleted` event lands in
//!    the gap and deletes nothing, then the insert resurrects it.
//!
//! None is recoverable from bucket state, because a staged deletion is
//! deliberately invisible in the bucket. So the staging is also written here,
//! as one key per block:
//!
//! ```text
//! SET <ns>/deleted/<block_uuid> "<staged_at>:<delete_eligible_at>" PX <ttl_ms>
//! ```
//!
//! read back with a client-side `SCAN` ([`list_staged_deletions`]). A booting
//! instance walks the bucket, then — before it serves anything — reads this set
//! and applies it, so it starts up already knowing what its peers know. The
//! same read after each poll and walk closes cases 2 and 3.
//!
//! # Entries are never deleted, only expired
//!
//! Nothing removes an entry when the block is finally reaped. That is
//! deliberate: for the rest of the window the entry is a **fence**, and case 3
//! above is exactly why one is needed. A walk that resurrects a hard-deleted
//! block finds the entry still present and hides it again. Applying an entry
//! for a block nobody has is a no-op, so a lingering entry costs nothing.
//!
//! It also keeps every operation single-key, which is what makes the registry
//! usable on a Valkey cluster: no multi-key `DEL` to hit `CROSSSLOT`.
//!
//! # Why the TTL is a floor, not a guess
//!
//! The entry has to outlive the *deletion*, not the deadline: a reaper that
//! crashes, loses its lease, or keeps failing against the bucket leaves the
//! objects in place indefinitely, and an entry that expired meanwhile would let
//! a freshly-booted instance serve data retention had deliberately hidden.
//! Every retention pass therefore re-stages the rows that are still pending, so
//! the TTL is renewed for as long as the work is outstanding and only lapses
//! once the block is really gone.
//!
//! # Why the value carries `staged_at`
//!
//! Deadlines must never be compared across machines. Pending-deletion reaping
//! is deliberately lease-free (see `scry_cluster::maintain`), so an absolute
//! deadline written by an instance with a slow clock would look *already past*
//! to every peer, and they would reap immediately — collapsing the grace window
//! the owner intended. Carrying the staging instant alongside the deadline lets
//! a reader take only the **duration** (`eligible - staged_at`) and re-base it
//! on its own clock. Erring long is safe (a late reap is idempotent and
//! `NotFound`-tolerant); erring short deletes data out from under live readers.

use std::time::Duration;

use anyhow::{Context, Result};
use fred::prelude::*;
use scry_catalog::CatalogHandle;
use scry_cluster::StagedDeletion;
use uuid::Uuid;

use crate::ValkeyClient;

/// Slack added to every entry's TTL on top of the grace window.
///
/// The entry must outlive the window it describes rather than expiring at the
/// boundary, because the reaper may run late (a missed tick, a lease handed
/// over, a slow bucket). This is only the *floor*: a reap that stalls for
/// longer keeps the entry alive by re-staging, so this margin covers ordinary
/// scheduling jitter, not failure.
const STAGED_TTL_MARGIN: Duration = Duration::from_secs(300);

/// How many keys to ask for per `SCAN` page, and how many commands to put in
/// one pipeline. Large enough that 100k entries is a manageable number of round
/// trips, small enough that no single response is unbounded.
const SCAN_PAGE: u32 = 512;
const PIPE_BATCH: usize = 512;

/// Encode the entry value: the staging instant and the deadline, so a reader
/// can recover the *duration* without trusting our clock.
fn encode_value(staged_at_unix_nano: u64, delete_eligible_at_unix_nano: u64) -> String {
    format!("{staged_at_unix_nano}:{delete_eligible_at_unix_nano}")
}

/// Inverse of [`encode_value`]. `None` for anything malformed — a truncated or
/// foreign value must be skipped, never defaulted, since a zero deadline would
/// make the block instantly reapable.
fn decode_value(raw: &str) -> Option<(u64, u64)> {
    let (staged, eligible) = raw.split_once(':')?;
    Some((staged.parse().ok()?, eligible.parse().ok()?))
}

/// Record that `uuids` have been staged for deletion and become reapable
/// `delete_eligible_at_unix_nano - staged_at_unix_nano` after the staging.
///
/// Both timestamps come from the caller's clock, and both are stored, so no
/// reader ever has to assume the two clocks agree.
///
/// Writes are **pipelined**: this runs inside the single sink worker, and one
/// awaited round trip per block would stall every other event behind a large
/// retention pass. Every uuid is attempted even when some fail, because
/// stopping at the first error would leave peers holding a prefix of the
/// staging while the `SoftDeleted` event announced all of it; the error says
/// how many of how many landed.
pub async fn stage_deletions(
    client: &ValkeyClient,
    uuids: &[Uuid],
    delete_eligible_at_unix_nano: u64,
    staged_at_unix_nano: u64,
) -> Result<()> {
    if uuids.is_empty() {
        return Ok(());
    }
    let grace_nanos = delete_eligible_at_unix_nano.saturating_sub(staged_at_unix_nano);
    let ttl_ms = (Duration::from_nanos(grace_nanos) + STAGED_TTL_MARGIN).as_millis();
    // Clamp: fred takes an i64, and a nonsense deadline must not wrap.
    let ttl_ms = ttl_ms.min(i64::MAX as u128) as i64;
    let value = encode_value(staged_at_unix_nano, delete_eligible_at_unix_nano);
    let keys = client.keys();
    let client = client.inner();

    let mut failed = 0usize;
    let mut last_err: Option<String> = None;
    for chunk in uuids.chunks(PIPE_BATCH) {
        let pipe = client.pipeline();
        for uuid in chunk {
            // Queuing into a pipeline is local — this await does not round-trip.
            pipe.set::<Value, _, _>(
                keys.staged(*uuid),
                value.as_str(),
                Some(Expiration::PX(ttl_ms)),
                None,
                false,
            )
            .await
            .context("queueing staged-deletion SET")?;
        }
        for r in pipe.try_all::<Value>().await {
            if let Err(e) = r {
                failed += 1;
                last_err = Some(e.to_string());
            }
        }
    }

    if failed > 0 {
        anyhow::bail!(
            "staged {} of {} deletions in Valkey ({failed} failed); late-booting \
             peers may re-list the rest until they are reaped: {}",
            uuids.len() - failed,
            uuids.len(),
            last_err.unwrap_or_default()
        );
    }
    tracing::debug!(
        blocks = uuids.len(),
        ttl_ms,
        "recorded staged deletions in Valkey"
    );
    Ok(())
}

/// Read the whole registry: every block a peer has staged whose entry has not
/// yet expired. Order is unspecified.
///
/// The `SCAN` runs **client-side**, one page at a time. It used to loop inside
/// a Lua script, which was wrong twice over: a script is atomic, so the server
/// could serve nobody — not a lease renewal, not a publish — until the whole
/// keyspace had been walked; and `SCAN` inside a zero-key script is routed to a
/// single node, so on a cluster it would silently see one shard's worth of
/// entries. (`MATCH` filters what comes back but does not reduce the work, so
/// the cost scales with the whole keyspace, not with our prefix.)
pub async fn list_staged_deletions(client: &ValkeyClient) -> Result<Vec<StagedDeletion>> {
    use futures::StreamExt;

    let keys_ns = client.keys().clone();
    let client = client.inner();
    let pattern = format!("{}*", keys_ns.staged_prefix());
    let mut keys: Vec<String> = Vec::new();
    {
        // On a cluster every primary has to be scanned; plain `scan` would only
        // reach whichever node the client happened to pick.
        let mut stream = if client.is_clustered() {
            client
                .scan_cluster_buffered(&pattern, Some(SCAN_PAGE), None)
                .boxed()
        } else {
            client
                .scan_buffered(&pattern, Some(SCAN_PAGE), None)
                .boxed()
        };
        while let Some(key) = stream.next().await {
            let key = key.context("scanning the staged-deletions registry")?;
            if let Some(s) = key.as_str() {
                keys.push(s.to_string());
            }
        }
    }

    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(PIPE_BATCH) {
        // A pipeline, not `MGET`: fred routes each command in a pipeline
        // independently, so this stays correct on a cluster where one chunk's
        // keys span hash slots.
        let pipe = client.pipeline();
        for key in chunk {
            pipe.get::<Value, _>(key.as_str())
                .await
                .context("queueing staged-deletion GET")?;
        }
        let values: Vec<Option<String>> = pipe
            .all()
            .await
            .context("reading the staged-deletions registry")?;

        for (key, value) in chunk.iter().zip(values) {
            // A key that expired between the SCAN and the GET is simply gone.
            let Some(raw) = value else { continue };
            let (Some(uuid), Some((staged_at, eligible_at))) =
                (keys_ns.staged_uuid(key), decode_value(&raw))
            else {
                tracing::warn!(key = %key, "skipping unparseable staged-deletion entry");
                continue;
            };
            out.push(StagedDeletion {
                uuid,
                staged_at_unix_nano: staged_at,
                delete_eligible_at_unix_nano: eligible_at,
            });
        }
    }
    Ok(out)
}

/// Read the staged set and apply it to `catalog`: the one call both daemons
/// make, at boot and after each poll and walk.
///
/// Errors are returned rather than logged so the caller can say which of its
/// phases failed; neither failure mode is fatal, because the fallback is the
/// behaviour that existed before this registry (a peer-deleted block healed at
/// query time by `EvictOnNotFound`).
///
/// Returns the number of rows *newly* hidden, so a caller can log convergence
/// without emitting a line on every quiet cycle.
pub async fn converge_staged_deletions<C: CatalogHandle + ?Sized>(
    client: &ValkeyClient,
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

/// [`converge_staged_deletions`] with the daemons' shared logging, and a
/// `None` client meaning "no Valkey, nothing to converge against".
///
/// Both daemons call this at the end of every poll and full walk, so it stays
/// quiet unless something actually changed — otherwise a healthy fleet would
/// emit two lines per poll interval forever. `after` names the phase that just
/// ran, so a log line says which insert path resurrected the block.
pub async fn converge_staged_deletions_logged<C>(
    client: Option<&crate::ValkeyClient>,
    catalog: &C,
    after: &str,
) where
    C: CatalogHandle + ?Sized,
{
    let Some(client) = client else { return };
    match converge_staged_deletions(client, catalog).await {
        Ok(n) if n > 0 => {
            tracing::info!(
                hidden = n,
                after,
                "hid blocks a peer had staged for deletion"
            )
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, after, "applying peers' staged deletions failed; blocks a peer has hidden may be listed until they are reaped")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_round_trips() {
        assert_eq!(decode_value(&encode_value(7, 11)), Some((7, 11)));
    }

    /// A truncated or foreign value must be skipped rather than parsed into a
    /// deadline of zero, which would make the block instantly reapable.
    #[test]
    fn malformed_values_are_rejected_not_defaulted() {
        assert_eq!(decode_value(""), None);
        assert_eq!(decode_value("12345"), None, "no separator");
        assert_eq!(decode_value("abc:def"), None);
        assert_eq!(decode_value("12:"), None);
    }
}
