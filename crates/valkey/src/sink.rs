//! [`ValkeySink`] — the [`BlockEventSink`] that fans block lifecycle events
//! out to peers over pub/sub.
//!
//! `emit` is called synchronously on hot paths (an upload's `insert_block`, a
//! compaction's supersede/delete), so it must never block or fail at the call
//! site. It just `try_send`s the event onto a bounded channel drained by a
//! background publisher task; if the channel is full (Valkey slow/down) the
//! event is **dropped** — polling and the full walk are the backstops, so a
//! lost publish only costs a little convergence latency, never correctness.
//!
//! The publisher task stamps each event with this instance's `origin` and a
//! monotonic `seq` (both advisory — for debugging / drop metrics) and
//! publishes it. Errors are logged and skipped.

use scry_block::{BlockEvent, BlockEventSink, Envelope};
use uuid::Uuid;

use crate::pubsub::publish_envelope;
use crate::ValkeyClient;

/// Bounded buffer of pending events. Generous enough to ride out brief Valkey
/// hiccups; overflow drops (backstopped by polling).
const SINK_CAPACITY: usize = 4096;

/// A [`BlockEventSink`] backed by Valkey pub/sub. Construct with
/// [`ValkeySink::spawn`]; clone freely (cheap — holds an mpsc sender).
#[derive(Clone)]
pub struct ValkeySink {
    tx: tokio::sync::mpsc::Sender<BlockEvent>,
}

impl ValkeySink {
    /// Spawn the background publisher and return the sink. `client` is the
    /// command/publish handle; `origin` is this instance's id (stamped on
    /// every envelope). The returned [`JoinHandle`](tokio::task::JoinHandle)
    /// ends when the last sink clone is dropped.
    pub fn spawn(client: ValkeyClient, origin: Uuid) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BlockEvent>(SINK_CAPACITY);
        let task = tokio::spawn(async move {
            let mut seq: u64 = 0;
            while let Some(event) = rx.recv().await {
                seq += 1;
                mirror_deletion_staging(&client, &event).await;
                let env = Envelope::new(origin, seq, event);
                if let Err(e) = publish_envelope(&client, &env).await {
                    tracing::warn!(error = %e, "publishing block event failed; dropping (polling will backstop)");
                }
            }
        });
        (Self { tx }, task)
    }
}

/// Keep the staged-deletions registry ([`crate::staged`]) in step with the
/// deletion events flowing past.
///
/// Pub/sub only reaches instances listening at that instant and is never
/// replayed, so a `SoftDeleted` is invisible to anyone who boots afterwards —
/// and a staged block is deliberately invisible in the bucket too, since its
/// objects are still there. Mirroring the staging into a key with a TTL gives
/// a late arrival somewhere to look.
///
/// Done here, in the publisher, rather than in the retention engine: the engine
/// takes a `&dyn BlockEventSink` precisely so it never has to know Valkey
/// exists, and this sink is already the Valkey-aware half of that seam.
///
/// Nothing is removed on `Deleted`. An entry stays until its TTL lapses, so
/// that for the rest of the window it acts as a **fence**: a peer's bucket walk
/// that fetched the block's sidecar just before the objects were reaped will
/// insert the row afterwards, and only a still-present entry hides it again.
/// Applying an entry for a block nobody has is a no-op, so a lingering entry
/// costs nothing — and never issuing a multi-key `DEL` keeps the registry
/// usable on a Valkey cluster.
///
/// Failures are logged, not propagated: a missed stage costs a peer one
/// planned-then-404'd query, which is the `EvictOnNotFound` self-heal that
/// existed before this registry.
async fn mirror_deletion_staging(client: &ValkeyClient, event: &BlockEvent) {
    match event {
        BlockEvent::SoftDeleted {
            uuids,
            deleted_at_unix_nano,
            delete_eligible_at_unix_nano,
            ..
        } => {
            if let Err(e) = crate::staged::stage_deletions(
                client,
                uuids,
                *delete_eligible_at_unix_nano,
                *deleted_at_unix_nano,
            )
            .await
            {
                tracing::warn!(error = %e, "recording staged deletions failed; a peer booting during the grace window may re-list these blocks until they are reaped");
            }
        }
        BlockEvent::Created { .. } | BlockEvent::Superseded { .. } | BlockEvent::Deleted { .. } => {
        }
    }
}

impl BlockEventSink for ValkeySink {
    fn emit(&self, event: BlockEvent) {
        if self.tx.try_send(event).is_err() {
            // Full or closed: drop. Convergence falls back to polling.
            tracing::debug!("block-event sink full; dropping event (polling will backstop)");
        }
    }
}
