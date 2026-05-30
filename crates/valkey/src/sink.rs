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
    pub fn spawn(
        client: fred::clients::Client,
        origin: Uuid,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BlockEvent>(SINK_CAPACITY);
        let task = tokio::spawn(async move {
            let mut seq: u64 = 0;
            while let Some(event) = rx.recv().await {
                seq += 1;
                let env = Envelope::new(origin, seq, event);
                if let Err(e) = publish_envelope(&client, &env).await {
                    tracing::warn!(error = %e, "publishing block event failed; dropping (polling will backstop)");
                }
            }
        });
        (Self { tx }, task)
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
