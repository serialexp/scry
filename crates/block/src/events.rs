//! Block lifecycle events for cross-instance catalog convergence.
//!
//! In a multi-instance deployment each instance keeps its own SQLite
//! catalog. When one instance uploads, compacts, or reaps a block, its
//! peers need to learn about it so their catalogs stay fresh and their
//! queries don't miss (or 404 on) recent changes. The low-latency path is
//! Valkey pub/sub (see `scry-valkey` / `scry-cluster`); polling and a
//! periodic full bucket walk are the source-of-truth backstops.
//!
//! This module defines only the **data** and the **emit seam**, both kept
//! dependency-free (serde + uuid, no tokio/Valkey) so the engines and the
//! ingest pipeline can emit events through a `&dyn BlockEventSink` without
//! depending on the transport. The Valkey-backed sink lives in
//! `scry-valkey`; the no-op sink ([`NoopSink`]) keeps the single-instance
//! path and tests transport-free.
//!
//! ## Idempotency
//!
//! Events may be **duplicated** (a publish retry, plus the same block later
//! rediscovered by polling) and **reordered** (network, or polling racing
//! pub/sub). Every consumer-side apply must therefore be idempotent; the
//! event shapes below are designed for that (e.g. [`BlockEvent::Superseded`]
//! carries the merged block's full meta so a receiver that missed the
//! corresponding [`BlockEvent::Created`] can still satisfy the catalog's
//! foreign-key constraint). The bucket is always the final authority.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BlockMeta;

/// A change to the set of blocks, broadcast from the instance that made it.
///
/// Serialised with serde_json: [`BlockEvent::Created`] is byte-identical to
/// the block's `meta.json` sidecar (it *is* a `BlockMeta`), so the same
/// forward-compatible `#[serde(default)]` fields that protect the sidecar
/// protect the event. Events are low-volume (one per block close / merge /
/// reap), so compactness is irrelevant and operator-debuggability of
/// `redis-cli SUBSCRIBE` output wins — control plane, not the hot wire path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BlockEvent {
    /// A new block exists: a freshly written L0 upload, or a compaction
    /// merge output. Apply = `insert_block(meta)` (idempotent INSERT OR
    /// IGNORE).
    Created { meta: BlockMeta },

    /// `inputs` were superseded by the merged block `by`. `by_meta` is the
    /// merged block's full metadata so a receiver that missed the `Created`
    /// for `by` can insert it first and satisfy the
    /// `superseded_by REFERENCES blocks(uuid)` foreign key. Apply = ensure
    /// `by_meta` present, then `mark_superseded(inputs, by)`.
    Superseded {
        inputs: Vec<Uuid>,
        by: Uuid,
        by_meta: BlockMeta,
    },

    /// `uuids` were hard-deleted from the bucket (compaction reaping its
    /// superseded inputs, or retention reaping expired blocks). `signal` is
    /// carried explicitly so the event routes to the right
    /// `scry/blocks/<signal>` channel without parsing the UUIDs. Apply =
    /// `delete_blocks(uuids)` (idempotent DELETE by key).
    Deleted { signal: String, uuids: Vec<Uuid> },
}

impl BlockEvent {
    /// The signal this event pertains to — the pub/sub channel selector
    /// (`scry/blocks/<signal>`).
    pub fn signal(&self) -> &str {
        match self {
            BlockEvent::Created { meta } => &meta.signal,
            BlockEvent::Superseded { by_meta, .. } => &by_meta.signal,
            BlockEvent::Deleted { signal, .. } => signal,
        }
    }
}

/// A pub/sub-framed [`BlockEvent`] with provenance. `origin` (the
/// publishing instance's id) and `seq` (a per-origin monotonic counter) are
/// **advisory only** — for debugging and dropped-message metrics. Correctness
/// never depends on them: applies are idempotent and self-delivered events
/// are harmless no-ops, so the consumer does not filter on `origin`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope {
    pub origin: Uuid,
    pub seq: u64,
    pub event: BlockEvent,
}

impl Envelope {
    pub fn new(origin: Uuid, seq: u64, event: BlockEvent) -> Self {
        Self { origin, seq, event }
    }

    /// Serialise to JSON bytes for publishing.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Parse JSON bytes received from a subscription.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Where an engine or the ingest pipeline hands block lifecycle events.
///
/// [`emit`](BlockEventSink::emit) is deliberately **synchronous and
/// infallible at the call site**: the real ([`scry-valkey`]) implementation
/// pushes onto a bounded channel drained by a background publisher task, so a
/// slow or dead Valkey never stalls an upload or a compaction. A dropped
/// event is fine — polling and the periodic full walk are the source-of-truth
/// backstops. The single-instance path uses [`NoopSink`].
pub trait BlockEventSink: Send + Sync {
    fn emit(&self, event: BlockEvent);
}

/// A [`BlockEventSink`] that discards everything — the single-instance path
/// (standalone `scry-compact` / `scry-retention` CLIs) and tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopSink;

impl BlockEventSink for NoopSink {
    #[inline]
    fn emit(&self, _event: BlockEvent) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(signal: &str) -> BlockMeta {
        BlockMeta {
            uuid: Uuid::now_v7(),
            signal: signal.to_string(),
            writer_id: Uuid::now_v7(),
            ts_min_unix_nano: 1,
            ts_max_unix_nano: 2,
            row_count: 10,
            byte_size: 100,
            schema_version: 1,
            level: 0,
            producer_version: String::new(),
            label_fingerprint_bloom: None,
            has_postings: false,
            postings_size_bytes: None,
            series_types: None,
            all_fingerprints: None,
            has_body_bloom: false,
            body_bloom_size_bytes: None,
            wal_seg_max: None,
            wal_shard: None,
        }
    }

    #[test]
    fn created_round_trips_and_reports_signal() {
        let ev = BlockEvent::Created {
            meta: meta("metrics"),
        };
        let env = Envelope::new(Uuid::now_v7(), 7, ev.clone());
        let bytes = env.to_bytes().unwrap();
        let back = Envelope::from_bytes(&bytes).unwrap();
        assert_eq!(back, env);
        assert_eq!(back.event.signal(), "metrics");
    }

    #[test]
    fn superseded_round_trips_and_reports_by_meta_signal() {
        let by = meta("logs");
        let ev = BlockEvent::Superseded {
            inputs: vec![Uuid::now_v7(), Uuid::now_v7()],
            by: by.uuid,
            by_meta: by,
        };
        let env = Envelope::new(Uuid::now_v7(), 1, ev);
        let back = Envelope::from_bytes(&env.to_bytes().unwrap()).unwrap();
        assert_eq!(back, env);
        assert_eq!(back.event.signal(), "logs");
    }

    #[test]
    fn deleted_round_trips() {
        let ev = BlockEvent::Deleted {
            signal: "traces".to_string(),
            uuids: vec![Uuid::now_v7()],
        };
        let env = Envelope::new(Uuid::now_v7(), 99, ev);
        let back = Envelope::from_bytes(&env.to_bytes().unwrap()).unwrap();
        assert_eq!(back, env);
        assert_eq!(back.event.signal(), "traces");
    }

    #[test]
    fn created_event_json_matches_block_meta_shape() {
        // The `meta` field of a Created event must serialise to exactly a
        // BlockMeta object (so a sidecar and a Created payload are the same
        // bytes for that field). Guard against accidental wrapping.
        let m = meta("metrics");
        let ev = BlockEvent::Created { meta: m.clone() };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["kind"], "created");
        assert_eq!(v["meta"], serde_json::to_value(&m).unwrap());
    }

    #[test]
    fn noop_sink_discards() {
        let sink: &dyn BlockEventSink = &NoopSink;
        sink.emit(BlockEvent::Deleted {
            signal: "metrics".into(),
            uuids: vec![],
        });
    }
}
