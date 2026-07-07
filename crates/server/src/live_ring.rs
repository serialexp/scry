//! Retained recent-window ring — the *live source* for the merged
//! history+live query (D-054).
//!
//! # Why a dedicated ring (not an active-builder snapshot)
//!
//! The merged query — "give me the last minute" — must union the stored
//! parquet blocks (history) with the records still in flight at the
//! ingesters (live), deduplicated across the block-commit seam. A live
//! source that only exposed the *current* block builder would go blind for
//! a window every time a block flushed (the builder resets to empty). So
//! each ingester keeps an always-on, bounded ring of the last `window`
//! seconds of **logs** records, independent of block lifecycle: a flush
//! empties the builder but not the ring.
//!
//! # The record tag is the dedup key
//!
//! Every record carries `(wal_shard, wal_seg)` — the ingest shard and the
//! WAL segment its batch was appended to (stamped in phase 2 of ingest, see
//! `Pipeline::ingest_decoded`). The query keeps a record iff
//! `wal_seg > H(writer, "logs", wal_shard)` where `H` is the catalog's
//! persistent WAL high-water: records at or below the high-water are already
//! durable in a block, everything above is live-only. The ring itself does
//! no dedup — it just retains; the query does the seam arithmetic.
//!
//! # Eviction is a memory bound, not a correctness input
//!
//! The ring evicts by age (`window`) and a hard byte cap. Neither needs to
//! be precise: the query applies its own `ts_min`/`ts_max` filter to the
//! snapshot, and dedup correctness comes entirely from the watermark. So
//! eviction only has to keep the ring's footprint bounded — an approximate
//! front-eviction (oldest *inserted* first) is fine even though records
//! aren't perfectly ts-sorted across shards.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use scry_proto::generated::LabelPair;
use scry_proto::streaming::LogsAppender;

/// One retained log record. `labels` are the stream-level labels (shared
/// across a stream's entries via `Arc`); `body`/`attributes` are per-entry.
/// `wal_shard`/`wal_seg` are the dedup tag, stamped after the WAL append
/// (they're `0` in the [`RetainingLogsAppender`]'s collected records until
/// [`LiveRing::push_stamped`] fills them in).
#[derive(Debug, Clone)]
pub struct LiveLogRecord {
    pub wal_shard: u32,
    pub wal_seg: u64,
    pub ts_unix_nano: u64,
    pub severity: u8,
    pub labels: Arc<Vec<LabelPair>>,
    pub body: String,
    pub attributes: Vec<LabelPair>,
}

impl LiveLogRecord {
    /// Approximate heap footprint for the ring's byte cap. Counts the
    /// string bytes we own; the `Arc<labels>` is shared, so we count it once
    /// per record as a rough proxy (over-counts shared streams slightly,
    /// which only makes the cap more conservative).
    fn heap_bytes(&self) -> usize {
        let labels: usize = self
            .labels
            .iter()
            .map(|p| p.key.len() + p.value.len())
            .sum();
        let attrs: usize = self
            .attributes
            .iter()
            .map(|p| p.key.len() + p.value.len())
            .sum();
        // + a fixed per-record overhead for the struct fields / deque slot.
        self.body.len() + labels + attrs + 48
    }
}

fn now_unix_nano() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

struct RingInner {
    records: VecDeque<LiveLogRecord>,
    bytes: usize,
}

/// A bounded, process-global ring of recent **logs** records for one
/// ingester (one `writer_uuid`, all shards). Shared `Arc`, internally
/// synchronized with a plain `Mutex` (the critical sections are short —
/// push a batch, or snapshot-filter — and never `.await`).
pub struct LiveRing {
    window_nanos: u64,
    max_bytes: usize,
    inner: Mutex<RingInner>,
}

impl LiveRing {
    /// A ring retaining `window` of history, capped at `max_bytes`. Both
    /// bounds are enforced on every push.
    pub fn new(window: Duration, max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            window_nanos: window.as_nanos() as u64,
            max_bytes: max_bytes.max(1),
            inner: Mutex::new(RingInner {
                records: VecDeque::new(),
                bytes: 0,
            }),
        })
    }

    /// Stamp `shard`/`seg` onto each collected record and push them, then
    /// evict past the age window and byte cap. Called from ingest phase 2
    /// with the `SegmentId` the batch's WAL frame landed in.
    pub fn push_stamped(&self, mut records: Vec<LiveLogRecord>, shard: u32, seg: u64) {
        if records.is_empty() {
            return;
        }
        for r in &mut records {
            r.wal_shard = shard;
            r.wal_seg = seg;
        }
        let now = now_unix_nano();
        let cutoff = now.saturating_sub(self.window_nanos);
        let mut inner = self.inner.lock().expect("live ring mutex poisoned");
        for r in records {
            inner.bytes += r.heap_bytes();
            inner.records.push_back(r);
        }
        // Age eviction: drop from the front while the oldest-inserted record
        // is older than the window. Approximate (front == oldest inserted,
        // not strictly oldest ts) — a memory bound, not a correctness input.
        while let Some(front) = inner.records.front() {
            if front.ts_unix_nano < cutoff {
                let b = front.heap_bytes();
                inner.records.pop_front();
                inner.bytes = inner.bytes.saturating_sub(b);
            } else {
                break;
            }
        }
        // Byte-cap eviction: drop oldest until under the cap.
        while inner.bytes > self.max_bytes {
            match inner.records.pop_front() {
                Some(front) => inner.bytes = inner.bytes.saturating_sub(front.heap_bytes()),
                None => break,
            }
        }
    }

    /// Snapshot the records matching `keep`, cloned out under the lock. The
    /// live-query handler passes a predicate combining the label filter,
    /// `body_contains`, and the ts range. Bounded by the ring size.
    pub fn collect<F: FnMut(&LiveLogRecord) -> bool>(&self, mut keep: F) -> Vec<LiveLogRecord> {
        let inner = self.inner.lock().expect("live ring mutex poisoned");
        inner.records.iter().filter(|r| keep(r)).cloned().collect()
    }

    /// Number of retained records (test/observability).
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("live ring mutex poisoned")
            .records
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A [`LogsAppender`] decorator that **collects** each decoded entry into a
/// `Vec<LiveLogRecord>` (with `wal_shard`/`wal_seg` left `0`) while
/// delegating all storage semantics to `inner` unchanged. Nests like
/// [`crate::tail::TappingLogsAppender`]: when a live-tail subscriber is also
/// present the tap wraps this. After decode the caller takes the records
/// with [`into_records`](Self::into_records) — which releases the `&mut
/// inner` borrow — then stamps + pushes them to the [`LiveRing`] in phase 2,
/// once the WAL segment is known.
pub struct RetainingLogsAppender<'a, A: LogsAppender> {
    inner: &'a mut A,
    /// fingerprint → stream labels (shared across the stream's entries).
    stream_labels: std::collections::HashMap<u64, Arc<Vec<LabelPair>>>,
    collected: Vec<LiveLogRecord>,
}

impl<'a, A: LogsAppender> RetainingLogsAppender<'a, A> {
    pub fn new(inner: &'a mut A) -> Self {
        Self {
            inner,
            stream_labels: std::collections::HashMap::new(),
            collected: Vec::new(),
        }
    }

    /// Consume the decorator (releasing the `&mut inner` borrow) and return
    /// the batch's collected records, ready to be stamped + pushed.
    pub fn into_records(self) -> Vec<LiveLogRecord> {
        self.collected
    }
}

impl<A: LogsAppender> LogsAppender for RetainingLogsAppender<'_, A> {
    fn observe_stream(&mut self, fingerprint: u64, labels: Vec<(Vec<u8>, Vec<u8>)>) {
        // Coerce to UTF-8 LabelPairs once per stream (cold path), shared
        // across the stream's entries via Arc. Same lossy policy as the
        // block builder / tap.
        let pairs: Vec<LabelPair> = labels
            .iter()
            .map(|(k, v)| LabelPair {
                key: String::from_utf8_lossy(k).into_owned(),
                value: String::from_utf8_lossy(v).into_owned(),
            })
            .collect();
        self.stream_labels.insert(fingerprint, Arc::new(pairs));
        // Storage path is authoritative and unchanged.
        self.inner.observe_stream(fingerprint, labels);
    }

    fn append_entry(
        &mut self,
        fingerprint: u64,
        ts_unix_nano: u64,
        severity: u8,
        body: Vec<u8>,
        attributes: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        if let Some(labels) = self.stream_labels.get(&fingerprint) {
            self.collected.push(LiveLogRecord {
                wal_shard: 0, // stamped in phase 2
                wal_seg: 0,   // stamped in phase 2
                ts_unix_nano,
                severity,
                labels: Arc::clone(labels),
                body: String::from_utf8_lossy(&body).into_owned(),
                attributes: attributes
                    .iter()
                    .map(|(k, v)| LabelPair {
                        key: String::from_utf8_lossy(k).into_owned(),
                        value: String::from_utf8_lossy(v).into_owned(),
                    })
                    .collect(),
            });
        }
        // Storage path is authoritative and unchanged.
        self.inner
            .append_entry(fingerprint, ts_unix_nano, severity, body, attributes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct NoopAppender;
    impl LogsAppender for NoopAppender {
        fn observe_stream(&mut self, _fp: u64, _labels: Vec<(Vec<u8>, Vec<u8>)>) {}
        fn append_entry(
            &mut self,
            _fp: u64,
            _ts: u64,
            _sev: u8,
            _body: Vec<u8>,
            _attrs: Vec<(Vec<u8>, Vec<u8>)>,
        ) {
        }
    }

    fn kv(k: &str, v: &str) -> (Vec<u8>, Vec<u8>) {
        (k.as_bytes().to_vec(), v.as_bytes().to_vec())
    }

    fn rec(seg: u64, ts: u64, body: &str) -> LiveLogRecord {
        LiveLogRecord {
            wal_shard: 0,
            wal_seg: seg,
            ts_unix_nano: ts,
            severity: 9,
            labels: Arc::new(vec![LabelPair {
                key: "service".into(),
                value: "api".into(),
            }]),
            body: body.into(),
            attributes: vec![],
        }
    }

    #[test]
    fn retaining_appender_collects_and_delegates() {
        let mut inner = NoopAppender;
        let mut ret = RetainingLogsAppender::new(&mut inner);
        ret.observe_stream(1, vec![kv("service", "api")]);
        ret.append_entry(1, 100, 9, b"hello".to_vec(), vec![kv("k", "v")]);
        ret.append_entry(1, 101, 9, b"world".to_vec(), vec![]);
        // An entry for an unobserved stream is dropped from the live set
        // (no labels to attach) but still delegated to storage.
        ret.append_entry(2, 102, 9, b"orphan".to_vec(), vec![]);
        let recs = ret.into_records();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].body, "hello");
        assert_eq!(recs[0].labels[0].value, "api");
        assert_eq!(recs[0].attributes.len(), 1);
        assert_eq!(recs[1].body, "world");
    }

    #[test]
    fn push_stamps_shard_and_seg() {
        let ring = LiveRing::new(Duration::from_secs(3600), 1 << 20);
        ring.push_stamped(vec![rec(0, now_unix_nano(), "a")], 3, 42);
        let got = ring.collect(|_| true);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].wal_shard, 3);
        assert_eq!(got[0].wal_seg, 42);
    }

    #[test]
    fn age_eviction_drops_stale_front() {
        let ring = LiveRing::new(Duration::from_secs(60), 1 << 20);
        let now = now_unix_nano();
        // Old record (2 minutes ago) then a fresh one; the push's eviction
        // should drop the old one.
        let old_ts = now.saturating_sub(120 * 1_000_000_000);
        ring.push_stamped(vec![rec(0, old_ts, "old")], 0, 1);
        ring.push_stamped(vec![rec(1, now, "fresh")], 0, 2);
        let got = ring.collect(|_| true);
        assert_eq!(got.len(), 1, "stale record evicted");
        assert_eq!(got[0].body, "fresh");
    }

    #[test]
    fn byte_cap_evicts_oldest() {
        // Tiny cap so a couple of records blow it.
        let ring = LiveRing::new(Duration::from_secs(3600), 120);
        let now = now_unix_nano();
        for i in 0..10u64 {
            ring.push_stamped(vec![rec(i, now, &format!("body-{i}"))], 0, i);
        }
        let got = ring.collect(|_| true);
        // Under the cap, and the survivors are the newest (highest seg).
        assert!(got.len() < 10, "byte cap evicted some");
        let min_seg = got.iter().map(|r| r.wal_seg).min().unwrap();
        let max_seg = got.iter().map(|r| r.wal_seg).max().unwrap();
        assert_eq!(max_seg, 9, "newest retained");
        assert!(min_seg > 0, "oldest evicted");
    }

    #[test]
    fn collect_applies_predicate() {
        let ring = LiveRing::new(Duration::from_secs(3600), 1 << 20);
        let now = now_unix_nano();
        ring.push_stamped(vec![rec(1, now, "keep me"), rec(2, now, "drop me")], 0, 5);
        let got = ring.collect(|r| r.body.contains("keep"));
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].body, "keep me");
    }
}
