//! Live-tail plumbing: a process-local subscription registry plus a
//! `LogsAppender` decorator that taps the ingest hot path.
//!
//! # Why this exists
//!
//! `scry tail` is a **separate, best-effort surface** — never merged with
//! stored blocks, never deduplicated, never durable. A subscriber opens a
//! connection to an ingest server, sends a `Subscribe`, and receives a
//! stream of `TailRecord`s for as long as the socket stays open. See
//! D-050 for the full rationale (why not a query mode, why lossy is fine).
//!
//! # Zero cost when nobody is tailing
//!
//! The ingest decode path checks [`SubscriptionRegistry::subscriber_count`]
//! (one relaxed atomic load per batch). When it is `0` the server takes the
//! ordinary, untapped decode path — byte-identical to pre-tail behaviour.
//! Only when at least one subscriber is registered does a batch get decoded
//! through [`TappingLogsAppender`], which snapshots the subscriber handles
//! **once per batch** (a single read-lock) and evaluates each subscriber's
//! label filter per entry.
//!
//! # Delivery is `try_send` — drops, never blocks
//!
//! Each subscriber owns a bounded channel. The tap `try_send`s into it; on a
//! full or closed channel it increments a drop counter and moves on. Ingest
//! is never backpressured by a slow tail client — the tail is explicitly
//! allowed to miss records.

use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use scry_match::LabelFilter;
use scry_proto::generated::LabelPair;
use scry_proto::streaming::LogsAppender;
use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock};

/// Opaque subscriber identity, handed back by [`SubscriptionRegistry::register`]
/// so the connection can [`deregister`](SubscriptionRegistry::deregister) on EOF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubId(u64);

/// A single record forwarded from the ingest tap to a subscriber. Cheap to
/// clone-by-`Arc` across multiple matching subscribers; the stream-level
/// `labels` are shared, only the per-entry `body`/`attributes` are owned.
#[derive(Debug)]
pub struct TailItem {
    pub signal: u8,
    pub ts_unix_nano: u64,
    pub severity: u8,
    /// Stream-level labels (shared across every entry of the same stream).
    pub labels: Arc<Vec<LabelPair>>,
    pub body: String,
    pub attributes: Vec<LabelPair>,
}

/// One registered subscriber. `filter` and `labels` are matched per entry;
/// `tx` is the delivery channel (bounded, drop-on-full).
#[derive(Clone)]
struct SubHandle {
    id: SubId,
    signal: u8,
    filter: Arc<LabelFilter>,
    tx: mpsc::Sender<Arc<TailItem>>,
}

struct Inner {
    next_id: u64,
    subs: Vec<SubHandle>,
}

/// Process-local registry of live-tail subscribers, shared (`Arc`) across
/// every connection handler in an ingest server.
pub struct SubscriptionRegistry {
    /// Fast-path gate: number of live subscribers. Read once per batch in
    /// the hot ingest path (relaxed — an off-by-one against a concurrent
    /// (de)register just means one batch is tapped-or-not a hair early/late,
    /// which is fine for a best-effort surface).
    count: AtomicUsize,
    /// Count of records dropped because a subscriber's channel was full.
    /// Surfaced for operator visibility; never affects ingest.
    dropped: AtomicU64,
    inner: RwLock<Inner>,
}

impl SubscriptionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
            dropped: AtomicU64::new(0),
            inner: RwLock::new(Inner {
                next_id: 0,
                subs: Vec::new(),
            }),
        })
    }

    /// Number of currently-registered subscribers. One relaxed atomic load;
    /// the ingest tap gates on this being `> 0`.
    #[inline]
    pub fn subscriber_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Total records dropped so far due to full subscriber channels.
    pub fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Register a subscriber for `signal` records matching `filter`. Returns
    /// the id (for later [`deregister`](Self::deregister)) and the receiving
    /// half of the delivery channel (`capacity` bounds in-flight records).
    pub async fn register(
        &self,
        signal: u8,
        filter: LabelFilter,
        capacity: usize,
    ) -> (SubId, mpsc::Receiver<Arc<TailItem>>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut inner = self.inner.write().await;
        let id = SubId(inner.next_id);
        inner.next_id += 1;
        inner.subs.push(SubHandle {
            id,
            signal,
            filter: Arc::new(filter),
            tx,
        });
        // Publish the new length *after* the push so a concurrent
        // `subscriber_count()` never sees a count that outruns the vec.
        self.count.store(inner.subs.len(), Ordering::Relaxed);
        (id, rx)
    }

    /// Remove a subscriber. Idempotent — deregistering an unknown id is a
    /// no-op (the connection may already have been reaped).
    pub async fn deregister(&self, id: SubId) {
        let mut inner = self.inner.write().await;
        inner.subs.retain(|s| s.id != id);
        self.count.store(inner.subs.len(), Ordering::Relaxed);
    }

    /// Snapshot the handles subscribed to `signal`. One read-lock; the
    /// returned handles are cheap (`Arc`/channel clones) and let the tap
    /// evaluate filters without holding the lock across a whole batch.
    async fn snapshot_for(&self, signal: u8) -> Vec<SubHandle> {
        let inner = self.inner.read().await;
        inner
            .subs
            .iter()
            .filter(|s| s.signal == signal)
            .cloned()
            .collect()
    }
}

/// A [`LogsAppender`] decorator that forwards matching entries to live-tail
/// subscribers while delegating **all** storage semantics to `inner`
/// unchanged — the block written to object storage is byte-identical to the
/// untapped path.
///
/// Constructed per batch (see the logs decode branch in `server.rs`) with a
/// snapshot of the current logs subscribers. Stream labels observed via
/// [`observe_stream`](LogsAppender::observe_stream) are cached by fingerprint
/// so [`append_entry`](LogsAppender::append_entry) can attach them to each
/// forwarded record.
pub struct TappingLogsAppender<'a, A: LogsAppender> {
    inner: &'a mut A,
    registry: &'a SubscriptionRegistry,
    subs: Vec<SubHandle>,
    /// fingerprint → stream labels (shared across the stream's entries).
    stream_labels: HashMap<u64, Arc<Vec<LabelPair>>>,
}

impl<'a, A: LogsAppender> TappingLogsAppender<'a, A> {
    /// Wrap `inner`, snapshotting the current logs subscribers from
    /// `registry`. Call this only when `registry.subscriber_count() > 0`.
    pub async fn new(
        inner: &'a mut A,
        registry: &'a SubscriptionRegistry,
        signal: u8,
    ) -> TappingLogsAppender<'a, A> {
        let subs = registry.snapshot_for(signal).await;
        TappingLogsAppender {
            inner,
            registry,
            subs,
            stream_labels: HashMap::new(),
        }
    }

    /// Whether any subscriber survived the snapshot. The caller can still
    /// decode through the tap when this is false (it's just a delegating
    /// no-op then), but it's a cheap way to skip per-entry work.
    pub fn has_subs(&self) -> bool {
        !self.subs.is_empty()
    }
}

impl<A: LogsAppender> LogsAppender for TappingLogsAppender<'_, A> {
    fn observe_stream(&mut self, fingerprint: u64, labels: Vec<(Vec<u8>, Vec<u8>)>) {
        if !self.subs.is_empty() {
            // Coerce to UTF-8 LabelPairs once per stream (cold path — a
            // handful of streams per batch), shared across the stream's
            // entries via Arc. Same lossy policy as the block builder.
            let pairs: Vec<LabelPair> = labels
                .iter()
                .map(|(k, v)| LabelPair {
                    key: String::from_utf8_lossy(k).into_owned(),
                    value: String::from_utf8_lossy(v).into_owned(),
                })
                .collect();
            self.stream_labels.insert(fingerprint, Arc::new(pairs));
        }
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
        if !self.subs.is_empty() {
            if let Some(labels) = self.stream_labels.get(&fingerprint) {
                // Does *any* subscriber want this? Build the item once,
                // lazily, and only if so.
                let mut item: Option<Arc<TailItem>> = None;
                for s in &self.subs {
                    if !s.filter.keeps(labels) {
                        continue;
                    }
                    let it = item.get_or_insert_with(|| {
                        Arc::new(TailItem {
                            signal: s.signal,
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
                        })
                    });
                    if s.tx.try_send(Arc::clone(it)).is_err() {
                        self.registry.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        // Storage path is authoritative and unchanged.
        self.inner
            .append_entry(fingerprint, ts_unix_nano, severity, body, attributes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal inner appender that just records what it received, so we
    /// can assert the tap delegates storage faithfully.
    #[derive(Default)]
    struct RecordingAppender {
        streams: Vec<(u64, usize)>,
        entries: Vec<(u64, String)>,
    }
    impl LogsAppender for RecordingAppender {
        fn observe_stream(&mut self, fingerprint: u64, labels: Vec<(Vec<u8>, Vec<u8>)>) {
            self.streams.push((fingerprint, labels.len()));
        }
        fn append_entry(
            &mut self,
            fingerprint: u64,
            _ts: u64,
            _severity: u8,
            body: Vec<u8>,
            _attrs: Vec<(Vec<u8>, Vec<u8>)>,
        ) {
            self.entries
                .push((fingerprint, String::from_utf8_lossy(&body).into_owned()));
        }
    }

    fn kv(k: &str, v: &str) -> (Vec<u8>, Vec<u8>) {
        (k.as_bytes().to_vec(), v.as_bytes().to_vec())
    }

    #[tokio::test]
    async fn no_subscribers_means_zero_count() {
        let reg = SubscriptionRegistry::new();
        assert_eq!(reg.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn register_and_deregister_track_count() {
        let reg = SubscriptionRegistry::new();
        let f = LabelFilter::parse(&[]).unwrap();
        let (id, _rx) = reg.register(0x10, f, 8).await;
        assert_eq!(reg.subscriber_count(), 1);
        reg.deregister(id).await;
        assert_eq!(reg.subscriber_count(), 0);
        // Idempotent.
        reg.deregister(id).await;
        assert_eq!(reg.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn matching_entry_is_forwarded_non_matching_is_not() {
        let reg = SubscriptionRegistry::new();
        let signal = 0x10;
        let filter = LabelFilter::parse(&["namespace=\"keepme\"".to_string()]).unwrap();
        let (_id, mut rx) = reg.register(signal, filter, 16).await;

        let mut inner = RecordingAppender::default();
        {
            let mut tap = TappingLogsAppender::new(&mut inner, &reg, signal).await;
            assert!(tap.has_subs());

            // Stream A matches, stream B does not.
            tap.observe_stream(1, vec![kv("namespace", "keepme")]);
            tap.observe_stream(2, vec![kv("namespace", "other")]);
            tap.append_entry(1, 100, 9, b"hello".to_vec(), vec![]);
            tap.append_entry(2, 101, 9, b"nope".to_vec(), vec![]);
        }

        // Exactly one forwarded item: the matching stream's entry.
        let got = rx.try_recv().expect("one forwarded record");
        assert_eq!(got.body, "hello");
        assert_eq!(got.ts_unix_nano, 100);
        assert!(rx.try_recv().is_err(), "no second forwarded record");

        // Inner storage saw *both* streams + both entries, unchanged.
        assert_eq!(inner.streams.len(), 2);
        assert_eq!(inner.entries.len(), 2);
        assert_eq!(inner.entries[0], (1, "hello".to_string()));
        assert_eq!(inner.entries[1], (2, "nope".to_string()));
    }

    #[tokio::test]
    async fn empty_filter_forwards_everything() {
        let reg = SubscriptionRegistry::new();
        let signal = 0x10;
        let (_id, mut rx) = reg
            .register(signal, LabelFilter::parse(&[]).unwrap(), 16)
            .await;

        let mut inner = RecordingAppender::default();
        {
            let mut tap = TappingLogsAppender::new(&mut inner, &reg, signal).await;
            tap.observe_stream(1, vec![kv("x", "y")]);
            tap.append_entry(1, 1, 0, b"a".to_vec(), vec![]);
            tap.append_entry(1, 2, 0, b"b".to_vec(), vec![]);
        }
        assert_eq!(rx.try_recv().unwrap().body, "a");
        assert_eq!(rx.try_recv().unwrap().body, "b");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn full_channel_drops_and_counts() {
        let reg = SubscriptionRegistry::new();
        let signal = 0x10;
        // capacity 1 → second match is dropped.
        let (_id, _rx) = reg
            .register(signal, LabelFilter::parse(&[]).unwrap(), 1)
            .await;
        let mut inner = RecordingAppender::default();
        {
            let mut tap = TappingLogsAppender::new(&mut inner, &reg, signal).await;
            tap.observe_stream(1, vec![kv("x", "y")]);
            tap.append_entry(1, 1, 0, b"a".to_vec(), vec![]);
            tap.append_entry(1, 2, 0, b"b".to_vec(), vec![]);
            tap.append_entry(1, 3, 0, b"c".to_vec(), vec![]);
        }
        // One slot filled, the other two dropped.
        assert_eq!(reg.dropped_total(), 2);
        // Storage still saw all three.
        assert_eq!(inner.entries.len(), 3);
    }

    #[tokio::test]
    async fn signal_mismatch_excluded_from_snapshot() {
        let reg = SubscriptionRegistry::new();
        // Subscriber on signal 0x20, tap for 0x10.
        let (_id, mut rx) = reg
            .register(0x20, LabelFilter::parse(&[]).unwrap(), 8)
            .await;
        let mut inner = RecordingAppender::default();
        {
            let mut tap = TappingLogsAppender::new(&mut inner, &reg, 0x10).await;
            assert!(!tap.has_subs());
            tap.observe_stream(1, vec![kv("x", "y")]);
            tap.append_entry(1, 1, 0, b"a".to_vec(), vec![]);
        }
        assert!(rx.try_recv().is_err());
        assert_eq!(inner.entries.len(), 1);
    }
}
