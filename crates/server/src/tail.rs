//! Live-tail plumbing: a process-local subscription registry plus the
//! per-signal appender decorators that tap the ingest hot path.
//!
//! # Two signals, two record shapes
//!
//! Logs and metrics are both tailable (D-050, D-065). They share the registry,
//! the filter evaluation, and the drop-on-full delivery; they differ only in
//! what a record *is* — a body with a severity versus a float with a series
//! fingerprint. That difference lives in [`TailPayload`], and on the wire in
//! two sibling frames (`TailRecord` / `TailSample`), so neither signal carries
//! fields belonging to the other.
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
use scry_proto::streaming::{LogsAppender, MetricsAppender};
use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock};

/// Opaque subscriber identity, handed back by [`SubscriptionRegistry::register`]
/// so the connection can [`deregister`](SubscriptionRegistry::deregister) on EOF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubId(u64);

/// The signal-specific half of a [`TailItem`] — what distinguishes a log line
/// from a metric sample once the timestamp and labels are accounted for.
#[derive(Debug)]
pub enum TailPayload {
    /// A log entry: severity, body, and per-entry attributes.
    Log {
        severity: u8,
        body: String,
        attributes: Vec<LabelPair>,
    },
    /// A metric sample: the series' type + fingerprint and the observed value.
    /// The fingerprint is carried so a client can line a live series up with a
    /// stored one without re-deriving the hash.
    Sample {
        metric_type: u8,
        series_fingerprint: u64,
        value: f64,
    },
}

/// A single record forwarded from the ingest tap to a subscriber. Cheap to
/// clone-by-`Arc` across multiple matching subscribers; the stream-level
/// `labels` are shared, only the per-record payload is owned.
#[derive(Debug)]
pub struct TailItem {
    pub signal: u8,
    pub ts_unix_nano: u64,
    /// Stream/series-level labels (shared across every record of the same
    /// stream or series).
    pub labels: Arc<Vec<LabelPair>>,
    pub payload: TailPayload,
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
                            labels: Arc::clone(labels),
                            payload: TailPayload::Log {
                                severity,
                                body: String::from_utf8_lossy(&body).into_owned(),
                                attributes: attributes
                                    .iter()
                                    .map(|(k, v)| LabelPair {
                                        key: String::from_utf8_lossy(k).into_owned(),
                                        value: String::from_utf8_lossy(v).into_owned(),
                                    })
                                    .collect(),
                            },
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

/// A [`MetricsAppender`] decorator that forwards matching samples to live-tail
/// subscribers while delegating **all** storage semantics to `inner`
/// unchanged — the exact counterpart of [`TappingLogsAppender`] for the
/// metrics signal (D-065).
///
/// Series labels observed via [`observe_series`](MetricsAppender::observe_series)
/// are cached by fingerprint — together with the series' `metric_type`, which
/// arrives on the dictionary entry and not on the sample — so
/// [`append_sample`](MetricsAppender::append_sample) can attach both to each
/// forwarded record.
pub struct TappingMetricsAppender<'a, A: MetricsAppender> {
    inner: &'a mut A,
    registry: &'a SubscriptionRegistry,
    subs: Vec<SubHandle>,
    /// fingerprint → (metric_type, series labels shared across its samples).
    series: HashMap<u64, (u8, Arc<Vec<LabelPair>>)>,
}

impl<'a, A: MetricsAppender> TappingMetricsAppender<'a, A> {
    /// Wrap `inner`, snapshotting the current metrics subscribers from
    /// `registry`. Call this only when `registry.subscriber_count() > 0`.
    pub async fn new(
        inner: &'a mut A,
        registry: &'a SubscriptionRegistry,
        signal: u8,
    ) -> TappingMetricsAppender<'a, A> {
        let subs = registry.snapshot_for(signal).await;
        TappingMetricsAppender {
            inner,
            registry,
            subs,
            series: HashMap::new(),
        }
    }

    /// Whether any subscriber survived the snapshot.
    pub fn has_subs(&self) -> bool {
        !self.subs.is_empty()
    }
}

impl<A: MetricsAppender> MetricsAppender for TappingMetricsAppender<'_, A> {
    fn observe_series(
        &mut self,
        fingerprint: u64,
        metric_type: u8,
        labels: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        if !self.subs.is_empty() {
            // Cold path — a handful of series per batch, shared across their
            // samples via Arc. Same lossy UTF-8 policy as the block builder.
            let pairs: Vec<LabelPair> = labels
                .iter()
                .map(|(k, v)| LabelPair {
                    key: String::from_utf8_lossy(k).into_owned(),
                    value: String::from_utf8_lossy(v).into_owned(),
                })
                .collect();
            self.series
                .insert(fingerprint, (metric_type, Arc::new(pairs)));
        }
        // Storage path is authoritative and unchanged.
        self.inner.observe_series(fingerprint, metric_type, labels);
    }

    fn append_sample(&mut self, fingerprint: u64, ts_unix_nano: u64, value: f64) {
        if !self.subs.is_empty() {
            if let Some((metric_type, labels)) = self.series.get(&fingerprint) {
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
                            labels: Arc::clone(labels),
                            payload: TailPayload::Sample {
                                metric_type: *metric_type,
                                series_fingerprint: fingerprint,
                                value,
                            },
                        })
                    });
                    if s.tx.try_send(Arc::clone(it)).is_err() {
                        self.registry.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        // Storage path is authoritative and unchanged.
        self.inner.append_sample(fingerprint, ts_unix_nano, value);
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

    /// The body of a forwarded log item, panicking if it isn't one — the
    /// logs tests only ever produce `TailPayload::Log`, so a `Sample` here
    /// would mean the tap built the wrong payload.
    fn body_of(item: &TailItem) -> &str {
        match &item.payload {
            TailPayload::Log { body, .. } => body,
            TailPayload::Sample { .. } => panic!("expected a Log payload, got a Sample"),
        }
    }

    /// The `(metric_type, fingerprint, value)` of a forwarded sample.
    fn sample_of(item: &TailItem) -> (u8, u64, f64) {
        match &item.payload {
            TailPayload::Sample {
                metric_type,
                series_fingerprint,
                value,
            } => (*metric_type, *series_fingerprint, *value),
            TailPayload::Log { .. } => panic!("expected a Sample payload, got a Log"),
        }
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
        assert_eq!(body_of(&got), "hello");
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
        assert_eq!(body_of(&rx.try_recv().unwrap()), "a");
        assert_eq!(body_of(&rx.try_recv().unwrap()), "b");
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

    // ── Metrics tap (D-065) ────────────────────────────────────────────
    //
    // The same five properties the logs tap is held to, for the metrics
    // appender: filter match/miss, storage delegated unchanged, the series
    // dictionary carried onto each sample, drop accounting, and per-signal
    // isolation.

    /// A minimal inner metrics appender recording what storage received.
    #[derive(Default)]
    struct RecordingMetrics {
        series: Vec<(u64, u8, usize)>,
        samples: Vec<(u64, u64, f64)>,
    }
    impl MetricsAppender for RecordingMetrics {
        fn observe_series(&mut self, fp: u64, metric_type: u8, labels: Vec<(Vec<u8>, Vec<u8>)>) {
            self.series.push((fp, metric_type, labels.len()));
        }
        fn append_sample(&mut self, fp: u64, ts: u64, value: f64) {
            self.samples.push((fp, ts, value));
        }
    }

    #[tokio::test]
    async fn matching_sample_is_forwarded_non_matching_is_not() {
        let reg = SubscriptionRegistry::new();
        let signal = 0x01; // Signal::Metrics
        let filter = LabelFilter::parse(&["job=\"api\"".to_string()]).unwrap();
        let (_id, mut rx) = reg.register(signal, filter, 16).await;

        let mut inner = RecordingMetrics::default();
        {
            let mut tap = TappingMetricsAppender::new(&mut inner, &reg, signal).await;
            assert!(tap.has_subs());

            tap.observe_series(1, 2, vec![kv("__name__", "reqs"), kv("job", "api")]);
            tap.observe_series(2, 2, vec![kv("__name__", "reqs"), kv("job", "worker")]);
            tap.append_sample(1, 100, 1.5);
            tap.append_sample(2, 101, 9.5);
        }

        // Exactly one forwarded sample: the matching series'.
        let got = rx.try_recv().expect("one forwarded sample");
        assert_eq!(got.ts_unix_nano, 100);
        // The dictionary's metric_type and the fingerprint ride along, so a
        // client can match a live line to a stored series without re-hashing.
        assert_eq!(sample_of(&got), (2, 1, 1.5));
        assert!(rx.try_recv().is_err(), "no second forwarded sample");

        // Inner storage saw *both* series + both samples, unchanged.
        assert_eq!(inner.series.len(), 2);
        assert_eq!(inner.samples, vec![(1, 100, 1.5), (2, 101, 9.5)]);
    }

    /// A sample whose fingerprint was never announced in the dictionary has
    /// no labels to filter on, so it cannot be forwarded — but it must still
    /// reach storage, which resolves the fingerprint by other means.
    #[tokio::test]
    async fn sample_without_a_series_entry_is_stored_but_not_forwarded() {
        let reg = SubscriptionRegistry::new();
        let signal = 0x01;
        let (_id, mut rx) = reg
            .register(signal, LabelFilter::parse(&[]).unwrap(), 8)
            .await;
        let mut inner = RecordingMetrics::default();
        {
            let mut tap = TappingMetricsAppender::new(&mut inner, &reg, signal).await;
            tap.append_sample(42, 7, 0.25); // never observed as a series
        }
        assert!(rx.try_recv().is_err(), "unknown series must not forward");
        assert_eq!(inner.samples, vec![(42, 7, 0.25)]);
    }

    #[tokio::test]
    async fn full_channel_drops_and_counts_samples() {
        let reg = SubscriptionRegistry::new();
        let signal = 0x01;
        let (_id, _rx) = reg
            .register(signal, LabelFilter::parse(&[]).unwrap(), 1)
            .await;
        let mut inner = RecordingMetrics::default();
        {
            let mut tap = TappingMetricsAppender::new(&mut inner, &reg, signal).await;
            tap.observe_series(1, 0, vec![kv("__name__", "g")]);
            tap.append_sample(1, 1, 1.0);
            tap.append_sample(1, 2, 2.0);
            tap.append_sample(1, 3, 3.0);
        }
        // One slot filled, the other two dropped — ingest never blocked.
        assert_eq!(reg.dropped_total(), 2);
        assert_eq!(inner.samples.len(), 3);
    }

    /// A logs subscriber must not receive metric samples: the registry
    /// snapshot is taken per signal, so the metrics tap sees nobody.
    #[tokio::test]
    async fn a_logs_subscriber_receives_no_samples() {
        let reg = SubscriptionRegistry::new();
        let (_id, mut rx) = reg
            .register(0x02, LabelFilter::parse(&[]).unwrap(), 8)
            .await;
        let mut inner = RecordingMetrics::default();
        {
            let mut tap = TappingMetricsAppender::new(&mut inner, &reg, 0x01).await;
            assert!(!tap.has_subs());
            tap.observe_series(1, 0, vec![kv("__name__", "g")]);
            tap.append_sample(1, 1, 1.0);
        }
        assert!(rx.try_recv().is_err());
        assert_eq!(inner.samples.len(), 1);
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
