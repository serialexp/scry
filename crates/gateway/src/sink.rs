//! Fan-out core: a decoded batch plus the set of downstream sinks it is offered
//! to.
//!
//! Every inbound path — the foreign HTTP handlers ([`crate::otlp`],
//! [`crate::pyroscope`], [`crate::promwrite`]) and the native wire listener
//! ([`crate::wire`]) — decodes its request into a typed `*Batch` and hands it to
//! [`AppState`], which offers it to every configured [`SinkHandle`] whose signal
//! mask accepts it.
//!
//! Offer is **non-blocking and best-effort**: each sink owns a bounded queue
//! drained by its own worker task (see [`spawn_sink`]), so a slow or dead
//! downstream never blocks the inbound, nor the other sinks — once its queue is
//! full it drops + counts instead of stalling. The trade-off (documented in
//! `docs/decisions.md` D-041): the inbound ACKs on enqueue, not on downstream
//! confirmation, so durability across a downstream outage is bounded by the
//! queue depth.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use scry_proto::{
    constants::{SIGNAL_BIT_LOGS, SIGNAL_BIT_METRICS, SIGNAL_BIT_PROFILES, SIGNAL_BIT_TRACES},
    generated::{LogsBatch, MetricsBatch, MetricsBatchV2, ProfilesBatch, TracesBatch},
};
use tokio::sync::mpsc;
use tracing::warn;

use crate::metrics::{GatewayMetrics, GatewaySignal, QueueSnapshot, SinkKind};

/// Every signal a sink could consume. The scry sink accepts this; the
/// Loki/OpenSearch sinks accept only [`SIGNAL_BIT_LOGS`].
pub const ACCEPT_ALL: u8 =
    SIGNAL_BIT_METRICS | SIGNAL_BIT_LOGS | SIGNAL_BIT_TRACES | SIGNAL_BIT_PROFILES;

/// A decoded batch ready to fan out. `Arc` so every sink shares one copy rather
/// than deep-cloning the payload once per destination.
#[derive(Clone)]
pub enum Fanout {
    Logs(Arc<LogsBatch>),
    Metrics(Arc<MetricsBatch>),
    StructuredMetrics(Arc<MetricsBatchV2>),
    Traces(Arc<TracesBatch>),
    Profiles(Arc<ProfilesBatch>),
}

impl Fanout {
    /// The `SIGNAL_BIT_*` this item belongs to.
    fn signal_bit(&self) -> u8 {
        match self {
            Fanout::Logs(_) => SIGNAL_BIT_LOGS,
            Fanout::Metrics(_) | Fanout::StructuredMetrics(_) => SIGNAL_BIT_METRICS,
            Fanout::Traces(_) => SIGNAL_BIT_TRACES,
            Fanout::Profiles(_) => SIGNAL_BIT_PROFILES,
        }
    }

    pub fn signal(&self) -> GatewaySignal {
        match self {
            Fanout::Logs(_) => GatewaySignal::Logs,
            Fanout::Metrics(_) | Fanout::StructuredMetrics(_) => GatewaySignal::Metrics,
            Fanout::Traces(_) => GatewaySignal::Traces,
            Fanout::Profiles(_) => GatewaySignal::Profiles,
        }
    }
}

/// A handle to one downstream destination: a bounded queue feeding a worker
/// task, the signal mask it accepts, and a dropped-item counter.
pub struct SinkHandle {
    name: String,
    kind: Option<SinkKind>,
    /// OR-combined `SIGNAL_BIT_*` this sink consumes; other signals are skipped
    /// at offer time so e.g. a traces batch never wakes the Loki worker.
    accepts: u8,
    tx: mpsc::Sender<Fanout>,
    queue_capacity: usize,
    dropped: Arc<AtomicU64>,
    metrics: Option<Arc<GatewayMetrics>>,
}

impl SinkHandle {
    /// Best-effort enqueue: returns immediately. On a full or closed queue the
    /// item is dropped and the per-sink `dropped` counter is bumped (logged on a
    /// sparse cadence so a sustained outage doesn't spam).
    fn offer(&self, item: Fanout) {
        let signal = item.signal();
        match self.tx.try_send(item) {
            Ok(()) => {
                if let (Some(metrics), Some(kind)) = (&self.metrics, self.kind) {
                    metrics.enqueued(kind, signal);
                }
            }
            Err(error) => {
                if let (Some(metrics), Some(kind)) = (&self.metrics, self.kind) {
                    match error {
                        mpsc::error::TrySendError::Full(_) => metrics.dropped_full(kind, signal),
                        mpsc::error::TrySendError::Closed(_) => {
                            metrics.dropped_closed(kind, signal)
                        }
                    }
                }
                let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if n == 1 || n.is_multiple_of(1000) {
                    warn!(sink = %self.name, dropped = n, "sink queue unavailable; dropping batch (best-effort)");
                }
            }
        }
    }

    /// Total batches dropped at enqueue because the queue was full/closed.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Spawn a sink worker over a bounded queue and return its [`SinkHandle`].
///
/// `worker` is handed the queue's receiver and runs until the queue closes
/// (every [`SinkHandle`] dropped). Concrete sinks expose an
/// `async fn run(self, rx)` and are spawned as `spawn_sink(name, mask, cap,
/// |rx| sink.run(rx))`.
pub fn spawn_sink<F, Fut>(name: impl Into<String>, accepts: u8, cap: usize, worker: F) -> SinkHandle
where
    F: FnOnce(mpsc::Receiver<Fanout>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    spawn_sink_instrumented(name, accepts, cap, None, worker)
}

pub fn spawn_sink_instrumented<F, Fut>(
    name: impl Into<String>,
    accepts: u8,
    cap: usize,
    metrics: Option<Arc<GatewayMetrics>>,
    worker: F,
) -> SinkHandle
where
    F: FnOnce(mpsc::Receiver<Fanout>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let name = name.into();
    let kind = SinkKind::from_name(&name);
    assert!(
        metrics.is_none() || kind.is_some(),
        "instrumented sinks use a canonical kind name"
    );
    let queue_capacity = cap.max(1);
    let (tx, rx) = mpsc::channel::<Fanout>(queue_capacity);
    let dropped = Arc::new(AtomicU64::new(0));
    tokio::spawn(worker(rx));
    SinkHandle {
        name,
        kind,
        accepts,
        tx,
        queue_capacity,
        dropped,
        metrics,
    }
}

/// The fan-out state shared by every inbound path. Cheap to clone (one `Arc`).
#[derive(Clone)]
pub struct AppState {
    sinks: Arc<Vec<SinkHandle>>,
    metrics: Option<Arc<GatewayMetrics>>,
}

impl AppState {
    pub fn new(sinks: Vec<SinkHandle>) -> Self {
        Self {
            sinks: Arc::new(sinks),
            metrics: None,
        }
    }

    pub fn with_metrics(sinks: Vec<SinkHandle>, metrics: Arc<GatewayMetrics>) -> Self {
        Self {
            sinks: Arc::new(sinks),
            metrics: Some(metrics),
        }
    }

    pub fn metrics(&self) -> Option<&Arc<GatewayMetrics>> {
        self.metrics.as_ref()
    }

    /// Offer one item to every sink whose mask accepts its signal. The `Arc` is
    /// cloned per accepting sink (a refcount bump, not a payload copy).
    fn fan(&self, item: Fanout) {
        let bit = item.signal_bit();
        for s in self.sinks.iter() {
            if s.accepts & bit != 0 {
                s.offer(item.clone());
            }
        }
    }

    pub fn offer_logs(&self, batch: LogsBatch) {
        if batch.streams.is_empty() {
            return;
        }
        if let Some(metrics) = &self.metrics {
            metrics.add_records(
                GatewaySignal::Logs,
                batch
                    .streams
                    .iter()
                    .map(|stream| stream.entries.len() as u64)
                    .sum(),
            );
        }
        self.fan(Fanout::Logs(Arc::new(batch)));
    }

    pub fn offer_metrics(&self, batch: MetricsBatch) {
        if batch.samples.is_empty() {
            return;
        }
        if let Some(metrics) = &self.metrics {
            metrics.add_records(GatewaySignal::Metrics, batch.samples.len() as u64);
        }
        self.fan(Fanout::Metrics(Arc::new(batch)));
    }

    pub fn offer_structured_metrics(&self, batch: MetricsBatchV2) {
        if batch.points.is_empty() {
            return;
        }
        if let Some(metrics) = &self.metrics {
            metrics.add_records(GatewaySignal::Metrics, batch.points.len() as u64);
        }
        self.fan(Fanout::StructuredMetrics(Arc::new(batch)));
    }

    pub fn offer_traces(&self, batch: TracesBatch) {
        if batch.spans.is_empty() {
            return;
        }
        if let Some(metrics) = &self.metrics {
            metrics.add_records(GatewaySignal::Traces, batch.spans.len() as u64);
        }
        self.fan(Fanout::Traces(Arc::new(batch)));
    }

    pub fn offer_profiles(&self, batch: ProfilesBatch) {
        if batch.samples.is_empty() {
            return;
        }
        if let Some(metrics) = &self.metrics {
            metrics.add_records(GatewaySignal::Profiles, batch.samples.len() as u64);
        }
        self.fan(Fanout::Profiles(Arc::new(batch)));
    }

    /// The configured sinks (for startup logging / introspection).
    pub fn sinks(&self) -> &[SinkHandle] {
        &self.sinks
    }

    pub fn queue_snapshots(&self) -> Vec<QueueSnapshot> {
        self.sinks
            .iter()
            .filter_map(|sink| {
                Some(QueueSnapshot {
                    kind: sink.kind?,
                    depth: sink.queue_capacity.saturating_sub(sink.tx.capacity()),
                    capacity: sink.queue_capacity,
                })
            })
            .collect()
    }
}
