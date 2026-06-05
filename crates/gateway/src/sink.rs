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
    generated::{LogsBatch, MetricsBatch, ProfilesBatch, TracesBatch},
};
use tokio::sync::mpsc;
use tracing::warn;

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
    Traces(Arc<TracesBatch>),
    Profiles(Arc<ProfilesBatch>),
}

impl Fanout {
    /// The `SIGNAL_BIT_*` this item belongs to.
    fn signal_bit(&self) -> u8 {
        match self {
            Fanout::Logs(_) => SIGNAL_BIT_LOGS,
            Fanout::Metrics(_) => SIGNAL_BIT_METRICS,
            Fanout::Traces(_) => SIGNAL_BIT_TRACES,
            Fanout::Profiles(_) => SIGNAL_BIT_PROFILES,
        }
    }
}

/// A handle to one downstream destination: a bounded queue feeding a worker
/// task, the signal mask it accepts, and a dropped-item counter.
pub struct SinkHandle {
    name: String,
    /// OR-combined `SIGNAL_BIT_*` this sink consumes; other signals are skipped
    /// at offer time so e.g. a traces batch never wakes the Loki worker.
    accepts: u8,
    tx: mpsc::Sender<Fanout>,
    dropped: Arc<AtomicU64>,
}

impl SinkHandle {
    /// Best-effort enqueue: returns immediately. On a full or closed queue the
    /// item is dropped and the per-sink `dropped` counter is bumped (logged on a
    /// sparse cadence so a sustained outage doesn't spam).
    fn offer(&self, item: Fanout) {
        if self.tx.try_send(item).is_err() {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n.is_multiple_of(1000) {
                warn!(sink = %self.name, dropped = n, "sink queue full; dropping batch (best-effort)");
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
    let name = name.into();
    let (tx, rx) = mpsc::channel::<Fanout>(cap.max(1));
    let dropped = Arc::new(AtomicU64::new(0));
    tokio::spawn(worker(rx));
    SinkHandle {
        name,
        accepts,
        tx,
        dropped,
    }
}

/// The fan-out state shared by every inbound path. Cheap to clone (one `Arc`).
#[derive(Clone)]
pub struct AppState {
    sinks: Arc<Vec<SinkHandle>>,
}

impl AppState {
    pub fn new(sinks: Vec<SinkHandle>) -> Self {
        Self {
            sinks: Arc::new(sinks),
        }
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
        self.fan(Fanout::Logs(Arc::new(batch)));
    }

    pub fn offer_metrics(&self, batch: MetricsBatch) {
        if batch.samples.is_empty() {
            return;
        }
        self.fan(Fanout::Metrics(Arc::new(batch)));
    }

    pub fn offer_traces(&self, batch: TracesBatch) {
        if batch.spans.is_empty() {
            return;
        }
        self.fan(Fanout::Traces(Arc::new(batch)));
    }

    pub fn offer_profiles(&self, batch: ProfilesBatch) {
        if batch.samples.is_empty() {
            return;
        }
        self.fan(Fanout::Profiles(Arc::new(batch)));
    }

    /// The configured sinks (for startup logging / introspection).
    pub fn sinks(&self) -> &[SinkHandle] {
        &self.sinks
    }
}
