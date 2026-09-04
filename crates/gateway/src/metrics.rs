//! Allocation-free gateway counters, serialized only on status snapshots.

use std::{
    array,
    sync::atomic::{AtomicU64, Ordering},
};

use serde_json::{json, Value};

pub const PROTOCOLS: usize = 7;
pub const SIGNALS: usize = 4;
pub const SINKS: usize = 4;

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum Inbound {
    OtlpHttp,
    OtlpGrpc,
    PromRemoteWriteHttp,
    LokiHttp,
    PyroscopeHttp,
    PyroscopePushHttp,
    NativeWire,
}
impl Inbound {
    pub const ALL: [Self; PROTOCOLS] = [
        Self::OtlpHttp,
        Self::OtlpGrpc,
        Self::PromRemoteWriteHttp,
        Self::LokiHttp,
        Self::PyroscopeHttp,
        Self::PyroscopePushHttp,
        Self::NativeWire,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::OtlpHttp => "otlp_http",
            Self::OtlpGrpc => "otlp_grpc",
            Self::PromRemoteWriteHttp => "prom_remote_write_http",
            Self::LokiHttp => "loki_http",
            Self::PyroscopeHttp => "pyroscope_http",
            Self::PyroscopePushHttp => "pyroscope_push_http",
            Self::NativeWire => "native_wire",
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum GatewaySignal {
    Logs,
    Metrics,
    Traces,
    Profiles,
}
impl GatewaySignal {
    pub const ALL: [Self; SIGNALS] = [Self::Logs, Self::Metrics, Self::Traces, Self::Profiles];
    pub fn name(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::Metrics => "metrics",
            Self::Traces => "traces",
            Self::Profiles => "profiles",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum SinkKind {
    Scry,
    Loki,
    OpenSearch,
    Mimir,
}
impl SinkKind {
    pub const ALL: [Self; SINKS] = [Self::Scry, Self::Loki, Self::OpenSearch, Self::Mimir];
    pub fn name(self) -> &'static str {
        match self {
            Self::Scry => "scry",
            Self::Loki => "loki",
            Self::OpenSearch => "opensearch",
            Self::Mimir => "mimir",
        }
    }
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.name() == name)
    }
}

#[derive(Default)]
struct InboundCounters {
    accepted: AtomicU64,
    rejected: AtomicU64,
}
#[derive(Default)]
struct SinkCounters {
    enqueued: AtomicU64,
    dropped_full: AtomicU64,
    dropped_closed: AtomicU64,
    attempts: AtomicU64,
    attempt_failures: AtomicU64,
    retries: AtomicU64,
    delivered: AtomicU64,
    failed: AtomicU64,
    partial_failure: AtomicU64,
    skipped_empty: AtomicU64,
}

pub struct GatewayMetrics {
    inbound: [InboundCounters; PROTOCOLS],
    records: [AtomicU64; SIGNALS],
    sinks: [[SinkCounters; SIGNALS]; SINKS],
    wire_connections_accepted: AtomicU64,
    wire_connections_rejected: AtomicU64,
    wire_connections_active: AtomicU64,
}

impl Default for GatewayMetrics {
    fn default() -> Self {
        Self {
            inbound: array::from_fn(|_| InboundCounters::default()),
            records: array::from_fn(|_| AtomicU64::new(0)),
            sinks: array::from_fn(|_| array::from_fn(|_| SinkCounters::default())),
            wire_connections_accepted: AtomicU64::new(0),
            wire_connections_rejected: AtomicU64::new(0),
            wire_connections_active: AtomicU64::new(0),
        }
    }
}

impl GatewayMetrics {
    #[inline]
    pub fn inbound_accepted(&self, protocol: Inbound) {
        self.inbound[protocol as usize]
            .accepted
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn inbound_rejected(&self, protocol: Inbound) {
        self.inbound[protocol as usize]
            .rejected
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_records(&self, signal: GatewaySignal, count: u64) {
        self.records[signal as usize].fetch_add(count, Ordering::Relaxed);
    }
    #[inline]
    pub fn enqueued(&self, sink: SinkKind, signal: GatewaySignal) {
        self.sinks[sink as usize][signal as usize]
            .enqueued
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn dropped_full(&self, sink: SinkKind, signal: GatewaySignal) {
        self.sinks[sink as usize][signal as usize]
            .dropped_full
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn dropped_closed(&self, sink: SinkKind, signal: GatewaySignal) {
        self.sinks[sink as usize][signal as usize]
            .dropped_closed
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn attempt(&self, sink: SinkKind, signal: GatewaySignal) {
        self.sinks[sink as usize][signal as usize]
            .attempts
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn attempt_failed(&self, sink: SinkKind, signal: GatewaySignal) {
        self.sinks[sink as usize][signal as usize]
            .attempt_failures
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn retry(&self, sink: SinkKind, signal: GatewaySignal) {
        self.sinks[sink as usize][signal as usize]
            .retries
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn delivered(&self, sink: SinkKind, signal: GatewaySignal) {
        self.sinks[sink as usize][signal as usize]
            .delivered
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn failed(&self, sink: SinkKind, signal: GatewaySignal) {
        self.sinks[sink as usize][signal as usize]
            .failed
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn partial_failure(&self, sink: SinkKind, signal: GatewaySignal) {
        self.sinks[sink as usize][signal as usize]
            .partial_failure
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn skipped_empty(&self, sink: SinkKind, signal: GatewaySignal) {
        self.sinks[sink as usize][signal as usize]
            .skipped_empty
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn wire_connection_accepted(&self) {
        self.wire_connections_accepted
            .fetch_add(1, Ordering::Relaxed);
        self.wire_connections_active.fetch_add(1, Ordering::Relaxed);
    }
    pub fn wire_connection_closed(&self) {
        self.wire_connections_active.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn wire_connection_rejected(&self) {
        self.wire_connections_rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self, queues: &[QueueSnapshot]) -> Value {
        let inbound = Inbound::ALL.into_iter().map(|p| { let c=&self.inbound[p as usize]; (p.name().to_string(), json!({"accepted":c.accepted.load(Ordering::Relaxed),"rejected":c.rejected.load(Ordering::Relaxed)})) }).collect::<serde_json::Map<_,_>>();
        let records = GatewaySignal::ALL
            .into_iter()
            .map(|s| {
                (
                    s.name().to_string(),
                    json!(self.records[s as usize].load(Ordering::Relaxed)),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let sinks = SinkKind::ALL.into_iter().filter_map(|kind| { let queue=queues.iter().find(|q|q.kind==kind)?; let signals=GatewaySignal::ALL.into_iter().map(|signal| { let c=&self.sinks[kind as usize][signal as usize]; (signal.name().to_string(), json!({"enqueued":c.enqueued.load(Ordering::Relaxed),"dropped_full":c.dropped_full.load(Ordering::Relaxed),"dropped_closed":c.dropped_closed.load(Ordering::Relaxed),"attempts":c.attempts.load(Ordering::Relaxed),"attempt_failures":c.attempt_failures.load(Ordering::Relaxed),"retries":c.retries.load(Ordering::Relaxed),"delivered":c.delivered.load(Ordering::Relaxed),"failed":c.failed.load(Ordering::Relaxed),"partial_failure":c.partial_failure.load(Ordering::Relaxed),"skipped_empty":c.skipped_empty.load(Ordering::Relaxed)})) }).collect::<serde_json::Map<_,_>>(); Some((kind.name().to_string(),json!({"queue_depth":queue.depth,"queue_capacity":queue.capacity,"signals":signals}))) }).collect::<serde_json::Map<_,_>>();
        json!({"inbound":inbound,"records":records,"wire_connections":{"accepted":self.wire_connections_accepted.load(Ordering::Relaxed),"rejected":self.wire_connections_rejected.load(Ordering::Relaxed),"active":self.wire_connections_active.load(Ordering::Relaxed)},"sinks":sinks})
    }
}

pub struct QueueSnapshot {
    pub kind: SinkKind,
    pub depth: usize,
    pub capacity: usize,
}

#[derive(Clone)]
pub struct SinkReporter {
    metrics: Option<std::sync::Arc<GatewayMetrics>>,
    kind: SinkKind,
}
impl SinkReporter {
    pub fn new(metrics: Option<std::sync::Arc<GatewayMetrics>>, kind: SinkKind) -> Self {
        Self { metrics, kind }
    }
    pub fn attempt(&self, signal: GatewaySignal) {
        if let Some(m) = &self.metrics {
            m.attempt(self.kind, signal)
        }
    }
    pub fn attempt_failed(&self, signal: GatewaySignal) {
        if let Some(m) = &self.metrics {
            m.attempt_failed(self.kind, signal)
        }
    }
    pub fn retry(&self, signal: GatewaySignal) {
        if let Some(m) = &self.metrics {
            m.retry(self.kind, signal)
        }
    }
    pub fn delivered(&self, signal: GatewaySignal) {
        if let Some(m) = &self.metrics {
            m.delivered(self.kind, signal)
        }
    }
    pub fn failed(&self, signal: GatewaySignal) {
        if let Some(m) = &self.metrics {
            m.failed(self.kind, signal)
        }
    }
    pub fn partial_failure(&self, signal: GatewaySignal) {
        if let Some(m) = &self.metrics {
            m.partial_failure(self.kind, signal)
        }
    }
    pub fn skipped_empty(&self, signal: GatewaySignal) {
        if let Some(m) = &self.metrics {
            m.skipped_empty(self.kind, signal)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_keeps_inbound_queue_and_delivery_stages_distinct() {
        let metrics = GatewayMetrics::default();
        metrics.inbound_accepted(Inbound::OtlpGrpc);
        metrics.inbound_rejected(Inbound::OtlpGrpc);
        metrics.inbound_accepted(Inbound::LokiHttp);
        metrics.inbound_accepted(Inbound::PyroscopePushHttp);
        metrics.add_records(GatewaySignal::Traces, 7);
        metrics.enqueued(SinkKind::Scry, GatewaySignal::Traces);
        metrics.dropped_full(SinkKind::Scry, GatewaySignal::Traces);
        metrics.attempt(SinkKind::Scry, GatewaySignal::Traces);
        metrics.retry(SinkKind::Scry, GatewaySignal::Traces);
        metrics.delivered(SinkKind::Scry, GatewaySignal::Traces);
        let snapshot = metrics.snapshot(&[QueueSnapshot {
            kind: SinkKind::Scry,
            depth: 2,
            capacity: 16,
        }]);
        assert_eq!(snapshot["inbound"]["otlp_grpc"]["accepted"], 1);
        assert_eq!(snapshot["inbound"]["otlp_grpc"]["rejected"], 1);
        assert_eq!(snapshot["inbound"]["loki_http"]["accepted"], 1);
        assert_eq!(snapshot["inbound"]["pyroscope_push_http"]["accepted"], 1);
        assert_eq!(snapshot["records"]["traces"], 7);
        assert_eq!(snapshot["sinks"]["scry"]["queue_depth"], 2);
        assert_eq!(
            snapshot["sinks"]["scry"]["signals"]["traces"]["enqueued"],
            1
        );
        assert_eq!(
            snapshot["sinks"]["scry"]["signals"]["traces"]["dropped_full"],
            1
        );
        assert_eq!(
            snapshot["sinks"]["scry"]["signals"]["traces"]["delivered"],
            1
        );
        assert_eq!(snapshot["sinks"]["scry"]["signals"]["traces"]["retries"], 1);
    }
}
