//! Live operator status: a tiny hand-rolled HTTP/1.1 server plus the
//! process-global metrics both daemons serve, and the typed snapshot they
//! publish into Valkey so any instance's status page shows the whole fleet.
//!
//! Three halves:
//!
//! 1. **Signal-agnostic HTTP plumbing** — [`serve_status`] + the
//!    [`LocalStatus`] and [`FleetSource`] traits. A minimal HTTP/1.1 responder
//!    over a `tokio` `TcpListener` with two GET routes: `/` (the fleet HTML
//!    dashboard) and `/stats.json` (a JSON array of instance snapshots).
//!    `Connection: close` per request, so there's no keep-alive state machine.
//!    This half knows nothing about ingest vs query — both wire in their own
//!    [`LocalStatus`] impl.
//!
//! 2. **The role providers** — [`ServerMetrics`] (+ per-signal [`UploadStats`])
//!    for ingest, [`QueryMetrics`] for query. Process-global atomics plus, for
//!    query, live reads of the shared caches / memory pool / catalog. Each
//!    implements [`LocalStatus`], producing a [`StatusSnapshot`].
//!
//! 3. **The fleet view** — every instance heartbeats its full [`StatusSnapshot`]
//!    (as JSON) into Valkey via `scry_valkey::StatusRegistration`; `/stats.json`
//!    reads *all* live snapshots back through a [`FleetSource`] (one Lua `SCAN`)
//!    and marks which one is self. With no Valkey the page falls back to the
//!    single local snapshot. Per the design, nothing is withheld from Redis —
//!    the local instance is rendered from its own published snapshot, marked
//!    only as "this instance".

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use scry_catalog::Catalog;
use scry_proto::constants::Signal;
use scry_query::{BloomCache, PostingsCache, QueryResultCache};
#[cfg(test)]
use scry_query::{BloomCacheConfig, PostingsCacheConfig};
use tokio::sync::watch;

// Generic status envelope and HTTP fleet page live in `scry-status`.

pub use scry_status::{
    rss_kib, serve_status, unix_ms_now, FleetSource, LocalStatus, StatusSnapshot,
};

/// System 1-minute load average used by adaptive compression.
pub fn load_avg_1m() -> Option<f64> {
    std::fs::read_to_string("/proc/loadavg")
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

// ─────────────────────────── ingest metrics ───────────────────────────────

/// Per-signal upload pipeline gauges. Shared (`Arc`) between [`ServerMetrics`]
/// and the signal's [`crate::Pipeline`], which bumps them from `spawn_upload` /
/// `run_upload`.
#[derive(Default, Debug)]
pub struct UploadStats {
    uploads_inflight: AtomicU64,
    upload_waiters: AtomicU64,
    upload_stall_nanos_total: AtomicU64,
    blocks_uploaded: AtomicU64,
    bytes_uploaded: AtomicU64,
    upload_failures: AtomicU64,
    upload_nanos_total: AtomicU64,
}

impl UploadStats {
    /// Ingest has begun blocking on a permit (no upload slot free).
    #[inline]
    pub fn begin_wait(&self) {
        self.upload_waiters.fetch_add(1, Ordering::Relaxed);
    }

    /// Ingest got its permit after blocking `nanos`.
    #[inline]
    pub fn end_wait(&self, nanos: u64) {
        self.upload_waiters.fetch_sub(1, Ordering::Relaxed);
        self.upload_stall_nanos_total
            .fetch_add(nanos, Ordering::Relaxed);
    }

    /// An upload acquired its permit and is now running.
    #[inline]
    pub fn start_inflight(&self) {
        self.uploads_inflight.fetch_add(1, Ordering::Relaxed);
    }

    /// The upload finished (success or failure): drop it from inflight.
    #[inline]
    pub fn finish_inflight(&self) {
        self.uploads_inflight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a successful upload of `bytes` parquet bytes taking `nanos`.
    #[inline]
    pub fn record_success(&self, bytes: u64, nanos: u64) {
        self.blocks_uploaded.fetch_add(1, Ordering::Relaxed);
        self.bytes_uploaded.fetch_add(bytes, Ordering::Relaxed);
        self.upload_nanos_total.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Record a failed upload.
    #[inline]
    pub fn record_failure(&self) {
        self.upload_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> serde_json::Value {
        let inflight = self.uploads_inflight.load(Ordering::Relaxed);
        let waiters = self.upload_waiters.load(Ordering::Relaxed);
        let bytes = self.bytes_uploaded.load(Ordering::Relaxed);
        let nanos = self.upload_nanos_total.load(Ordering::Relaxed);
        let eff_bps = if nanos > 0 {
            bytes as f64 / (nanos as f64 / 1e9)
        } else {
            0.0
        };
        serde_json::json!({
            "uploads_inflight": inflight,
            "upload_waiters": waiters,
            "upload_stall_seconds_total":
                self.upload_stall_nanos_total.load(Ordering::Relaxed) as f64 / 1e9,
            "blocks_uploaded": self.blocks_uploaded.load(Ordering::Relaxed),
            "bytes_uploaded": bytes,
            "upload_failures": self.upload_failures.load(Ordering::Relaxed),
            "effective_upload_bytes_per_sec": eff_bps,
        })
    }
}

/// One completed compaction pass, recorded into [`ServerMetrics`].
#[derive(Debug, Clone, Copy, Default)]
pub struct CompactionPassStats {
    pub merges: u64,
    pub blocks_in: u64,
    pub blocks_out: u64,
    pub bytes_out: u64,
    pub aborted: u64,
    pub reaped: u64,
    pub reap_failed: u64,
    pub partition_failed: u64,
    pub lease_held: u64,
    pub lease_unavailable: u64,
    /// Partitions the planner declined because the merged output's ancestor
    /// closure would exceed the sidecar cap. A non-zero value that never
    /// returns to zero means those partitions will never compact again.
    pub oversized: u64,
}

/// Process-global ingest metrics. Record counters are bumped once per *batch*
/// (the same cadence as the per-connection `Counters` in `server.rs`), so
/// exposing them adds no per-record hot-path cost. Construct with
/// [`ServerMetrics::new`], set identity with [`ServerMetrics::with_identity`],
/// share via `Arc`.
pub struct ServerMetrics {
    started: Instant,
    /// Instance id + advertised addr for the status envelope. Empty until
    /// [`with_identity`](Self::with_identity) is called (e.g. in tests).
    instance_id: String,
    addr: String,
    active_connections: AtomicU64,
    total_connections: AtomicU64,
    batches: AtomicU64,
    metric_samples: AtomicU64,
    log_entries: AtomicU64,
    spans: AtomicU64,
    profile_blobs: AtomicU64,
    dummy_records: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    rejected: AtomicU64,
    upload_concurrency: u64,
    metrics_upload: Arc<UploadStats>,
    logs_upload: Arc<UploadStats>,
    traces_upload: Arc<UploadStats>,
    profiles_upload: Arc<UploadStats>,
    dummy_upload: Arc<UploadStats>,
    compaction_enabled: AtomicU64,
    compaction_grace_secs: AtomicU64,
    compaction_passes: AtomicU64,
    compaction_pass_failed: AtomicU64,
    compaction_merges: AtomicU64,
    compaction_blocks_in: AtomicU64,
    compaction_blocks_out: AtomicU64,
    compaction_bytes_out: AtomicU64,
    compaction_aborted: AtomicU64,
    compaction_reaped: AtomicU64,
    compaction_reap_failed: AtomicU64,
    compaction_partition_failed: AtomicU64,
    compaction_lease_held: AtomicU64,
    compaction_lease_unavailable: AtomicU64,
    /// Gauge, not a counter: stuck partitions as of the most recent pass.
    compaction_oversized: AtomicU64,
    compaction_last_pass_unix_ms: AtomicU64,
    compaction_last_pass_duration_ms: AtomicU64,
}

impl ServerMetrics {
    /// `upload_concurrency` is the shared cap on concurrent block encode+upload
    /// tasks across all signals. Sized to the host's physical core count.
    pub fn new(upload_concurrency: usize) -> Self {
        Self {
            started: Instant::now(),
            instance_id: String::new(),
            addr: String::new(),
            active_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            metric_samples: AtomicU64::new(0),
            log_entries: AtomicU64::new(0),
            spans: AtomicU64::new(0),
            profile_blobs: AtomicU64::new(0),
            dummy_records: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            upload_concurrency: upload_concurrency as u64,
            metrics_upload: Arc::new(UploadStats::default()),
            logs_upload: Arc::new(UploadStats::default()),
            traces_upload: Arc::new(UploadStats::default()),
            profiles_upload: Arc::new(UploadStats::default()),
            dummy_upload: Arc::new(UploadStats::default()),
            compaction_enabled: AtomicU64::new(0),
            compaction_grace_secs: AtomicU64::new(0),
            compaction_passes: AtomicU64::new(0),
            compaction_pass_failed: AtomicU64::new(0),
            compaction_merges: AtomicU64::new(0),
            compaction_blocks_in: AtomicU64::new(0),
            compaction_blocks_out: AtomicU64::new(0),
            compaction_bytes_out: AtomicU64::new(0),
            compaction_aborted: AtomicU64::new(0),
            compaction_reaped: AtomicU64::new(0),
            compaction_reap_failed: AtomicU64::new(0),
            compaction_partition_failed: AtomicU64::new(0),
            compaction_lease_held: AtomicU64::new(0),
            compaction_lease_unavailable: AtomicU64::new(0),
            compaction_oversized: AtomicU64::new(0),
            compaction_last_pass_unix_ms: AtomicU64::new(0),
            compaction_last_pass_duration_ms: AtomicU64::new(0),
        }
    }

    /// Stamp the instance identity used in the published status envelope.
    pub fn with_identity(mut self, instance_id: String, addr: String) -> Self {
        self.instance_id = instance_id;
        self.addr = addr;
        self
    }

    pub fn metrics_upload(&self) -> Arc<UploadStats> {
        self.metrics_upload.clone()
    }
    pub fn logs_upload(&self) -> Arc<UploadStats> {
        self.logs_upload.clone()
    }
    pub fn traces_upload(&self) -> Arc<UploadStats> {
        self.traces_upload.clone()
    }
    pub fn profiles_upload(&self) -> Arc<UploadStats> {
        self.profiles_upload.clone()
    }
    pub fn dummy_upload(&self) -> Arc<UploadStats> {
        self.dummy_upload.clone()
    }

    /// Describe this daemon's compaction policy in fleet snapshots.
    pub fn configure_compaction(&self, enabled: bool, grace: Duration) {
        self.compaction_enabled
            .store(u64::from(enabled), Ordering::Relaxed);
        self.compaction_grace_secs
            .store(grace.as_secs(), Ordering::Relaxed);
    }

    /// Accumulate one completed compaction pass and retain its latest timing.
    pub fn record_compaction_pass(&self, pass: CompactionPassStats, duration: Duration) {
        self.compaction_passes.fetch_add(1, Ordering::Relaxed);
        self.compaction_merges
            .fetch_add(pass.merges, Ordering::Relaxed);
        self.compaction_blocks_in
            .fetch_add(pass.blocks_in, Ordering::Relaxed);
        self.compaction_blocks_out
            .fetch_add(pass.blocks_out, Ordering::Relaxed);
        self.compaction_bytes_out
            .fetch_add(pass.bytes_out, Ordering::Relaxed);
        self.compaction_aborted
            .fetch_add(pass.aborted, Ordering::Relaxed);
        self.compaction_reaped
            .fetch_add(pass.reaped, Ordering::Relaxed);
        self.compaction_reap_failed
            .fetch_add(pass.reap_failed, Ordering::Relaxed);
        self.compaction_partition_failed
            .fetch_add(pass.partition_failed, Ordering::Relaxed);
        self.compaction_lease_held
            .fetch_add(pass.lease_held, Ordering::Relaxed);
        self.compaction_lease_unavailable
            .fetch_add(pass.lease_unavailable, Ordering::Relaxed);
        // Replaced, not accumulated — this is "how many are stuck right now".
        self.compaction_oversized
            .store(pass.oversized, Ordering::Relaxed);
        self.compaction_last_pass_duration_ms.store(
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.compaction_last_pass_unix_ms
            .store(unix_ms_now(), Ordering::Release);
    }

    /// Record a pass-level failure that produced no [`CompactionPassStats`].
    pub fn record_compaction_failure(&self, duration: Duration) {
        self.compaction_pass_failed.fetch_add(1, Ordering::Relaxed);
        self.compaction_last_pass_duration_ms.store(
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.compaction_last_pass_unix_ms
            .store(unix_ms_now(), Ordering::Release);
    }

    // ── ingest-path bumps (called from server.rs) ──────────────────────

    #[inline]
    pub fn conn_open(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn conn_close(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_batch(&self, bytes_in: u64) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_bytes_out(&self, bytes_out: u64) {
        self.bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
    }
    #[inline]
    pub fn add_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }
    /// Add accepted records to the counter for `signal`.
    #[inline]
    pub fn add_records(&self, signal: Signal, n: u64) {
        let counter = match signal {
            Signal::Metrics => &self.metric_samples,
            Signal::Logs => &self.log_entries,
            Signal::Traces => &self.spans,
            Signal::Profiles => &self.profile_blobs,
            Signal::Dummy => &self.dummy_records,
        };
        counter.fetch_add(n, Ordering::Relaxed);
    }

    // ── snapshot / classify ────────────────────────────────────────────

    /// Classify where the pipeline is currently bottlenecked. Returns
    /// `(status, severity, message)`.
    fn bottleneck(&self) -> (&'static str, &'static str, String) {
        let uploads = [
            &self.metrics_upload,
            &self.logs_upload,
            &self.traces_upload,
            &self.profiles_upload,
            &self.dummy_upload,
        ];
        let total_waiters: u64 = uploads
            .iter()
            .map(|u| u.upload_waiters.load(Ordering::Relaxed))
            .sum();
        let total_inflight: u64 = uploads
            .iter()
            .map(|u| u.uploads_inflight.load(Ordering::Relaxed))
            .sum();
        let cap = self.upload_concurrency;

        if total_waiters > 0 {
            let plural = if total_waiters == 1 { "" } else { "s" };
            (
                "upload_bound",
                "warn",
                format!(
                    "Ingest is stalling on S3 upload — {total_waiters} pipeline{plural} blocked \
                     waiting for an upload slot. Throughput is capped at bucket write speed \
                     (memory stays bounded — ingest backpressures rather than buffering)."
                ),
            )
        } else if cap > 0 && total_inflight >= cap {
            (
                "upload_saturated",
                "info",
                format!(
                    "Uploads running at max concurrency ({cap}) but keeping pace — \
                     no blocks queued in memory."
                ),
            )
        } else {
            (
                "healthy",
                "ok",
                "Ingest absorbed; the limit is network/decode, not the bucket.".to_string(),
            )
        }
    }

    /// The ingest-specific payload embedded in the status snapshot's `data`.
    fn ingest_data(&self) -> serde_json::Value {
        let (status, severity, message) = self.bottleneck();
        serde_json::json!({
            "active_connections": self.active_connections.load(Ordering::Relaxed),
            "total_connections": self.total_connections.load(Ordering::Relaxed),
            "batches": self.batches.load(Ordering::Relaxed),
            "metric_samples": self.metric_samples.load(Ordering::Relaxed),
            "log_entries": self.log_entries.load(Ordering::Relaxed),
            "spans": self.spans.load(Ordering::Relaxed),
            "profile_blobs": self.profile_blobs.load(Ordering::Relaxed),
            "dummy_records": self.dummy_records.load(Ordering::Relaxed),
            "bytes_in": self.bytes_in.load(Ordering::Relaxed),
            "bytes_out": self.bytes_out.load(Ordering::Relaxed),
            "rejected": self.rejected.load(Ordering::Relaxed),
            "max_inflight_uploads": self.upload_concurrency,
            "uploads": {
                "metrics": self.metrics_upload.snapshot(),
                "logs": self.logs_upload.snapshot(),
                "traces": self.traces_upload.snapshot(),
                "profiles": self.profiles_upload.snapshot(),
                "dummy": self.dummy_upload.snapshot(),
            },
            "compaction": {
                "enabled": self.compaction_enabled.load(Ordering::Relaxed) != 0,
                "grace_secs": self.compaction_grace_secs.load(Ordering::Relaxed),
                "passes": self.compaction_passes.load(Ordering::Relaxed),
                "pass_failed": self.compaction_pass_failed.load(Ordering::Relaxed),
                "merges": self.compaction_merges.load(Ordering::Relaxed),
                "blocks_in": self.compaction_blocks_in.load(Ordering::Relaxed),
                "blocks_out": self.compaction_blocks_out.load(Ordering::Relaxed),
                "bytes_out": self.compaction_bytes_out.load(Ordering::Relaxed),
                "aborted": self.compaction_aborted.load(Ordering::Relaxed),
                "reaped": self.compaction_reaped.load(Ordering::Relaxed),
                "reap_failed": self.compaction_reap_failed.load(Ordering::Relaxed),
                "partition_failed": self.compaction_partition_failed.load(Ordering::Relaxed),
                "lease_held": self.compaction_lease_held.load(Ordering::Relaxed),
                "lease_unavailable": self.compaction_lease_unavailable.load(Ordering::Relaxed),
                "oversized": self.compaction_oversized.load(Ordering::Relaxed),
                "last_pass_unix_ms": self.compaction_last_pass_unix_ms.load(Ordering::Acquire),
                "last_pass_duration_ms": self.compaction_last_pass_duration_ms.load(Ordering::Relaxed),
            },
            "bottleneck": {
                "status": status,
                "severity": severity,
                "message": message,
            },
        })
    }
}

impl LocalStatus for ServerMetrics {
    fn snapshot(&self) -> StatusSnapshot {
        StatusSnapshot {
            role: "ingest".to_string(),
            instance_id: self.instance_id.clone(),
            addr: self.addr.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            now_unix_ms: unix_ms_now(),
            uptime_secs: self.started.elapsed().as_secs_f64(),
            rss_kib: rss_kib(),
            data: self.ingest_data(),
        }
    }
}

// ─────────────────────────── query metrics ────────────────────────────────

/// Process-global query metrics. The counters below are bumped once per *query*
/// (not per row), so they're cheap; the rest of the snapshot is live reads of
/// state the [`crate::QueryService`] already owns (the sidecar caches, the
/// DataFusion memory pool, and the catalog), so there is no extra hot-path cost.
/// Construct with [`QueryMetrics::new`], share via `Arc`.
const QUERY_LATENCY_BUCKET_MS: [u64; 8] = [10, 50, 100, 500, 1_000, 5_000, 30_000, u64::MAX];
const QUERY_RANGE_BUCKET_SECS: [u64; 5] = [3_600, 21_600, 86_400, 604_800, u64::MAX];

fn atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn histogram_observe<const N: usize>(buckets: &[AtomicU64; N], bounds: &[u64; N], value: u64) {
    let index = bounds
        .iter()
        .position(|bound| value <= *bound)
        .unwrap_or(N - 1);
    buckets[index].fetch_add(1, Ordering::Relaxed);
}

fn histogram_quantile_ms(buckets: &[u64], quantile: f64) -> Option<u64> {
    let total: u64 = buckets.iter().sum();
    if total == 0 {
        return None;
    }
    let target = ((total as f64 * quantile).ceil() as u64).max(1);
    let mut cumulative = 0;
    for (index, count) in buckets.iter().enumerate() {
        cumulative += count;
        if cumulative >= target {
            return (QUERY_LATENCY_BUCKET_MS[index] != u64::MAX)
                .then_some(QUERY_LATENCY_BUCKET_MS[index]);
        }
    }
    None
}

pub struct QueryMetrics {
    started: Instant,
    instance_id: String,
    addr: String,
    queries_total: AtomicU64,
    queries_in_flight: AtomicU64,
    query_errors_total: AtomicU64,
    query_nanos_total: AtomicU64,
    rows_returned_total: AtomicU64,
    bytes_scanned_total: AtomicU64,
    /// Cumulative count of candidate blocks selected for scanning (pre-pruning),
    /// summed across all queries. This is what explodes on an unbounded query,
    /// so the periodic activity logger reports its per-interval delta as the
    /// headline "blocks scanned recently" number.
    blocks_scanned_total: AtomicU64,
    query_latency_buckets: [AtomicU64; QUERY_LATENCY_BUCKET_MS.len()],
    query_range_buckets: [AtomicU64; QUERY_RANGE_BUCKET_SECS.len()],
    query_range_seconds_total: AtomicU64,
    query_range_observations: AtomicU64,
    query_range_max_seconds: AtomicU64,
    query_unbounded_start_total: AtomicU64,
    query_unbounded_end_total: AtomicU64,
    query_defaulted_range_total: AtomicU64,
    memory_observed_peak_reserved_bytes: AtomicU64,
    admission_waiting: AtomicU64,
    admission_waited_total: AtomicU64,
    admission_wait_nanos_total: AtomicU64,
    admission_wait_max_nanos: AtomicU64,
    admission_timeout_total: AtomicU64,
    admission_rejected_total: AtomicU64,
    response_resets_total: AtomicU64,
    repair_attempts_total: AtomicU64,
    repair_successes_total: AtomicU64,
    repair_failures_total: AtomicU64,
    repair_stability_retries_total: AtomicU64,
    // Live-read handles into the query service's shared state.
    postings_cache: Arc<PostingsCache>,
    bloom_cache: Arc<BloomCache>,
    result_cache: Arc<QueryResultCache>,
    memory_pool: Arc<GreedyMemoryPool>,
    catalog: Arc<Mutex<Catalog>>,
    /// Connection health of this daemon's Valkey link, if any. `None` ⇒ no
    /// Valkey configured; `Some(false)` ⇒ configured but currently down.
    valkey_health: Option<watch::Receiver<bool>>,
}

impl QueryMetrics {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance_id: String,
        addr: String,
        postings_cache: Arc<PostingsCache>,
        bloom_cache: Arc<BloomCache>,
        result_cache: Arc<QueryResultCache>,
        memory_pool: Arc<GreedyMemoryPool>,
        catalog: Arc<Mutex<Catalog>>,
        valkey_health: Option<watch::Receiver<bool>>,
    ) -> Self {
        Self {
            started: Instant::now(),
            instance_id,
            addr,
            queries_total: AtomicU64::new(0),
            queries_in_flight: AtomicU64::new(0),
            query_errors_total: AtomicU64::new(0),
            query_nanos_total: AtomicU64::new(0),
            rows_returned_total: AtomicU64::new(0),
            bytes_scanned_total: AtomicU64::new(0),
            blocks_scanned_total: AtomicU64::new(0),
            query_latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            query_range_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            query_range_seconds_total: AtomicU64::new(0),
            query_range_observations: AtomicU64::new(0),
            query_range_max_seconds: AtomicU64::new(0),
            query_unbounded_start_total: AtomicU64::new(0),
            query_unbounded_end_total: AtomicU64::new(0),
            query_defaulted_range_total: AtomicU64::new(0),
            memory_observed_peak_reserved_bytes: AtomicU64::new(0),
            admission_waiting: AtomicU64::new(0),
            admission_waited_total: AtomicU64::new(0),
            admission_wait_nanos_total: AtomicU64::new(0),
            admission_wait_max_nanos: AtomicU64::new(0),
            admission_timeout_total: AtomicU64::new(0),
            admission_rejected_total: AtomicU64::new(0),
            response_resets_total: AtomicU64::new(0),
            repair_attempts_total: AtomicU64::new(0),
            repair_successes_total: AtomicU64::new(0),
            repair_failures_total: AtomicU64::new(0),
            repair_stability_retries_total: AtomicU64::new(0),
            postings_cache,
            bloom_cache,
            result_cache,
            memory_pool,
            catalog,
            valkey_health,
        }
    }

    /// Begin tracking one in-flight query: bumps `queries_total` and the
    /// in-flight gauge, and returns an RAII guard that — on drop — decrements
    /// the gauge, folds the query's wall-time into `query_nanos_total`, and (if
    /// [`mark_ok`](QueryInFlight::mark_ok) was not called) counts an error. So
    /// every early return / `?` in the handler is accounted correctly.
    pub fn begin(self: &Arc<Self>) -> QueryInFlight {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        self.queries_in_flight.fetch_add(1, Ordering::Relaxed);
        QueryInFlight {
            metrics: self.clone(),
            start: Instant::now(),
            ok: false,
        }
    }

    /// Record a completed scan's row + object-store byte counts. Called once
    /// per terminal path from `emit_scan_complete`.
    pub fn record_scan(&self, rows: u64, bytes_scanned: u64) {
        self.rows_returned_total.fetch_add(rows, Ordering::Relaxed);
        self.bytes_scanned_total
            .fetch_add(bytes_scanned, Ordering::Relaxed);
    }

    /// Record how many candidate blocks a query selected for scanning
    /// (pre-pruning). Called once per query, right after `list_candidates`.
    pub fn record_candidates(&self, blocks: u64) {
        self.blocks_scanned_total
            .fetch_add(blocks, Ordering::Relaxed);
    }

    /// Record the effective candidate-selection range once per data query.
    pub fn record_query_range(&self, ts_min: Option<u64>, ts_max: Option<u64>, defaulted: bool) {
        if defaulted {
            self.query_defaulted_range_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let Some(min) = ts_min else {
            self.query_unbounded_start_total
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let max = ts_max.unwrap_or_else(|| {
            self.query_unbounded_end_total
                .fetch_add(1, Ordering::Relaxed);
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        });
        let seconds = max.saturating_sub(min) / 1_000_000_000;
        self.query_range_seconds_total
            .fetch_add(seconds, Ordering::Relaxed);
        self.query_range_observations
            .fetch_add(1, Ordering::Relaxed);
        atomic_max(&self.query_range_max_seconds, seconds);
        histogram_observe(&self.query_range_buckets, &QUERY_RANGE_BUCKET_SECS, seconds);
    }

    pub fn record_memory_reservation(&self, reserved: usize) {
        atomic_max(&self.memory_observed_peak_reserved_bytes, reserved as u64);
    }

    pub fn admission_wait_started(&self) -> Instant {
        self.admission_waiting.fetch_add(1, Ordering::Relaxed);
        Instant::now()
    }

    pub fn admission_wait_finished(&self, started: Instant) {
        self.admission_waiting.fetch_sub(1, Ordering::Relaxed);
        let nanos = started.elapsed().as_nanos() as u64;
        self.admission_waited_total.fetch_add(1, Ordering::Relaxed);
        self.admission_wait_nanos_total
            .fetch_add(nanos, Ordering::Relaxed);
        atomic_max(&self.admission_wait_max_nanos, nanos);
    }

    pub fn record_admission_timeout(&self) {
        self.admission_timeout_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_admission_rejected(&self) {
        self.admission_rejected_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_response_reset(&self) {
        self.response_resets_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_repair_attempt(&self) {
        self.repair_attempts_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_repair_success(&self) {
        self.repair_successes_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_repair_failure(&self) {
        self.repair_failures_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_repair_stability_retry(&self) {
        self.repair_stability_retries_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Cheap atomic snapshot for the periodic activity logger:
    /// `(queries_total, queries_in_flight, blocks_scanned_total)`. The logger
    /// diffs the cumulative counters between ticks to report per-interval rates.
    pub fn activity_snapshot(&self) -> (u64, u64, u64) {
        (
            self.queries_total.load(Ordering::Relaxed),
            self.queries_in_flight.load(Ordering::Relaxed),
            self.blocks_scanned_total.load(Ordering::Relaxed),
        )
    }

    fn query_data(&self) -> serde_json::Value {
        let queries = self.queries_total.load(Ordering::Relaxed);
        let nanos = self.query_nanos_total.load(Ordering::Relaxed);
        let avg_ms = if queries > 0 {
            (nanos as f64 / queries as f64) / 1e6
        } else {
            0.0
        };
        let p = self.postings_cache.stats();
        let b = self.bloom_cache.stats();
        let r = self.result_cache.stats();
        // Catalog: one indexed COUNT/SUM under the brief mutex. On a poisoned
        // lock (a panicked query holder) fall back to zeros rather than panic
        // the status server.
        let (catalog_blocks, catalog_rows, catalog_lineage_rows) = match self.catalog.lock() {
            Ok(cat) => (
                cat.block_count().unwrap_or(0) as u64,
                cat.live_row_count().unwrap_or(0),
                cat.lineage_row_count().unwrap_or(0),
            ),
            Err(_) => (0, 0, 0),
        };
        let valkey_connected = self.valkey_health.as_ref().map(|rx| *rx.borrow());
        let latency_buckets: Vec<u64> = self
            .query_latency_buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        let range_buckets: Vec<u64> = self
            .query_range_buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        let range_observations = self.query_range_observations.load(Ordering::Relaxed);
        let range_total = self.query_range_seconds_total.load(Ordering::Relaxed);
        let waited = self.admission_waited_total.load(Ordering::Relaxed);
        let wait_nanos = self.admission_wait_nanos_total.load(Ordering::Relaxed);
        let memory_reserved = self.memory_pool.reserved();
        self.record_memory_reservation(memory_reserved);
        serde_json::json!({
            "queries_total": queries,
            "queries_in_flight": self.queries_in_flight.load(Ordering::Relaxed),
            "query_errors_total": self.query_errors_total.load(Ordering::Relaxed),
            "avg_query_ms": avg_ms,
            "query_latency": {
                "p50_ms_upper": histogram_quantile_ms(&latency_buckets, 0.50),
                "p95_ms_upper": histogram_quantile_ms(&latency_buckets, 0.95),
                "p99_ms_upper": histogram_quantile_ms(&latency_buckets, 0.99),
                "buckets": latency_buckets,
            },
            "query_ranges": {
                "average_seconds": if range_observations == 0 { 0.0 } else { range_total as f64 / range_observations as f64 },
                "max_seconds": self.query_range_max_seconds.load(Ordering::Relaxed),
                "unbounded_start_total": self.query_unbounded_start_total.load(Ordering::Relaxed),
                "unbounded_end_total": self.query_unbounded_end_total.load(Ordering::Relaxed),
                "defaulted_total": self.query_defaulted_range_total.load(Ordering::Relaxed),
                "buckets": range_buckets,
            },
            "rows_returned_total": self.rows_returned_total.load(Ordering::Relaxed),
            "bytes_scanned_total": self.bytes_scanned_total.load(Ordering::Relaxed),
            "blocks_scanned_total": self.blocks_scanned_total.load(Ordering::Relaxed),
            "postings_cache": {
                "hits": p.hits, "misses": p.misses, "evictions": p.evictions,
                "entries": p.entries, "bytes_in": p.bytes_in, "budget_bytes": p.budget_bytes,
            },
            "bloom_cache": {
                "hits": b.hits, "misses": b.misses, "evictions": b.evictions,
                "entries": b.entries, "bytes_in": b.bytes_in, "budget_bytes": b.budget_bytes,
            },
            "result_cache": {
                "hits": r.hits, "misses": r.misses, "inserts": r.inserts, "evictions": r.evictions,
                "entries": r.entries, "bytes_in": r.bytes_in, "budget_bytes": r.budget_bytes,
            },
            "memory_reserved_bytes": memory_reserved,
            "memory_observed_peak_reserved_bytes": self.memory_observed_peak_reserved_bytes.load(Ordering::Relaxed),
            "admission": {
                "waiting": self.admission_waiting.load(Ordering::Relaxed),
                "waited_total": waited,
                "average_wait_ms": if waited == 0 { 0.0 } else { (wait_nanos as f64 / waited as f64) / 1e6 },
                "max_wait_ms": self.admission_wait_max_nanos.load(Ordering::Relaxed) as f64 / 1e6,
                "timeouts_total": self.admission_timeout_total.load(Ordering::Relaxed),
                "rejected_total": self.admission_rejected_total.load(Ordering::Relaxed),
            },
            "recovery": {
                "response_resets_total": self.response_resets_total.load(Ordering::Relaxed),
                "repair_attempts_total": self.repair_attempts_total.load(Ordering::Relaxed),
                "repair_successes_total": self.repair_successes_total.load(Ordering::Relaxed),
                "repair_failures_total": self.repair_failures_total.load(Ordering::Relaxed),
                "repair_stability_retries_total": self.repair_stability_retries_total.load(Ordering::Relaxed),
            },
            "catalog_blocks": catalog_blocks,
            "catalog_rows": catalog_rows,
            "catalog_lineage_rows": catalog_lineage_rows,
            "valkey_connected": valkey_connected,
        })
    }
}

impl LocalStatus for QueryMetrics {
    fn snapshot(&self) -> StatusSnapshot {
        StatusSnapshot {
            role: "query".to_string(),
            instance_id: self.instance_id.clone(),
            addr: self.addr.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            now_unix_ms: unix_ms_now(),
            uptime_secs: self.started.elapsed().as_secs_f64(),
            rss_kib: rss_kib(),
            data: self.query_data(),
        }
    }
}

/// RAII in-flight guard for one query (see [`QueryMetrics::begin`]).
pub struct QueryInFlight {
    metrics: Arc<QueryMetrics>,
    start: Instant,
    ok: bool,
}

impl QueryInFlight {
    /// Mark the query as having completed successfully (reached its
    /// `EndOfStream` terminator or served a cache hit). Without this, the drop
    /// counts an error.
    #[inline]
    pub fn mark_ok(&mut self) {
        self.ok = true;
    }
}

impl Drop for QueryInFlight {
    fn drop(&mut self) {
        self.metrics
            .queries_in_flight
            .fetch_sub(1, Ordering::Relaxed);
        let elapsed = self.start.elapsed();
        self.metrics
            .query_nanos_total
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
        histogram_observe(
            &self.metrics.query_latency_buckets,
            &QUERY_LATENCY_BUCKET_MS,
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        );
        if !self.ok {
            self.metrics
                .query_errors_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifier_states() {
        const CAP: usize = 4;
        let m = ServerMetrics::new(CAP);

        let (status, severity, _) = m.bottleneck();
        assert_eq!((status, severity), ("healthy", "ok"));

        m.metrics_upload
            .uploads_inflight
            .store(CAP as u64 - 1, Ordering::Relaxed);
        let (status, severity, _) = m.bottleneck();
        assert_eq!((status, severity), ("healthy", "ok"));

        m.dummy_upload.uploads_inflight.store(1, Ordering::Relaxed);
        let (status, severity, _) = m.bottleneck();
        assert_eq!((status, severity), ("upload_saturated", "info"));

        m.metrics_upload.upload_waiters.store(1, Ordering::Relaxed);
        let (status, severity, _) = m.bottleneck();
        assert_eq!((status, severity), ("upload_bound", "warn"));
    }

    #[test]
    fn ingest_snapshot_is_valid_and_complete() {
        const CAP: usize = 8;
        let m =
            ServerMetrics::new(CAP).with_identity("iid".to_string(), "127.0.0.1:4000".to_string());
        m.add_batch(100);
        m.add_bytes_out(400);
        m.add_records(Signal::Metrics, 50);
        m.add_records(Signal::Logs, 7);
        m.conn_open();

        let snap = m.snapshot();
        assert_eq!(snap.role, "ingest");
        assert_eq!(snap.instance_id, "iid");
        assert_eq!(snap.addr, "127.0.0.1:4000");
        let d = &snap.data;
        assert_eq!(d["metric_samples"], serde_json::json!(50));
        assert_eq!(d["log_entries"], serde_json::json!(7));
        assert_eq!(d["batches"], serde_json::json!(1));
        assert_eq!(d["bytes_in"], serde_json::json!(100));
        assert_eq!(d["active_connections"], serde_json::json!(1));
        assert_eq!(d["max_inflight_uploads"], serde_json::json!(CAP));
        assert!(d["uploads"]["metrics"].is_object());
        assert!(d["bottleneck"]["status"].is_string());
        assert_eq!(d["compaction"]["enabled"], serde_json::json!(false));
        assert_eq!(d["compaction"]["passes"], serde_json::json!(0));
    }

    #[test]
    fn ingest_snapshot_accumulates_compaction_activity() {
        let m = ServerMetrics::new(1);
        m.configure_compaction(true, Duration::from_secs(600));
        m.record_compaction_pass(
            CompactionPassStats {
                merges: 2,
                blocks_in: 16,
                blocks_out: 2,
                bytes_out: 4096,
                aborted: 1,
                reaped: 8,
                reap_failed: 2,
                partition_failed: 3,
                lease_held: 4,
                lease_unavailable: 5,
                oversized: 6,
            },
            Duration::from_millis(125),
        );
        m.record_compaction_pass(
            CompactionPassStats {
                merges: 1,
                blocks_in: 8,
                blocks_out: 1,
                bytes_out: 2048,
                reaped: 4,
                ..Default::default()
            },
            Duration::from_millis(25),
        );
        m.record_compaction_failure(Duration::from_millis(9));

        let snap = m.snapshot();
        let c = &snap.data["compaction"];
        assert_eq!(c["enabled"], serde_json::json!(true));
        assert_eq!(c["grace_secs"], serde_json::json!(600));
        assert_eq!(c["passes"], serde_json::json!(2));
        assert_eq!(c["pass_failed"], serde_json::json!(1));
        assert_eq!(c["merges"], serde_json::json!(3));
        assert_eq!(c["blocks_in"], serde_json::json!(24));
        assert_eq!(c["blocks_out"], serde_json::json!(3));
        assert_eq!(c["bytes_out"], serde_json::json!(6144));
        assert_eq!(c["aborted"], serde_json::json!(1));
        assert_eq!(c["reaped"], serde_json::json!(12));
        assert_eq!(c["reap_failed"], serde_json::json!(2));
        assert_eq!(c["partition_failed"], serde_json::json!(3));
        assert_eq!(c["lease_held"], serde_json::json!(4));
        assert_eq!(c["lease_unavailable"], serde_json::json!(5));
        // A gauge: the second pass reported none stuck, which replaces the 6
        // from the first pass rather than adding to it.
        assert_eq!(c["oversized"], serde_json::json!(0));
        assert_eq!(c["last_pass_duration_ms"], serde_json::json!(9));
        assert!(c["last_pass_unix_ms"].as_u64().unwrap() > 0);
    }

    #[test]
    fn query_latency_quantiles_distinguish_empty_and_overflow() {
        assert_eq!(histogram_quantile_ms(&[0; 8], 0.95), None);
        assert_eq!(
            histogram_quantile_ms(&[1, 0, 0, 0, 0, 0, 0, 0], 0.95),
            Some(10)
        );
        assert_eq!(histogram_quantile_ms(&[0, 0, 0, 0, 0, 0, 0, 1], 0.95), None);
    }

    #[test]
    fn query_snapshot_reports_ranges_latency_memory_admission_and_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let catalog = Arc::new(Mutex::new(
            Catalog::open(&temp.path().join("catalog.sqlite"), "test-bucket").unwrap(),
        ));
        let memory_pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let metrics = Arc::new(QueryMetrics::new(
            "query-id".into(),
            "127.0.0.1:4100".into(),
            Arc::new(PostingsCache::new(PostingsCacheConfig {
                budget_bytes: 1024,
                max_concurrent_fills: 1,
            })),
            Arc::new(BloomCache::new(BloomCacheConfig {
                budget_bytes: 1024,
                max_concurrent_fills: 1,
            })),
            Arc::new(QueryResultCache::with_budget_bytes(1024)),
            memory_pool,
            catalog,
            None,
        ));

        metrics.record_query_range(Some(0), Some(7_200_000_000_000), false);
        metrics.record_query_range(Some(0), Some(86_400_000_000_000), true);
        metrics.record_query_range(Some(unix_ms_now() * 1_000_000 - 1_000_000_000), None, false);
        metrics.record_query_range(None, None, false);
        metrics.record_memory_reservation(4096);
        let wait = metrics.admission_wait_started();
        metrics.admission_wait_finished(wait);
        metrics.record_admission_timeout();
        metrics.record_admission_rejected();
        metrics.record_response_reset();
        metrics.record_repair_attempt();
        metrics.record_repair_success();
        metrics.record_repair_failure();
        metrics.record_repair_stability_retry();
        let mut query = metrics.begin();
        query.mark_ok();
        drop(query);

        let data = metrics.snapshot().data;
        assert_eq!(
            data["query_ranges"]["average_seconds"],
            serde_json::json!(31_200.333333333332)
        );
        assert_eq!(
            data["query_ranges"]["max_seconds"],
            serde_json::json!(86_400)
        );
        assert_eq!(
            data["query_ranges"]["unbounded_start_total"],
            serde_json::json!(1)
        );
        assert_eq!(
            data["query_ranges"]["unbounded_end_total"],
            serde_json::json!(1)
        );
        assert_eq!(
            data["query_ranges"]["defaulted_total"],
            serde_json::json!(1)
        );
        assert_eq!(
            data["memory_observed_peak_reserved_bytes"],
            serde_json::json!(4096)
        );
        assert_eq!(data["admission"]["waiting"], serde_json::json!(0));
        assert_eq!(data["admission"]["waited_total"], serde_json::json!(1));
        assert_eq!(data["admission"]["timeouts_total"], serde_json::json!(1));
        assert_eq!(data["admission"]["rejected_total"], serde_json::json!(1));
        assert_eq!(
            data["recovery"]["response_resets_total"],
            serde_json::json!(1)
        );
        assert_eq!(
            data["recovery"]["repair_attempts_total"],
            serde_json::json!(1)
        );
        assert_eq!(
            data["recovery"]["repair_successes_total"],
            serde_json::json!(1)
        );
        assert_eq!(
            data["recovery"]["repair_failures_total"],
            serde_json::json!(1)
        );
        assert_eq!(
            data["recovery"]["repair_stability_retries_total"],
            serde_json::json!(1)
        );
        assert_eq!(data["query_latency"]["buckets"][0], serde_json::json!(1));
    }

    #[test]
    fn upload_stats_transitions() {
        let u = UploadStats::default();
        u.begin_wait();
        assert_eq!(u.upload_waiters.load(Ordering::Relaxed), 1);
        u.end_wait(500_000_000);
        assert_eq!(u.upload_waiters.load(Ordering::Relaxed), 0);
        u.start_inflight();
        assert_eq!(u.uploads_inflight.load(Ordering::Relaxed), 1);
        u.record_success(1024, 1_000_000_000);
        u.finish_inflight();
        assert_eq!(u.uploads_inflight.load(Ordering::Relaxed), 0);
        let snap = u.snapshot();
        assert_eq!(snap["blocks_uploaded"], serde_json::json!(1));
        assert_eq!(snap["bytes_uploaded"], serde_json::json!(1024));
        assert_eq!(
            snap["effective_upload_bytes_per_sec"],
            serde_json::json!(1024.0)
        );
        assert_eq!(snap["upload_stall_seconds_total"], serde_json::json!(0.5));
    }
}
