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

use std::borrow::Cow;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use scry_catalog::Catalog;
use scry_proto::constants::Signal;
use scry_query::{BloomCache, PostingsCache, QueryResultCache};
#[cfg(test)]
use scry_query::{BloomCacheConfig, PostingsCacheConfig};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{info, warn};

// ─────────────────────────── status snapshot ──────────────────────────────

/// One instance's status, the unit published into Valkey and rendered by the
/// dashboard. The envelope fields are common to both roles; `data` carries the
/// role-specific payload (ingest throughput / upload pipeline, or query rates /
/// cache hit-rates), rendered by the JS that branches on `role`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StatusSnapshot {
    /// `"ingest"` or `"query"`.
    pub role: String,
    /// Stable per-instance id (ingest: the writer UUID; query: the daemon's
    /// ephemeral Valkey holder UUID). Also the Valkey status key suffix and
    /// what the page matches against `self_id` to mark "this instance".
    pub instance_id: String,
    /// The instance's primary listen/advertise address, for display.
    pub addr: String,
    pub now_unix_ms: u64,
    pub uptime_secs: f64,
    pub rss_kib: Option<u64>,
    /// Role-specific payload (see `ServerMetrics::ingest_data` /
    /// `QueryMetrics::query_data`).
    pub data: serde_json::Value,
}

/// Produces this process's [`StatusSnapshot`]. Implemented by [`ServerMetrics`]
/// (ingest) and [`QueryMetrics`] (query). Cheap: read a handful of atomics (+
/// for query, one indexed catalog SELECT) and serialise.
pub trait LocalStatus: Send + Sync + 'static {
    fn snapshot(&self) -> StatusSnapshot;
}

/// Supplies the *other* instances' snapshots as raw JSON blobs. The Valkey-
/// backed impl (in the daemon crates) enumerates `scry/status/*` with one Lua
/// `SCAN`. Kept as a trait so `scry-server` stays Valkey-agnostic.
#[async_trait::async_trait]
pub trait FleetSource: Send + Sync + 'static {
    /// Human-readable aggregation source reported by `/stats.json`.
    fn source(&self) -> &'static str {
        "valkey"
    }

    /// All live status blobs currently registered (including this instance's
    /// own, since it publishes too). Best-effort — returns empty on error.
    async fn blobs(&self) -> Vec<String>;
}

// ───────────────────────────── HTTP plumbing ──────────────────────────────

/// Shared state for the status server: the local snapshot source, the optional
/// fleet source, and this instance's id (to mark self in the rendered fleet).
struct StatusState {
    local: Arc<dyn LocalStatus>,
    fleet: Option<Arc<dyn FleetSource>>,
    self_id: String,
}

impl StatusState {
    /// Build the `/stats.json` body: the full fleet plus which entry is self.
    async fn stats_json(&self) -> String {
        let (source, mut instances): (&str, Vec<StatusSnapshot>) = match &self.fleet {
            Some(f) => {
                let source = f.source();
                let blobs = f.blobs().await;
                let parsed = blobs
                    .iter()
                    .filter_map(|b| serde_json::from_str::<StatusSnapshot>(b).ok())
                    .collect();
                (source, parsed)
            }
            None => ("local", Vec::new()),
        };
        // Ensure this instance is represented even before its first heartbeat
        // has landed in Redis (startup race), and always in the no-Valkey case.
        if !instances.iter().any(|s| s.instance_id == self.self_id) {
            instances.push(self.local.snapshot());
        }
        serde_json::json!({
            "source": source,
            "self_id": self.self_id,
            "instances": instances,
        })
        .to_string()
    }
}

/// Bind `listen_addr` and serve the status page until `shutdown` resolves.
///
/// Errors only on the initial bind; per-connection failures are logged and
/// dropped (a broken status poll must never take down ingest or query).
pub async fn serve_status<F>(
    listen_addr: String,
    local: Arc<dyn LocalStatus>,
    fleet: Option<Arc<dyn FleetSource>>,
    self_id: String,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()>,
{
    let listener = TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("binding status endpoint {listen_addr}"))?;
    info!(addr = %listen_addr, "status HTTP endpoint listening (GET / and /stats.json)");

    let state = Arc::new(StatusState {
        local,
        fleet,
        self_id,
    });

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((sock, _peer)) => {
                        let state = state.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_http(sock, state).await {
                                tracing::debug!(error = %e, "status connection ended with error");
                            }
                        });
                    }
                    Err(e) => warn!(error = %e, "status accept failed"),
                }
            }
            _ = &mut shutdown => {
                info!("status endpoint shutting down");
                break;
            }
        }
    }
    Ok(())
}

/// Read one request, route it, write the response, close. We only care about
/// the request line (`GET <path> HTTP/1.1`); headers and body are ignored for
/// GET. Request reads are capped so a hostile or broken client can't make us
/// buffer unbounded bytes.
async fn handle_http(mut sock: TcpStream, state: Arc<StatusState>) -> Result<()> {
    const MAX_REQUEST_BYTES: usize = 8 * 1024;
    let mut buf = Vec::with_capacity(256);
    let mut tmp = [0u8; 1024];
    loop {
        if find_subslice(&buf, b"\r\n\r\n").is_some() {
            break;
        }
        if buf.len() >= MAX_REQUEST_BYTES {
            break;
        }
        let n = sock.read(&mut tmp).await.context("reading HTTP request")?;
        if n == 0 {
            break; // peer closed before a full request line; bail quietly
        }
        buf.extend_from_slice(&tmp[..n]);
    }

    let (method, path) = parse_request_line(&buf);

    let (status, content_type, body): (&str, &str, Cow<'static, str>) = match (method, path) {
        (Some("GET"), Some("/")) => (
            "200 OK",
            "text/html; charset=utf-8",
            Cow::Borrowed(FLEET_DASHBOARD_HTML),
        ),
        (Some("GET"), Some("/stats.json")) => (
            "200 OK",
            "application/json",
            Cow::Owned(state.stats_json().await),
        ),
        (Some("GET"), Some(_)) => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            Cow::Borrowed("not found\n"),
        ),
        (Some(_), _) => (
            "405 Method Not Allowed",
            "text/plain; charset=utf-8",
            Cow::Borrowed("method not allowed\n"),
        ),
        _ => (
            "400 Bad Request",
            "text/plain; charset=utf-8",
            Cow::Borrowed("bad request\n"),
        ),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Connection: close\r\n\
         \r\n{body}",
        body.len(),
    );
    sock.write_all(response.as_bytes())
        .await
        .context("writing HTTP response")?;
    sock.flush().await.ok();
    let _ = sock.shutdown().await;
    Ok(())
}

/// Extract `(method, path)` from the first line of an HTTP request.
fn parse_request_line(buf: &[u8]) -> (Option<&str>, Option<&str>) {
    let line_end = find_subslice(buf, b"\r\n").unwrap_or(buf.len());
    let line = match std::str::from_utf8(&buf[..line_end]) {
        Ok(l) => l,
        Err(_) => return (None, None),
    };
    let mut parts = line.split(' ');
    let method = parts.next().filter(|s| !s.is_empty());
    let path = parts.next().filter(|s| !s.is_empty());
    (method, path)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Current process resident set size in KiB, read from `/proc/self/status`
/// (`VmRSS:`). `None` on non-Linux or if the file can't be read/parsed.
pub fn rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.split_whitespace().next().and_then(|v| v.parse().ok());
        }
    }
    None
}

/// System 1-minute load average, read from `/proc/loadavg`. `None` on non-Linux
/// or if the file can't be read/parsed. Used by the adaptive-compression policy.
pub fn load_avg_1m() -> Option<f64> {
    let loadavg = std::fs::read_to_string("/proc/loadavg").ok()?;
    loadavg
        .split_whitespace()
        .next()
        .and_then(|v| v.parse().ok())
}

pub(crate) fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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

// ─────────────────────────── fleet dashboard ──────────────────────────────

/// The fleet dashboard: a single self-contained page (no framework, no build
/// step) that polls `/stats.json` once a second and renders one card per live
/// instance, grouped into Ingest and Query sections, computing per-instance
/// rates by diffing successive polls keyed on `instance_id`. The card for this
/// instance (matching `self_id`) is badged "this instance".
const FLEET_DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>scry status</title>
<style>
  :root { color-scheme: dark; }
  body { font: 14px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace;
         margin: 0; padding: 1.5rem; background: #0d1117; color: #c9d1d9; }
  h1 { font-size: 1.1rem; margin: 0 0 .25rem; color: #58a6ff; }
  #meta { color: #6e7681; margin-bottom: 1.25rem; }
  h2 { font-size: .95rem; color: #8b949e; margin: 1.5rem 0 .5rem;
       border-bottom: 1px solid #21262d; padding-bottom: .25rem; }
  .cards { display: flex; flex-wrap: wrap; gap: 1rem; }
  .card { background: #161b22; border: 1px solid #21262d; border-radius: 8px;
          padding: .9rem 1.1rem; min-width: 300px; flex: 1 1 300px; max-width: 460px; }
  .card.self { border-color: #1f6feb; box-shadow: 0 0 0 1px #1f6feb inset; }
  .card h3 { margin: 0 0 .1rem; font-size: .95rem; color: #c9d1d9; }
  .badge { font-size: .7rem; font-weight: 600; padding: .05rem .4rem; border-radius: 4px;
           background: #1f6feb; color: #fff; margin-left: .4rem; vertical-align: middle; }
  .addr { color: #6e7681; font-size: .8rem; margin-bottom: .5rem; }
  table { border-collapse: collapse; width: 100%; }
  th, td { text-align: left; padding: .1rem .5rem .1rem 0; }
  th { color: #8b949e; font-weight: 500; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .banner { padding: .35rem .6rem; border-radius: 5px; margin: .4rem 0 .6rem;
            font-size: .8rem; border: 1px solid transparent; }
  .ok       { background: #0f2a16; border-color: #238636; color: #7ee787; }
  .info     { background: #0d2233; border-color: #1f6feb; color: #79c0ff; }
  .warn     { background: #332701; border-color: #9e6a03; color: #e3b341; }
  .critical { background: #3a0d0d; border-color: #da3633; color: #ff7b72; }
  .empty { color: #6e7681; font-style: italic; }
</style>
</head>
<body>
<h1>scry &mdash; fleet status</h1>
<div id="meta">connecting&hellip;</div>

<h2>agent</h2>
<div class="cards" id="agent"><span class="empty">none</span></div>

<h2>ingest</h2>
<div class="cards" id="ingest"><span class="empty">none</span></div>

<h2>query</h2>
<div class="cards" id="query"><span class="empty">none</span></div>

<script>
const prev = new Map();      // instance_id -> last DISTINCT snapshot (rate baseline)
const cardCache = new Map(); // instance_id -> last rendered card HTML (held across duplicate polls)
const $ = id => document.getElementById(id);
const fmt = n => (n == null ? "n/a" : Number(n).toLocaleString("en-US", {maximumFractionDigits: 2}));
const mib = b => (b == null ? "n/a" : (b / 1048576).toFixed(1) + " MiB");
const mibps = b => (b == null ? "n/a" : (b / 1048576).toFixed(2) + " MiB/s");
const pct = (h, m) => { const t = (h||0) + (m||0); return t === 0 ? "—" : ((100*h/t).toFixed(0) + "% (" + fmt(t) + ")"); };

function rateFn(s, p) {
  const dt = (p && s.now_unix_ms > p.now_unix_ms) ? (s.now_unix_ms - p.now_unix_ms) / 1000 : 0;
  return (curPath, prevPath) => {
    if (!p || dt <= 0) return null;
    return (curPath - prevPath) / dt;
  };
}

function tbl(pairs) {
  return "<table>" + pairs.map(([k, v]) =>
    `<tr><th>${k}</th><td class="num">${v}</td></tr>`).join("") + "</table>";
}

function agentCard(s, p, isSelf) {
  const d = s.data, pd = p ? p.data : null;
  const r = rateFn(s, p);
  const rate = (k) => (pd ? r(d[k], pd[k]) : null);
  const body = tbl([
    ["metric samples/s", fmt(rate("metric_samples"))],
    ["log records/s",    fmt(rate("log_records"))],
    ["metric queue",     fmt(d.metric_queue_depth) + " / " + fmt(d.metric_queue_capacity)],
    ["log queue",        fmt(d.log_queue_depth) + " / " + fmt(d.log_queue_capacity)],
    ["pending samples",  fmt(d.metric_pending_samples)],
    ["pending logs",     fmt(d.log_pending_records)],
    ["reconnects",       fmt(d.reconnect_successes) + " / " + fmt(d.reconnect_attempts)],
    ["filtered metrics", fmt(d.metric_dropped)],
    ["filtered logs",    fmt(d.log_dropped)],
  ]);
  return card(s, isSelf, body);
}

function ingestCard(s, p, isSelf) {
  const d = s.data, pd = p ? p.data : null;
  const r = rateFn(s, p);
  const b = d.bottleneck || {severity: "info", message: "", status: ""};
  const rate = (k) => (pd ? r(d[k], pd[k]) : null);
  const body =
    `<div class="banner ${b.severity}">${b.message}</div>` +
    tbl([
      ["metric samples/s", fmt(rate("metric_samples"))],
      ["log entries/s",    fmt(rate("log_entries"))],
      ["spans/s",          fmt(rate("spans"))],
      ["batches/s",        fmt(rate("batches"))],
      ["ingest in",        mibps(rate("bytes_in"))],
      ["active conns",     fmt(d.active_connections)],
      ["rejected",         fmt(d.rejected)],
      ["max inflight up.", fmt(d.max_inflight_uploads)],
      ["compact grace",    d.compaction?.enabled ? fmt(d.compaction.grace_secs) + "s" : "disabled"],
      ["compact passes",   fmt(d.compaction?.passes)],
      ["pass failures",    fmt(d.compaction?.pass_failed)],
      ["compact merges",   fmt(d.compaction?.merges)],
      ["blocks compacted", fmt(d.compaction?.blocks_in)],
      ["compact output",   mib(d.compaction?.bytes_out)],
      ["inputs reaped",    fmt(d.compaction?.reaped)],
      ["reap failures",    fmt(d.compaction?.reap_failed)],
      ["partition errors", fmt(d.compaction?.partition_failed)],
      ["last compact pass", d.compaction?.last_pass_unix_ms ? new Date(d.compaction.last_pass_unix_ms).toLocaleTimeString() + " (" + fmt(d.compaction.last_pass_duration_ms) + " ms)" : "—"],
    ]);
  return card(s, isSelf, body);
}

function queryCard(s, p, isSelf) {
  const d = s.data, pd = p ? p.data : null;
  const r = rateFn(s, p);
  const rate = (k) => (pd ? r(d[k], pd[k]) : null);
  const vk = d.valkey_connected;
  const vkTxt = vk == null ? "—" : (vk ? "connected" : "down");
  const pc = d.postings_cache || {}, bc = d.bloom_cache || {}, rc = d.result_cache || {};
  const lat = d.query_latency || {}, ranges = d.query_ranges || {};
  const admission = d.admission || {}, recovery = d.recovery || {};
  const body = tbl([
    ["queries/s",        fmt(rate("queries_total"))],
    ["in-flight",        fmt(d.queries_in_flight)],
    ["queries (total)",  fmt(d.queries_total)],
    ["errors (total)",   fmt(d.query_errors_total)],
    ["avg latency",      fmt(d.avg_query_ms) + " ms"],
    ["p95 latency ≤",    fmt(lat.p95_ms_upper) + " ms"],
    ["avg query range",  ranges.average_seconds == null ? "—" : fmt(ranges.average_seconds / 3600) + " h"],
    ["max query range",  ranges.max_seconds == null ? "—" : fmt(ranges.max_seconds / 3600) + " h"],
    ["rows scanned/s",   fmt(rate("rows_returned_total"))],
    ["bytes scanned/s",  mibps(rate("bytes_scanned_total"))],
    ["postings hit",     pct(pc.hits, pc.misses)],
    ["bloom hit",        pct(bc.hits, bc.misses)],
    ["result hit",       pct(rc.hits, rc.misses)],
    ["mem reserved",     mib(d.memory_reserved_bytes)],
    ["observed mem high",mib(d.memory_observed_peak_reserved_bytes)],
    ["admission waiting",fmt(admission.waiting)],
    ["admission rejected",fmt((admission.timeouts_total || 0) + (admission.rejected_total || 0))],
    ["response resets",  fmt(recovery.response_resets_total)],
    ["repair failures",  fmt(recovery.repair_failures_total)],
    ["catalog blocks",   fmt(d.catalog_blocks)],
    ["catalog rows",     fmt(d.catalog_rows)],
    ["lineage claims",   fmt(d.catalog_lineage_rows)],
    ["valkey",           vkTxt],
  ]);
  return card(s, isSelf, body);
}

function card(s, isSelf, body) {
  const shortId = (s.instance_id || "?").slice(0, 8);
  const badge = isSelf ? '<span class="badge">this instance</span>' : "";
  const up = s.uptime_secs == null ? "" : " · up " + Math.round(s.uptime_secs) + "s";
  const rss = s.rss_kib == null ? "" : " · " + (s.rss_kib / 1024).toFixed(0) + " MiB rss";
  return `<div class="card${isSelf ? ' self' : ''}">` +
    `<h3>${shortId}${badge}</h3>` +
    `<div class="addr">${s.addr || "?"}${up}${rss}</div>` +
    body + `</div>`;
}

function render(payload) {
  const insts = payload.instances || [];
  const selfId = payload.self_id;
  $("meta").textContent =
    `${insts.length} instance(s) · source=${payload.source} · ` +
    `self=${(selfId||"?").slice(0,8)} · ${new Date().toLocaleTimeString()}`;

  const groups = {agent: [], ingest: [], query: []};
  for (const s of insts) (groups[s.role] || (groups[s.role] = [])).push(s);

  for (const role of ["agent", "ingest", "query"]) {
    const el = $(role);
    const list = (groups[role] || []).sort((a, b) => a.instance_id.localeCompare(b.instance_id));
    if (list.length === 0) { el.innerHTML = '<span class="empty">none</span>'; continue; }
    el.innerHTML = list.map(s => {
      const p = prev.get(s.instance_id);
      const isSelf = s.instance_id === selfId;
      // A snapshot only advances once per heartbeat (~STATUS_TTL/3), which is
      // slower than our 1s poll — so the same blob is served for several polls.
      // Diff rates against the last *distinct* snapshot and hold the rendered
      // card across duplicate polls, so nothing flips to n/a between beats.
      const stale = p && s.now_unix_ms <= p.now_unix_ms;
      if (stale && cardCache.has(s.instance_id)) return cardCache.get(s.instance_id);
      const html = role === "agent" ? agentCard(s, p, isSelf) :
        (role === "ingest" ? ingestCard(s, p, isSelf) : queryCard(s, p, isSelf));
      cardCache.set(s.instance_id, html);
      prev.set(s.instance_id, s);   // advance the rate baseline only on a fresh snapshot
      return html;
    }).join("");
  }

  // Drop bookkeeping for instances that vanished from the fleet.
  const live = new Set(insts.map(s => s.instance_id));
  for (const k of [...prev.keys()]) if (!live.has(k)) prev.delete(k);
  for (const k of [...cardCache.keys()]) if (!live.has(k)) cardCache.delete(k);
}

async function poll() {
  try {
    const r = await fetch("/stats.json", {cache: "no-store"});
    render(await r.json());
  } catch (e) {
    $("meta").textContent = "status endpoint unreachable: " + e;
  }
}
poll();
setInterval(poll, 1000);
</script>
</body>
</html>
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tokio::io::AsyncReadExt;
    use tokio::sync::Notify;

    struct StubLocal(StatusSnapshot);
    impl LocalStatus for StubLocal {
        fn snapshot(&self) -> StatusSnapshot {
            self.0.clone()
        }
    }

    fn stub_snapshot(role: &str, id: &str) -> StatusSnapshot {
        StatusSnapshot {
            role: role.to_string(),
            instance_id: id.to_string(),
            addr: "127.0.0.1:1".to_string(),
            now_unix_ms: 1,
            uptime_secs: 1.0,
            rss_kib: Some(1),
            data: serde_json::json!({"ok": true}),
        }
    }

    struct StubFleet(Vec<String>);
    #[async_trait::async_trait]
    impl FleetSource for StubFleet {
        async fn blobs(&self) -> Vec<String> {
            self.0.clone()
        }
    }

    async fn request(addr: std::net::SocketAddr, line: &str) -> String {
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(format!("{line}\r\nHost: x\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut resp = String::new();
        sock.read_to_string(&mut resp).await.unwrap();
        resp
    }

    #[tokio::test]
    async fn http_routes_and_local_fallback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let shutdown = Arc::new(Notify::new());
        let shutdown_wait = shutdown.clone();
        let local: Arc<dyn LocalStatus> = Arc::new(StubLocal(stub_snapshot("query", "self-1")));
        let handle = tokio::spawn(async move {
            serve_status(
                addr.to_string(),
                local,
                None,
                "self-1".to_string(),
                async move { shutdown_wait.notified().await },
            )
            .await
            .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let index = request(addr, "GET / HTTP/1.1").await;
        assert!(index.starts_with("HTTP/1.1 200 OK"), "index: {index}");
        assert!(index.contains("fleet status"));

        let json = request(addr, "GET /stats.json HTTP/1.1").await;
        let body = json.split("\r\n\r\n").nth(1).unwrap();
        let v: Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["source"], serde_json::json!("local"));
        assert_eq!(v["self_id"], serde_json::json!("self-1"));
        assert_eq!(v["instances"].as_array().unwrap().len(), 1);
        assert_eq!(
            v["instances"][0]["instance_id"],
            serde_json::json!("self-1")
        );

        let missing = request(addr, "GET /nope HTTP/1.1").await;
        assert!(missing.starts_with("HTTP/1.1 404 Not Found"));

        shutdown.notify_waiters();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn fleet_merges_and_marks_self() {
        // Fleet has two peers (an ingest and a query). Self is the query one,
        // and it's already present in the blob set (no local push needed).
        let blobs = vec![
            serde_json::to_string(&stub_snapshot("ingest", "ingest-1")).unwrap(),
            serde_json::to_string(&stub_snapshot("query", "query-self")).unwrap(),
        ];
        let state = StatusState {
            local: Arc::new(StubLocal(stub_snapshot("query", "query-self"))),
            fleet: Some(Arc::new(StubFleet(blobs))),
            self_id: "query-self".to_string(),
        };
        let v: Value = serde_json::from_str(&state.stats_json().await).unwrap();
        assert_eq!(v["source"], serde_json::json!("valkey"));
        let insts = v["instances"].as_array().unwrap();
        // Both peers present, self not duplicated.
        assert_eq!(insts.len(), 2);
        assert_eq!(
            insts
                .iter()
                .filter(|s| s["instance_id"] == "query-self")
                .count(),
            1
        );
        // Both roles represented.
        assert!(insts.iter().any(|s| s["role"] == "ingest"));
        assert!(insts.iter().any(|s| s["role"] == "query"));
    }

    #[tokio::test]
    async fn fleet_injects_self_before_first_heartbeat() {
        // Valkey present but self's heartbeat hasn't landed yet — the local
        // snapshot is injected so the page never omits the reporting instance.
        let blobs = vec![serde_json::to_string(&stub_snapshot("ingest", "ingest-1")).unwrap()];
        let state = StatusState {
            local: Arc::new(StubLocal(stub_snapshot("query", "query-self"))),
            fleet: Some(Arc::new(StubFleet(blobs))),
            self_id: "query-self".to_string(),
        };
        let v: Value = serde_json::from_str(&state.stats_json().await).unwrap();
        let insts = v["instances"].as_array().unwrap();
        assert_eq!(insts.len(), 2);
        assert!(insts.iter().any(|s| s["instance_id"] == "query-self"));
    }

    #[test]
    fn snapshot_round_trips() {
        let s = stub_snapshot("ingest", "abc");
        let json = serde_json::to_string(&s).unwrap();
        let back: StatusSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.role, "ingest");
        assert_eq!(back.instance_id, "abc");
        assert_eq!(back.data, serde_json::json!({"ok": true}));
    }

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
