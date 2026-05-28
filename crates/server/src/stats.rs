//! Live operator stats: a tiny hand-rolled HTTP/1.1 server plus the
//! process-global ingest metrics it serves.
//!
//! Two halves:
//!
//! 1. **Signal-agnostic HTTP plumbing** — [`serve_stats`] + the
//!    [`StatsProvider`] trait. A minimal HTTP/1.1 responder over a
//!    `tokio` `TcpListener` with exactly two GET routes: `/` (an HTML
//!    page) and `/stats.json` (a JSON snapshot). `Connection: close`
//!    per request, so there's no keep-alive state machine — every XHR
//!    poll is a fresh, cheap connection. This half knows nothing about
//!    ingest; `scry-queryd` can implement [`StatsProvider`] later and
//!    reuse it verbatim.
//!
//! 2. **The ingest provider** — [`ServerMetrics`] (+ per-signal
//!    [`UploadStats`]). Process-global atomics bumped at *batch*
//!    granularity by the ingest path (`server.rs`) and at
//!    *block-rotation* granularity by the upload pipeline
//!    (`pipeline.rs`). It implements [`StatsProvider`] to render both
//!    the JSON snapshot and the live HTML dashboard.
//!
//! The headline feature is the `uploads_queued` gauge: under sustained
//! ingest above bucket bandwidth, finished block builders pile up
//! waiting for an upload permit, and this counter is exactly the depth
//! of that in-memory pile. The dashboard turns its status banner
//! yellow/red when it climbs, so "we're bounded by S3 push speed"
//! becomes a visible warning rather than something reconstructed from
//! logs.

use std::borrow::Cow;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use scry_proto::constants::Signal;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

// ───────────────────────────── HTTP plumbing ──────────────────────────────

/// Supplies the bodies for the two stats routes. Each binary that wants
/// a stats endpoint implements this (ingest does so via [`ServerMetrics`]
/// below; the query daemon will get its own impl later).
pub trait StatsProvider: Send + Sync + 'static {
    /// Body for `GET /stats.json`. Called once per poll; should be
    /// cheap (read a handful of atomics + serialise).
    fn stats_json(&self) -> String;

    /// Body for `GET /`. Typically a `'static` HTML string with inline
    /// JS that polls `/stats.json`.
    fn index_html(&self) -> Cow<'static, str>;
}

/// Bind `listen_addr` and serve stats until `shutdown` resolves.
///
/// Errors only on the initial bind; per-connection failures are logged
/// and dropped (a broken stats poll must never take down ingest).
pub async fn serve_stats<P, F>(listen_addr: String, provider: Arc<P>, shutdown: F) -> Result<()>
where
    P: StatsProvider,
    F: Future<Output = ()>,
{
    let listener = TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("binding stats endpoint {listen_addr}"))?;
    info!(addr = %listen_addr, "stats HTTP endpoint listening (GET / and /stats.json)");

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((sock, _peer)) => {
                        let provider = provider.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_http(sock, provider).await {
                                tracing::debug!(error = %e, "stats connection ended with error");
                            }
                        });
                    }
                    Err(e) => warn!(error = %e, "stats accept failed"),
                }
            }
            _ = &mut shutdown => {
                info!("stats endpoint shutting down");
                break;
            }
        }
    }
    Ok(())
}

/// Read one request, route it, write the response, close. We only care
/// about the request line (`GET <path> HTTP/1.1`); headers and body are
/// ignored for GET. Request reads are capped so a hostile or broken
/// client can't make us buffer unbounded bytes.
async fn handle_http<P: StatsProvider>(mut sock: TcpStream, provider: Arc<P>) -> Result<()> {
    const MAX_REQUEST_BYTES: usize = 8 * 1024;
    let mut buf = Vec::with_capacity(256);
    let mut tmp = [0u8; 1024];
    // Read until we've seen the end of the request headers (\r\n\r\n) or
    // hit the cap. GET requests have no body we consume.
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
            provider.index_html(),
        ),
        (Some("GET"), Some("/stats.json")) => (
            "200 OK",
            "application/json",
            Cow::Owned(provider.stats_json()),
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
    // Best-effort half-close; ignore errors (peer may have already gone).
    let _ = sock.shutdown().await;
    Ok(())
}

/// Extract `(method, path)` from the first line of an HTTP request.
/// Returns `None`s if the buffer doesn't contain a parseable line.
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
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

/// Current process resident set size in KiB, read from
/// `/proc/self/status` (`VmRSS:`). `None` on non-Linux or if the file
/// can't be read/parsed — the dashboard shows "n/a" in that case.
pub fn rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.split_whitespace().next().and_then(|v| v.parse().ok());
        }
    }
    None
}

// ─────────────────────────── ingest metrics ───────────────────────────────

/// Per-signal upload pipeline gauges. Shared (`Arc`) between
/// [`ServerMetrics`] and the signal's [`crate::Pipeline`], which bumps
/// them from `spawn_upload` / `run_upload`.
#[derive(Default, Debug)]
pub struct UploadStats {
    /// Uploads holding a semaphore permit and actively encoding +
    /// PUTting. Bounded by the shared upload-concurrency cap.
    uploads_inflight: AtomicU64,
    /// Ingest paths *currently blocked* waiting for an upload permit.
    /// Because the upload permit is now acquired inside `spawn_upload`
    /// while the pipeline mutex is held (see `pipeline.rs`), a non-zero
    /// value means ingest for this signal is being throttled to bucket
    /// write speed — the live, memory-safe "bounded by S3" signal.
    /// Bounded by the number of signals (the per-signal mutex serialises
    /// ingest), so it's effectively 0 or 1 per signal.
    upload_waiters: AtomicU64,
    /// Σ wall-clock nanos ingest has spent blocked waiting for an upload
    /// permit since process start — the cumulative cost of being
    /// upload-bound.
    upload_stall_nanos_total: AtomicU64,
    /// Blocks successfully uploaded since process start.
    blocks_uploaded: AtomicU64,
    /// Total uploaded parquet bytes (block `byte_size` summed).
    bytes_uploaded: AtomicU64,
    /// Uploads that failed (block left in WAL for replay).
    upload_failures: AtomicU64,
    /// Σ wall-clock nanos spent inside `finish_and_upload` — divide
    /// `bytes_uploaded` by this (in seconds) for effective bandwidth.
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

    /// Record a successful upload of `bytes` parquet bytes taking
    /// `nanos` wall-clock.
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

/// Process-global ingest metrics. Counters that track inbound records
/// are bumped once per *batch* (the same cadence as the per-connection
/// `Counters` in `server.rs`), so exposing them adds no per-record
/// hot-path cost. Construct one with [`ServerMetrics::new`], share it
/// via `Arc`.
pub struct ServerMetrics {
    started: Instant,
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
    /// The shared upload-concurrency cap (physical core count in
    /// production). Reported as `max_inflight_uploads` and used by the
    /// classifier to decide when the *pool* is saturated.
    upload_concurrency: u64,
    metrics_upload: Arc<UploadStats>,
    logs_upload: Arc<UploadStats>,
    dummy_upload: Arc<UploadStats>,
}

impl ServerMetrics {
    /// `upload_concurrency` is the shared cap on concurrent block
    /// encode+upload tasks across all signals (see
    /// `Pipeline::with_upload_sem`). Sized to the host's physical core
    /// count in production.
    pub fn new(upload_concurrency: usize) -> Self {
        Self {
            started: Instant::now(),
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
            dummy_upload: Arc::new(UploadStats::default()),
        }
    }

    /// The per-signal upload gauges, for handing to the matching
    /// `Pipeline` via `Pipeline::with_upload_stats`.
    pub fn metrics_upload(&self) -> Arc<UploadStats> {
        self.metrics_upload.clone()
    }
    pub fn logs_upload(&self) -> Arc<UploadStats> {
        self.logs_upload.clone()
    }
    pub fn dummy_upload(&self) -> Arc<UploadStats> {
        self.dummy_upload.clone()
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
            &self.dummy_upload,
        ];
        let total_waiters: u64 = uploads
            .iter()
            .map(|u| u.upload_waiters.load(Ordering::Relaxed))
            .sum();
        // The pool is shared across signals, so saturation is a
        // pool-wide condition: total in-flight uploads have filled every
        // permit.
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

    fn snapshot(&self) -> serde_json::Value {
        let now_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let (status, severity, message) = self.bottleneck();
        serde_json::json!({
            "now_unix_ms": now_unix_ms,
            "uptime_secs": self.started.elapsed().as_secs_f64(),
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
            "rss_kib": rss_kib(),
            "uploads": {
                "metrics": self.metrics_upload.snapshot(),
                "logs": self.logs_upload.snapshot(),
                "dummy": self.dummy_upload.snapshot(),
            },
            "bottleneck": {
                "status": status,
                "severity": severity,
                "message": message,
            },
        })
    }
}


impl StatsProvider for ServerMetrics {
    fn stats_json(&self) -> String {
        self.snapshot().to_string()
    }
    fn index_html(&self) -> Cow<'static, str> {
        Cow::Borrowed(INGEST_DASHBOARD_HTML)
    }
}

/// The ingest dashboard: a single self-contained page (no framework, no
/// build step) that polls `/stats.json` once a second, diffs successive
/// snapshots into rec/s + MiB/s, and renders a status banner that turns
/// yellow/red when the pipeline goes upload-bound.
const INGEST_DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>scry ingest</title>
<style>
  :root { color-scheme: dark; }
  body { font: 14px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace;
         margin: 0; padding: 1.5rem; background: #0d1117; color: #c9d1d9; }
  h1 { font-size: 1.1rem; margin: 0 0 1rem; color: #58a6ff; }
  #banner { padding: .75rem 1rem; border-radius: 6px; margin-bottom: 1.25rem;
            font-weight: 600; border: 1px solid transparent; }
  .ok       { background: #0f2a16; border-color: #238636; color: #7ee787; }
  .info     { background: #0d2233; border-color: #1f6feb; color: #79c0ff; }
  .warn     { background: #332701; border-color: #9e6a03; color: #e3b341; }
  .critical { background: #3a0d0d; border-color: #da3633; color: #ff7b72; }
  table { border-collapse: collapse; margin-bottom: 1.5rem; min-width: 320px; }
  th, td { text-align: left; padding: .25rem 1.25rem .25rem 0; }
  th { color: #8b949e; font-weight: 500; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  h2 { font-size: .95rem; color: #8b949e; margin: 1.25rem 0 .5rem; }
  .muted { color: #6e7681; }
  .grid { display: flex; flex-wrap: wrap; gap: 2.5rem; }
</style>
</head>
<body>
<h1>scry ingest &mdash; live stats</h1>
<div id="banner" class="info">connecting&hellip;</div>

<div class="grid">
  <div>
    <h2>throughput</h2>
    <table id="tp"></table>
  </div>
  <div>
    <h2>process</h2>
    <table id="proc"></table>
  </div>
</div>

<h2>per-signal upload pipeline</h2>
<table id="up"></table>

<p class="muted" id="foot"></p>

<script>
let prev = null;
const $ = id => document.getElementById(id);
const fmt = n => (n == null ? "n/a" : n.toLocaleString("en-US", {maximumFractionDigits: 2}));
const mib = b => (b == null ? "n/a" : (b / 1048576).toFixed(2) + " MiB");
const mibps = b => (b == null ? "n/a" : (b / 1048576).toFixed(2) + " MiB/s");

function rows(tbl, pairs) {
  tbl.innerHTML = pairs.map(([k, v]) =>
    `<tr><th>${k}</th><td class="num">${v}</td></tr>`).join("");
}

function render(s) {
  let dt = 0;
  if (prev) dt = (s.now_unix_ms - prev.now_unix_ms) / 1000;
  const rate = (cur, key) => (prev && dt > 0) ? (cur - prev[key]) / dt : null;

  const b = s.bottleneck;
  const banner = $("banner");
  banner.className = b.severity;
  banner.textContent = b.message;

  rows($("tp"), [
    ["metric samples/s", fmt(rate(s.metric_samples, "metric_samples"))],
    ["log entries/s",    fmt(rate(s.log_entries, "log_entries"))],
    ["batches/s",        fmt(rate(s.batches, "batches"))],
    ["ingest in",        mibps(rate(s.bytes_in, "bytes_in"))],
    ["ingest out (decoded)", mibps(rate(s.bytes_out, "bytes_out"))],
    ["rejected (total)", fmt(s.rejected)],
  ]);

  rows($("proc"), [
    ["RSS",               s.rss_kib == null ? "n/a" : (s.rss_kib / 1024).toFixed(1) + " MiB"],
    ["active conns",      fmt(s.active_connections)],
    ["total conns",       fmt(s.total_connections)],
    ["uptime",            fmt(s.uptime_secs) + " s"],
    ["max inflight up.",  fmt(s.max_inflight_uploads)],
    ["samples (total)",   fmt(s.metric_samples)],
    ["log entries (total)", fmt(s.log_entries)],
  ]);

  const up = $("up");
  const sigs = ["metrics", "logs", "dummy"];
  let html = "<tr><th>signal</th><th class='num'>waiting</th><th class='num'>inflight</th>" +
             "<th class='num'>blocks</th><th class='num'>uploaded</th>" +
             "<th class='num'>eff. bw</th><th class='num'>stalled</th>" +
             "<th class='num'>failures</th></tr>";
  for (const sig of sigs) {
    const u = s.uploads[sig];
    const w = u.upload_waiters;
    const cls = w > 0 ? "warn" : "";
    html += `<tr><th>${sig}</th>` +
      `<td class="num ${cls}">${fmt(w)}</td>` +
      `<td class="num">${fmt(u.uploads_inflight)}</td>` +
      `<td class="num">${fmt(u.blocks_uploaded)}</td>` +
      `<td class="num">${mib(u.bytes_uploaded)}</td>` +
      `<td class="num">${mibps(u.effective_upload_bytes_per_sec)}</td>` +
      `<td class="num">${fmt(u.upload_stall_seconds_total)} s</td>` +
      `<td class="num">${fmt(u.upload_failures)}</td></tr>`;
  }
  up.innerHTML = html;

  $("foot").textContent = "polled " + new Date(s.now_unix_ms).toLocaleTimeString() +
    " · status=" + b.status;
  prev = s;
}

async function poll() {
  try {
    const r = await fetch("/stats.json", {cache: "no-store"});
    render(await r.json());
  } catch (e) {
    const banner = $("banner");
    banner.className = "critical";
    banner.textContent = "stats endpoint unreachable: " + e;
    prev = null;
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

    struct StubProvider;
    impl StatsProvider for StubProvider {
        fn stats_json(&self) -> String {
            r#"{"ok":true}"#.to_string()
        }
        fn index_html(&self) -> Cow<'static, str> {
            Cow::Borrowed("<html>hi</html>")
        }
    }

    /// Send a raw request line over a fresh connection and return the
    /// full response text.
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
    async fn http_routes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // free the port; serve_stats rebinds it

        let shutdown = Arc::new(Notify::new());
        let shutdown_wait = shutdown.clone();
        let handle = tokio::spawn(async move {
            serve_stats(
                addr.to_string(),
                Arc::new(StubProvider),
                async move { shutdown_wait.notified().await },
            )
            .await
            .unwrap();
        });
        // Give the server a moment to bind.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let index = request(addr, "GET / HTTP/1.1").await;
        assert!(index.starts_with("HTTP/1.1 200 OK"), "index: {index}");
        assert!(index.contains("text/html"));
        assert!(index.contains("<html>hi</html>"));

        let json = request(addr, "GET /stats.json HTTP/1.1").await;
        assert!(json.starts_with("HTTP/1.1 200 OK"), "json: {json}");
        assert!(json.contains("application/json"));
        let body = json.split("\r\n\r\n").nth(1).unwrap();
        let v: Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["ok"], serde_json::json!(true));

        let missing = request(addr, "GET /nope HTTP/1.1").await;
        assert!(missing.starts_with("HTTP/1.1 404 Not Found"), "404: {missing}");

        let method = request(addr, "POST / HTTP/1.1").await;
        assert!(
            method.starts_with("HTTP/1.1 405 Method Not Allowed"),
            "405: {method}"
        );

        shutdown.notify_waiters();
        handle.await.unwrap();
    }

    #[test]
    fn classifier_states() {
        const CAP: usize = 4;
        let m = ServerMetrics::new(CAP);

        // Nothing in flight → healthy.
        let (status, severity, _) = m.bottleneck();
        assert_eq!((status, severity), ("healthy", "ok"));

        // Inflight below the pool cap, no ingest blocked → still healthy.
        m.metrics_upload
            .uploads_inflight
            .store(CAP as u64 - 1, Ordering::Relaxed);
        let (status, severity, _) = m.bottleneck();
        assert_eq!((status, severity), ("healthy", "ok"));

        // Inflight summed across signals fills the pool, no ingest
        // blocked → saturated but keeping pace.
        m.dummy_upload.uploads_inflight.store(1, Ordering::Relaxed);
        let (status, severity, _) = m.bottleneck();
        assert_eq!((status, severity), ("upload_saturated", "info"));

        // An ingest path blocked waiting for a slot → upload_bound / warn.
        m.metrics_upload.upload_waiters.store(1, Ordering::Relaxed);
        let (status, severity, _) = m.bottleneck();
        assert_eq!((status, severity), ("upload_bound", "warn"));
    }

    #[test]
    fn stats_json_is_valid_and_complete() {
        const CAP: usize = 8;
        let m = ServerMetrics::new(CAP);
        m.add_batch(100);
        m.add_bytes_out(400);
        m.add_records(Signal::Metrics, 50);
        m.add_records(Signal::Logs, 7);
        m.conn_open();

        let v: Value = serde_json::from_str(&m.stats_json()).unwrap();
        assert_eq!(v["metric_samples"], serde_json::json!(50));
        assert_eq!(v["log_entries"], serde_json::json!(7));
        assert_eq!(v["batches"], serde_json::json!(1));
        assert_eq!(v["bytes_in"], serde_json::json!(100));
        assert_eq!(v["active_connections"], serde_json::json!(1));
        assert_eq!(v["max_inflight_uploads"], serde_json::json!(CAP));
        assert!(v["uploads"]["metrics"].is_object());
        assert!(v["bottleneck"]["status"].is_string());
        assert!(v.get("now_unix_ms").is_some());
    }

    #[test]
    fn upload_stats_transitions() {
        let u = UploadStats::default();
        // Ingest blocks waiting for a slot, then gets one.
        u.begin_wait();
        assert_eq!(u.upload_waiters.load(Ordering::Relaxed), 1);
        u.end_wait(500_000_000); // stalled 0.5 s
        assert_eq!(u.upload_waiters.load(Ordering::Relaxed), 0);
        // Upload runs to completion.
        u.start_inflight();
        assert_eq!(u.uploads_inflight.load(Ordering::Relaxed), 1);
        u.record_success(1024, 1_000_000_000);
        u.finish_inflight();
        assert_eq!(u.uploads_inflight.load(Ordering::Relaxed), 0);
        let snap = u.snapshot();
        assert_eq!(snap["blocks_uploaded"], serde_json::json!(1));
        assert_eq!(snap["bytes_uploaded"], serde_json::json!(1024));
        // 1024 bytes in 1.0 s → 1024 B/s.
        assert_eq!(snap["effective_upload_bytes_per_sec"], serde_json::json!(1024.0));
        assert_eq!(snap["upload_stall_seconds_total"], serde_json::json!(0.5));
    }
}
