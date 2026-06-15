//! scry-agent — Kubernetes log-collection agent.
//!
//! Discovers pods on this node via the Kubernetes API, tails their CRI
//! container logs, and ships them as `Signal::Logs` batches over the native
//! binschema wire to a scry ingest server. Logs only, ingest only — the
//! first dogfood signal.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use scry_proto::{
    build,
    constants::{Signal, COMPRESSION_ZSTD, SIGNAL_BIT_LOGS, SIGNAL_BIT_METRICS},
    generated::{LogEntry, LogStream, LogsBatch, MetricSample, MetricsBatch, SeriesDictEntry},
    LabelPair,
};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use uuid::Uuid;

mod config;
mod cri;
mod discovery;
mod filter;
mod promparse;
mod scrape;
mod stream;

use cri::RawLog;
use scry_client::Client;

const ZSTD_LEVEL: i32 = 3;

/// CLI arguments for the `scry agent` subcommand.
#[derive(Parser, Debug)]
#[command(about = "Kubernetes log-collection + Prometheus-scrape agent")]
pub struct Args {
    /// Ingest server address (host:port).
    #[arg(long, env = "SCRY_SERVER_ADDR", default_value = "127.0.0.1:4000")]
    server_addr: String,

    /// This node's name (used as the `node` stream label and the pod-watch
    /// field selector). In-cluster, wire it from the downward API.
    #[arg(long, env = "NODE_NAME")]
    node_name: Option<String>,

    /// This node's IP, used only to interpolate `${NODE_IP}` in the kubelet
    /// scrape address. In-cluster, wire it from the downward API
    /// (`fieldRef: status.hostIP`).
    #[arg(long, env = "NODE_IP")]
    node_ip: Option<String>,

    /// Root of the CRI pod-log tree.
    #[arg(long, default_value = "/var/log/pods")]
    logs_root: PathBuf,

    /// Disable the Kubernetes pod watch (ship with path-derived labels only).
    /// Useful for local testing against a fake log tree.
    #[arg(long)]
    no_discovery: bool,

    /// Replay existing log files from the start instead of beginning at EOF.
    /// Off in production (we don't want history on every restart); handy for
    /// pointing at a static fixture tree.
    #[arg(long)]
    from_start: bool,

    /// Flush a batch once it reaches this many entries.
    #[arg(long, default_value_t = 5000)]
    batch_max_lines: u32,

    /// Flush a batch once its uncompressed payload estimate reaches this many bytes.
    #[arg(long, default_value_t = 1024 * 1024)]
    batch_max_bytes: usize,

    /// Maximum time a partial batch waits before being flushed.
    #[arg(long, value_parser = parse_duration, default_value = "5s")]
    flush_interval: Duration,

    /// How often to poll each tailed file for new bytes.
    #[arg(long, value_parser = parse_duration, default_value = "1s")]
    poll_interval: Duration,

    /// How often to rescan `logs_root` for new/removed container log files.
    #[arg(long, value_parser = parse_duration, default_value = "5s")]
    scan_interval: Duration,

    /// Path to the TOML processing-pipeline config (usually a k8s
    /// ConfigMap-mounted file). Read once at startup; restart the DaemonSet to
    /// apply a change. Owns the per-signal `keep`, label maps, static labels,
    /// JSON extraction, and metric relabeling. When set, the global `--keep`
    /// flag is rejected (move it into the file's `[logs]`/`[metrics] keep`).
    #[arg(long, env = "SCRY_AGENT_CONFIG")]
    config: Option<PathBuf>,

    /// Keep-only label allow-list (repeatable). A log stream is shipped only if
    /// it matches ALL `--keep` matchers; everything else is dropped at the node
    /// before it goes on the wire. Omit to ship everything (the default).
    ///
    /// Each matcher is `key=value` | `key!=value` | `key=~regex` | `key!~regex`
    /// (regex is whole-string-anchored; values may be double-quoted). Matches
    /// against stream labels: `namespace`, `pod`, `container`, `node`, and pod
    /// labels exposed as `k8s_<key>` — e.g. `--keep 'namespace=~"prod-.*"'
    /// --keep k8s_app=api`. An absent label is treated as empty.
    ///
    /// The same allow-list is applied to scraped metric series (matched against
    /// their full label set, including `job`/`instance`/`__name__`).
    #[arg(long = "keep")]
    keep: Vec<String>,

    /// Static Prometheus scrape target URL (repeatable), e.g.
    /// `--scrape-target http://127.0.0.1:9100/metrics`. Scraped in addition to
    /// any pods discovered via `prometheus.io/scrape` annotations.
    #[arg(long = "scrape-target")]
    scrape_target: Vec<String>,

    /// Bearer token presented to every static scrape target. Prefer
    /// `@/path/to/file` (read from a file) over passing the secret on argv.
    #[arg(long)]
    scrape_bearer: Option<String>,

    /// `job` label for static scrape targets (discovered targets derive `job`
    /// from pod labels).
    #[arg(long, default_value = "scrape")]
    scrape_default_job: String,

    /// How often to scrape each target.
    #[arg(long, value_parser = parse_duration, default_value = "15s")]
    scrape_interval: Duration,

    /// Per-scrape HTTP timeout.
    #[arg(long, value_parser = parse_duration, default_value = "10s")]
    scrape_timeout: Duration,
}

/// Run the log-collection / scrape agent until ctrl-c.
pub async fn run(args: Args) -> Result<()> {
    // Config owns the processing pipeline; flags own runtime. Without --config,
    // the global --keep flag is synthesized into a degenerate per-signal config.
    let (log_pipeline, metric_pipeline) = config::resolve(args.config.as_deref(), &args.keep)?;
    if let Some(path) = &args.config {
        info!(config = %path.display(), "loaded agent pipeline config");
    }
    if !log_pipeline.keep.is_empty() {
        info!(
            matchers = log_pipeline.keep.len(),
            "logs keep-only allow-list active"
        );
    }
    if !metric_pipeline.keep.is_empty() {
        info!(
            matchers = metric_pipeline.keep.len(),
            "metrics keep-only allow-list active"
        );
    }

    let hostname = hostname_string();
    let node = args.node_name.clone().unwrap_or_else(|| hostname.clone());
    let agent_id = *Uuid::now_v7().as_bytes();

    // ── Shared state + shutdown ────────────────────────────────────────
    let registry = discovery::new_registry();
    let target_registry = discovery::new_target_registry();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (log_tx, mut log_rx) = mpsc::channel::<RawLog>(8192);
    let (metrics_tx, mut metrics_rx) = mpsc::channel::<scrape::ScrapeResult>(1024);

    // Config-driven pod-label SD jobs + kubelet scraping (shared by the
    // enable-decision, the pod watch, and the scheduler).
    let scrape_pods = Arc::new(metric_pipeline.scrape_pods.clone());
    let kubelet_cfg = metric_pipeline.kubelet.clone();
    let kubelet_enabled = kubelet_cfg.as_ref().is_some_and(|k| k.enabled);

    // Metrics scraping is active when there are static targets, kubelet scraping
    // is on, a pod-SD job is configured, or the pod watch is on (so
    // `prometheus.io/scrape` annotations can be honored).
    let mut metrics_enabled = !args.no_discovery
        || !args.scrape_target.is_empty()
        || kubelet_enabled
        || !scrape_pods.is_empty();

    // Pod-label SD needs the watch; warn if it's configured but discovery is off.
    if args.no_discovery && !scrape_pods.is_empty() {
        warn!(
            jobs = scrape_pods.len(),
            "[[metrics.scrape_pods]] configured but --no-discovery is set; pod-label SD is inert"
        );
    }

    // ── Discovery: pod watch (optional) + filesystem scan ──────────────
    let watcher_handle = if args.no_discovery {
        info!("pod discovery disabled; using path-derived labels only");
        None
    } else {
        Some(discovery::spawn_pod_watcher(
            node.clone(),
            registry.clone(),
            target_registry.clone(),
            scrape_pods.clone(),
            shutdown_rx.clone(),
        ))
    };

    let scanner_handle = discovery::spawn_log_scanner(
        args.logs_root.clone(),
        args.from_start,
        args.poll_interval,
        args.scan_interval,
        log_tx.clone(),
        shutdown_rx.clone(),
    );
    drop(log_tx); // only the scanner's tailers should keep the channel alive

    // ── Metrics: scrape scheduler (optional) ───────────────────────────
    let scheduler_handle = if metrics_enabled {
        let pool = Arc::new(scrape::ClientPool::new(args.scrape_timeout));
        let bearer = resolve_bearer(args.scrape_bearer.clone())?;
        let mut static_targets =
            build_static_targets(&args.scrape_target, &args.scrape_default_job, &node, bearer);
        if let Some(kubelet) = &kubelet_cfg {
            if kubelet.enabled {
                let kubelet_targets =
                    build_kubelet_targets(kubelet, &node, args.node_ip.as_deref())?;
                info!(
                    targets = kubelet_targets.len(),
                    address = %kubelet.address,
                    "kubelet scraping enabled"
                );
                static_targets.extend(kubelet_targets);
            }
        }
        info!(
            static_targets = static_targets.len(),
            discovery = !args.no_discovery,
            "metrics scraping enabled"
        );
        Some(discovery::spawn_scrape_scheduler(
            target_registry.clone(),
            static_targets,
            pool,
            args.scrape_interval,
            args.scan_interval,
            metrics_tx.clone(),
            shutdown_rx.clone(),
            metric_pipeline.static_labels.clone(),
            metric_pipeline.label_map.clone(),
        ))
    } else {
        None
    };
    drop(metrics_tx); // only the scheduler's scrape tasks keep the channel alive

    // ── Connect ────────────────────────────────────────────────────────
    let signals = if metrics_enabled {
        SIGNAL_BIT_LOGS | SIGNAL_BIT_METRICS
    } else {
        SIGNAL_BIT_LOGS
    };
    let mut conn = Client::connect(
        &args.server_addr,
        agent_id,
        &hostname,
        signals,
        vec![
            LabelPair {
                key: "service".into(),
                value: "scry-agent".into(),
            },
            LabelPair {
                key: "node".into(),
                value: node.clone(),
            },
        ],
    )
    .await?;

    // ── Signal → watch, observable from both the batcher loop and a flush
    // stuck mid-reconnect (so SIGTERM can't be swallowed by a backoff sleep).
    let (sig_tx, sig_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        info!("shutdown signal received");
        let _ = sig_tx.send(true);
    });
    let mut main_sig = sig_rx.clone();
    let mut flush_sig = sig_rx;

    // ── Batcher loop ───────────────────────────────────────────────────
    let mut pending = Pending::default();
    let mut metrics_pending = MetricsPending::default();
    // Per-fingerprint keep/drop decision, so the (possibly regex) allow-list
    // runs once per distinct stream rather than once per line. Lives across
    // flushes (Pending is drained each flush); a stream's labels — and thus its
    // fingerprint — are stable, so the cached decision stays valid. Logs and
    // metrics keep separate caches (distinct fingerprint spaces).
    let mut keep_cache: HashMap<u64, bool> = HashMap::new();
    let mut metrics_keep_cache: HashMap<u64, bool> = HashMap::new();
    // Enriched (fingerprint, labels) cached by base fingerprint, used only when
    // the log pipeline enriches but JSON does not add labels — then the enriched
    // label set is stable per stream, so we rebuild the BTreeMap once per stream
    // instead of once per line. When JSON adds labels the fingerprint varies per
    // line and this cache is bypassed.
    let mut enrich_cache: HashMap<u64, (u64, Vec<LabelPair>)> = HashMap::new();
    let mut dropped: u64 = 0;
    let mut metrics_dropped: u64 = 0;
    let mut batch_id: u64 = 0;
    let mut metrics_batch_id: u64 = 0;
    let mut flush_timer = tokio::time::interval(args.flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut shutting_down = false;
    while !shutting_down {
        tokio::select! {
            maybe = log_rx.recv() => {
                match maybe {
                    Some(rec) => {
                        ingest(&mut pending, &registry, &node, rec, &log_pipeline, &mut keep_cache, &mut enrich_cache, &mut dropped).await;
                        if pending.record_count >= args.batch_max_lines
                            || pending.approx_bytes >= args.batch_max_bytes
                        {
                            flush(&mut conn, &mut pending, &mut batch_id, &mut flush_sig).await?;
                        }
                    }
                    None => {
                        // All tailers gone (shouldn't happen until shutdown).
                        break;
                    }
                }
            }
            maybe = metrics_rx.recv(), if metrics_enabled => {
                match maybe {
                    Some(result) => {
                        ingest_metrics(&mut metrics_pending, result, &metric_pipeline.keep, &mut metrics_keep_cache, &mut metrics_dropped);
                        if metrics_pending.record_count >= args.batch_max_lines
                            || metrics_pending.approx_bytes >= args.batch_max_bytes
                        {
                            flush_metrics(&mut conn, &mut metrics_pending, &mut metrics_batch_id, &mut flush_sig).await?;
                        }
                    }
                    None => {
                        // Scheduler gone; disable the arm so we don't spin.
                        metrics_enabled = false;
                    }
                }
            }
            _ = flush_timer.tick() => {
                flush(&mut conn, &mut pending, &mut batch_id, &mut flush_sig).await?;
                flush_metrics(&mut conn, &mut metrics_pending, &mut metrics_batch_id, &mut flush_sig).await?;
            }
            _ = main_sig.changed() => {
                shutting_down = true;
            }
        }
    }

    // ── Drain + graceful close ─────────────────────────────────────────
    let _ = shutdown_tx.send(true);
    while let Ok(rec) = log_rx.try_recv() {
        ingest(
            &mut pending,
            &registry,
            &node,
            rec,
            &log_pipeline,
            &mut keep_cache,
            &mut enrich_cache,
            &mut dropped,
        )
        .await;
    }
    while let Ok(result) = metrics_rx.try_recv() {
        ingest_metrics(
            &mut metrics_pending,
            result,
            &metric_pipeline.keep,
            &mut metrics_keep_cache,
            &mut metrics_dropped,
        );
    }
    flush(&mut conn, &mut pending, &mut batch_id, &mut flush_sig).await?;
    flush_metrics(
        &mut conn,
        &mut metrics_pending,
        &mut metrics_batch_id,
        &mut flush_sig,
    )
    .await?;
    // Best-effort Goodbye: if the upstream is down at shutdown the socket is
    // dead and there's nothing to gracefully close.
    if let Err(e) = conn.shutdown("agent shutdown").await {
        warn!(error = %e, "graceful goodbye failed (upstream likely down)");
    }

    if let Some(h) = watcher_handle {
        let _ = h.await;
    }
    if let Some(h) = scheduler_handle {
        let _ = h.await;
    }
    let _ = scanner_handle.await;
    info!(
        batches = batch_id,
        metrics_batches = metrics_batch_id,
        dropped_by_filter = dropped,
        metrics_dropped_by_filter = metrics_dropped,
        "agent done"
    );
    Ok(())
}

/// A batch under construction: one `LogStream` per fingerprint.
#[derive(Default)]
struct Pending {
    streams: HashMap<u64, LogStream>,
    record_count: u32,
    approx_bytes: usize,
    ts_min: u64,
    ts_max: u64,
}

impl Pending {
    fn reset(&mut self) {
        self.streams.clear();
        self.record_count = 0;
        self.approx_bytes = 0;
        self.ts_min = 0;
        self.ts_max = 0;
    }
}

/// A metrics batch under construction: a deduplicated series dictionary plus the
/// samples that reference it. `record_count` counts samples (the batch's record
/// unit), matching the wire `Batch.record_count` semantics for metrics.
#[derive(Default)]
struct MetricsPending {
    series: HashMap<u64, SeriesDictEntry>,
    samples: Vec<MetricSample>,
    record_count: u32,
    approx_bytes: usize,
    ts_min: u64,
    ts_max: u64,
}

impl MetricsPending {
    fn reset(&mut self) {
        self.series.clear();
        self.samples.clear();
        self.record_count = 0;
        self.approx_bytes = 0;
        self.ts_min = 0;
        self.ts_max = 0;
    }
}

/// Fold one tailed log line into the pending batch.
///
/// Records whose stream labels don't pass the keep-only allow-list are dropped
/// here — before any batch state is touched — so they never go on the wire. The
/// keep/drop decision is cached per fingerprint (`keep_cache`) to keep the
/// allow-list off the per-line hot path.
#[allow(clippy::too_many_arguments)]
async fn ingest(
    pending: &mut Pending,
    registry: &discovery::PodRegistry,
    node: &str,
    rec: RawLog,
    pipeline: &config::LogPipeline,
    keep_cache: &mut HashMap<u64, bool>,
    enrich_cache: &mut HashMap<u64, (u64, Vec<LabelPair>)>,
    dropped: &mut u64,
) {
    // Stream identity comes from the CRI path + pod labels (registry). Hold the
    // read guard only long enough to build the base labels — JSON parsing and
    // enrichment happen after it's dropped so a slow parse can't block the watch.
    let (base_fp, base_labels) = {
        let guard = registry.read().await;
        stream::stream_labels(&rec.pod, node, guard.get(&rec.pod.uid))
    };

    // Stream is Copy; capture the cheap scalars before the body is moved.
    let stream_name = rec.stream.name();
    let severity = rec.stream.severity();
    let ts = rec.ts_unix_nano;

    // Resolve final (fingerprint, labels, attributes, body). The no-op pipeline
    // path is byte-identical to the pre-config behavior.
    let (fp, labels, attributes, body) = if pipeline.enriches() {
        // Parse the JSON body once, only when extraction needs it.
        let parsed = if pipeline.needs_json() {
            serde_json::from_str::<serde_json::Value>(&rec.body).ok()
        } else {
            None
        };
        let obj = parsed.as_ref().and_then(|v| v.as_object());

        let (fp, labels) = if pipeline.json_adds_labels() {
            // JSON labels vary per line → fingerprint isn't cacheable per stream.
            stream::enrich_labels(&base_labels, pipeline, obj)
        } else {
            // Labels stable per stream → rebuild once, cache by base fingerprint.
            enrich_cache
                .entry(base_fp)
                .or_insert_with(|| stream::enrich_labels(&base_labels, pipeline, None))
                .clone()
        };
        let (attrs, body) = stream::enrich_entry(stream_name, rec.body, pipeline, obj);
        (fp, labels, attrs, body)
    } else {
        (
            base_fp,
            base_labels,
            vec![LabelPair {
                key: "stream".into(),
                value: stream_name.into(),
            }],
            rec.body,
        )
    };

    if !pipeline.keep.is_empty() {
        let keep = *keep_cache
            .entry(fp)
            .or_insert_with(|| pipeline.keep.keeps(&labels));
        if !keep {
            *dropped += 1;
            // Sparse heartbeat so operators can see the filter cutting volume
            // without flooding the log on every dropped line.
            if dropped.is_multiple_of(100_000) {
                info!(
                    dropped = *dropped,
                    "dropping log streams not matching keep allow-list"
                );
            }
            return;
        }
    }

    if pending.record_count == 0 {
        pending.ts_min = ts;
        pending.ts_max = ts;
    } else {
        pending.ts_min = pending.ts_min.min(ts);
        pending.ts_max = pending.ts_max.max(ts);
    }
    pending.approx_bytes += body.len() + 48;
    pending.record_count += 1;

    let entry = LogEntry {
        ts_unix_nano: ts,
        severity,
        body,
        attributes,
    };

    pending
        .streams
        .entry(fp)
        .or_insert_with(|| LogStream {
            fingerprint: fp,
            labels,
            entries: Vec::new(),
        })
        .entries
        .push(entry);
}

/// Fold one scrape result into the pending metrics batch.
///
/// Series whose labels don't pass the keep-only allow-list are dropped (along
/// with their samples) before any batch state is touched. The keep/drop
/// decision is cached per fingerprint, like the log path.
fn ingest_metrics(
    pending: &mut MetricsPending,
    result: scrape::ScrapeResult,
    keep_filter: &filter::LabelFilter,
    keep_cache: &mut HashMap<u64, bool>,
    dropped: &mut u64,
) {
    // Decide keep/drop per series; insert kept series into the dictionary.
    let mut kept: HashSet<u64> = HashSet::with_capacity(result.series.len());
    for s in result.series {
        let keep = keep_filter.is_empty()
            || *keep_cache
                .entry(s.fingerprint)
                .or_insert_with(|| keep_filter.keeps(&s.labels));
        if keep {
            kept.insert(s.fingerprint);
            pending.series.entry(s.fingerprint).or_insert(s);
        }
    }

    for smp in result.samples {
        if !kept.contains(&smp.fingerprint) {
            *dropped += 1;
            if dropped.is_multiple_of(100_000) {
                info!(
                    dropped = *dropped,
                    "dropping metric series not matching keep allow-list"
                );
            }
            continue;
        }
        if pending.record_count == 0 {
            pending.ts_min = smp.ts_unix_nano;
            pending.ts_max = smp.ts_unix_nano;
        } else {
            pending.ts_min = pending.ts_min.min(smp.ts_unix_nano);
            pending.ts_max = pending.ts_max.max(smp.ts_unix_nano);
        }
        pending.approx_bytes += 24; // fingerprint + ts + value, roughly
        pending.record_count += 1;
        pending.samples.push(smp);
    }
}

/// Encode + compress the pending batch and ship it, reconnecting with capped
/// exponential backoff if the ingest server has gone away (e.g. a rolling
/// restart). No-op when empty.
///
/// Logs are at-most-once, so when `shutdown` is signalled mid-reconnect we
/// abandon the in-flight batch rather than block process termination behind a
/// dead upstream.
async fn flush(
    conn: &mut Client,
    pending: &mut Pending,
    batch_id: &mut u64,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    if pending.record_count == 0 {
        return Ok(());
    }
    let streams: Vec<LogStream> = pending.streams.drain().map(|(_, v)| v).collect();
    let record_count = pending.record_count;
    let (ts_min, ts_max) = (pending.ts_min, pending.ts_max);

    let payload = LogsBatch { streams }
        .encode()
        .expect("LogsBatch encode is infallible for well-formed inputs");
    let uncompressed_size = payload.len() as u32;
    let compressed = zstd::encode_all(payload.as_slice(), ZSTD_LEVEL)
        .expect("zstd encode_all is infallible on Vec input");

    // The session id changes on every reconnect, so `send_batch_stamped`
    // stamps the live one into the frame on each attempt — build a placeholder.
    let mut frame = build::batch(build::BatchArgs {
        session_id: 0,
        batch_id: *batch_id,
        signal: Signal::Logs.as_u8(),
        ts_min_unix_nano: ts_min,
        ts_max_unix_nano: ts_max,
        record_count,
        compression: COMPRESSION_ZSTD,
        uncompressed_size,
        payload: compressed,
    });

    if ship_frame(conn, &mut frame, shutdown).await? {
        info!(
            batch_id = *batch_id,
            records = record_count,
            "shipped log batch"
        );
        *batch_id += 1;
    }
    pending.reset();
    Ok(())
}

/// Encode + compress the pending metrics batch and ship it. Same reconnect /
/// at-most-once semantics as [`flush`]; no-op when empty.
async fn flush_metrics(
    conn: &mut Client,
    pending: &mut MetricsPending,
    batch_id: &mut u64,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    if pending.record_count == 0 {
        return Ok(());
    }
    let series: Vec<SeriesDictEntry> = pending.series.drain().map(|(_, v)| v).collect();
    let samples = std::mem::take(&mut pending.samples);
    let record_count = pending.record_count;
    let (ts_min, ts_max) = (pending.ts_min, pending.ts_max);

    let payload = MetricsBatch { series, samples }
        .encode()
        .expect("MetricsBatch encode is infallible for well-formed inputs");
    let uncompressed_size = payload.len() as u32;
    let compressed = zstd::encode_all(payload.as_slice(), ZSTD_LEVEL)
        .expect("zstd encode_all is infallible on Vec input");

    let mut frame = build::batch(build::BatchArgs {
        session_id: 0,
        batch_id: *batch_id,
        signal: Signal::Metrics.as_u8(),
        ts_min_unix_nano: ts_min,
        ts_max_unix_nano: ts_max,
        record_count,
        compression: COMPRESSION_ZSTD,
        uncompressed_size,
        payload: compressed,
    });

    if ship_frame(conn, &mut frame, shutdown).await? {
        info!(
            batch_id = *batch_id,
            samples = record_count,
            "shipped metrics batch"
        );
        *batch_id += 1;
    }
    pending.reset();
    Ok(())
}

/// Send a built batch frame, reconnecting with capped exponential backoff if the
/// ingest server has gone away. Returns `Ok(true)` once shipped, `Ok(false)` if
/// `shutdown` fired mid-reconnect (the caller drops the in-flight batch —
/// at-most-once, like logs). `send_batch_stamped` re-stamps the live session id
/// on each attempt.
async fn ship_frame(
    conn: &mut Client,
    frame: &mut scry_proto::Frame,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool> {
    let mut backoff = Duration::from_millis(200);
    loop {
        match conn.send_batch_stamped(frame).await {
            Ok(()) => return Ok(true),
            Err(e) => {
                warn!(error = %e, "batch send failed; reconnecting to ingest server");
            }
        }
        // Re-establish the session, backing off between failed attempts, then
        // loop back to re-stamp + resend the same frame.
        loop {
            if *shutdown.borrow() {
                warn!("shutdown during reconnect; dropping in-flight batch");
                return Ok(false);
            }
            match conn.reconnect().await {
                Ok(()) => {
                    info!("reconnected to ingest server");
                    break;
                }
                Err(re) => {
                    warn!(error = %re, "reconnect attempt failed; will retry");
                    tokio::select! {
                        _ = shutdown.changed() => {}
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }
}

/// Resolve on SIGINT or (on unix) SIGTERM — the latter is what Kubernetes
/// sends on pod termination.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "cannot install SIGTERM handler");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn hostname_string() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".into())
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, mult) = if let Some(v) = s.strip_suffix("ms") {
        (v, 1u64)
    } else if let Some(v) = s.strip_suffix('s') {
        (v, 1000)
    } else if let Some(v) = s.strip_suffix('m') {
        (v, 60_000)
    } else {
        (s, 1000)
    };
    let base: u64 = num.parse().map_err(|_| format!("invalid duration: {s}"))?;
    Ok(Duration::from_millis(base * mult))
}

/// Resolve a `--scrape-bearer` argument: a literal token, or `@/path` to read
/// the token from a file (re-read per scrape, so rotation is followed). Prefer
/// the file form so the secret never appears in argv / process listings.
fn resolve_bearer(arg: Option<String>) -> Result<Option<scrape::BearerSource>> {
    match arg {
        None => Ok(None),
        Some(s) => match s.strip_prefix('@') {
            Some(path) => Ok(Some(scrape::BearerSource::File(PathBuf::from(path)))),
            None => Ok(Some(scrape::BearerSource::Literal(s))),
        },
    }
}

/// Build [`scrape::ScrapeTarget`]s for the static `--scrape-target` URLs. Each
/// carries `job` (the configured default), `instance` (host:port parsed from the
/// URL), and `node`, mirroring the discovered-target label convention.
fn build_static_targets(
    urls: &[String],
    job: &str,
    node: &str,
    bearer: Option<scrape::BearerSource>,
) -> Vec<scrape::ScrapeTarget> {
    urls.iter()
        .map(|url| scrape::ScrapeTarget {
            url: url.clone(),
            labels: vec![
                LabelPair {
                    key: "job".into(),
                    value: job.to_string(),
                },
                LabelPair {
                    key: "instance".into(),
                    value: instance_from_url(url),
                },
                LabelPair {
                    key: "node".into(),
                    value: node.to_string(),
                },
            ],
            bearer: bearer.clone(),
            tls: Default::default(),
            // Static labels + relabel map are applied uniformly to all targets
            // by the scrape scheduler (see discovery::stamp_target).
            label_map: Default::default(),
        })
        .collect()
}

/// Build the kubelet scrape targets: one per enabled endpoint (`/metrics/cadvisor`
/// → `job=cadvisor`, `/metrics` → `job=kubelet`), each carrying the kubelet TLS
/// profile and a file-backed bearer source (re-read per scrape for SA-token
/// rotation). The address template is interpolated with `${NODE_IP}`/`${NODE_NAME}`.
fn build_kubelet_targets(
    cfg: &config::KubeletConfig,
    node: &str,
    node_ip: Option<&str>,
) -> Result<Vec<scrape::ScrapeTarget>> {
    let base = resolve_kubelet_address(&cfg.address, node_ip, node)?;
    let base = base.trim_end_matches('/');
    let bearer = scrape::BearerSource::File(cfg.bearer_file.clone());
    let mut targets = Vec::new();
    let mut push = |path: &str, job: &str| {
        let url = format!("{base}{path}");
        targets.push(scrape::ScrapeTarget {
            url: url.clone(),
            labels: vec![
                LabelPair {
                    key: "job".into(),
                    value: job.to_string(),
                },
                LabelPair {
                    key: "instance".into(),
                    value: instance_from_url(&url),
                },
                LabelPair {
                    key: "node".into(),
                    value: node.to_string(),
                },
            ],
            bearer: Some(bearer.clone()),
            tls: cfg.tls.clone(),
            label_map: Default::default(),
        });
    };
    if cfg.cadvisor {
        push("/metrics/cadvisor", "cadvisor");
    }
    if cfg.kubelet {
        push("/metrics", "kubelet");
    }
    Ok(targets)
}

/// Interpolate `${NODE_IP}` / `${NODE_NAME}` in a kubelet address template.
/// Errors if a referenced token has no value (e.g. `${NODE_IP}` used but
/// `--node-ip`/`NODE_IP` is unset).
fn resolve_kubelet_address(template: &str, node_ip: Option<&str>, node: &str) -> Result<String> {
    let mut out = template.to_string();
    if out.contains("${NODE_IP}") {
        let ip = node_ip.filter(|s| !s.is_empty()).ok_or_else(|| {
            anyhow::anyhow!(
                "kubelet address {template:?} references ${{NODE_IP}} but --node-ip/NODE_IP is unset"
            )
        })?;
        out = out.replace("${NODE_IP}", ip);
    }
    if out.contains("${NODE_NAME}") {
        if node.is_empty() {
            anyhow::bail!(
                "kubelet address {template:?} references ${{NODE_NAME}} but the node name is empty"
            );
        }
        out = out.replace("${NODE_NAME}", node);
    }
    Ok(out)
}

/// Extract the `host:port` authority from a URL for the `instance` label.
fn instance_from_url(url: &str) -> String {
    let after_scheme = url.split("://").nth(1).unwrap_or(url);
    after_scheme
        .split('/')
        .next()
        .unwrap_or(after_scheme)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scrape::{BearerSource, TlsProfile};

    fn label(t: &scrape::ScrapeTarget, key: &str) -> Option<String> {
        t.labels
            .iter()
            .find(|p| p.key == key)
            .map(|p| p.value.clone())
    }

    #[test]
    fn resolve_kubelet_address_interpolates_node_ip() {
        let got = resolve_kubelet_address("https://${NODE_IP}:10250", Some("10.0.0.7"), "n1")
            .expect("ip set");
        assert_eq!(got, "https://10.0.0.7:10250");
    }

    #[test]
    fn resolve_kubelet_address_interpolates_node_name() {
        let got = resolve_kubelet_address("https://${NODE_NAME}:10250", None, "node-1")
            .expect("name set");
        assert_eq!(got, "https://node-1:10250");
    }

    #[test]
    fn resolve_kubelet_address_errors_when_node_ip_missing() {
        assert!(resolve_kubelet_address("https://${NODE_IP}:10250", None, "n1").is_err());
        // Empty string counts as missing.
        assert!(resolve_kubelet_address("https://${NODE_IP}:10250", Some(""), "n1").is_err());
    }

    #[test]
    fn resolve_kubelet_address_passes_literal_through() {
        let got = resolve_kubelet_address("https://127.0.0.1:9999", None, "n1").expect("literal");
        assert_eq!(got, "https://127.0.0.1:9999");
    }

    fn kubelet_cfg(cadvisor: bool, kubelet: bool) -> config::KubeletConfig {
        config::KubeletConfig {
            enabled: true,
            address: "https://${NODE_IP}:10250".into(),
            cadvisor,
            kubelet,
            bearer_file: PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount/token"),
            tls: TlsProfile {
                insecure_skip_verify: true,
                ca_file: None,
            },
        }
    }

    #[test]
    fn build_kubelet_targets_emits_both_endpoints() {
        let cfg = kubelet_cfg(true, true);
        let targets = build_kubelet_targets(&cfg, "n1", Some("10.0.0.7")).expect("targets");
        assert_eq!(targets.len(), 2);

        let cadvisor = &targets[0];
        assert_eq!(cadvisor.url, "https://10.0.0.7:10250/metrics/cadvisor");
        assert_eq!(label(cadvisor, "job").as_deref(), Some("cadvisor"));
        assert_eq!(
            label(cadvisor, "instance").as_deref(),
            Some("10.0.0.7:10250")
        );
        assert_eq!(label(cadvisor, "node").as_deref(), Some("n1"));
        assert!(cadvisor.tls.insecure_skip_verify);
        match &cadvisor.bearer {
            Some(BearerSource::File(p)) => {
                assert_eq!(p, &cfg.bearer_file);
            }
            other => panic!("expected file bearer, got {other:?}"),
        }

        let kubelet = &targets[1];
        assert_eq!(kubelet.url, "https://10.0.0.7:10250/metrics");
        assert_eq!(label(kubelet, "job").as_deref(), Some("kubelet"));
    }

    #[test]
    fn build_kubelet_targets_honors_endpoint_toggles() {
        let only_cadvisor =
            build_kubelet_targets(&kubelet_cfg(true, false), "n1", Some("1.2.3.4")).unwrap();
        assert_eq!(only_cadvisor.len(), 1);
        assert_eq!(label(&only_cadvisor[0], "job").as_deref(), Some("cadvisor"));

        let only_kubelet =
            build_kubelet_targets(&kubelet_cfg(false, true), "n1", Some("1.2.3.4")).unwrap();
        assert_eq!(only_kubelet.len(), 1);
        assert_eq!(label(&only_kubelet[0], "job").as_deref(), Some("kubelet"));

        let none =
            build_kubelet_targets(&kubelet_cfg(false, false), "n1", Some("1.2.3.4")).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn build_kubelet_targets_errors_without_node_ip() {
        assert!(build_kubelet_targets(&kubelet_cfg(true, true), "n1", None).is_err());
    }

    #[test]
    fn resolve_bearer_file_form_is_lazy() {
        match resolve_bearer(Some("@/some/path".into())).unwrap() {
            Some(BearerSource::File(p)) => assert_eq!(p, PathBuf::from("/some/path")),
            other => panic!("expected file bearer, got {other:?}"),
        }
    }

    #[test]
    fn resolve_bearer_literal_form() {
        match resolve_bearer(Some("tok123".into())).unwrap() {
            Some(BearerSource::Literal(s)) => assert_eq!(s, "tok123"),
            other => panic!("expected literal bearer, got {other:?}"),
        }
        assert!(resolve_bearer(None).unwrap().is_none());
    }
}
