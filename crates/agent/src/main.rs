//! scry-agent — Kubernetes log-collection agent.
//!
//! Discovers pods on this node via the Kubernetes API, tails their CRI
//! container logs, and ships them as `Signal::Logs` batches over the native
//! binschema wire to a scry ingest server. Logs only, ingest only — the
//! first dogfood signal.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use scry_proto::{
    build,
    constants::{Signal, COMPRESSION_ZSTD, SIGNAL_BIT_LOGS},
    generated::{LogEntry, LogStream, LogsBatch},
    LabelPair,
};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use uuid::Uuid;

mod cri;
mod discovery;
mod filter;
mod stream;

use cri::RawLog;
use scry_client::Client;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ZSTD_LEVEL: i32 = 3;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Ingest server address (host:port).
    #[arg(long, env = "SCRY_SERVER_ADDR", default_value = "127.0.0.1:4000")]
    server_addr: String,

    /// This node's name (used as the `node` stream label and the pod-watch
    /// field selector). In-cluster, wire it from the downward API.
    #[arg(long, env = "NODE_NAME")]
    node_name: Option<String>,

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

    /// Keep-only label allow-list (repeatable). A log stream is shipped only if
    /// it matches ALL `--keep` matchers; everything else is dropped at the node
    /// before it goes on the wire. Omit to ship everything (the default).
    ///
    /// Each matcher is `key=value` | `key!=value` | `key=~regex` | `key!~regex`
    /// (regex is whole-string-anchored; values may be double-quoted). Matches
    /// against stream labels: `namespace`, `pod`, `container`, `node`, and pod
    /// labels exposed as `k8s_<key>` — e.g. `--keep 'namespace=~"prod-.*"'
    /// --keep k8s_app=api`. An absent label is treated as empty.
    #[arg(long = "keep")]
    keep: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let keep_filter = filter::LabelFilter::parse(&args.keep)?;
    if !keep_filter.is_empty() {
        info!(
            matchers = keep_filter.len(),
            "keep-only label allow-list active"
        );
    }

    let hostname = hostname_string();
    let node = args.node_name.clone().unwrap_or_else(|| hostname.clone());
    let agent_id = *Uuid::now_v7().as_bytes();

    // ── Shared state + shutdown ────────────────────────────────────────
    let registry = discovery::new_registry();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (log_tx, mut log_rx) = mpsc::channel::<RawLog>(8192);

    // ── Discovery: pod watch (optional) + filesystem scan ──────────────
    let watcher_handle = if args.no_discovery {
        info!("pod discovery disabled; using path-derived labels only");
        None
    } else {
        Some(discovery::spawn_pod_watcher(
            node.clone(),
            registry.clone(),
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

    // ── Connect ────────────────────────────────────────────────────────
    let mut conn = Client::connect(
        &args.server_addr,
        agent_id,
        &hostname,
        SIGNAL_BIT_LOGS,
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
    // Per-fingerprint keep/drop decision, so the (possibly regex) allow-list
    // runs once per distinct stream rather than once per line. Lives across
    // flushes (Pending is drained each flush); a stream's labels — and thus its
    // fingerprint — are stable, so the cached decision stays valid.
    let mut keep_cache: HashMap<u64, bool> = HashMap::new();
    let mut dropped: u64 = 0;
    let mut batch_id: u64 = 0;
    let mut flush_timer = tokio::time::interval(args.flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut shutting_down = false;
    while !shutting_down {
        tokio::select! {
            maybe = log_rx.recv() => {
                match maybe {
                    Some(rec) => {
                        ingest(&mut pending, &registry, &node, rec, &keep_filter, &mut keep_cache, &mut dropped).await;
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
            _ = flush_timer.tick() => {
                flush(&mut conn, &mut pending, &mut batch_id, &mut flush_sig).await?;
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
            &keep_filter,
            &mut keep_cache,
            &mut dropped,
        )
        .await;
    }
    flush(&mut conn, &mut pending, &mut batch_id, &mut flush_sig).await?;
    // Best-effort Goodbye: if the upstream is down at shutdown the socket is
    // dead and there's nothing to gracefully close.
    if let Err(e) = conn.shutdown("agent shutdown").await {
        warn!(error = %e, "graceful goodbye failed (upstream likely down)");
    }

    if let Some(h) = watcher_handle {
        let _ = h.await;
    }
    let _ = scanner_handle.await;
    info!(
        batches = batch_id,
        dropped_by_filter = dropped,
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
    keep_filter: &filter::LabelFilter,
    keep_cache: &mut HashMap<u64, bool>,
    dropped: &mut u64,
) {
    let (fp, labels) = {
        let guard = registry.read().await;
        stream::stream_labels(&rec.pod, node, guard.get(&rec.pod.uid))
    };

    if !keep_filter.is_empty() {
        let keep = *keep_cache
            .entry(fp)
            .or_insert_with(|| keep_filter.keeps(&labels));
        if !keep {
            *dropped += 1;
            // Sparse heartbeat so operators can see the filter cutting volume
            // without flooding the log on every dropped line.
            if dropped.is_multiple_of(100_000) {
                info!(
                    dropped = *dropped,
                    "dropping log streams not matching --keep allow-list"
                );
            }
            return;
        }
    }

    if pending.record_count == 0 {
        pending.ts_min = rec.ts_unix_nano;
        pending.ts_max = rec.ts_unix_nano;
    } else {
        pending.ts_min = pending.ts_min.min(rec.ts_unix_nano);
        pending.ts_max = pending.ts_max.max(rec.ts_unix_nano);
    }
    pending.approx_bytes += rec.body.len() + 48;
    pending.record_count += 1;

    let entry = LogEntry {
        ts_unix_nano: rec.ts_unix_nano,
        severity: rec.stream.severity(),
        body: rec.body,
        attributes: vec![LabelPair {
            key: "stream".into(),
            value: rec.stream.name().into(),
        }],
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

    let mut backoff = Duration::from_millis(200);
    loop {
        match conn.send_batch_stamped(&mut frame).await {
            Ok(()) => {
                info!(
                    batch_id = *batch_id,
                    records = record_count,
                    "shipped log batch"
                );
                *batch_id += 1;
                pending.reset();
                return Ok(());
            }
            Err(e) => {
                warn!(error = %e, "batch send failed; reconnecting to ingest server");
            }
        }
        // Re-establish the session, backing off between failed attempts, then
        // loop back to re-stamp + resend the same frame.
        loop {
            if *shutdown.borrow() {
                warn!("shutdown during reconnect; dropping in-flight log batch");
                pending.reset();
                return Ok(());
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
