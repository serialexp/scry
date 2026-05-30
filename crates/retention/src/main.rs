//! scry-retention — v0.8 per-signal TTL retention tool.
//!
//! Opens a SQLite catalog, (optionally) reconciles it against the bucket,
//! then runs one retention pass (`--once`, the default) or loops on an
//! interval (`--watch`). Each pass reaps every block whose newest record
//! is older than the TTL configured for its signal.
//!
//! **Dry-run by default** — a normal run only *previews* what would be
//! reaped and touches nothing. Pass `--apply` to actually delete.
//!
//! TTLs are **opt-in**: a signal is reaped only if you give it a TTL,
//! either per-signal (`--ttl-logs 30d`) or via a blanket `--ttl 30d`
//! default. A signal with no TTL is never touched.
//!
//! Run (after `source docker/garage/.env`):
//!
//! ```bash
//! # preview what a 30-day logs TTL would reap
//! scry-retention --catalog ./catalog.sqlite --ttl-logs 30d
//! # actually reap (metrics 90d, logs 14d, others untouched)
//! scry-retention --catalog ./catalog.sqlite --ttl-metrics 90d --ttl-logs 14d --apply
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use scry_catalog::Catalog;
use scry_objstore::{open as open_objstore, ObjStoreConfig};
use scry_retention::{retain_once, RetentionConfig};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the SQLite catalog file. Created (with schema) if absent.
    #[arg(long)]
    catalog: PathBuf,

    /// Blanket TTL applied to every signal with no per-signal override.
    /// Omit for opt-in: only signals with an explicit `--ttl-<signal>`
    /// are eligible. Accepts `30d`, `12h`, `90m`, `45s`, `500ms`.
    #[arg(long, value_parser = parse_duration)]
    ttl: Option<Duration>,

    /// Per-signal TTL override for metrics.
    #[arg(long, value_parser = parse_duration)]
    ttl_metrics: Option<Duration>,
    /// Per-signal TTL override for logs.
    #[arg(long, value_parser = parse_duration)]
    ttl_logs: Option<Duration>,
    /// Per-signal TTL override for traces.
    #[arg(long, value_parser = parse_duration)]
    ttl_traces: Option<Duration>,
    /// Per-signal TTL override for profiles.
    #[arg(long, value_parser = parse_duration)]
    ttl_profiles: Option<Duration>,

    /// Seconds to wait between soft-deleting expired blocks (queries stop
    /// listing them) and removing their objects. 0 is safe single-instance;
    /// raise it if other readers share the bucket.
    #[arg(long, default_value_t = 0)]
    grace: u64,

    /// Actually delete. Without this flag the run is a dry-run: it prints
    /// what would be reaped and touches nothing.
    #[arg(long)]
    apply: bool,

    /// Skip the bucket reconcile before reaping. By default the catalog is
    /// reconciled from the bucket first so the tool works against a shared
    /// bucket without an online catalog.
    #[arg(long)]
    no_reconcile: bool,

    /// Loop forever, reaping every `--interval` seconds, instead of running
    /// a single pass and exiting.
    #[arg(long)]
    watch: bool,

    /// Seconds between passes in `--watch` mode.
    #[arg(long, default_value_t = 3600)]
    interval: u64,
}

/// Tiny duration parser: integer + suffix (`ms`/`s`/`m`/`h`/`d`; bare
/// number = seconds). Mirrors `noise-spewer`'s parser, plus `d` for days
/// (the natural unit for retention TTLs).
fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, unit) = s
        .find(|c: char| c.is_alphabetic())
        .map(|i| (&s[..i], &s[i..]))
        .unwrap_or((s, "s"));
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("bad number in {s:?}"))?;
    let dur = match unit.trim() {
        "ms" => Duration::from_millis(n),
        "s" | "" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        "d" => Duration::from_secs(n * 86_400),
        other => return Err(format!("unknown duration unit {other:?}")),
    };
    Ok(dur)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    let mut overrides: BTreeMap<String, Duration> = BTreeMap::new();
    if let Some(d) = args.ttl_metrics {
        overrides.insert("metrics".into(), d);
    }
    if let Some(d) = args.ttl_logs {
        overrides.insert("logs".into(), d);
    }
    if let Some(d) = args.ttl_traces {
        overrides.insert("traces".into(), d);
    }
    if let Some(d) = args.ttl_profiles {
        overrides.insert("profiles".into(), d);
    }

    let cfg = RetentionConfig {
        default_ttl: args.ttl,
        overrides,
        grace: Duration::from_secs(args.grace),
        apply: args.apply,
    };

    // A pass that can't reap anything is surely a mistake — fail loudly
    // rather than silently no-op.
    anyhow::ensure!(
        cfg.any_ttl_configured(),
        "no TTL configured: pass --ttl <dur> for a blanket default or --ttl-<signal> <dur> for a specific signal"
    );

    let obj_cfg = ObjStoreConfig::from_env()
        .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;
    let bucket = obj_cfg.bucket.clone();
    let store = open_objstore(&obj_cfg)?;

    let catalog = Catalog::open(&args.catalog, &bucket)
        .with_context(|| format!("opening catalog at {}", args.catalog.display()))?;

    if !args.no_reconcile {
        let report = catalog.reconcile_from_bucket(store.as_ref()).await?;
        tracing::info!(
            seen = report.seen,
            inserted = report.inserted,
            already_present = report.already_present,
            failed = report.failed,
            "reconcile complete"
        );
    }

    if args.apply {
        tracing::warn!("--apply set: expired blocks WILL be deleted");
    } else {
        tracing::info!("dry-run (no --apply): previewing reapable blocks, nothing will be deleted");
    }

    if args.watch {
        tracing::info!(
            interval_secs = args.interval,
            apply = args.apply,
            "starting retention watch loop (Ctrl-C to stop)"
        );
        loop {
            run_pass(&store, &catalog, &cfg).await?;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(args.interval)) => {}
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("shutdown signalled; exiting watch loop");
                    break;
                }
            }
        }
    } else {
        run_pass(&store, &catalog, &cfg).await?;
    }

    Ok(())
}

async fn run_pass(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    catalog: &Catalog,
    cfg: &RetentionConfig,
) -> Result<()> {
    let now_unix_nano = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_nanos() as u64;
    let report = retain_once(store.clone(), catalog, cfg, now_unix_nano).await?;

    let verb = if report.dry_run { "would reap" } else { "reaped" };
    if report.reaped == 0 {
        tracing::info!(scanned = report.scanned, "nothing to reap this pass");
    } else {
        tracing::info!(
            scanned = report.scanned,
            reaped = report.reaped,
            bytes = report.bytes_reaped,
            dry_run = report.dry_run,
            "retention pass complete: {verb} {} block(s), {} bytes",
            report.reaped,
            report.bytes_reaped
        );
        for (signal, (count, bytes)) in &report.by_signal {
            tracing::info!(%signal, count, bytes, "  per-signal: {verb} {count} block(s)");
        }
    }
    Ok(())
}
