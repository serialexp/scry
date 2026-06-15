//! `scry-retention` — v0.8 per-signal TTL retention (single-instance).
//!
//! Retention reclaims storage by deleting blocks whose data is entirely
//! past a per-signal age limit. It is the **delete tail of compaction's
//! lifecycle with no merge** — it reuses [`scry_block::delete_block_objects`]
//! and [`scry_catalog::Catalog::delete_blocks`], and the same
//! object-before-row ordering (the catalog is derived state).
//!
//! Two safety properties shape the design:
//!
//! - **Opt-in, no implicit deletion.** A signal is only eligible if a TTL
//!   is configured for it ([`RetentionConfig::ttl_for`]); a signal with no
//!   TTL is never touched.
//! - **Whole-block criterion.** A block is reaped only when its *newest*
//!   record (`ts_max_unix_nano`) is past the TTL, so a block still holding
//!   in-window data is never dropped.
//!
//! This crate is the engine plus a thin CLI (`src/main.rs`). The standalone
//! [`retain_once`](engine::retain_once) entry point is single-instance (one
//! reaper, no lease). The v0.9 multi-instance daemon drives
//! [`retain_planned`](engine::retain_planned) under the global retention lease
//! (a [`Fence`](scry_block::Fence)) and emits `Deleted` events through a
//! [`BlockEventSink`](scry_block::BlockEventSink) so peers evict reaped blocks.
//!
//! - [`policy`] — which blocks are expired ([`RetentionConfig`],
//!   [`plan_reaping`]).
//! - [`engine`] — the dry-run / apply lifecycle
//!   ([`retain_once`](engine::retain_once) /
//!   [`retain_planned`](engine::retain_planned)).

pub mod engine;
pub mod policy;

pub use engine::{retain_once, retain_planned, RetentionReport};
pub use policy::{plan_reaping, RetentionConfig};

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use scry_catalog::Catalog;
use scry_objstore::{open as open_objstore, ObjStoreConfig};

/// CLI arguments for the `scry retention` subcommand.
#[derive(Parser, Debug)]
#[command(about = "Per-signal TTL retention: reap blocks whose data is entirely past an age limit")]
pub struct Args {
    /// Path to the SQLite catalog file. Created (with schema) if absent.
    #[arg(long)]
    pub catalog: PathBuf,

    /// Blanket TTL applied to every signal with no per-signal override.
    /// Omit for opt-in: only signals with an explicit `--ttl-<signal>`
    /// are eligible. Accepts `30d`, `12h`, `90m`, `45s`, `500ms`.
    #[arg(long, value_parser = parse_duration)]
    pub ttl: Option<Duration>,

    /// Per-signal TTL override for metrics.
    #[arg(long, value_parser = parse_duration)]
    pub ttl_metrics: Option<Duration>,
    /// Per-signal TTL override for logs.
    #[arg(long, value_parser = parse_duration)]
    pub ttl_logs: Option<Duration>,
    /// Per-signal TTL override for traces.
    #[arg(long, value_parser = parse_duration)]
    pub ttl_traces: Option<Duration>,
    /// Per-signal TTL override for profiles.
    #[arg(long, value_parser = parse_duration)]
    pub ttl_profiles: Option<Duration>,

    /// Seconds to wait between soft-deleting expired blocks (queries stop
    /// listing them) and removing their objects. 0 is safe single-instance;
    /// raise it if other readers share the bucket.
    #[arg(long, default_value_t = 0)]
    pub grace: u64,

    /// Actually delete. Without this flag the run is a dry-run: it prints
    /// what would be reaped and touches nothing.
    #[arg(long)]
    pub apply: bool,

    /// Skip the bucket reconcile before reaping. By default the catalog is
    /// reconciled from the bucket first so the tool works against a shared
    /// bucket without an online catalog.
    #[arg(long)]
    pub no_reconcile: bool,

    /// Loop forever, reaping every `--interval` seconds, instead of running
    /// a single pass and exiting.
    #[arg(long)]
    pub watch: bool,

    /// Seconds between passes in `--watch` mode.
    #[arg(long, default_value_t = 3600)]
    pub interval: u64,
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

/// Run the standalone (single-instance) retention tool: one pass, or a
/// `--watch` loop. Dry-run unless `--apply`.
pub async fn run(args: Args) -> Result<()> {
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

    let verb = if report.dry_run {
        "would reap"
    } else {
        "reaped"
    };
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
