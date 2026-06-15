//! scry-list — v0.1 catalog inspector (the `scry list` subcommand).
//!
//! Opens a SQLite catalog (creating an empty one if it doesn't yet
//! exist), reconciles it against the bucket via
//! [`scry_catalog::Catalog::reconcile_from_bucket`], and prints one
//! line per known block:
//!
//! ```text
//!   <uuid>  <date>  rows=<n>  bytes=<n>  ts_min..ts_max  writer=<short>
//! ```
//!
//! The point of the exercise is the v0.1 exit criterion: "drop into
//! an empty catalog dir, point at the bucket, recover exactly what's
//! there." If the catalog row count matches the sum of writes the
//! sink performed, the storage layer round-trips.
//!
//! Run (after `source docker/garage/.env`):
//!
//! ```bash
//! scry list --catalog ./catalog.sqlite
//! ```
//!
//! With `--interval <secs>` it instead runs as a long-running reconciler:
//! reconcile against the bucket every `<secs>` seconds forever, logging a
//! summary each cycle (no per-block listing). This is the sidecar mode that
//! keeps `scry query`'s catalog fresh — the query daemon opens the same SQLite
//! file and SQLite's WAL makes the sidecar's cross-process writes visible to
//! the daemon's per-query catalog lookups.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use scry_catalog::Catalog;
use scry_objstore::{open as open_objstore, ObjStoreConfig};

/// CLI arguments for the `scry list` subcommand.
#[derive(Parser, Debug)]
#[command(about = "Catalog inspector: reconcile a local catalog against a bucket and list blocks")]
pub struct Args {
    /// Path to the SQLite catalog file. Created (with schema) if it
    /// doesn't exist.
    #[arg(long)]
    pub catalog: PathBuf,

    /// Skip the bucket walk and just print what's already in the
    /// catalog. Useful for sanity-checking the online insert path
    /// (`scry ingest`'s [`scry_catalog::Catalog::insert_block`] calls)
    /// without depending on the bucket.
    #[arg(long)]
    pub no_reconcile: bool,

    /// Run as a long-running reconciler: reconcile against the bucket every
    /// `<secs>` seconds forever, logging a summary each cycle instead of
    /// reconciling once and printing the block listing. This is the sidecar
    /// mode that keeps the query daemon's catalog fresh. Conflicts with
    /// `--no-reconcile`.
    #[arg(long, value_name = "SECS", conflicts_with = "no_reconcile")]
    pub interval: Option<u64>,
}

/// Reconcile + list (one-shot), or run as a periodic reconciler sidecar
/// (`--interval`).
pub async fn run(args: Args) -> Result<()> {
    let cfg = ObjStoreConfig::from_env()
        .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;
    let bucket = cfg.bucket.clone();

    let catalog = Catalog::open(&args.catalog, &bucket)
        .with_context(|| format!("opening catalog at {}", args.catalog.display()))?;

    // Long-running reconciler sidecar mode: reconcile on a fixed interval
    // forever, beside the query daemon, so its per-query catalog lookups see
    // newly-flushed blocks. Never returns; the one-shot listing below is the
    // non-`--interval` path.
    if let Some(secs) = args.interval {
        let store = open_objstore(&cfg)?;
        let period = Duration::from_secs(secs.max(1));
        tracing::info!(
            interval_secs = secs,
            catalog = %args.catalog.display(),
            bucket = %bucket,
            "scry-list reconciler: starting periodic catalog reconcile"
        );
        loop {
            match catalog.reconcile_from_bucket(store.as_ref()).await {
                Ok(report) => tracing::info!(
                    seen = report.seen,
                    inserted = report.inserted,
                    already_present = report.already_present,
                    failed = report.failed,
                    "reconcile cycle complete"
                ),
                // A transient bucket/network error must not kill the sidecar;
                // log it and retry on the next tick.
                Err(e) => {
                    tracing::error!(error = %e, "reconcile cycle failed; will retry next tick")
                }
            }
            tokio::time::sleep(period).await;
        }
    }

    if !args.no_reconcile {
        let store = open_objstore(&cfg)?;
        let report = catalog.reconcile_from_bucket(store.as_ref()).await?;
        eprintln!(
            "reconcile: seen={} inserted={} already_present={} failed={}",
            report.seen, report.inserted, report.already_present, report.failed
        );
    }

    let blocks = catalog.list_blocks()?;
    println!("# {} block(s) in catalog (bucket={})", blocks.len(), bucket);
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;
    for b in &blocks {
        total_rows += b.meta.row_count;
        total_bytes += b.meta.byte_size;
        // writer_id short = first 8 hex chars
        let writer_short = b
            .meta
            .writer_id
            .as_simple()
            .to_string()
            .chars()
            .take(8)
            .collect::<String>();
        println!(
            "{}  {}  rows={:>9}  bytes={:>11}  ts={}..{}  signal={}  writer={}",
            b.meta.uuid,
            b.date,
            b.meta.row_count,
            b.meta.byte_size,
            b.meta.ts_min_unix_nano,
            b.meta.ts_max_unix_nano,
            b.meta.signal,
            writer_short,
        );
    }
    println!("# total rows={} bytes={}", total_rows, total_bytes);
    Ok(())
}
