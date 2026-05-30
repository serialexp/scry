//! scry-compact — v0.8 size-tiered compaction tool.
//!
//! Opens a SQLite catalog, (optionally) reconciles it against the
//! bucket, then runs one compaction pass (`--once`, the default) or
//! loops on an interval (`--watch`). Each pass merges every
//! `(signal, date, level)` partition that has accumulated at least
//! `--fanout` blocks into one block at the next level up.
//!
//! Run (after `source docker/garage/.env`):
//!
//! ```bash
//! scry-compact --catalog ./catalog.sqlite --fanout 8
//! scry-compact --catalog ./catalog.sqlite --watch --interval 60
//! ```

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use scry_block::BlockBuilderConfig;
use scry_catalog::Catalog;
use scry_compact::{compact_once, CompactConfig};
use scry_objstore::{open as open_objstore, ObjStoreConfig};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the SQLite catalog file. Created (with schema) if absent.
    #[arg(long)]
    catalog: PathBuf,

    /// Minimum blocks in a `(signal, date, level)` partition to trigger a
    /// merge, and how many are merged per pass (size-tiered fan-out).
    #[arg(short = 'k', long, default_value_t = 8)]
    fanout: usize,

    /// Don't compact blocks at or above this level (L3 is the practical
    /// ceiling).
    #[arg(long, default_value_t = 3)]
    max_level: u32,

    /// Seconds to wait between superseding inputs and deleting their
    /// objects. 0 is safe single-instance (queries skip superseded
    /// blocks immediately); raise it if other readers share the bucket.
    #[arg(long, default_value_t = 0)]
    grace: u64,

    /// Only compact this signal (e.g. `logs`). Default: all signals.
    #[arg(long)]
    signal: Option<String>,

    /// Skip the bucket reconcile before compacting. By default the
    /// catalog is reconciled from the bucket first so the tool works
    /// against a shared bucket without an online catalog.
    #[arg(long)]
    no_reconcile: bool,

    /// Loop forever, compacting every `--interval` seconds, instead of
    /// running a single pass and exiting.
    #[arg(long)]
    watch: bool,

    /// Seconds between passes in `--watch` mode.
    #[arg(long, default_value_t = 60)]
    interval: u64,
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

    let obj_cfg = ObjStoreConfig::from_env()
        .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;
    let bucket = obj_cfg.bucket.clone();
    let store = open_objstore(&obj_cfg)?;

    let catalog = Catalog::open(&args.catalog, &bucket)
        .with_context(|| format!("opening catalog at {}", args.catalog.display()))?;

    let compact_cfg = CompactConfig {
        fanout: args.fanout,
        max_level: args.max_level,
        grace: Duration::from_secs(args.grace),
        signal_filter: args.signal.clone(),
    };
    let block_cfg = BlockBuilderConfig::default();

    // Bring the catalog in line with the bucket once before compacting,
    // so the tool works against a shared bucket with no online catalog.
    if !args.no_reconcile {
        reconcile(&catalog, &store).await?;
    }

    if args.watch {
        tracing::info!(
            interval_secs = args.interval,
            fanout = args.fanout,
            "starting compaction watch loop (Ctrl-C to stop)"
        );
        loop {
            run_pass(&store, &catalog, &bucket, &compact_cfg, &block_cfg).await?;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(args.interval)) => {}
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("shutdown signalled; exiting watch loop");
                    break;
                }
            }
        }
    } else {
        run_pass(&store, &catalog, &bucket, &compact_cfg, &block_cfg).await?;
    }

    Ok(())
}

async fn reconcile(
    catalog: &Catalog,
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
) -> Result<()> {
    let report = catalog.reconcile_from_bucket(store.as_ref()).await?;
    tracing::info!(
        seen = report.seen,
        inserted = report.inserted,
        already_present = report.already_present,
        failed = report.failed,
        "reconcile complete"
    );
    Ok(())
}

async fn run_pass(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    catalog: &Catalog,
    bucket: &str,
    compact_cfg: &CompactConfig,
    block_cfg: &BlockBuilderConfig,
) -> Result<()> {
    let report = compact_once(store.clone(), catalog, bucket, compact_cfg, block_cfg).await?;
    if report.merges == 0 {
        tracing::info!("nothing to compact this pass");
    } else {
        tracing::info!(
            merges = report.merges,
            blocks_in = report.blocks_in,
            blocks_out = report.blocks_out,
            bytes_out = report.bytes_out,
            "compaction pass complete"
        );
    }
    Ok(())
}
