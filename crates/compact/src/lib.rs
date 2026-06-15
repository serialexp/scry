//! `scry-compact` — v0.8 size-tiered compaction (single-instance).
//!
//! Compaction merges the many small blocks a busy writer fans out (one
//! per WAL rotation per shard) into fewer, larger ones, so queries open
//! fewer objects and load less per-block metadata. Blocks live at a
//! `level`; a `(signal, date, level)` partition with at least `fanout`
//! blocks is merged into one block at `level + 1` (size-tiered, per
//! `ARCHITECTURE.md § Compaction`).
//!
//! This crate is the engine plus a thin CLI (`src/main.rs`). The standalone
//! [`compact_once`](engine::compact_once) entry point is single-instance: one
//! compactor, no lease. The v0.9 multi-instance daemon drives
//! [`compact_partition`](engine::compact_partition) instead, passing a
//! [`Fence`](scry_block::Fence) (the Valkey lease guard) so exactly one
//! instance commits a given partition's merge, and a
//! [`BlockEventSink`](scry_block::BlockEventSink) so peers converge. The
//! merge's `meta.json` PUT is the fenced commit point: a lost lease aborts
//! before it, leaving inputs intact (see [`merge_blocks`](merge::merge_blocks)).
//!
//! - [`policy`] — which blocks to merge ([`CompactConfig`],
//!   [`plan_merges`]).
//! - [`merge`] — read K inputs, stream-sort via DataFusion, rebuild
//!   sidecars, upload ([`merge_blocks`](merge::merge_blocks)).
//! - [`engine`] — the full per-merge lifecycle
//!   ([`compact_once`](engine::compact_once) /
//!   [`compact_partition`](engine::compact_partition)).

pub mod engine;
pub mod merge;
pub mod policy;

pub use engine::{compact_once, compact_partition, CompactReport, PartitionOutcome};
pub use policy::{plan_merges, CompactConfig, PlannedMerge};

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use scry_block::BlockBuilderConfig;
use scry_catalog::Catalog;
use scry_objstore::{open as open_objstore, ObjStoreConfig};

/// CLI arguments for the `scry compact` subcommand.
#[derive(Parser, Debug)]
#[command(about = "Size-tiered compaction: merge many small blocks into fewer larger ones")]
pub struct Args {
    /// Path to the SQLite catalog file. Created (with schema) if absent.
    #[arg(long)]
    pub catalog: PathBuf,

    /// Minimum blocks in a `(signal, date, level)` partition to trigger a
    /// merge, and how many are merged per pass (size-tiered fan-out).
    #[arg(short = 'k', long, default_value_t = 8)]
    pub fanout: usize,

    /// Don't compact blocks at or above this level (L3 is the practical
    /// ceiling).
    #[arg(long, default_value_t = 3)]
    pub max_level: u32,

    /// Seconds to wait between superseding inputs and deleting their
    /// objects. 0 is safe single-instance (queries skip superseded
    /// blocks immediately); raise it if other readers share the bucket.
    #[arg(long, default_value_t = 0)]
    pub grace: u64,

    /// Only compact this signal (e.g. `logs`). Default: all signals.
    #[arg(long)]
    pub signal: Option<String>,

    /// Skip the bucket reconcile before compacting. By default the
    /// catalog is reconciled from the bucket first so the tool works
    /// against a shared bucket without an online catalog.
    #[arg(long)]
    pub no_reconcile: bool,

    /// Loop forever, compacting every `--interval` seconds, instead of
    /// running a single pass and exiting.
    #[arg(long)]
    pub watch: bool,

    /// Seconds between passes in `--watch` mode.
    #[arg(long, default_value_t = 60)]
    pub interval: u64,
}

/// Run the standalone (single-instance) compaction tool: one pass, or a
/// `--watch` loop.
pub async fn run(args: Args) -> Result<()> {
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
