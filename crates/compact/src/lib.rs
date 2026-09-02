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
pub mod resource;

pub use engine::{
    compact_once, compact_partition, reap_pending, warn_oversized, CompactReport, PartitionOutcome,
};
pub use merge::merge_blocks;
pub use policy::{
    plan_merges, projected_ancestry_len, validate_against_catalog, CompactConfig, CompactionPlan,
    OversizedPartition, PlannedMerge,
};
pub use resource::{
    CompactResources, ResourceConfig, ResourceError, ResourcePermit, ResourceTelemetry,
};

use std::path::PathBuf;
use std::sync::Arc;
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

    /// Maximum partitions to merge concurrently. Each partition merges
    /// independent blocks and (with Valkey) takes its own lease, so
    /// parallelism multiplies throughput with no data-level conflict.
    #[arg(long, default_value_t = 1)]
    pub parallelism: usize,
}

/// Run the standalone (single-instance) compaction tool: one pass, or a
/// `--watch` loop.
pub async fn run(args: Args) -> Result<()> {
    let obj_cfg = ObjStoreConfig::from_env()
        .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;
    let bucket = obj_cfg.bucket.clone();
    let store = open_objstore(&obj_cfg).await?;

    let catalog = Catalog::open(&args.catalog, &bucket)
        .with_context(|| format!("opening catalog at {}", args.catalog.display()))?;

    let compact_cfg = CompactConfig {
        fanout: args.fanout,
        max_level: args.max_level,
        grace: Duration::from_secs(args.grace),
        signal_filter: args.signal.clone(),
        parallelism: args.parallelism,
    };
    compact_cfg
        .validate()
        .context("invalid compaction policy")?;
    let block_cfg = BlockBuilderConfig::default();
    let resources = CompactResources::new(ResourceConfig::default())
        .context("constructing compaction resource envelope")?;

    // Bring the catalog in line with the bucket once before compacting,
    // so the tool works against a shared bucket with no online catalog.
    if !args.no_reconcile {
        reconcile(&catalog, &store).await?;
        // `reconcile_from_bucket` applies lineage — a merge that committed
        // its `meta.json` and then crashed leaves its inputs correctly marked
        // superseded, so queries already read the merged block. But it does
        // not stage them for physical cleanup, and `list_pending_reaps`
        // requires `reap_eligible_at IS NOT NULL`: without this the recovered
        // inputs' objects are invisible to every reaper and leak forever.
        //
        // The multi-instance path stages inside `reconcile_partition` /
        // `full_walk`; the standalone CLI is the one flow that had no
        // equivalent. Staged here rather than inside `reconcile_from_bucket`
        // so read-only tools (`scry list`, `scry get`) don't schedule
        // deletions as a side effect of looking at the bucket.
        stage_recovered_reaps(&catalog, compact_cfg.grace)?;
    }

    // `CompactConfig::validate` above only proves a *uniform* tree fits. Now
    // that the catalog reflects the bucket, check the blocks that actually
    // exist — a fanout changed between runs can leave partitions that can
    // never compact again, and the operator should hear that at startup, not
    // infer it from a partition that quietly stops shrinking.
    report_stuck_partitions(&catalog, &compact_cfg)?;

    // Wrapped for shared access across concurrent partitions.
    let catalog = Arc::new(std::sync::Mutex::new(catalog));

    if args.watch {
        tracing::info!(
            interval_secs = args.interval,
            fanout = args.fanout,
            parallelism = args.parallelism,
            "starting compaction watch loop (Ctrl-C to stop)"
        );
        loop {
            run_pass(
                &store,
                &catalog,
                &bucket,
                &compact_cfg,
                &block_cfg,
                resources.clone(),
            )
            .await?;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(args.interval)) => {}
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("shutdown signalled; exiting watch loop");
                    break;
                }
            }
        }
    } else {
        run_pass(
            &store,
            &catalog,
            &bucket,
            &compact_cfg,
            &block_cfg,
            resources,
        )
        .await?;
    }

    Ok(())
}

/// Give lineage-superseded rows learned from bucket reconciliation the same
/// `reap_eligible_at` a `Superseded` event would have, so the pass's existing
/// `list_pending_reaps` can clean up after an interrupted merge.
///
/// Idempotent (`COALESCE` — an already-staged row keeps its deadline), so
/// running it every invocation is safe.
fn stage_recovered_reaps(catalog: &Catalog, grace: Duration) -> Result<()> {
    let eligible = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        + grace)
        .as_nanos() as u64;
    let staged = catalog
        .stage_unstaged_superseded(eligible)
        .context("stage reconciled superseded rows for reaping")?;
    if staged > 0 {
        tracing::info!(
            staged,
            grace_secs = grace.as_secs(),
            "staged superseded blocks recovered from the bucket for cleanup"
        );
    }
    Ok(())
}

/// Startup check: warn about partitions whose next merge cannot encode its
/// ancestry under the configured policy. Deliberately **not** fatal — the
/// other partitions still compact fine, and refusing to start would turn a
/// localized stall into a total outage of compaction.
fn report_stuck_partitions(catalog: &Catalog, cfg: &CompactConfig) -> Result<()> {
    let live = catalog.list_blocks().context("list live blocks")?;
    let stuck = policy::validate_against_catalog(&live, cfg);
    if stuck.is_empty() {
        return Ok(());
    }
    tracing::warn!(
        partitions = stuck.len(),
        fanout = cfg.fanout,
        max_level = cfg.max_level,
        limit = scry_block::MAX_COMPACTED_ANCESTORS,
        "existing blocks cannot compact under this policy; they will be skipped every pass \
         (were they built with a different --fanout?)"
    );
    warn_oversized(&stuck);
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
    catalog: &Arc<std::sync::Mutex<Catalog>>,
    bucket: &str,
    compact_cfg: &CompactConfig,
    block_cfg: &BlockBuilderConfig,
    resources: Arc<CompactResources>,
) -> Result<()> {
    let report = compact_once(
        store.clone(),
        catalog,
        bucket,
        compact_cfg,
        block_cfg,
        resources,
    )
    .await?;
    if report.merges == 0 {
        tracing::info!(oversized = report.oversized, "nothing to compact this pass");
    } else {
        tracing::info!(
            merges = report.merges,
            blocks_in = report.blocks_in,
            blocks_out = report.blocks_out,
            bytes_out = report.bytes_out,
            oversized = report.oversized,
            "compaction pass complete"
        );
    }
    Ok(())
}
