//! `scry-compactd` — the compaction daemon role.
//!
//! This crate is the CLI/daemon wrapper around the `scry-compact` engine,
//! mirroring the `scry-ingestd` / `scry-queryd` pattern: it owns the CLI
//! `Args`, builds the Valkey + convergence + status plumbing, and dispatches
//! to the engine.
//!
//! **Without `--valkey-url`** — the standalone single-instance path. Uses
//! [`compact_once`](scry_compact::compact_once): no lease, no peers, no
//! convergence. Reconciles from the bucket once at boot (unless
//! `--no-reconcile`). Backward-compatible with v0.8.
//!
//! **With `--valkey-url`** — a coordinated fleet member. Runs the full
//! three-tier catalog convergence (pub/sub consumer + cursor poll + full
//! walk), acquires per-partition leases via
//! [`run_compaction_pass`](scry_cluster::run_compaction_pass), and publishes
//! to the Fleet status page as `role = "compact"`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use object_store::ObjectStore;
use scry_block::BlockBuilderConfig;
use scry_catalog::Catalog;
use scry_compact::{
    compact_once, validate_against_catalog, warn_oversized, CompactConfig, CompactReport,
    CompactResources, ResourceConfig,
};
use scry_objstore::{open as open_objstore, ObjStoreConfig};
use tracing::{info, warn};
use uuid::Uuid;

pub mod memory;

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
    /// Default 0 without Valkey, 600 with Valkey.
    #[arg(long)]
    pub grace: Option<u64>,

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

    /// Shared compaction memory budget in MiB. If omitted, derive a
    /// conservative budget from the Linux cgroup limit or use a fixed fallback.
    #[arg(long)]
    pub memory_budget_mib: Option<u64>,

    /// Directory for bounded DataFusion spill files.
    #[arg(long)]
    pub spill_dir: Option<PathBuf>,

    /// Maximum spill-disk usage in MiB.
    #[arg(long, default_value_t = 4096)]
    pub spill_max_mib: u64,

    /// Bounded staged-output buffer size in MiB.
    #[arg(long, default_value_t = 8)]
    pub output_buffer_mib: u64,

    // ── Multi-instance (D-069) ──────────────────────────────────
    /// Valkey URL for per-partition lease coordination, pub/sub catalog
    /// convergence, and fleet status. Without this the tool compacts
    /// in single-instance mode (no lease, no peer awareness). With it
    /// the tool is a proper fleet member that coexists with `scry ingest
    /// --mode full`.
    #[arg(long)]
    pub valkey_url: Option<String>,

    /// Valkey key-namespace for this deployment. Two deployments sharing
    /// one Valkey need different namespaces to avoid contending for each
    /// other's leases. Same flag and semantics as on `scry ingest` /
    /// `scry query`.
    #[arg(long, default_value = "scry")]
    pub valkey_namespace: String,

    /// Lease TTL in seconds. Renewed at ttl/3; a crashed holder's
    /// partition frees up within this window.
    #[arg(long, default_value_t = 30)]
    pub lease_ttl: u64,

    /// Bind a local HTTP status dashboard (`/` + `/stats.json`). With
    /// Valkey the fleet page renders every instance; without it only
    /// this process.
    #[arg(long)]
    pub stats_listen: Option<String>,

    /// Seconds between incremental cursor convergence polls.
    #[arg(long, default_value_t = 5)]
    pub poll_interval: u64,

    /// Seconds between full catalog reconciliation walks (measured from
    /// the end of the previous walk, not on a fixed timer — D-066).
    #[arg(long, default_value_t = 1800)]
    pub full_walk_interval: u64,

    /// Disable restoring the catalog from a bucket snapshot on cold boot
    /// (D-055).
    #[arg(long)]
    pub no_snapshot_restore: bool,
}

/// All signals we subscribe to for catalog convergence.
const ALL_SIGNALS: &[&str] = &["metrics", "logs", "traces", "profiles", "dummy"];

/// How long to wait for the pub/sub consumer to establish its subscription
/// before giving up and proceeding. Bounded so a dead Valkey delays readiness
/// rather than preventing boot.
const CONSUMER_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Run the compaction tool: one-shot or `--watch`, single-instance or leased.
pub async fn run(mut args: Args) -> Result<()> {
    let obj_cfg = ObjStoreConfig::from_env()
        .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;
    let bucket = obj_cfg.bucket.clone();
    let store: Arc<dyn ObjectStore> = open_objstore(&obj_cfg).await?;
    let instance_id = Uuid::now_v7();

    // Fall back to env vars for Valkey (same convention as ingestd/queryd).
    if args.valkey_url.is_none() {
        args.valkey_url = std::env::var(scry_valkey::VALKEY_URL_ENV).ok();
    }
    if args.valkey_namespace == "scry" {
        if let Ok(ns) = std::env::var("SCRY_VALKEY_NAMESPACE") {
            args.valkey_namespace = ns;
        }
    }

    let grace_default = if args.valkey_url.is_some() { 600 } else { 0 };
    let compact_cfg = CompactConfig {
        fanout: args.fanout,
        max_level: args.max_level,
        grace: Duration::from_secs(args.grace.unwrap_or(grace_default)),
        signal_filter: args.signal.clone(),
        parallelism: args.parallelism,
    };
    compact_cfg
        .validate()
        .context("invalid compaction policy")?;
    let block_cfg = BlockBuilderConfig::default();
    let detected = memory::detect_cgroup_memory_limit();
    let budget = memory::resolve_memory_budget(args.memory_budget_mib, detected)?;
    let mut resource_cfg = ResourceConfig::from_envelope(budget.bytes);
    resource_cfg.spill_dir = args.spill_dir.clone();
    resource_cfg.spill_bytes = args
        .spill_max_mib
        .checked_mul(1024 * 1024)
        .context("--spill-max-mib is too large")?;
    resource_cfg.output_buffer_bytes = args
        .output_buffer_mib
        .checked_mul(1024 * 1024)
        .and_then(|v| usize::try_from(v).ok())
        .context("--output-buffer-mib is too large")?;
    let resources = CompactResources::new(resource_cfg)
        .context("constructing process-wide compaction resources")?;
    info!(
        source = %budget.source,
        cgroup_limit_bytes = ?budget.cgroup_limit_bytes,
        memory_budget_bytes = budget.bytes,
        "resolved compaction memory budget"
    );

    if args.valkey_url.is_some() {
        run_leased(
            args,
            store,
            bucket,
            instance_id,
            compact_cfg,
            block_cfg,
            resources,
        )
        .await
    } else {
        run_standalone(args, store, bucket, compact_cfg, block_cfg, resources).await
    }
}

// ── The standalone (single-instance) path ──────────────────────────────
async fn run_standalone(
    args: Args,
    store: Arc<dyn ObjectStore>,
    bucket: String,
    compact_cfg: CompactConfig,
    block_cfg: BlockBuilderConfig,
    resources: Arc<CompactResources>,
) -> Result<()> {
    let catalog = Catalog::open(&args.catalog, &bucket)
        .with_context(|| format!("opening catalog at {}", args.catalog.display()))?;

    if !args.no_reconcile {
        reconcile(&catalog, &store).await?;
        stage_recovered_reaps(&catalog, compact_cfg.grace)?;
    }
    report_stuck_partitions(&catalog, &compact_cfg)?;

    let catalog = Arc::new(Mutex::new(catalog));

    if args.watch {
        info!(
            interval_secs = args.interval,
            fanout = args.fanout,
            parallelism = args.parallelism,
            "starting compaction watch loop (single-instance, Ctrl-C to stop)"
        );
        loop {
            run_unfenced_pass(
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
                    info!("shutdown signalled; exiting watch loop");
                    break;
                }
            }
        }
    } else {
        run_unfenced_pass(
            &store,
            &catalog,
            &bucket,
            &compact_cfg,
            &block_cfg,
            resources.clone(),
        )
        .await?;
    }
    Ok(())
}

// ── The leased (multi-instance) path ───────────────────────────────────
async fn run_leased(
    args: Args,
    store: Arc<dyn ObjectStore>,
    bucket: String,
    instance_id: Uuid,
    compact_cfg: CompactConfig,
    block_cfg: BlockBuilderConfig,
    resources: Arc<CompactResources>,
) -> Result<()> {
    use scry_cluster::{apply_event, full_walk, poll_once, run_compaction_pass};
    use scry_server::{
        serve_status, CatalogGauge, CompactionResourceStats, FleetSource, LocalStatus,
        ServerMetrics, CATALOG_GAUGE_INTERVAL,
    };
    use scry_valkey::{
        discover_status_blobs, parse_envelope, subscribe_blocks, Keyspace, StatusRegistration,
        ValkeyClient, ValkeyLeaseProvider, ValkeySink, STATUS_TTL,
    };
    use tokio::sync::broadcast::error::RecvError;

    let valkey_url = args.valkey_url.as_deref().unwrap();
    let valkey_keys =
        Keyspace::new(&args.valkey_namespace).context("invalid --valkey-namespace")?;

    // ── Catalog bootstrap (snapshot restore, then open) ─────────────
    let catalog_was_absent = !args.catalog.exists();
    let mut needs_cold_seed = args.no_snapshot_restore && catalog_was_absent;
    if !args.no_snapshot_restore && catalog_was_absent {
        match scry_catalog::restore_snapshot(
            &args.catalog,
            store.as_ref(),
            scry_catalog::CATALOG_SCHEMA_VERSION,
        )
        .await
        {
            Ok(scry_catalog::RestoreOutcome::Restored { blocks }) => {
                info!(blocks, "restored catalog from bucket snapshot");
            }
            Ok(scry_catalog::RestoreOutcome::NoSnapshot) => {
                needs_cold_seed = true;
                info!("no catalog snapshot in bucket; a full catalog seed is required");
            }
            Ok(scry_catalog::RestoreOutcome::VersionMismatch { found, expected }) => {
                needs_cold_seed = true;
                warn!(
                    found,
                    expected, "catalog snapshot schema version mismatch; seeding from bucket"
                );
            }
            Err(e) => {
                needs_cold_seed = true;
                warn!(error = %e, "catalog snapshot restore failed; seeding from bucket");
            }
        }
    }

    let catalog = Arc::new(Mutex::new(
        Catalog::open(&args.catalog, &bucket)
            .with_context(|| format!("opening catalog at {}", args.catalog.display()))?,
    ));
    let conv_catalog = catalog.clone();

    // ── Valkey client, sink, lease provider ──────────────────────────
    let valkey = ValkeyClient::connect(valkey_url, instance_id, valkey_keys.clone())
        .await
        .context("connecting to Valkey")?;
    let (raw_sink, sink_task) = ValkeySink::spawn(valkey.clone(), instance_id);
    let sink: Arc<dyn scry_block::BlockEventSink> = Arc::new(raw_sink);
    let provider = ValkeyLeaseProvider::new(valkey.clone());

    // ── pub/sub convergence consumer (before the seed walk) ────────
    let mut bg_tasks: Vec<tokio::task::JoinHandle<()>> = vec![sink_task];
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let cat = conv_catalog.clone();
        let url = valkey_url.to_string();
        let keys = valkey_keys.clone();
        bg_tasks.push(tokio::spawn(async move {
            let mut subscribed = Some(tx);
            loop {
                match subscribe_blocks(&url, &keys, ALL_SIGNALS).await {
                    Ok((_sub, mut rx_chan)) => {
                        info!("subscribed to block-event channels for catalog convergence");
                        if let Some(tx) = subscribed.take() {
                            let _ = tx.send(());
                        }
                        loop {
                            match rx_chan.recv().await {
                                Ok(msg) => {
                                    if let Some(env) = parse_envelope(&msg) {
                                        if let Err(e) = apply_event(cat.as_ref(), &env.event) {
                                            warn!(error = %e, "applying block event failed");
                                        }
                                    }
                                }
                                Err(RecvError::Lagged(n)) => {
                                    warn!(skipped = n, "convergence consumer lagged");
                                }
                                Err(RecvError::Closed) => {
                                    warn!("convergence subscription closed; reconnecting");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => warn!(error = %e, "subscribing to block channels failed; retrying"),
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }));
    }

    // Wait for the subscription before the seed walk.
    match tokio::time::timeout(CONSUMER_READY_TIMEOUT, rx).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => warn!("block-event consumer stopped before it subscribed"),
        Err(_) => warn!(
            timeout_secs = CONSUMER_READY_TIMEOUT.as_secs(),
            "block-event subscription not established in time; continuing"
        ),
    }

    // ── Cold seed ───────────────────────────────────────────────────
    if needs_cold_seed {
        info!("catalog seed starting from bucket");
        let started = std::time::Instant::now();
        let report = full_walk(store.as_ref(), catalog.as_ref(), &bucket)
            .await
            .context("seeding catalog from bucket")?;
        info!(
            seen = report.seen,
            inserted = report.inserted,
            failed = report.failed,
            elapsed_secs = started.elapsed().as_secs_f64(),
            "catalog seed complete"
        );
    }

    // Apply peers' staged deletions before starting compaction.
    match scry_valkey::converge_staged_deletions(&valkey, conv_catalog.as_ref()).await {
        Ok(n) if n > 0 => info!(staged = n, "applied peers' staged deletions at boot"),
        Ok(_) => {}
        Err(e) => warn!(error = %e, "applying peers' staged deletions failed"),
    }

    // Report stuck partitions after the catalog is settled.
    {
        let live = catalog
            .lock()
            .expect("catalog mutex poisoned")
            .list_blocks()
            .context("list live blocks")?;
        let stuck = validate_against_catalog(&live, &compact_cfg);
        if !stuck.is_empty() {
            warn_oversized(&stuck);
        }
    }

    // ── Metrics, gauge, status ─────────────────────────────────────
    let gauge = CatalogGauge::new(args.catalog.clone());
    bg_tasks.push(gauge.clone().spawn(CATALOG_GAUGE_INTERVAL));

    let compaction_progress = Arc::new(scry_block::CompactionProgress::new());
    let resource_stats = Arc::new(CompactionResourceStats::default());
    update_resource_stats(&resource_stats, &resources);
    let metrics = Arc::new(
        ServerMetrics::new(0)
            .with_identity(instance_id.to_string(), String::new())
            .with_role("compact")
            .with_catalog_gauge(gauge)
            .with_compaction_progress(compaction_progress.clone())
            .with_compaction_resource_stats(resource_stats.clone()),
    );
    metrics.configure_compaction(true, compact_cfg.grace);

    // Fleet publication.
    let _status_reg = {
        let m = metrics.clone();
        let producer: scry_valkey::StatusProducer =
            Arc::new(move || serde_json::to_string(&m.snapshot()).unwrap_or_default());
        Some(
            StatusRegistration::spawn(&valkey, instance_id, STATUS_TTL, producer)
                .await
                .context("registering compact status in Valkey")?,
        )
    };

    // Local HTTP dashboard.
    let shutdown = scry_server::shutdown::channel();
    if let Some(addr) = args.stats_listen.clone() {
        struct ValkeyFleet(ValkeyClient);
        #[async_trait::async_trait]
        impl FleetSource for ValkeyFleet {
            async fn blobs(&self) -> Vec<String> {
                discover_status_blobs(&self.0).await.unwrap_or_default()
            }
        }
        let fleet: Option<Arc<dyn FleetSource>> = Some(Arc::new(ValkeyFleet(valkey.clone())));
        let local: Arc<dyn LocalStatus> = metrics.clone();
        let self_id = instance_id.to_string();
        let s = shutdown.clone();
        bg_tasks.push(tokio::spawn(async move {
            if let Err(e) =
                serve_status(addr, local, fleet, self_id, scry_server::shutdown::wait(s)).await
            {
                warn!(error = %e, "status endpoint failed");
            }
        }));
    }

    // ── Convergence loops ──────────────────────────────────────────
    let staged_client = Some(valkey.clone());

    // Incremental cursor poller.
    {
        let staged_client = staged_client.clone();
        let store = store.clone();
        let bucket = bucket.clone();
        let cat = conv_catalog.clone();
        let interval = Duration::from_secs(args.poll_interval.max(1));
        bg_tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                match poll_once(store.as_ref(), cat.as_ref(), &bucket).await {
                    Ok(r) if r.inserted > 0 => {
                        info!(inserted = r.inserted, "convergence poll applied new blocks")
                    }
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "convergence poll failed"),
                }
                scry_valkey::converge_staged_deletions_logged(
                    staged_client.as_ref(),
                    cat.as_ref(),
                    "poll",
                )
                .await;
            }
        }));
    }

    // Periodic full walk (sleep-after-completion per D-066).
    {
        let staged_client = staged_client.clone();
        let store = store.clone();
        let bucket = bucket.clone();
        let cat = conv_catalog.clone();
        let interval = Duration::from_secs(args.full_walk_interval.max(1));
        bg_tasks.push(tokio::spawn(async move {
            loop {
                match full_walk(store.as_ref(), cat.as_ref(), &bucket).await {
                    Ok(r) if r.inserted > 0 => info!(
                        inserted = r.inserted,
                        seen = r.seen,
                        "convergence full-walk applied new blocks"
                    ),
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "convergence full-walk failed"),
                }
                scry_valkey::converge_staged_deletions_logged(
                    staged_client.as_ref(),
                    cat.as_ref(),
                    "full-walk",
                )
                .await;
                tokio::time::sleep(interval).await;
            }
        }));
    }

    // ── Compaction loop ────────────────────────────────────────────
    let lease_ttl = Duration::from_secs(args.lease_ttl.max(1));
    let interval = Duration::from_secs(args.interval.max(1));
    info!(
        interval_secs = interval.as_secs(),
        fanout = compact_cfg.fanout,
        parallelism = compact_cfg.parallelism,
        grace_secs = compact_cfg.grace.as_secs(),
        lease_ttl_secs = lease_ttl.as_secs(),
        "starting leased compaction loop (role=compact)"
    );

    loop {
        let started = std::time::Instant::now();
        update_resource_stats(&resource_stats, &resources);
        match run_compaction_pass(
            &provider,
            store.clone(),
            catalog.as_ref(),
            &bucket,
            &compact_cfg,
            &block_cfg,
            &*sink,
            lease_ttl,
            Some(&compaction_progress),
            resources.clone(),
        )
        .await
        {
            Ok(r)
                if r.merges > 0
                    || r.partition_failed > 0
                    || r.resource_failed > 0
                    || r.oversized > 0 =>
            {
                let duration = started.elapsed();
                info!(
                    merges = r.merges,
                    blocks_in = r.blocks_in,
                    blocks_out = r.blocks_out,
                    bytes_out = r.bytes_out,
                    reaped = r.reaped,
                    reap_failed = r.reap_failed,
                    partition_failed = r.partition_failed,
                    lease_held = r.lease_held,
                    lease_unavailable = r.lease_unavailable,
                    oversized = r.oversized,
                    elapsed_ms = duration.as_millis() as u64,
                    "compaction pass completed"
                );
                record_compaction_metrics(&metrics, &r, duration);
            }
            Ok(r) => record_compaction_metrics(&metrics, &r, started.elapsed()),
            Err(e) => {
                let duration = started.elapsed();
                metrics.record_compaction_failure(duration);
                warn!(error = %e, "compaction pass failed");
            }
        }
        update_resource_stats(&resource_stats, &resources);

        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signalled; exiting compaction loop");
                break;
            }
        }
    }

    Ok(())
}

fn update_resource_stats(
    stats: &scry_server::CompactionResourceStats,
    resources: &CompactResources,
) {
    let cfg = resources.config();
    let t = resources.telemetry();
    stats.update(
        cfg.envelope_bytes,
        cfg.datafusion_memory_bytes,
        t.datafusion_reserved_bytes as u64,
        cfg.non_datafusion_memory_bytes,
        t.weighted_running_bytes,
        t.weighted_waiters as u64,
        cfg.spill_bytes,
        t.spill_used_bytes,
        t.spill_active_files as u64,
        t.admissions,
        t.rejected,
        t.cumulative_wait_micros,
    );
}

fn record_compaction_metrics(
    metrics: &scry_server::ServerMetrics,
    report: &CompactReport,
    duration: Duration,
) {
    use scry_server::CompactionPassStats;
    metrics.record_compaction_pass(
        CompactionPassStats {
            merges: report.merges as u64,
            blocks_in: report.blocks_in as u64,
            blocks_out: report.blocks_out as u64,
            bytes_out: report.bytes_out,
            aborted: report.aborted as u64,
            reaped: report.reaped as u64,
            reap_failed: report.reap_failed as u64,
            partition_failed: report.partition_failed as u64,
            resource_failed: report.resource_failed as u64,
            lease_held: report.lease_held as u64,
            lease_unavailable: report.lease_unavailable as u64,
            oversized: report.oversized as u64,
        },
        duration,
    );
}

// ── Helpers shared by both paths ───────────────────────────────────────

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
        info!(
            staged,
            grace_secs = grace.as_secs(),
            "staged superseded blocks recovered from the bucket for cleanup"
        );
    }
    Ok(())
}

fn report_stuck_partitions(catalog: &Catalog, cfg: &CompactConfig) -> Result<()> {
    let live = catalog.list_blocks().context("list live blocks")?;
    let stuck = validate_against_catalog(&live, cfg);
    if stuck.is_empty() {
        return Ok(());
    }
    warn!(
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

async fn reconcile(catalog: &Catalog, store: &Arc<dyn ObjectStore>) -> Result<()> {
    let report = catalog.reconcile_from_bucket(store.as_ref()).await?;
    info!(
        seen = report.seen,
        inserted = report.inserted,
        already_present = report.already_present,
        failed = report.failed,
        "reconcile complete"
    );
    Ok(())
}

async fn run_unfenced_pass(
    store: &Arc<dyn ObjectStore>,
    catalog: &Arc<Mutex<Catalog>>,
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
        info!(oversized = report.oversized, "nothing to compact this pass");
    } else {
        info!(
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
