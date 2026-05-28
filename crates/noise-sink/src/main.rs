//! noise-sink — thin CLI shell around `scry-server`.
//!
//! Parses flags, optionally constructs a `DummyPipeline` (WAL + parquet
//! builder + optional online catalog targeting object storage), then
//! hands everything to `scry-server::Server::serve_with_shutdown`.
//! Ctrl-C triggers a graceful flush of the in-progress block.
//!
//! Run (no storage):
//!   noise-sink --listen 127.0.0.1:4000
//!
//! Run (v0.1 storage path):
//!   source docker/garage/.env
//!   noise-sink --listen 127.0.0.1:4000 --storage --wal-dir ./wal

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use scry_catalog::Catalog;
use scry_objstore::{open as open_objstore, ObjStoreConfig};
use scry_server::{
    decode, serve_stats, BlockBuilderConfig, DummyShards, LogsShards, MetricsShards, Server,
    ServerConfig, ServerMetrics, ShardedPipeline, INGEST_SHARDS,
};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, Semaphore};
use tracing::{info, warn};
use uuid::Uuid;

/// Swap glibc's malloc for mimalloc.
///
/// The ingest hot path makes ~2 M small allocations/sec in the
/// happy-path steady state (parquet build buffer, zstd inflate, batch
/// payload Vecs, tracing string interpolation). glibc's per-thread
/// arenas hold a high-water mark and rarely return memory to the OS,
/// which kept measured RSS pinned ~3× the live working set across
/// stress runs. mimalloc decommits aggressively and runs the small-
/// allocation path noticeably faster. One line, no behavioural change
/// — the only artefact is a smaller, less ragged RSS curve.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Listen address.
    #[arg(long, default_value = "127.0.0.1:4000")]
    listen: String,

    /// writer_id reported in HelloAck. Default: random per-process.
    #[arg(long)]
    writer_id: Option<String>,

    /// Enable the v0.1 storage path: Dummy batches are durably
    /// recorded in the WAL, accumulated into parquet blocks, and
    /// uploaded to object storage. Requires `--wal-dir` and the
    /// `SCRY_OBJSTORE_*` env vars (see `docker/garage/.env`).
    #[arg(long)]
    storage: bool,

    /// Root directory for the WAL. A `dummy/` subdirectory is created
    /// for v0.1; real signals get their own subdirs later. Required
    /// when `--storage` is set.
    #[arg(long)]
    wal_dir: Option<PathBuf>,

    /// Path to the SQLite catalog file. If provided, every uploaded
    /// block is recorded into the catalog inline (no reconcile loop
    /// needed for catalog freshness). The file is created with the
    /// canonical schema if it doesn't already exist. Optional —
    /// scry-list can always rebuild the catalog from the bucket via
    /// `reconcile_from_bucket`.
    #[arg(long)]
    catalog: Option<PathBuf>,

    /// Optional address for the live stats HTTP endpoint (e.g.
    /// `127.0.0.1:4098`). Serves a self-updating dashboard at `/` and a
    /// JSON snapshot at `/stats.json` (ingest rates, per-signal upload
    /// state, RSS, and a bottleneck classification that flags when we're
    /// bounded by S3 upload speed). When unset, no stats server runs and
    /// the ingest path pays no metrics cost. Bind to loopback — there's
    /// no auth.
    #[arg(long)]
    stats_listen: Option<String>,

    /// Block compression mode (only meaningful with `--storage`). The
    /// parquet encode is CPU-bound on zstd, so this is the dial between
    /// ingest throughput and stored size:
    ///   `dense` = zstd-3 (default) — smallest blocks; right when the
    ///             object-store upload is the bottleneck.
    ///   `fast`  = zstd-1 — ~1.5× encode throughput for ~+31% bytes;
    ///             right when the box is CPU-bound on encode.
    ///   `auto`  = pick per block from live load: when the upload pool is
    ///             full the bucket is the wall, so compress dense; when
    ///             uploads have slack but the box is CPU-busy, drop to
    ///             fast; when there's CPU headroom, dense.
    #[arg(long, value_enum, default_value_t = Compression::Dense)]
    compression: Compression,
}

/// Block compression mode. Maps to a zstd level; see `--compression`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Compression {
    /// zstd-3: smallest blocks (default).
    Dense,
    /// zstd-1: faster encode, larger blocks.
    Fast,
    /// Pick per block from live load (see `--compression`).
    Auto,
}

impl Compression {
    /// The static ZSTD level. For `Auto` this is the baseline the active
    /// builder is constructed with; the real level is chosen per block at
    /// close time (see `ShardedPipeline` adaptive compression), so the
    /// baseline only matters until the first rotation — `3` (dense) is the
    /// safe default there.
    fn zstd_level(self) -> i32 {
        match self {
            Compression::Dense | Compression::Auto => 3,
            Compression::Fast => 1,
        }
    }

    /// Whether the server should choose each block's level from live load.
    fn is_adaptive(self) -> bool {
        matches!(self, Compression::Auto)
    }
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

    let writer_id = args
        .writer_id
        .unwrap_or_else(|| format!("noise-sink-{}", rand_short()));
    let writer_uuid = Uuid::now_v7();

    // Shared upload-concurrency cap = physical core count. The dominant
    // cost of closing a block is the parquet encode (sort + Arrow build +
    // zstd), which is CPU-bound and runs on the blocking pool. Sizing the
    // pool to physical cores (not logical — hyperthreads don't scale for
    // this kind of saturating compress) and sharing it across all signals
    // lets one hot signal use every core while still bounding the number
    // of blocks held in memory at once.
    let upload_concurrency = num_cpus::get_physical().max(1);
    info!(
        physical_cores = upload_concurrency,
        "upload concurrency cap (shared encode+upload pool across all signals)"
    );

    // Process-global ingest stats. Only built when the stats endpoint is
    // enabled, so the no-stats path is byte-for-byte the old behaviour.
    // When present, it's shared three ways: the ingest path bumps its
    // counters, each signal's pipeline reports its upload gauges into
    // it, and the stats HTTP server reads snapshots from it.
    let stats_metrics: Option<Arc<ServerMetrics>> = args
        .stats_listen
        .as_ref()
        .map(|_| Arc::new(ServerMetrics::new(upload_concurrency)));

    // Build the storage pipelines up front. Failing fast on a missing
    // bucket or unreadable WAL dir is much better than failing on the
    // first batch from an agent that's already mid-stream. All signals
    // share the same object store + catalog; each gets its own WAL
    // subdir (`<wal>/dummy/`, `<wal>/metrics/`, `<wal>/logs/`) via the
    // `BlockBuilder::SIGNAL` constant inside `Pipeline::open`.
    type Pipelines = (
        Option<DummyShards>,
        Option<MetricsShards>,
        Option<LogsShards>,
    );
    let (dummy_pipeline, metrics_pipeline, logs_pipeline): Pipelines = if args.storage {
        let wal_dir = args
            .wal_dir
            .clone()
            .context("--storage requires --wal-dir")?;
        let cfg = ObjStoreConfig::from_env()
            .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;
        let bucket = cfg.bucket.clone();
        info!(
            endpoint = %cfg.endpoint,
            bucket   = %bucket,
            wal_dir  = %wal_dir.display(),
            catalog  = ?args.catalog,
            "storage mode: WAL + parquet blocks → object storage (dummy + metrics + logs)"
        );
        let store = open_objstore(&cfg)?;
        let catalog = match args.catalog.as_ref() {
            Some(p) => Some(Arc::new(Mutex::new(
                Catalog::open(p, &bucket)
                    .with_context(|| format!("opening catalog at {}", p.display()))?,
            ))),
            None => None,
        };
        // One semaphore, shared by every signal's pipeline (and every
        // shard) — the global encode+upload concurrency cap (see
        // `upload_concurrency` above). Sharing it across shards is what
        // keeps total in-flight encodes bounded even with N×signals
        // independent ingest pipelines.
        let upload_sem = Arc::new(Semaphore::new(upload_concurrency));
        // Block config: default close triggers, plus the chosen zstd
        // level (see `--compression`). Shared by every signal/shard.
        let block_cfg = BlockBuilderConfig {
            compression_level: args.compression.zstd_level(),
            ..Default::default()
        };
        // Adaptive mode (`--compression auto`): every shard picks its
        // closing block's ZSTD level from live load instead of the static
        // baseline above.
        let adaptive_compression = args.compression.is_adaptive();
        info!(
            shards = INGEST_SHARDS,
            compression = ?args.compression,
            zstd_level = block_cfg.compression_level,
            adaptive_compression,
            "per-signal ingest sharding (connections striped across shards by session id)"
        );
        // Each signal becomes INGEST_SHARDS independent pipelines, one
        // WAL subtree per shard, all sharing store/catalog/sem and the
        // per-signal upload-stats gauge (so the endpoint aggregates
        // across shards).
        let dummy = ShardedPipeline::open_with_config(
            INGEST_SHARDS,
            wal_dir.clone(),
            store.clone(),
            catalog.clone(),
            writer_uuid,
            decode::dummy,
            block_cfg,
            upload_sem.clone(),
            stats_metrics.as_ref().map(|m| m.dummy_upload()),
            adaptive_compression,
        )
        .await?;
        let metrics = ShardedPipeline::open_with_config(
            INGEST_SHARDS,
            wal_dir.clone(),
            store.clone(),
            catalog.clone(),
            writer_uuid,
            decode::metrics,
            block_cfg,
            upload_sem.clone(),
            stats_metrics.as_ref().map(|m| m.metrics_upload()),
            adaptive_compression,
        )
        .await?;
        let logs = ShardedPipeline::open_with_config(
            INGEST_SHARDS,
            wal_dir,
            store,
            catalog,
            writer_uuid,
            decode::logs,
            block_cfg,
            upload_sem.clone(),
            stats_metrics.as_ref().map(|m| m.logs_upload()),
            adaptive_compression,
        )
        .await?;
        (Some(dummy), Some(metrics), Some(logs))
    } else {
        if args.wal_dir.is_some() {
            warn!("--wal-dir set but --storage is not; ignoring WAL");
        }
        if args.catalog.is_some() {
            warn!("--catalog set but --storage is not; ignoring catalog");
        }
        (None, None, None)
    };

    let mut server = Server::new(
        ServerConfig {
            listen_addr: args.listen,
            writer_id,
            writer_uuid,
        },
        dummy_pipeline,
        metrics_pipeline,
        logs_pipeline,
    );
    if let Some(m) = stats_metrics.as_ref() {
        server = server.with_metrics(m.clone());
    }

    // Optional stats HTTP endpoint. It shares its shutdown signal with
    // the ingest server: both listen for Ctrl-C independently (tokio's
    // `ctrl_c()` resolves every pending future on SIGINT), so a single
    // Ctrl-C drains the ingest pipeline *and* stops the stats server.
    let stats_task = match (args.stats_listen.clone(), stats_metrics.clone()) {
        (Some(addr), Some(metrics)) => Some(tokio::spawn(async move {
            if let Err(e) = serve_stats(addr, metrics, async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await
            {
                warn!(error = %e, "stats endpoint failed");
            }
        })),
        _ => None,
    };

    let serve_result = server
        .serve_with_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;

    if let Some(task) = stats_task {
        let _ = task.await;
    }
    serve_result
}

fn rand_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    format!("{:08x}", ns & 0xFFFF_FFFF)
}
