//! scry-ingestd — the scry ingest server daemon; a thin CLI shell around
//! `scry-server`.
//!
//! Parses flags, optionally constructs a `DummyPipeline` (WAL + parquet
//! builder + optional online catalog targeting object storage), then
//! hands everything to `scry-server::Server::serve_with_shutdown`.
//! Ctrl-C / SIGTERM triggers a graceful flush of the in-progress block.
//!
//! Run (no storage):
//!   scry-ingestd --listen 127.0.0.1:4000
//!
//! Run (storage path):
//!   source docker/garage/.env
//!   scry-ingestd --listen 127.0.0.1:4000 --storage --wal-dir ./wal

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use object_store::ObjectStore;
use scry_block::{BlockEventSink, NoopSink};
use scry_catalog::Catalog;
use scry_cluster::{
    apply_event, full_walk, poll_once, run_compaction_pass, run_retention_pass, LeaseProvider,
    LocalLeaseProvider,
};
use scry_compact::CompactConfig;
use scry_objstore::{open as open_objstore, ObjStoreConfig};
use scry_retention::RetentionConfig;
use scry_server::{
    decode, serve_stats, BlockBuilderConfig, DummyShards, LiveRing, LogsShards, MetricsShards,
    ProfilesShards, Server, ServerConfig, ServerMetrics, ShardedPipeline, TracesShards,
    INGEST_SHARDS,
};
use scry_valkey::{
    parse_envelope, subscribe_blocks, TailRegistration, ValkeyClient, ValkeyLeaseProvider,
    ValkeySink, VALKEY_URL_ENV,
};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tracing::{info, warn};
use uuid::Uuid;

/// The convergence/maintenance channels cover every signal that has a
/// pipeline (Dummy included — the smoke harness exercises it).
const ALL_SIGNALS: [&str; 5] = ["dummy", "metrics", "logs", "traces", "profiles"];

/// CLI arguments for the `scry ingest` subcommand.
#[derive(Parser, Debug)]
#[command(about = "Ingest server daemon (native wire; optional storage + multi-instance)")]
pub struct Args {
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

    /// Override the per-block row-count seal trigger
    /// (`BlockBuilderConfig::max_rows`, default 1,000,000). Lower it to
    /// force a block to close after fewer rows — handy for tests that
    /// need several blocks from a modest ingest volume (e.g. exercising
    /// the per-block body-bloom skip across multiple blocks). Unset or 0
    /// keeps the default. Only meaningful with `--storage`.
    #[arg(long)]
    block_max_rows: Option<u64>,

    /// Maximum age (seconds) of an open block before it's sealed +
    /// uploaded regardless of size. Without this a low-volume or idle
    /// signal never crosses the size-based seal threshold
    /// (`block_max_rows` / 128 MiB), so its records sit in RAM (and
    /// re-replay from the WAL on every restart) and never become
    /// queryable. Default 60s. Set to 0 to disable (size-only sealing).
    /// Only meaningful with `--storage`.
    #[arg(long, default_value_t = 60)]
    block_max_age_secs: u64,

    // ---- Multi-instance coordination (v0.9) ----------------------------
    /// Valkey URL for multi-instance coordination (lease + pub/sub
    /// convergence). Falls back to `$SCRY_VALKEY_URL`. When neither is set
    /// the daemon runs a correct single-instance path: catalog convergence
    /// falls back to polling/full-walk and maintenance pauses (no lease ⇒
    /// no destructive work) unless `--allow-unfenced-maintenance`.
    #[arg(long)]
    valkey_url: Option<String>,

    /// Instance role. `full` ingests, converges, and contends for
    /// maintenance leases; `ingest-only` ingests + converges but never runs
    /// maintenance; `query-only` only converges (no ingest pipelines, no
    /// maintenance — `--storage` still builds catalogs for the convergence
    /// loops). Default `full`.
    #[arg(long, value_enum, default_value_t = Mode::Full)]
    mode: Mode,

    /// Force-disable the maintenance loop even in `full` mode.
    #[arg(long)]
    no_maintenance: bool,

    /// Run maintenance even without a Valkey lease (single-instance:
    /// asserts sole ownership). Without this, maintenance pauses when Valkey
    /// is absent. Dangerous with >1 instance on one bucket.
    #[arg(long)]
    allow_unfenced_maintenance: bool,

    /// Seconds between catalog convergence polls (incremental cursor poll).
    #[arg(long, default_value_t = 5)]
    poll_interval: u64,

    /// Seconds between exhaustive full-walk reconciles (the ultimate
    /// convergence backstop; discovers brand-new prefixes).
    #[arg(long, default_value_t = 1800)]
    full_walk_interval: u64,

    /// Seconds between compaction passes.
    #[arg(long, default_value_t = 60)]
    compact_interval: u64,

    /// Seconds between retention passes.
    #[arg(long, default_value_t = 3600)]
    retention_interval: u64,

    /// Lease TTL (seconds) for compaction/retention partitions. Renewed at
    /// ttl/3; a held lease auto-expires server-side this long after the last
    /// renew, so a crashed holder's partition frees up within ~this window.
    #[arg(long, default_value_t = 30)]
    lease_ttl: u64,

    /// Number of blocks merged per compaction step (the `fanout` smallest in
    /// a `(signal, date, level)` partition).
    #[arg(long, default_value_t = 8)]
    compact_fanout: usize,

    /// Grace seconds between superseding merged-away inputs and deleting
    /// their objects. Default 0 single-instance; 600 when a Valkey lease
    /// provider is present (peers may still hold stale catalog rows briefly).
    #[arg(long)]
    compact_grace: Option<u64>,

    /// Blanket retention TTL applied to every signal (opt-in: omit to leave
    /// all signals un-reaped). Accepts `s`/`m`/`h`/`d` suffixes.
    #[arg(long, value_parser = parse_duration)]
    ttl: Option<Duration>,
    /// Per-signal retention TTL override (metrics).
    #[arg(long, value_parser = parse_duration)]
    ttl_metrics: Option<Duration>,
    /// Per-signal retention TTL override (logs).
    #[arg(long, value_parser = parse_duration)]
    ttl_logs: Option<Duration>,
    /// Per-signal retention TTL override (traces).
    #[arg(long, value_parser = parse_duration)]
    ttl_traces: Option<Duration>,
    /// Per-signal retention TTL override (profiles).
    #[arg(long, value_parser = parse_duration)]
    ttl_profiles: Option<Duration>,

    /// Actually delete expired blocks. Without it retention is dry-run
    /// (reports candidates, deletes nothing).
    #[arg(long)]
    retention_apply: bool,

    /// Grace seconds between marking a block deleted and removing its
    /// objects (retention). Default 0 single-instance; 600 with a lease.
    #[arg(long)]
    retention_grace: Option<u64>,

    /// Address to advertise into the Valkey tail registry so the `scry query`
    /// live-tail front-door can dial this ingester (D-053). Only used when
    /// Valkey is configured. If unset: derived from a concrete `--listen` IP,
    /// else from `$NODE_IP` (k8s downward API) + the listen port. A wildcard
    /// `--listen` (`0.0.0.0`/`::`) with neither set is a startup error — set
    /// this, or `$NODE_IP`, or bind a concrete IP. Falls back to
    /// `$SCRY_TAIL_ADVERTISE_ADDR`.
    #[arg(long)]
    tail_advertise_addr: Option<String>,

    /// Retained recent-window for the merged history+live query (D-054): how
    /// far back the logs live ring keeps in-flight records so a `scry query
    /// --live` sees a full recent window even across block flushes. Logs only.
    /// 0 disables the ring (live queries against this ingester return nothing).
    #[arg(long, default_value_t = 90)]
    live_window_secs: u64,

    /// Hard byte cap on the logs live ring (D-054), a backstop against a spike
    /// blowing memory when the window holds a lot. Oldest records evicted first.
    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    live_window_max_bytes: usize,

    /// Interval between catalog snapshots to the bucket (D-055). This ingester
    /// periodically uploads its online catalog as a single object
    /// (`_catalog/snapshot.sqlite`) so a cold `scry query` can restore it in one
    /// GET instead of walking every block sidecar. Non-destructive (overwrites
    /// one key), so it needs no lease. `0` disables. Requires `--storage`
    /// with `--catalog`. Accepts `ms`/`s`/`m`/`h`/`d` (bare number = seconds).
    #[arg(long, value_parser = parse_duration, default_value = "300s")]
    catalog_snapshot_interval: Duration,
}

/// Instance role; see `--mode`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// Ingest + converge + maintenance.
    Full,
    /// Ingest + converge, no maintenance.
    IngestOnly,
    /// Converge only (no ingest, no maintenance).
    QueryOnly,
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

/// Run the ingest server daemon until ctrl-c / SIGTERM.
pub async fn run(args: Args) -> Result<()> {
    let writer_id = args
        .writer_id
        .unwrap_or_else(|| format!("scry-ingestd-{}", rand_short()));
    // Stable instance identity. When a WAL dir is configured we persist it to
    // `<wal_dir>/writer_id` so a restart reuses its prefix (every block path
    // is `<signal>/.../<writer_uuid>/<block>`, and the poll cursors fan out
    // per `(signal, writer, date)` — a fresh UUID each restart would bloat
    // that fan-out indefinitely). With no WAL dir (no-storage mode) it's
    // ephemeral.
    let writer_uuid = match args.wal_dir.as_ref() {
        Some(dir) => load_or_create_writer_uuid(dir)?,
        None => Uuid::now_v7(),
    };

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

    // ---- Multi-instance coordination (v0.9) -------------------------------
    // Resolve the Valkey URL (flag overrides env). When present we connect a
    // command handle (used for the lease + the pub/sub publish side) and spawn
    // the `ValkeySink` that fans block-lifecycle events to peers. With it
    // absent everything degrades to a correct single-instance path: no sink,
    // no lease (maintenance pauses unless `--allow-unfenced-maintenance`),
    // convergence falls back to polling + full-walk against the bucket.
    let valkey_url = args
        .valkey_url
        .clone()
        .or_else(|| std::env::var(VALKEY_URL_ENV).ok());
    let valkey = match valkey_url.as_deref() {
        Some(url) => Some(
            ValkeyClient::connect(url, writer_uuid)
                .await
                .with_context(|| format!("connecting to Valkey at {url}"))?,
        ),
        None => {
            info!("{VALKEY_URL_ENV} unset and no --valkey-url; running single-instance (no pub/sub; maintenance paused unless --allow-unfenced-maintenance)");
            None
        }
    };
    // The pub/sub event sink, injected into every ingest pipeline so each
    // uploaded block is announced to peers. `None` (single-instance) makes
    // `with_event_sink` a no-op.
    let (event_sink, sink_task): (Option<Arc<dyn BlockEventSink>>, _) = match valkey.as_ref() {
        Some(c) => {
            let (sink, task) = ValkeySink::spawn(c.inner().clone(), writer_uuid);
            (Some(Arc::new(sink)), Some(task))
        }
        None => (None, None),
    };

    // Tail-address registry (D-053): with Valkey present, advertise this
    // ingester's tail-serving address (its ingest `--listen` endpoint, which
    // already serves Subscribe/TailRecord) so the `scry query` front-door can
    // discover and fan-in from it. Resolution can hard-error on an
    // unadvertisable wildcard bind — surfaced at startup, before we serve.
    let tail_registration: Option<TailRegistration> = match valkey.as_ref() {
        Some(c) => {
            let node_ip = std::env::var("NODE_IP").ok();
            let explicit = args
                .tail_advertise_addr
                .clone()
                .or_else(|| std::env::var("SCRY_TAIL_ADVERTISE_ADDR").ok());
            let addr =
                resolve_tail_advertise_addr(explicit.as_deref(), &args.listen, node_ip.as_deref())
                    .context("resolving tail advertise address")?;
            let ttl = Duration::from_secs(args.lease_ttl.max(1));
            Some(
                TailRegistration::spawn(c.inner().clone(), writer_uuid, addr, ttl)
                    .await
                    .context("registering tail address in Valkey")?,
            )
        }
        None => None,
    };

    // Background-task context captured out of the storage block: the shared
    // object store, bucket name, online catalog, and block config that the
    // convergence loops + maintenance loop need. Only `Some` when `--storage`
    // built an online catalog (`--catalog`); without a catalog there's nothing
    // to converge into.
    struct BgCtx {
        store: Arc<dyn ObjectStore>,
        bucket: String,
        catalog: Arc<std::sync::Mutex<Catalog>>,
        block_cfg: BlockBuilderConfig,
        /// On-disk path of the online catalog — the snapshot producer (D-055)
        /// reads it directly (via `VACUUM INTO`) off the shared mutex.
        catalog_path: PathBuf,
    }
    let mut bg_ctx: Option<BgCtx> = None;

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
        Option<TracesShards>,
        Option<ProfilesShards>,
    );
    let (dummy_pipeline, metrics_pipeline, logs_pipeline, traces_pipeline, profiles_pipeline): Pipelines =
        if args.storage {
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
            "storage mode: WAL + parquet blocks → object storage (dummy + metrics + logs + traces + profiles)"
        );
        let store = open_objstore(&cfg)?;
        let catalog = match args.catalog.as_ref() {
            Some(p) => Some(Arc::new(std::sync::Mutex::new(
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
        let mut block_cfg = BlockBuilderConfig {
            compression_level: args.compression.zstd_level(),
            ..Default::default()
        };
        // Optional small-block override for tests (see `--block-max-rows`).
        // 0 is treated as "unset" so `--block-max-rows 0` can't wedge the
        // builder into sealing every row.
        if let Some(n) = args.block_max_rows {
            if n > 0 {
                block_cfg.max_rows = n;
            }
        }
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
        // Capture the background-task context before `store`/`catalog` get
        // moved into the last pipeline below. Only when an online catalog
        // exists — the convergence loops and maintenance loop have nothing to
        // converge into otherwise.
        if let Some(cat) = catalog.clone() {
            bg_ctx = Some(BgCtx {
                store: store.clone(),
                bucket: bucket.clone(),
                catalog: cat,
                block_cfg,
                // `catalog` is `Some` iff `--catalog` was given.
                catalog_path: args
                    .catalog
                    .clone()
                    .expect("online catalog implies --catalog path is set"),
            });
        }

        // `query-only` instances build the catalog (for convergence) but no
        // ingest pipelines — they never accept data, only follow the bucket.
        if args.mode == Mode::QueryOnly {
            info!("mode=query-only: catalog convergence only, no ingest pipelines");
            (None, None, None, None, None)
        } else {
            // Each signal becomes INGEST_SHARDS independent pipelines, one
            // WAL subtree per shard, all sharing store/catalog/sem and the
            // per-signal upload-stats gauge (so the endpoint aggregates
            // across shards). When a Valkey sink is configured it's attached
            // to every shard so each uploaded block is announced to peers.
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
                wal_dir.clone(),
                store.clone(),
                catalog.clone(),
                writer_uuid,
                decode::logs,
                block_cfg,
                upload_sem.clone(),
                stats_metrics.as_ref().map(|m| m.logs_upload()),
                adaptive_compression,
            )
            .await?;
            let traces = ShardedPipeline::open_with_config(
                INGEST_SHARDS,
                wal_dir.clone(),
                store.clone(),
                catalog.clone(),
                writer_uuid,
                decode::traces,
                block_cfg,
                upload_sem.clone(),
                stats_metrics.as_ref().map(|m| m.traces_upload()),
                adaptive_compression,
            )
            .await?;
            let profiles = ShardedPipeline::open_with_config(
                INGEST_SHARDS,
                wal_dir,
                store,
                catalog,
                writer_uuid,
                decode::profiles,
                block_cfg,
                upload_sem.clone(),
                stats_metrics.as_ref().map(|m| m.profiles_upload()),
                adaptive_compression,
            )
            .await?;
            // Attach the pub/sub event sink to every shard (no-op when None).
            let (dummy, metrics, logs, traces, profiles) = match event_sink.as_ref() {
                Some(s) => (
                    dummy.with_event_sink(s.clone()).await,
                    metrics.with_event_sink(s.clone()).await,
                    logs.with_event_sink(s.clone()).await,
                    traces.with_event_sink(s.clone()).await,
                    profiles.with_event_sink(s.clone()).await,
                ),
                None => (dummy, metrics, logs, traces, profiles),
            };
            (
                Some(dummy),
                Some(metrics),
                Some(logs),
                Some(traces),
                Some(profiles),
            )
        }
    } else {
        if args.wal_dir.is_some() {
            warn!("--wal-dir set but --storage is not; ignoring WAL");
        }
        if args.catalog.is_some() {
            warn!("--catalog set but --storage is not; ignoring catalog");
        }
        (None, None, None, None, None)
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
        traces_pipeline,
        profiles_pipeline,
    );
    if let Some(m) = stats_metrics.as_ref() {
        server = server.with_metrics(m.clone());
    }
    // Time-based block flush: seal idle/low-volume blocks so they become
    // queryable promptly (0 = disabled). Only relevant when storage is on
    // — with no pipelines there's nothing to flush.
    server = server.with_block_max_age(
        (args.storage && args.block_max_age_secs > 0)
            .then(|| Duration::from_secs(args.block_max_age_secs)),
    );

    // Retained recent-window live ring (D-054): the *live* source for a `scry
    // query --live` merged view. Logs-only, always-on but bounded (age +
    // byte cap). `--live-window-secs 0` disables it (no live half). The ring
    // is fed off the logs ingest phase-2 seam inside `Server` and snapshotted
    // to answer `LiveQuery` frames on the same `--listen` port.
    let live_ring = (args.live_window_secs > 0).then(|| {
        LiveRing::new(
            Duration::from_secs(args.live_window_secs),
            args.live_window_max_bytes,
        )
    });
    server = server.with_live_ring(live_ring);

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

    // ---- Multi-instance background loops (v0.9) ---------------------------
    // Spawned as siblings to the stats task; aborted after the serve future
    // completes. All are no-ops / absent in the single-instance path (no
    // catalog ⇒ no `bg_ctx`; no Valkey ⇒ no consumer and paused maintenance).
    let mut bg_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    if let Some(ctx) = bg_ctx {
        let BgCtx {
            store,
            bucket,
            catalog,
            block_cfg,
            catalog_path,
        } = ctx;

        // 0. catalog snapshot producer (D-055): periodically upload this
        // ingester's online catalog as one object so a cold `scry query` can
        // restore it in a single GET. Non-destructive (overwrites one key) so it
        // runs without any lease — deliberately independent of the maintenance
        // loop (which is paused without a Valkey lease). Disabled with `0`.
        if !args.catalog_snapshot_interval.is_zero() {
            let store = store.clone();
            let path = catalog_path.clone();
            let interval = args.catalog_snapshot_interval;
            info!(
                interval_secs = interval.as_secs(),
                "catalog snapshot producer enabled"
            );
            bg_tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                // Skip the immediate first tick — nothing to snapshot at t=0.
                tick.tick().await;
                loop {
                    tick.tick().await;
                    match scry_catalog::save_snapshot(&path, store.as_ref()).await {
                        Ok(r) => info!(bytes = r.bytes, "catalog snapshot uploaded"),
                        Err(e) => warn!(error = %e, "catalog snapshot failed"),
                    }
                }
            }));
        }

        // 1. pub/sub convergence consumer (low-latency hint; only with Valkey).
        if let Some(url) = valkey_url.clone() {
            let cat = catalog.clone();
            bg_tasks.push(tokio::spawn(run_consumer(url, cat)));
        }

        // 2. incremental cursor poller (backstops dropped events).
        {
            let store = store.clone();
            let bucket = bucket.clone();
            let cat = catalog.clone();
            let interval = Duration::from_secs(args.poll_interval.max(1));
            bg_tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tick.tick().await;
                    match poll_once(store.as_ref(), cat.as_ref(), &bucket).await {
                        Ok(r) if r.inserted > 0 => info!(
                            inserted = r.inserted,
                            cursors = r.cursors,
                            "convergence poll applied new blocks"
                        ),
                        Ok(_) => {}
                        Err(e) => warn!(error = %e, "convergence poll failed"),
                    }
                }
            }));
        }

        // 3. periodic full walk (ultimate backstop; discovers new prefixes).
        {
            let store = store.clone();
            let bucket = bucket.clone();
            let cat = catalog.clone();
            let interval = Duration::from_secs(args.full_walk_interval.max(1));
            bg_tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tick.tick().await;
                    match full_walk(store.as_ref(), cat.as_ref(), &bucket).await {
                        Ok(r) if r.inserted > 0 => info!(
                            inserted = r.inserted,
                            seen = r.seen,
                            "convergence full-walk applied new blocks"
                        ),
                        Ok(_) => {}
                        Err(e) => warn!(error = %e, "convergence full-walk failed"),
                    }
                }
            }));
        }

        // 4. lease-guarded maintenance loop (compaction + retention). `full`
        // mode only; pauses without a lease unless --allow-unfenced-maintenance.
        let maintenance_enabled = args.mode == Mode::Full && !args.no_maintenance;
        if maintenance_enabled {
            // Grace defaults: 0 single-instance (list_blocks already filters
            // superseded/deleted), 600s with a lease provider (peers may hold
            // stale catalog rows until their next poll).
            let grace_default = if valkey.is_some() { 600 } else { 0 };
            let compact_cfg = CompactConfig {
                fanout: args.compact_fanout,
                grace: Duration::from_secs(args.compact_grace.unwrap_or(grace_default)),
                ..Default::default()
            };
            let mut overrides = BTreeMap::new();
            if let Some(d) = args.ttl_metrics {
                overrides.insert("metrics".to_string(), d);
            }
            if let Some(d) = args.ttl_logs {
                overrides.insert("logs".to_string(), d);
            }
            if let Some(d) = args.ttl_traces {
                overrides.insert("traces".to_string(), d);
            }
            if let Some(d) = args.ttl_profiles {
                overrides.insert("profiles".to_string(), d);
            }
            let retention_cfg = RetentionConfig {
                default_ttl: args.ttl,
                overrides,
                grace: Duration::from_secs(args.retention_grace.unwrap_or(grace_default)),
                apply: args.retention_apply,
            };
            let lease_ttl = Duration::from_secs(args.lease_ttl.max(1));
            let compact_interval = Duration::from_secs(args.compact_interval.max(1));
            let retention_interval = Duration::from_secs(args.retention_interval.max(1));

            match valkey.as_ref() {
                Some(c) => {
                    let provider = ValkeyLeaseProvider::new(c.inner().clone());
                    bg_tasks.push(tokio::spawn(run_maintenance_loop(
                        provider,
                        store,
                        bucket,
                        catalog,
                        compact_cfg,
                        block_cfg,
                        retention_cfg,
                        event_sink.clone(),
                        compact_interval,
                        retention_interval,
                        lease_ttl,
                    )));
                }
                None if args.allow_unfenced_maintenance => {
                    warn!("--allow-unfenced-maintenance: running maintenance under a local single-process lease; UNSAFE with >1 instance on one bucket");
                    let provider = LocalLeaseProvider::new();
                    bg_tasks.push(tokio::spawn(run_maintenance_loop(
                        provider,
                        store,
                        bucket,
                        catalog,
                        compact_cfg,
                        block_cfg,
                        retention_cfg,
                        event_sink.clone(),
                        compact_interval,
                        retention_interval,
                        lease_ttl,
                    )));
                }
                None => info!(
                    "no Valkey lease and no --allow-unfenced-maintenance: maintenance paused (convergence still runs via polling)"
                ),
            }
        }
    }

    let serve_result = server
        .serve_with_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;

    // Stop the convergence/maintenance loops and the pub/sub publisher; the
    // process is exiting, so abort rather than draining (events are advisory).
    for t in &bg_tasks {
        t.abort();
    }
    if let Some(t) = &sink_task {
        t.abort();
    }
    // Remove our tail-registry entry promptly (else it lingers until TTL).
    if let Some(reg) = tail_registration {
        reg.deregister().await;
    }
    if let Some(c) = valkey {
        c.quit().await;
    }
    if let Some(task) = stats_task {
        let _ = task.await;
    }
    serve_result
}

/// Background pub/sub convergence consumer: subscribe to every block-event
/// channel and apply each event to the catalog idempotently. Reconnects on a
/// closed subscription; lag just drops events (the cursor poller backstops).
async fn run_consumer(url: String, catalog: Arc<std::sync::Mutex<Catalog>>) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match subscribe_blocks(&url, &ALL_SIGNALS).await {
            Ok((_sub, mut rx)) => {
                info!("subscribed to block-event channels for catalog convergence");
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            if let Some(env) = parse_envelope(&msg) {
                                if let Err(e) = apply_event(catalog.as_ref(), &env.event) {
                                    warn!(error = %e, "applying block event to catalog failed");
                                }
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            warn!(
                                skipped = n,
                                "convergence consumer lagged; polling will backstop"
                            )
                        }
                        Err(RecvError::Closed) => {
                            warn!("convergence subscription closed; reconnecting");
                            break;
                        }
                    }
                }
            }
            Err(e) => warn!(error = %e, "subscribing to Valkey block channels failed; retrying"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// The lease-guarded maintenance loop: fire a compaction pass on
/// `compact_interval` and (if any TTL is configured) a retention pass on
/// `retention_interval`. Generic over the lease provider so the Valkey
/// provider and the single-process `LocalLeaseProvider` share one body.
#[allow(clippy::too_many_arguments)]
async fn run_maintenance_loop<L: LeaseProvider>(
    provider: L,
    store: Arc<dyn ObjectStore>,
    bucket: String,
    catalog: Arc<std::sync::Mutex<Catalog>>,
    compact_cfg: CompactConfig,
    block_cfg: BlockBuilderConfig,
    retention_cfg: RetentionConfig,
    sink: Option<Arc<dyn BlockEventSink>>,
    compact_interval: Duration,
    retention_interval: Duration,
    lease_ttl: Duration,
) {
    let noop = NoopSink;
    let retention_active = retention_cfg.any_ttl_configured();
    let mut compact_tick = tokio::time::interval(compact_interval);
    compact_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut retention_tick = tokio::time::interval(retention_interval);
    retention_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    info!(
        retention_active,
        apply = retention_cfg.apply,
        "maintenance loop started"
    );
    loop {
        let sink_ref: &dyn BlockEventSink = match sink.as_ref() {
            Some(s) => s.as_ref(),
            None => &noop,
        };
        tokio::select! {
            _ = compact_tick.tick() => {
                match run_compaction_pass(
                    &provider, store.clone(), catalog.as_ref(), &bucket,
                    &compact_cfg, &block_cfg, sink_ref, lease_ttl,
                ).await {
                    Ok(r) if r.merges > 0 => info!(
                        merges = r.merges, blocks_in = r.blocks_in,
                        blocks_out = r.blocks_out, "compaction pass merged partitions"
                    ),
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "compaction pass failed"),
                }
            }
            _ = retention_tick.tick(), if retention_active => {
                let now = now_unix_nano();
                match run_retention_pass(
                    &provider, store.clone(), catalog.as_ref(),
                    &retention_cfg, now, sink_ref, lease_ttl,
                ).await {
                    Ok(r) if r.reaped > 0 => info!(
                        reaped = r.reaped, bytes = r.bytes_reaped,
                        dry_run = r.dry_run, "retention pass"
                    ),
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "retention pass failed"),
                }
            }
        }
    }
}

/// Current wall-clock time as nanoseconds since the Unix epoch (retention age
/// cutoff). Injected into the engine so reaping is deterministic per pass.
fn now_unix_nano() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Parse a duration with an optional `ms`/`s`/`m`/`h`/`d` suffix (bare number
/// = seconds). Mirrors `scry-retention`'s CLI parser.
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

/// Resolve the address to advertise into the Valkey tail registry (D-053).
///
/// Precedence: an explicit `--tail-advertise-addr` wins; else a concrete
/// `--listen` IP is advertised verbatim; else (wildcard bind) `$NODE_IP` + the
/// listen port; else a hard error (an un-advertisable wildcard). A non-IP
/// `--listen` host (e.g. `myhost:4000`) is routable as-is and advertised
/// verbatim.
fn resolve_tail_advertise_addr(
    explicit: Option<&str>,
    listen: &str,
    node_ip: Option<&str>,
) -> Result<String> {
    use std::net::SocketAddr;

    if let Some(a) = explicit.map(str::trim).filter(|s| !s.is_empty()) {
        return Ok(a.to_string());
    }

    let listen = listen.trim();
    match listen.parse::<SocketAddr>() {
        // Concrete IP bind → advertise it verbatim.
        Ok(sa) if !sa.ip().is_unspecified() => Ok(listen.to_string()),
        // Wildcard bind → need a routable IP from NODE_IP (k8s downward API).
        Ok(sa) => match node_ip.map(str::trim).filter(|s| !s.is_empty()) {
            Some(ip) => Ok(format!("{ip}:{}", sa.port())),
            None => anyhow::bail!(
                "cannot determine tail advertise address: --listen {listen:?} is a wildcard \
                 and neither --tail-advertise-addr nor $NODE_IP is set. Set one of those, or \
                 bind a concrete --listen IP."
            ),
        },
        // Non-IP host (hostname:port) — routable as-is.
        Err(_) => Ok(listen.to_string()),
    }
}

/// Load this instance's stable writer UUID from `<wal_dir>/writer_id`, or
/// generate one and persist it there. See the call site for why persistence
/// matters (prefix/cursor-fan-out stability across restarts).
fn load_or_create_writer_uuid(wal_dir: &std::path::Path) -> Result<Uuid> {
    let path = wal_dir.join("writer_id");
    match std::fs::read_to_string(&path) {
        Ok(s) => Uuid::parse_str(s.trim())
            .with_context(|| format!("parsing writer id from {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(wal_dir)
                .with_context(|| format!("creating wal dir {}", wal_dir.display()))?;
            let uuid = Uuid::now_v7();
            std::fs::write(&path, uuid.to_string())
                .with_context(|| format!("persisting writer id to {}", path.display()))?;
            info!(writer_uuid = %uuid, path = %path.display(), "generated and persisted writer id");
            Ok(uuid)
        }
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

fn rand_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    format!("{:08x}", ns & 0xFFFF_FFFF)
}

#[cfg(test)]
mod tests {
    use super::resolve_tail_advertise_addr;

    #[test]
    fn explicit_wins_over_everything() {
        // Even a wildcard listen + NODE_IP is overridden by an explicit addr.
        let got =
            resolve_tail_advertise_addr(Some("1.2.3.4:9000"), "0.0.0.0:4000", Some("10.0.0.7"))
                .unwrap();
        assert_eq!(got, "1.2.3.4:9000");
    }

    #[test]
    fn concrete_listen_ip_is_advertised_verbatim() {
        let got = resolve_tail_advertise_addr(None, "192.168.1.5:4000", Some("10.0.0.7")).unwrap();
        // NODE_IP is ignored when the bind is already a concrete IP.
        assert_eq!(got, "192.168.1.5:4000");
    }

    #[test]
    fn wildcard_listen_uses_node_ip_and_listen_port() {
        let got = resolve_tail_advertise_addr(None, "0.0.0.0:4321", Some("10.0.0.7")).unwrap();
        assert_eq!(got, "10.0.0.7:4321");
        // IPv6 wildcard too.
        let got6 = resolve_tail_advertise_addr(None, "[::]:4321", Some("10.0.0.7")).unwrap();
        assert_eq!(got6, "10.0.0.7:4321");
    }

    #[test]
    fn wildcard_listen_without_node_ip_or_explicit_is_error() {
        let err = resolve_tail_advertise_addr(None, "0.0.0.0:4000", None).unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot determine tail advertise address"),
            "unexpected: {err}"
        );
        // Empty NODE_IP / empty explicit are treated as unset.
        assert!(resolve_tail_advertise_addr(Some("  "), "0.0.0.0:4000", Some("")).is_err());
    }

    #[test]
    fn hostname_listen_is_advertised_verbatim() {
        // A non-IP host is routable as-is (queryd resolves it).
        let got = resolve_tail_advertise_addr(None, "myhost:4000", None).unwrap();
        assert_eq!(got, "myhost:4000");
    }
}
