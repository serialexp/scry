//! `scry query` — long-running query daemon (binschema-over-TCP).
//!
//! The architectural counterpart to `scry ingest`: where ingest exposes
//! `scry-server::Server` (ingest) as a process, this exposes
//! `scry-server::QueryService` (query) over the same length-prefixed
//! binschema framing pattern as ingest — `QueryFrame`s defined in
//! `proto/query.schema.json`, one TCP connection per query. Same shape
//! end-to-end:
//!
//! 1. Parse flags + env (`SCRY_OBJSTORE_*` for store, `SCRY_OBJSTORE_POOL_*`
//!    for buffer pool, `RUST_LOG` for tracing).
//! 2. Build the object store + pre-warmed `BufPool`.
//! 3. Open the SQLite catalog (read-only from the daemon's perspective;
//!    concurrent ingest writers update it via separate processes — the
//!    SQLite WAL handles cross-process visibility).
//! 4. Construct a [`QueryService`] and serve until SIGINT or SIGTERM.
//!
//! The daemon's job is to amortise the cold-start cost — DataFusion
//! init, ZSTD work areas, glibc → mimalloc reservations, and pool
//! warmup pages — across every query that follows. The first query
//! pays the warm-up; the rest run at hot-process speed.
//!
//! Run (after `source docker/garage/.env`):
//!
//! ```bash
//! scry query \
//!     --catalog ./online.sqlite \
//!     --listen 127.0.0.1:4100 \
//!     --pool-warmup-count 8
//! ```
//!
//! Connect from the CLI:
//!
//! ```bash
//! scry get --remote 127.0.0.1:4100 \
//!     --matcher __name__=scry_http_requests_total
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use scry_catalog::Catalog;
use scry_cluster::{apply_event, full_walk, poll_once};
use scry_objstore::{open_with_pool_config, BufPoolConfig, ObjStoreConfig};
use scry_query::{
    BloomCache, BloomCacheConfig, PostingsCache, PostingsCacheConfig, QueryResultCache,
};
use scry_server::{
    serve_status, CatalogGauge, CgroupMemoryGuard, FleetSource, LiveDiscovery, LiveFetchLimits,
    LocalStatus, QueryMemoryGuard, QueryMetrics, QueryService, CATALOG_GAUGE_INTERVAL,
};
use scry_valkey::{
    discover_status_blobs, discover_tail_endpoints, parse_envelope, subscribe_blocks,
    StatusRegistration, ValkeyClient, STATUS_TTL, VALKEY_URL_ENV,
};
use tracing::{info, warn};
use uuid::Uuid;

mod tail_relay;

/// Fleet deregistration is advisory; a degraded Valkey must not hold a pod in
/// Terminating until Kubernetes resorts to SIGKILL. TTLs clean up missed exits.
const SHUTDOWN_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

/// Valkey-backed [`LiveDiscovery`] for the D-054 merged history+live query.
/// `scry-server` is Valkey-agnostic (it takes a `&dyn LiveDiscovery`); this is
/// the query daemon's injected impl, reusing the D-053 tail registry — an
/// ingester that advertises for tail advertises for live-query too (same
/// ingest addr/port).
struct ValkeyLiveDiscovery {
    valkey: ValkeyClient,
}

#[async_trait::async_trait]
impl LiveDiscovery for ValkeyLiveDiscovery {
    async fn discover(&self) -> anyhow::Result<Vec<String>> {
        discover_tail_endpoints(&self.valkey).await
    }
}

/// Valkey-backed [`FleetSource`] for the status page (D-057): enumerates every
/// live instance's published snapshot with one Lua `SCAN`. Keeps `scry-server`
/// Valkey-agnostic (it takes a `&dyn FleetSource`).
struct ValkeyFleetSource {
    valkey: ValkeyClient,
}

#[async_trait::async_trait]
impl FleetSource for ValkeyFleetSource {
    async fn blobs(&self) -> Vec<String> {
        discover_status_blobs(&self.valkey)
            .await
            .unwrap_or_else(|e| {
                warn!(error = %e, "status fleet discovery failed");
                Vec::new()
            })
    }
}

/// Block-event channels the convergence loops follow (every signal).
const ALL_SIGNALS: [&str; 5] = ["dummy", "metrics", "logs", "traces", "profiles"];

/// CLI arguments for the `scry query` subcommand (the query daemon).
#[derive(Parser, Debug)]
#[command(about = "Long-running query daemon (binschema QueryFrame wire over TCP)")]
pub struct Args {
    /// Listen address for the query wire server.
    #[arg(long, default_value = "127.0.0.1:4100")]
    listen: SocketAddr,

    /// Path to the SQLite catalog file. The daemon opens it read-only-
    /// in-spirit (ingest writers update it from separate processes;
    /// SQLite's WAL handles cross-process visibility).
    #[arg(long)]
    catalog: PathBuf,

    /// Disable restoring the catalog from a bucket snapshot on cold boot
    /// (D-055). By default, when the catalog file is absent, the daemon
    /// downloads `_catalog/snapshot.sqlite` (one GET) instead of waiting on a
    /// full bucket walk; its own poll + full-walk loops then fill the delta.
    /// Set this to force a cold catalog (e.g. to reproduce a full reconcile).
    #[arg(long)]
    no_snapshot_restore: bool,

    // ── Buffer-pool knobs (override env / defaults) ──────────────
    //
    // Identical semantics to `scry get`'s `--pool-*` flags. For the
    // daemon, set `--pool-warmup-count` high enough that the *first*
    // query against the daemon doesn't pay the page-fault tax for
    // the per-fetch response Vecs; subsequent queries reuse via the
    // pool LIFO.
    /// Pool buffers to pre-allocate + page-fault at startup.
    #[arg(long)]
    pool_warmup_count: Option<usize>,

    /// Capacity (MiB) of each warmup buffer.
    #[arg(long)]
    pool_warmup_size_mib: Option<usize>,

    /// Starting free-list cap.
    #[arg(long)]
    pool_initial_capacity: Option<usize>,

    /// Hard ceiling that autoscale won't cross.
    #[arg(long)]
    pool_max_capacity: Option<usize>,

    /// Aggregate MiB retained by idle object-store buffers. This byte budget is
    /// enforced in addition to the buffer-count cap; 0 disables retention.
    #[arg(long)]
    pool_max_retained_mib: Option<usize>,

    /// Autoscale grows capacity by this many slots when peak in-flight
    /// exceeds current capacity. 0 disables autoscale.
    #[arg(long)]
    pool_autoscale_headroom: Option<usize>,

    /// Postings sidecar cache byte budget. Overrides
    /// `SCRY_POSTINGS_CACHE_BYTES` if both are set. Postings files
    /// run "a few MB per block" per `ARCHITECTURE.md`, and blocks
    /// are immutable, so caching them across queries is a pure win
    /// after the first hit. Set to 0 to disable caching entirely
    /// (every query refetches every block's postings, same as
    /// pre-v0.3.x behaviour).
    #[arg(long)]
    postings_cache_bytes: Option<usize>,

    /// Maximum distinct postings sidecars fetched/decoded concurrently.
    #[arg(long)]
    postings_cache_max_fills: Option<usize>,

    /// Body-bloom sidecar cache byte budget for the logs full-text
    /// path. Overrides `SCRY_BLOOM_CACHE_BYTES` if both are set. Blooms
    /// run ~2% of body size (tens to hundreds of KB per block), so the
    /// default budget holds many more blocks than postings needs. Set
    /// to 0 to disable (every `--grep` query refetches each block's
    /// bloom; correctness is unaffected, it's a pure accelerator).
    #[arg(long)]
    bloom_cache_bytes: Option<usize>,

    /// Maximum distinct body-bloom sidecars fetched/decoded concurrently.
    #[arg(long)]
    bloom_cache_max_fills: Option<usize>,

    /// Query-result cache byte budget (default 256 MiB; `0` disables).
    /// Caches the exact framed response bytes of *data* queries keyed by
    /// the normalized request ⊕ the candidate block-UUID set, so a
    /// repeated dashboard-style query over a closed past range is served
    /// from memory in ~ms with no scan. Folding the candidate set into
    /// the key makes invalidation free: any ingest / compaction /
    /// retention that changes which blocks a range touches changes the
    /// key → automatic miss, so a hit is always for an identical block
    /// set. Byte-weighted LRU eviction to this budget.
    #[arg(long, default_value_t = scry_query::DEFAULT_QUERY_CACHE_BYTES)]
    query_cache_bytes: usize,

    /// Per-entry cap for the query-result cache (default 8 MiB). A
    /// response whose framed bytes exceed this is streamed to the client
    /// but never cached, so large log dumps don't evict the small
    /// aggregation / metadata results the dashboard actually re-hits.
    #[arg(long, default_value_t = scry_query::DEFAULT_QUERY_CACHE_ENTRY_BYTES)]
    query_cache_entry_bytes: usize,

    /// Process-wide DataFusion memory budget, in MiB. Every per-
    /// request `SessionContext` shares the same `GreedyMemoryPool`
    /// behind a shared `RuntimeEnv`, so this cap is total across
    /// concurrent queries, not per-query. A query that asks for
    /// more than the remaining budget returns a
    /// `QueryFrame::StreamError` with code `QUERY_ERR_RESOURCES`
    /// cleanly; the daemon keeps running and the next query starts
    /// with the budget freshly available (DataFusion drops
    /// reservations on plan teardown).
    ///
    /// Sizing rule of thumb: DataFusion only tracks "large"
    /// allocations (hash aggregates, sorts). Streaming operators
    /// like `ParquetSource` aren't accounted, so the true RSS
    /// ceiling is higher than this number; reserve some OS-level
    /// headroom (e.g. cap this at ~70% of available RAM).
    #[arg(long, default_value_t = 1024)]
    query_memory_budget_mib: usize,

    /// Headroom below the Linux cgroup memory limit at which queryd refuses new
    /// work and cancels running query streams. Covers Arrow/Parquet/cache and
    /// allocator memory that DataFusion's pool does not account for. `0`
    /// disables the cgroup guard; hosts with an unlimited cgroup skip it.
    #[arg(long, default_value_t = 256)]
    query_memory_reserve_mib: u64,

    /// Ingester live-window requests in flight per merged live query.
    #[arg(long, default_value_t = 8)]
    live_fetch_concurrency: usize,

    /// Maximum estimated MiB accepted from one ingester's live response.
    #[arg(long, default_value_t = 16)]
    live_fetch_peer_max_mib: usize,

    /// Maximum estimated MiB retained across the live half of one query.
    #[arg(long, default_value_t = 128)]
    live_fetch_max_mib: usize,

    /// Maximum retained live rows in one merged query.
    #[arg(long, default_value_t = 1_000_000)]
    live_fetch_max_rows: usize,

    /// Query requests executing concurrently.
    #[arg(long, default_value_t = 32)]
    query_max_active: usize,

    /// Accepted query sockets allowed to wait for an active slot.
    #[arg(long, default_value_t = 64)]
    query_max_waiting: usize,

    /// Maximum seconds an accepted query may wait for an active slot.
    #[arg(long, default_value_t = 5)]
    query_queue_timeout: u64,

    // ── Multi-instance convergence (v0.9) ─────────────────────────
    /// Valkey URL for pub/sub catalog convergence. Falls back to
    /// `$SCRY_VALKEY_URL`. The query daemon is **query-only**: it never
    /// runs maintenance (no lease), it only *follows* the bucket so peers'
    /// blocks become queryable promptly. With Valkey absent, convergence
    /// still runs via polling + full-walk (just higher latency).
    #[arg(long)]
    valkey_url: Option<String>,

    /// Valkey key namespace for this deployment. Every key and channel scry
    /// uses lives under it (`<ns>/lease/…`, `<ns>/blocks/<signal>`,
    /// `<ns>/tail/…`, `<ns>/deleted/…`, `<ns>/status/…`). Falls back to
    /// `$SCRY_VALKEY_NAMESPACE`, then `scry`. Give two deployments sharing one
    /// Valkey two different namespaces: otherwise they contend for each
    /// other's leases and converge each other's block events.
    #[arg(long)]
    valkey_namespace: Option<String>,

    /// Seconds between incremental cursor convergence polls.
    #[arg(long, default_value_t = 5)]
    poll_interval: u64,

    /// Seconds between exhaustive full-walk reconciles (backstop that also
    /// discovers brand-new prefixes).
    #[arg(long, default_value_t = 1800)]
    full_walk_interval: u64,

    // ── Live-tail front-door (D-053) ──────────────────────────────
    /// Listen address for the **live-tail relay** (`scry tail --queryd`). A
    /// *separate* port from `--listen`: the tail sub-protocol (`Frame`) and the
    /// query wire (`QueryFrame`) are different binschema unions whose first
    /// bytes collide, so they can't share a socket. Unset ⇒ no tail front-door.
    /// Requires Valkey (to discover ingesters); with `--tail-listen` set but no
    /// Valkey, each subscription is refused with `ERR_TAIL_UNAVAILABLE`.
    #[arg(long)]
    tail_listen: Option<SocketAddr>,

    /// Seconds between re-discovering the live ingester set from the Valkey
    /// registry while a tail is streaming (new ingesters are dialed in, gone
    /// ones dropped).
    #[arg(long, default_value_t = 5)]
    tail_rediscover_interval: u64,

    /// Maximum simultaneous long-lived tail subscriptions.
    #[arg(long, default_value_t = 128)]
    tail_max_connections: usize,

    // ── Status page (D-057) ───────────────────────────────────────
    /// Live status HTTP endpoint (the fleet dashboard, D-057). A bare
    /// `--stats-listen` binds `127.0.0.1:4098`; pass an explicit `host:port`
    /// to override. Serves a self-updating dashboard at `/` and a JSON
    /// snapshot at `/stats.json` (queries in-flight/total, cache hit rates,
    /// DataFusion memory reserved, catalog block + row counts). With Valkey
    /// configured the page shows the whole fleet — every ingest and query
    /// instance — because each one heartbeats its snapshot into Valkey. Bind
    /// to loopback — no auth.
    #[arg(long, num_args = 0..=1, default_missing_value = "127.0.0.1:4098")]
    stats_listen: Option<String>,

    // ── Query safety net (D-059) ──────────────────────────────────
    /// Default query look-back window, in seconds. When a query request carries
    /// **neither** a lower nor an upper time bound, `ts_min` is clamped to
    /// `now - this` before candidate selection, so an unbounded query no longer
    /// fans out over every block in the bucket. An explicit bound of either kind
    /// is honored exactly. `0` disables the default (queries with no bounds scan
    /// everything again). See D-059.
    #[arg(long, default_value_t = scry_query::DEFAULT_QUERY_WINDOW_SECS)]
    default_query_window_secs: u64,

    /// Seconds between periodic query-activity log lines (queries started /
    /// in-flight / candidate blocks scanned since the last tick). Always on by
    /// default so runaway queries are visible in `kubectl logs`; `0` disables.
    /// Independent of `--stats-listen` (the lightweight query counters are
    /// always built now). See D-059.
    #[arg(long, default_value_t = 30)]
    stats_log_interval: u64,
}

/// Run the query daemon until SIGINT or SIGTERM.
pub async fn run(args: Args) -> Result<()> {
    let cfg = ObjStoreConfig::from_env()
        .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;

    // Pool config: env defaults, overridden by --pool-* flags.
    let mut pool_cfg =
        BufPoolConfig::from_env().context("parsing SCRY_OBJSTORE_POOL_* env vars")?;
    if let Some(v) = args.pool_warmup_count {
        pool_cfg.warmup_count = v;
    }
    if let Some(v) = args.pool_warmup_size_mib {
        pool_cfg.warmup_size = v * 1024 * 1024;
    }
    if let Some(v) = args.pool_initial_capacity {
        pool_cfg.initial_capacity = v;
    }
    if let Some(v) = args.pool_max_capacity {
        pool_cfg.max_capacity = v;
    }
    if let Some(v) = args.pool_max_retained_mib {
        pool_cfg.max_retained_bytes = v
            .checked_mul(1024 * 1024)
            .context("--pool-max-retained-mib overflows usize when converted to bytes")?;
    }
    if let Some(v) = args.pool_autoscale_headroom {
        pool_cfg.autoscale_headroom = v;
    }
    let (store, pool) = open_with_pool_config(&cfg, pool_cfg).await?;

    // Cold-start bootstrap (D-055): if the catalog file is absent, restore it
    // from the bucket snapshot in a single GET instead of paying an O(all
    // blocks) reconcile before the first query. If the snapshot is unavailable
    // or unusable, remember that this process must perform a full seed below.
    // That seed runs in the main container before the TCP listener opens: in
    // Kubernetes the pod is visibly Running but remains unready, and operators
    // can follow its progress through the normal queryd logs (unlike an init
    // container, which leaves the application container stuck at Initializing).
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
                    expected,
                    "catalog snapshot schema version mismatch; a full catalog seed is required"
                );
            }
            Err(e) => {
                needs_cold_seed = true;
                warn!(error = %e, "catalog snapshot restore failed; a full catalog seed is required");
            }
        }
    }

    // Wrapped in `Mutex` so the `QueryService` is `Sync` (the
    // underlying `rusqlite::Connection` is `!Sync`). The daemon only
    // holds the guard for the brief synchronous `list_blocks` call
    // per request — async work happens after the guard drops, so
    // concurrent queries serialize on a single SELECT each.
    let catalog = Arc::new(Mutex::new(
        Catalog::open(&args.catalog, &cfg.bucket)
            .with_context(|| format!("opening catalog at {}", args.catalog.display()))?,
    ));

    // Clones for the convergence loops, captured before `catalog`/`store` are
    // moved into the service. The daemon and the loops share one catalog
    // connection (`std::sync::Mutex<Catalog>` is a `CatalogHandle`).
    let conv_catalog = catalog.clone();
    let conv_store = store.clone();
    let conv_bucket = cfg.bucket.clone();

    // ── Valkey (v0.9 convergence + D-054 live discovery) ──────────
    // Built before the service so the live-merge (D-054) discovery source can
    // be injected: `--live` logs queries fan in to the ingesters discovered
    // via the D-053 tail registry. With no Valkey the live half is refused
    // (`QUERY_ERR_LIVE_UNAVAILABLE`), so we only attach a discovery when it's
    // present.
    let valkey_url = args
        .valkey_url
        .clone()
        .or_else(|| std::env::var(VALKEY_URL_ENV).ok());
    // Ephemeral per-process identity — the query daemon has no persistent
    // writer_id (it writes no blocks), so it mints one UUID at startup and
    // reuses it as the Valkey client id AND the status-registry / self_id key.
    let instance_uuid = Uuid::now_v7();
    let valkey_keys = scry_valkey::Keyspace::resolve(args.valkey_namespace.as_deref())
        .context("resolving the Valkey key namespace")?;
    let valkey = match valkey_url.as_deref() {
        Some(url) => Some(
            ValkeyClient::connect(url, instance_uuid, valkey_keys.clone())
                .await
                .with_context(|| format!("connecting to Valkey at {url}"))?,
        ),
        None => {
            info!("{VALKEY_URL_ENV} unset and no --valkey-url; convergence via polling + full-walk only");
            None
        }
    };
    // The convergence consumer starts **here**, before the seed walk rather
    // than alongside the other background loops far below. The walk is O(every
    // block in the bucket) and can run for minutes; a consumer started after it
    // hears nothing published during that window, and pub/sub is never
    // replayed. Most such misses self-correct (the next poll or walk re-reads
    // the same bucket truth), but a `SoftDeleted` does not: a staged block's
    // objects are deliberately still in the bucket, so a walk that misses the
    // event re-lists the block as live. See `apply_staged_deletions` below for
    // the other half of that fix.
    let mut bg_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut consumer_ready: Option<tokio::sync::oneshot::Receiver<()>> = None;
    if let Some(url) = valkey_url.clone() {
        let cat = conv_catalog.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        consumer_ready = Some(rx);
        bg_tasks.push(tokio::spawn(run_consumer(
            url,
            valkey_keys.clone(),
            cat,
            Some(tx),
        )));
    }

    // Spawning the consumer is not the same as being subscribed: `run_consumer`
    // does its `subscribe_blocks` inside the task, so without this barrier the
    // seed walk and the staged-deletions read below could both complete while
    // the subscription was still being established — and a staging published in
    // that gap would be missed by both halves of the fix. Bounded, because a
    // Valkey that never answers must not keep the daemon from booting: on
    // timeout we carry on and let the poll loops converge.
    if let Some(rx) = consumer_ready {
        match tokio::time::timeout(CONSUMER_READY_TIMEOUT, rx).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => warn!("block-event consumer stopped before it subscribed"),
            Err(_) => warn!(
                timeout_secs = CONSUMER_READY_TIMEOUT.as_secs(),
                "block-event subscription not established in time; continuing (polling will backstop)"
            ),
        }
    }

    if needs_cold_seed {
        info!(
            catalog = %args.catalog.display(),
            bucket = %cfg.bucket,
            "catalog seed starting; query listener will remain unready until it completes"
        );
        let started = std::time::Instant::now();
        let report = full_walk(store.as_ref(), catalog.as_ref(), &cfg.bucket)
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

    // Whatever we just seeded from the bucket, our peers may already have
    // hidden some of it. A staged deletion is invisible in the bucket by
    // design — the objects stay put for the grace window — so the walk above
    // inserted those blocks as live, and the `SoftDeleted` that said otherwise
    // was published before this process existed and is never replayed.
    //
    // Reading the staged set *after* the inserts is what makes this work with
    // no memory of events for blocks we did not yet have: by now the rows
    // exist, so a plain `mark_deleted` lands. And this runs before the
    // listener opens, so there is no window in which we serve a block a peer
    // considers gone.
    if let Some(vk) = valkey.as_ref() {
        match scry_valkey::converge_staged_deletions(vk, conv_catalog.as_ref()).await {
            Ok(n) if n > 0 => info!(
                staged = n,
                "applied peers' staged deletions before opening the listener"
            ),
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "applying peers' staged deletions failed; blocks a peer has hidden may be listed until they are reaped")
            }
        }
    }

    // Postings cache: env defaults, overridden by --postings-cache-bytes.
    let mut cache_cfg =
        PostingsCacheConfig::from_env().context("parsing SCRY_POSTINGS_CACHE_BYTES env var")?;
    if let Some(v) = args.postings_cache_bytes {
        cache_cfg.budget_bytes = v;
    }
    if let Some(v) = args.postings_cache_max_fills {
        cache_cfg.max_concurrent_fills = v.max(1);
    }
    let postings_cache = Arc::new(PostingsCache::new(cache_cfg));

    // Bloom cache: env defaults, overridden by --bloom-cache-bytes.
    let mut bloom_cache_cfg =
        BloomCacheConfig::from_env().context("parsing SCRY_BLOOM_CACHE_BYTES env var")?;
    if let Some(v) = args.bloom_cache_bytes {
        bloom_cache_cfg.budget_bytes = v;
    }
    if let Some(v) = args.bloom_cache_max_fills {
        bloom_cache_cfg.max_concurrent_fills = v.max(1);
    }
    let bloom_cache = Arc::new(BloomCache::new(bloom_cache_cfg));

    // Query-result cache: byte-budgeted LRU over exact framed response
    // bytes. `--query-cache-bytes 0` disables it entirely.
    let result_cache = Arc::new(QueryResultCache::with_budget_bytes(args.query_cache_bytes));

    // ── Memory pool + shared RuntimeEnv ───────────────────────────
    //
    // The pool is constructed once and lives for the lifetime of the
    // daemon process. Sharing it across every per-request
    // `SessionContext` is what gives us the cross-query budget —
    // DataFusion only enforces the limit when `SessionContext`s are
    // built from the same `RuntimeEnv` (see
    // `datafusion/execution/src/runtime_env.rs`). We keep a concrete
    // `Arc<GreedyMemoryPool>` next to the dyn-typed pool inside the
    // RuntimeEnv so the daemon can sample `reserved()` per query
    // without downcasting.
    let memory_budget_bytes = args
        .query_memory_budget_mib
        .checked_mul(1024 * 1024)
        .context("--query-memory-budget-mib overflows usize when converted to bytes")?;
    let memory_pool = Arc::new(GreedyMemoryPool::new(memory_budget_bytes));
    let runtime_env = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool.clone())
            .build()
            .context("building shared DataFusion RuntimeEnv")?,
    );
    let memory_guard: Option<Arc<dyn QueryMemoryGuard>> = if args.query_memory_reserve_mib == 0 {
        None
    } else {
        let reserve_bytes = args.query_memory_reserve_mib.saturating_mul(1024 * 1024);
        CgroupMemoryGuard::detect(reserve_bytes)
            .context("detecting Linux cgroup memory limit")?
            .map(|guard| {
                info!(
                    cgroup_memory_limit_bytes = guard.limit_bytes(),
                    query_reject_at_bytes = guard.reject_at_bytes(),
                    query_memory_reserve_bytes = reserve_bytes,
                    "enabled cgroup-aware query memory guard"
                );
                Arc::new(guard) as Arc<dyn QueryMemoryGuard>
            })
    };

    // Install handlers before the Valkey connection and listener setup. As PID 1
    // in a container, queryd must explicitly catch Kubernetes SIGTERM. The watch
    // value also prevents later listeners from missing an earlier signal.
    let shutdown = scry_server::shutdown::channel();

    let live_discovery: Option<Arc<dyn LiveDiscovery>> = valkey
        .clone()
        .map(|vk| Arc::new(ValkeyLiveDiscovery { valkey: vk }) as Arc<dyn LiveDiscovery>);
    // Fleet discovery is part of the query protocol, not the optional standalone
    // status HTTP page. A Valkey-connected queryd can therefore serve the Web UI
    // even when `--stats-listen` is disabled.
    let fleet_source: Option<Arc<dyn FleetSource>> = valkey
        .clone()
        .map(|vk| Arc::new(ValkeyFleetSource { valkey: vk }) as Arc<dyn FleetSource>);

    // Catalog size + trend, sampled on the gauge's own read-only connection
    // rather than read under the shared catalog mutex. Before this, every
    // status heartbeat (~2s) ran three full scans of `blocks` on the same lock
    // queries take; now the scan happens once a minute, off to the side, and
    // the status path is a struct read. The trend it accumulates is the point:
    // a block count alone cannot say whether compaction is winning.
    let catalog_gauge = CatalogGauge::new(args.catalog.clone());
    catalog_gauge.clone().spawn(CATALOG_GAUGE_INTERVAL);

    // Query metrics: **always built now** (D-059) — the lightweight query
    // counters (`queries_total`/`queries_in_flight`/`blocks_scanned_total`, a
    // few `Relaxed` atomics) back the periodic activity log, which is on by
    // default so runaway queries are visible in `kubectl logs`. This narrows
    // D-057's opt-in to just the `--stats-listen` HTTP page + Valkey fleet
    // heartbeat below; the counters themselves are free. Shares the caches /
    // memory pool the service already holds, so a snapshot is a handful of
    // live reads with no hot-path cost.
    let query_metrics: Arc<QueryMetrics> = Arc::new(QueryMetrics::new(
        instance_uuid.to_string(),
        args.listen.to_string(),
        postings_cache.clone(),
        bloom_cache.clone(),
        result_cache.clone(),
        memory_pool.clone(),
        catalog_gauge,
        valkey.as_ref().map(|c| c.health()),
    ));

    let service = Arc::new(
        QueryService::new(
            catalog,
            store,
            pool.clone(),
            postings_cache.clone(),
            bloom_cache.clone(),
            runtime_env.clone(),
            memory_pool.clone(),
            result_cache.clone(),
            args.query_cache_entry_bytes,
        )
        .with_live_discovery(live_discovery)
        .with_fleet_source(fleet_source.clone())
        // Same id this instance publishes to the Valkey status registry, so a
        // per-query timing breakdown can be traced back to a specific daemon on
        // the fleet page rather than just "one of the queriers".
        .with_node_id(instance_uuid.to_string())
        .with_metrics(Some(query_metrics.clone()))
        .with_default_window(args.default_query_window_secs)
        .with_live_fetch_limits(LiveFetchLimits {
            concurrency: args.live_fetch_concurrency,
            max_peer_bytes: args
                .live_fetch_peer_max_mib
                .checked_mul(1024 * 1024)
                .context("--live-fetch-peer-max-mib overflows usize")?,
            max_total_bytes: args
                .live_fetch_max_mib
                .checked_mul(1024 * 1024)
                .context("--live-fetch-max-mib overflows usize")?,
            max_total_rows: args.live_fetch_max_rows,
        })
        .with_query_admission(
            args.query_max_active,
            args.query_max_waiting,
            Duration::from_secs(args.query_queue_timeout),
        )
        .with_memory_guard(memory_guard),
    );

    info!(
        listen = %args.listen,
        catalog = %args.catalog.display(),
        bucket  = %cfg.bucket,
        pool_warmup_parked          = pool.free_count(),
        pool_retained_bytes         = pool.free_bytes(),
        pool_max_retained_bytes     = pool.max_retained_bytes(),
        pool_capacity               = pool.capacity(),
        postings_cache_budget_bytes = cache_cfg.budget_bytes,
        postings_cache_max_fills    = cache_cfg.max_concurrent_fills,
        bloom_cache_budget_bytes    = bloom_cache_cfg.budget_bytes,
        bloom_cache_max_fills       = bloom_cache_cfg.max_concurrent_fills,
        query_cache_budget_bytes    = result_cache.budget_bytes(),
        query_cache_entry_bytes     = args.query_cache_entry_bytes,
        query_memory_budget_bytes   = memory_budget_bytes,
        live_fetch_concurrency      = args.live_fetch_concurrency.max(1),
        live_fetch_peer_max_mib     = args.live_fetch_peer_max_mib,
        live_fetch_max_mib          = args.live_fetch_max_mib,
        live_fetch_max_rows         = args.live_fetch_max_rows,
        query_max_active            = args.query_max_active.max(1),
        query_max_waiting           = args.query_max_waiting,
        query_queue_timeout_secs    = args.query_queue_timeout,
        "query daemon ready"
    );

    // ── Catalog convergence (v0.9) ────────────────────────────────
    // Query-only: pub/sub apply (low-latency), incremental cursor poll, and
    // periodic full-walk all converge this daemon's catalog onto the shared
    // bucket so peers' freshly-written/compacted/reaped blocks become
    // queryable. No maintenance loop (no lease) — the daemon never does
    // destructive work. Stale rows a peer deleted are healed at query time by
    // the `EvictOnNotFound` re-plan in `QueryService`. (`valkey`/`valkey_url`
    // were built above so the live-discovery source could be injected.)
    // (the pub/sub convergence consumer was started before the seed walk)

    // The staged-deletions read (D-063) is sequenced at the *end* of each poll
    // and walk rather than run on its own timer. Both of those can insert a
    // block a peer has already staged — the objects are still in the bucket
    // during the grace window, so there is nothing in the bucket to say
    // otherwise — and an independent timer would leave it listed as live until
    // the next tick. `None` when there is no Valkey: nothing to converge
    // against, and the call is skipped entirely.
    let staged_client = valkey.clone();

    // Incremental cursor poller.
    {
        let staged_client = staged_client.clone();
        let store = conv_store.clone();
        let bucket = conv_bucket.clone();
        let cat = conv_catalog.clone();
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
                scry_valkey::converge_staged_deletions_logged(
                    staged_client.as_ref(),
                    cat.as_ref(),
                    "poll",
                )
                .await;
            }
        }));
    }

    // Periodic full walk.
    {
        let staged_client = staged_client.clone();
        let store = conv_store.clone();
        let bucket = conv_bucket.clone();
        let cat = conv_catalog.clone();
        let interval = Duration::from_secs(args.full_walk_interval.max(1));
        bg_tasks.push(tokio::spawn(async move {
            loop {
                // Walk first, then sleep — so the gap is measured from the end
                // of a walk, not on a fixed schedule (D-066).
                //
                // A fixed-rate `tokio::time::interval` is what let gothab walk
                // its bucket continuously for five days: a pass took 15-20
                // hours against a 30-minute period, so the next tick was always
                // already due and the walk restarted the instant it finished.
                // Measuring from completion means an overrunning walk idles
                // afterwards instead.
                //
                // The *first* pass still runs immediately, and that is
                // deliberate. On a snapshot-restored boot (D-055) the boot seed
                // walk is skipped, and a restored catalog carries no poll
                // cursors — so this walk is the only thing that seeds them.
                // Without it `poll_once` has no prefixes to poll and the
                // incremental path is blind until the first full interval
                // elapses. It is cheap now: a converged walk costs a LIST and
                // no GETs.
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

    // ── Periodic query-activity log (D-059) ───────────────────────
    // On by default: every `--stats-log-interval` secs, log the delta since the
    // last tick (queries started, candidate blocks scanned) plus the current
    // in-flight count, so a runaway unbounded query is visible in `kubectl logs`
    // instead of looking like a silent hang. `0` disables.
    if args.stats_log_interval > 0 {
        let metrics = query_metrics.clone();
        let interval = Duration::from_secs(args.stats_log_interval);
        bg_tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Prime the baseline so the first logged delta covers one interval,
            // not since-boot.
            let (mut prev_queries, _, mut prev_blocks) = metrics.activity_snapshot();
            loop {
                tick.tick().await;
                let (queries_total, in_flight, blocks_total) = metrics.activity_snapshot();
                let queries_started = queries_total.saturating_sub(prev_queries);
                let blocks_scanned = blocks_total.saturating_sub(prev_blocks);
                prev_queries = queries_total;
                prev_blocks = blocks_total;
                info!(
                    queries_in_flight = in_flight,
                    queries_started,
                    blocks_scanned,
                    interval_secs = args.stats_log_interval,
                    "queryd activity"
                );
            }
        }));
    }

    // ── Live-tail front-door (D-053) ──────────────────────────────
    // Optional: a separate listener that speaks the ingest tail sub-protocol.
    // It discovers live ingesters from Valkey and fans their records back to
    // the `scry tail --queryd` client. Without Valkey it accepts connections
    // but refuses each subscription (`ERR_TAIL_UNAVAILABLE`) — there is nothing
    // to discover. Runs until aborted alongside the convergence loops.
    if let Some(tail_listen) = args.tail_listen {
        let valkey = valkey.clone();
        let rediscover = Duration::from_secs(args.tail_rediscover_interval.max(1));
        let max_connections = args.tail_max_connections;
        let tail_shutdown = shutdown.clone();
        bg_tasks.push(tokio::spawn(async move {
            if let Err(e) = tail_relay::serve_tail_relay(
                tail_listen,
                valkey,
                rediscover,
                max_connections,
                scry_server::shutdown::wait(tail_shutdown),
            )
            .await
            {
                warn!(error = %format!("{e:#}"), "tail-relay listener exited with error");
            }
        }));
    }

    // ── Fleet status publication + optional HTTP page (D-057) ─────
    // A Valkey-connected queryd always publishes its snapshot. Fleet status is
    // part of the query protocol, so publication must not depend on whether the
    // standalone HTTP dashboard is enabled.
    let status_registration = match valkey.as_ref() {
        Some(c) => {
            let metrics = query_metrics.clone();
            let producer: scry_valkey::StatusProducer =
                Arc::new(move || serde_json::to_string(&metrics.snapshot()).unwrap_or_default());
            Some(
                StatusRegistration::spawn(c, instance_uuid, STATUS_TTL, producer)
                    .await
                    .context("registering status in Valkey")?,
            )
        }
        None => None,
    };

    // The HTTP status page remains opt-in. With Valkey it renders the whole
    // fleet; without Valkey it serves just this instance (`source: "local"`).
    if let Some(addr) = args.stats_listen.clone() {
        let fleet = fleet_source.clone();
        let local: Arc<dyn LocalStatus> = query_metrics.clone();
        let self_id = instance_uuid.to_string();
        let status_shutdown = shutdown.clone();
        bg_tasks.push(tokio::spawn(async move {
            if let Err(e) = serve_status(
                addr,
                local,
                fleet,
                self_id,
                scry_server::shutdown::wait(status_shutdown),
            )
            .await
            {
                warn!(error = %e, "status endpoint failed");
            }
        }));
    }

    let serve_result = service
        .serve_with_shutdown(args.listen, scry_server::shutdown::wait(shutdown.clone()))
        .await;

    // Process is exiting — stop the convergence loops and close Valkey.
    for t in &bg_tasks {
        t.abort();
    }
    // Best-effort prompt cleanup. Registry TTLs are the correctness backstop;
    // never let a degraded Valkey consume the pod's whole termination grace.
    if let Some(reg) = status_registration {
        if tokio::time::timeout(SHUTDOWN_CLEANUP_TIMEOUT, reg.deregister())
            .await
            .is_err()
        {
            warn!("timed out deregistering query status during shutdown");
        }
    }
    if let Some(c) = valkey {
        if tokio::time::timeout(SHUTDOWN_CLEANUP_TIMEOUT, c.quit())
            .await
            .is_err()
        {
            warn!("timed out closing Valkey during shutdown");
        }
    }
    serve_result
}

/// Background pub/sub convergence consumer: subscribe to every block-event
/// channel and apply each event to the catalog idempotently. Reconnects on a
/// closed subscription; lag drops events (the cursor poller backstops).
/// How long the boot path waits for the pub/sub subscription to come up before
/// proceeding without it. Long enough to cover a slow Valkey handshake, short
/// enough that an unreachable Valkey delays readiness rather than preventing it.
const CONSUMER_READY_TIMEOUT: Duration = Duration::from_secs(10);

async fn run_consumer(
    url: String,
    keys: scry_valkey::Keyspace,
    catalog: Arc<Mutex<Catalog>>,
    mut subscribed: Option<tokio::sync::oneshot::Sender<()>>,
) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match subscribe_blocks(&url, &keys, &ALL_SIGNALS).await {
            Ok((_sub, mut rx)) => {
                info!("subscribed to block-event channels for catalog convergence");
                // Tell the boot path the subscription is live. Only the first
                // success matters; a later reconnect has no one waiting.
                if let Some(tx) = subscribed.take() {
                    let _ = tx.send(());
                }
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
