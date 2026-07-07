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
//! 4. Construct a [`QueryService`] and serve until ctrl-c.
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
use scry_server::{LiveDiscovery, QueryService};
use scry_valkey::{
    discover_tail_endpoints, parse_envelope, subscribe_blocks, ValkeyClient, VALKEY_URL_ENV,
};
use tracing::{info, warn};
use uuid::Uuid;

mod tail_relay;

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
        discover_tail_endpoints(self.valkey.inner()).await
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

    /// Body-bloom sidecar cache byte budget for the logs full-text
    /// path. Overrides `SCRY_BLOOM_CACHE_BYTES` if both are set. Blooms
    /// run ~2% of body size (tens to hundreds of KB per block), so the
    /// default budget holds many more blocks than postings needs. Set
    /// to 0 to disable (every `--grep` query refetches each block's
    /// bloom; correctness is unaffected, it's a pure accelerator).
    #[arg(long)]
    bloom_cache_bytes: Option<usize>,

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

    // ── Multi-instance convergence (v0.9) ─────────────────────────
    /// Valkey URL for pub/sub catalog convergence. Falls back to
    /// `$SCRY_VALKEY_URL`. The query daemon is **query-only**: it never
    /// runs maintenance (no lease), it only *follows* the bucket so peers'
    /// blocks become queryable promptly. With Valkey absent, convergence
    /// still runs via polling + full-walk (just higher latency).
    #[arg(long)]
    valkey_url: Option<String>,

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
}

/// Run the query daemon until ctrl-c.
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
    if let Some(v) = args.pool_autoscale_headroom {
        pool_cfg.autoscale_headroom = v;
    }
    let (store, pool) = open_with_pool_config(&cfg, pool_cfg)?;

    // Cold-start bootstrap (D-055): if the catalog file is absent, restore it
    // from the bucket snapshot in a single GET instead of paying an O(all
    // blocks) reconcile before the first query. Best-effort — on no snapshot,
    // a schema-version mismatch, or any error we fall through to an empty
    // catalog and let the poll + full-walk convergence loops (spawned below,
    // Valkey or not) repopulate it exactly as before.
    if !args.no_snapshot_restore && !args.catalog.exists() {
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
                info!("no catalog snapshot in bucket; starting cold (convergence will fill it)");
            }
            Ok(scry_catalog::RestoreOutcome::VersionMismatch { found, expected }) => {
                warn!(
                    found,
                    expected,
                    "catalog snapshot schema version mismatch; ignoring it, starting cold"
                );
            }
            Err(e) => warn!(error = %e, "catalog snapshot restore failed; starting cold"),
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

    // Postings cache: env defaults, overridden by --postings-cache-bytes.
    let mut cache_cfg =
        PostingsCacheConfig::from_env().context("parsing SCRY_POSTINGS_CACHE_BYTES env var")?;
    if let Some(v) = args.postings_cache_bytes {
        cache_cfg.budget_bytes = v;
    }
    let postings_cache = Arc::new(PostingsCache::new(cache_cfg));

    // Bloom cache: env defaults, overridden by --bloom-cache-bytes.
    let mut bloom_cache_cfg =
        BloomCacheConfig::from_env().context("parsing SCRY_BLOOM_CACHE_BYTES env var")?;
    if let Some(v) = args.bloom_cache_bytes {
        bloom_cache_cfg.budget_bytes = v;
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
    let valkey = match valkey_url.as_deref() {
        Some(url) => Some(
            ValkeyClient::connect(url, Uuid::now_v7())
                .await
                .with_context(|| format!("connecting to Valkey at {url}"))?,
        ),
        None => {
            info!("{VALKEY_URL_ENV} unset and no --valkey-url; convergence via polling + full-walk only");
            None
        }
    };
    let live_discovery: Option<Arc<dyn LiveDiscovery>> = valkey
        .clone()
        .map(|vk| Arc::new(ValkeyLiveDiscovery { valkey: vk }) as Arc<dyn LiveDiscovery>);

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
        .with_live_discovery(live_discovery),
    );

    info!(
        listen = %args.listen,
        catalog = %args.catalog.display(),
        bucket  = %cfg.bucket,
        pool_warmup_parked          = pool.free_count(),
        pool_capacity               = pool.capacity(),
        postings_cache_budget_bytes = cache_cfg.budget_bytes,
        bloom_cache_budget_bytes    = bloom_cache_cfg.budget_bytes,
        query_cache_budget_bytes    = result_cache.budget_bytes(),
        query_cache_entry_bytes     = args.query_cache_entry_bytes,
        query_memory_budget_bytes   = memory_budget_bytes,
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
    let mut bg_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // pub/sub convergence consumer (only with Valkey).
    if let Some(url) = valkey_url.clone() {
        let cat = conv_catalog.clone();
        bg_tasks.push(tokio::spawn(run_consumer(url, cat)));
    }

    // Incremental cursor poller.
    {
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
            }
        }));
    }

    // Periodic full walk.
    {
        let store = conv_store.clone();
        let bucket = conv_bucket.clone();
        let cat = conv_catalog.clone();
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

    // ── Live-tail front-door (D-053) ──────────────────────────────
    // Optional: a separate listener that speaks the ingest tail sub-protocol.
    // It discovers live ingesters from Valkey and fans their records back to
    // the `scry tail --queryd` client. Without Valkey it accepts connections
    // but refuses each subscription (`ERR_TAIL_UNAVAILABLE`) — there is nothing
    // to discover. Runs until aborted alongside the convergence loops.
    if let Some(tail_listen) = args.tail_listen {
        let valkey = valkey.clone();
        let rediscover = Duration::from_secs(args.tail_rediscover_interval.max(1));
        bg_tasks.push(tokio::spawn(async move {
            if let Err(e) = tail_relay::serve_tail_relay(
                tail_listen,
                valkey,
                rediscover,
                std::future::pending::<()>(),
            )
            .await
            {
                warn!(error = %format!("{e:#}"), "tail-relay listener exited with error");
            }
        }));
    }

    let serve_result = service
        .serve_with_shutdown(args.listen, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;

    // Process is exiting — stop the convergence loops and close Valkey.
    for t in &bg_tasks {
        t.abort();
    }
    if let Some(c) = valkey {
        c.quit().await;
    }
    serve_result
}

/// Background pub/sub convergence consumer: subscribe to every block-event
/// channel and apply each event to the catalog idempotently. Reconnects on a
/// closed subscription; lag drops events (the cursor poller backstops).
async fn run_consumer(url: String, catalog: Arc<Mutex<Catalog>>) {
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
