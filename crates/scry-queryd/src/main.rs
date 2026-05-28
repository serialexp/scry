//! scry-queryd — long-running query daemon (binschema-over-TCP).
//!
//! The architectural counterpart to `scry-ingestd`: where `scry-ingestd`
//! exposes `scry-server::Server` (ingest) as a process, this binary
//! exposes `scry-server::QueryService` (query) over the same length-
//! prefixed binschema framing pattern as ingest — `QueryFrame`s
//! defined in `proto/query.schema.json`, one TCP connection per
//! query. (Arrow Flight was the v0.3 step 2 transport; step 5
//! replaced it with binschema to keep one wire vocabulary across
//! ingest + query.) Same shape end-to-end:
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
//! scry-queryd \
//!     --catalog ./online.sqlite \
//!     --listen 127.0.0.1:4100 \
//!     --pool-warmup-count 8
//! ```
//!
//! Connect from the CLI:
//!
//! ```bash
//! scry-query --remote 127.0.0.1:4100 \
//!     --matcher __name__=scry_http_requests_total
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use scry_catalog::Catalog;
use scry_objstore::{open_with_pool_config, BufPoolConfig, ObjStoreConfig};
use scry_query::{PostingsCache, PostingsCacheConfig};
use scry_server::QueryService;
use tracing::info;

/// Swap glibc's malloc for mimalloc — same reasoning as `scry-query`
/// (large response Vecs go via `mmap` under glibc which forces a fresh
/// kernel page-zero on first write; mimalloc keeps these inside its
/// own segments and reuses them across allocations within the
/// process). For the daemon the win compounds: every query reuses the
/// allocator's warmth.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Listen address for the Arrow Flight server.
    #[arg(long, default_value = "127.0.0.1:4100")]
    listen: SocketAddr,

    /// Path to the SQLite catalog file. The daemon opens it read-only-
    /// in-spirit (ingest writers update it from separate processes;
    /// SQLite's WAL handles cross-process visibility).
    #[arg(long)]
    catalog: PathBuf,

    // ── Buffer-pool knobs (override env / defaults) ──────────────
    //
    // Identical semantics to scry-query's `--pool-*` flags. For the
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
    let mut cache_cfg = PostingsCacheConfig::from_env()
        .context("parsing SCRY_POSTINGS_CACHE_BYTES env var")?;
    if let Some(v) = args.postings_cache_bytes {
        cache_cfg.budget_bytes = v;
    }
    let postings_cache = Arc::new(PostingsCache::new(cache_cfg));

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

    let service = Arc::new(QueryService::new(
        catalog,
        store,
        pool.clone(),
        postings_cache.clone(),
        runtime_env.clone(),
        memory_pool.clone(),
    ));

    info!(
        listen = %args.listen,
        catalog = %args.catalog.display(),
        bucket  = %cfg.bucket,
        pool_warmup_parked          = pool.free_count(),
        pool_capacity               = pool.capacity(),
        postings_cache_budget_bytes = cache_cfg.budget_bytes,
        query_memory_budget_bytes   = memory_budget_bytes,
        "query daemon ready"
    );

    service
        .serve_with_shutdown(args.listen, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}
