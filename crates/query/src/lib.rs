//! DataFusion-backed metrics querier (v0.3 step 1).
//!
//! The v0.2 querier wrote its own per-block orchestrator, row-group
//! prune loop, and `RowFilter` predicate. Profiling showed that
//! 1.5 % of wall time was the parquet decode and ~63 % was the
//! CLI's per-sample `println!` + a SipHash-backed `HashSet<u64>`
//! predicate — both of which DataFusion replaces wholesale.
//!
//! The architectural sketch in `ARCHITECTURE.md` always pointed at
//! DataFusion as the eventual query engine (scatter-gather, memory
//! budgets, PromQL execution). This module is the substrate that
//! later signals (logs/traces/profiles) and PromQL-on-metrics layer
//! on top of.
//!
//! ## Public shape
//!
//! - [`MetricsQuery`] — the same `Vec<(name, value)>` AND'd-equality
//!   shape as v0.2. Stays as the entry-point because (a) it's the
//!   right preselect for the postings sidecar, (b) it survives
//!   whether a later layer adds SQL/PromQL on top.
//! - [`register_metrics_table`] — async helper that does the
//!   postings + catalog work *once*, builds a [`MetricsTable`], and
//!   registers it under the name `"metrics"` on the caller's
//!   `SessionContext`. After this returns, the caller can use
//!   `ctx.sql(...)` / `ctx.table("metrics").await?` freely.
//! - [`metrics_query`] — convenience that wraps the above and
//!   returns a `DataFrame` for the common shape (no SQL desired).
//!
//! The v0.2 `Sample` and `BlockHit` structs are intentionally gone —
//! results stream as `RecordBatch`es and pruning stats come from
//! DataFusion's `MetricsSet` on the produced `ExecutionPlan`.

pub mod flight_proto;
pub mod postings;
pub mod postings_cache;
mod table;

use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;
use object_store::ObjectStore;
use scry_catalog::{Catalog, CatalogEntry};
use serde::{Deserialize, Serialize};

pub use flight_proto::QueryRequest;
pub use postings::resolve_fingerprints;
pub use postings_cache::{
    PostingsCache, PostingsCacheConfig, PostingsCacheStats, PostingsIndex,
    DEFAULT_BUDGET_BYTES as DEFAULT_POSTINGS_CACHE_BYTES,
};
pub use table::{time_overlaps, BlockEntry, MetricsTable};

/// AND of equality matchers over a metrics block. An empty matcher
/// set returns every series in every overlapping block — useful as a
/// "scan everything" sanity primitive. The fingerprint set for the
/// empty-matcher case is derived from `BlockMeta::series_types`
/// (sidecar JSON) rather than from a postings scan, since the
/// postings file is keyed by `(label_name, label_value)` and has no
/// natural "all series" row.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsQuery {
    pub matchers: Vec<(String, String)>,
    pub ts_min: Option<u64>,
    pub ts_max: Option<u64>,
}

/// Default name the `MetricsTable` is registered under in a
/// `SessionContext`. Both `register_metrics_table` and the CLI's
/// `--sql` path agree on this so users can write
/// `SELECT … FROM metrics …` without thinking about it.
pub const METRICS_TABLE_NAME: &str = "metrics";

/// Resolve postings + narrow the catalog block list into a ready-to-
/// register [`MetricsTable`]. All async I/O happens here — once it
/// returns the table is pure planning data.
///
/// Use this directly when you want to inspect the narrowed block
/// list (tests, diagnostics) before handing the table to DataFusion.
/// In the common case [`register_metrics_table`] or [`metrics_query`]
/// wrap this and register on a context.
pub async fn build_metrics_table(
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &MetricsQuery,
) -> Result<MetricsTable> {
    // Two-phase split: list_metrics_candidates is pure sync work over
    // the catalog handle; the rest is the async postings-resolve dance.
    // Splitting them lets callers that wrap the catalog in a mutex
    // (the Flight daemon) lock once for the sync part, drop the lock,
    // and run the async path under no lock at all.
    let candidates = list_metrics_candidates(catalog, q)?;
    build_metrics_table_from_candidates(candidates, store, None, q).await
}

/// Synchronous step: list catalog blocks, filter by signal=`"metrics"`
/// and the query's time bounds. Pure compute over the connection +
/// returns owned data, so callers wrapping the catalog in a mutex
/// can drop the guard before doing any async work.
pub fn list_metrics_candidates(
    catalog: &Catalog,
    q: &MetricsQuery,
) -> Result<Vec<CatalogEntry>> {
    Ok(catalog
        .list_blocks()
        .context("listing blocks from catalog")?
        .into_iter()
        .filter(|e| e.meta.signal == "metrics")
        .filter(|e| table::time_overlaps(&e.meta, q.ts_min, q.ts_max))
        .collect())
}

/// Async step: take the already-narrowed catalog list (per
/// [`list_metrics_candidates`]), run postings resolve per block, and
/// produce a ready-to-register [`MetricsTable`].
pub async fn build_metrics_table_from_candidates(
    candidates: Vec<CatalogEntry>,
    store: Arc<dyn ObjectStore>,
    cache: Option<&PostingsCache>,
    q: &MetricsQuery,
) -> Result<MetricsTable> {
    // All candidates must share a bucket — otherwise the single
    // `object_store_url` we pick is wrong. The catalog can in
    // principle hold rows for multiple buckets (compaction across
    // sites), but the v0.2 catalog only ever sees one. Defensively
    // assert; if it ever fires we add per-bucket `TableProvider`
    // splitting at registration time rather than discover it at
    // scan() in production.
    let bucket = match candidates.first() {
        Some(first) => {
            let b = first.bucket.clone();
            anyhow::ensure!(
                candidates.iter().all(|e| e.bucket == b),
                "metrics blocks span multiple buckets; multi-bucket queries not yet supported"
            );
            b
        }
        None => {
            // No overlapping blocks at all. Return an empty table so
            // SQL like `SELECT count(*) FROM metrics` still works.
            return Ok(MetricsTable::new("", Vec::new(), q.ts_min, q.ts_max)?);
        }
    };

    // ── Step 2: postings resolve (per block) ──────────────────────
    //
    // Serial for now — at v0.2/v0.3 scale (≤ low hundreds of blocks
    // per query) the dominant cost is the parquet scan, not the
    // postings GETs. If postings start dominating a query, the v0.2
    // querier's `buffer_unordered` is the template — but the right
    // answer is probably to cache the sidecar contents (blocks are
    // immutable; v0.3 step 3 does exactly that via `PostingsCache`).
    //
    // v0.3 step 4: we keep each block's fingerprint set *separately*
    // rather than unioning them. `MetricsTable::scan` emits one
    // `DataSourceExec` per block with that block's own fp predicate,
    // which lets row-group pruning fire against the tightest possible
    // set per file.
    let mut blocks: Vec<BlockEntry> = Vec::with_capacity(candidates.len());
    let matchers_empty = q.matchers.is_empty();
    for entry in candidates {
        // Use the cache when one is provided. The cache resolves
        // empty matchers via the un-cached fallback path so callers
        // don't have to branch on `matchers.is_empty()` themselves.
        let resolved = match cache {
            Some(c) => c.resolve(store.clone(), &entry.meta, &q.matchers).await?,
            None => postings::resolve_fingerprints(store.clone(), &entry.meta, &q.matchers).await?,
        };
        match resolved {
            None => {
                // Postings intersect was empty → block fully pruned;
                // don't even add it to `blocks`. Preserves the v0.2
                // "matched 0 fingerprints (postings pruned)" outcome
                // — DataFusion never opens the parquet.
            }
            Some(set) => {
                let fp_set = if matchers_empty {
                    // Empty matcher set deliberately skips attaching a
                    // fingerprint filter — the v0.2 contract is "return
                    // every sample in every overlapping block".
                    None
                } else {
                    // Sort once for stable test output and slightly
                    // tighter physical-eval (DataFusion's `InListExpr`
                    // handles either shape, but sorted means
                    // predictable + better row-group min/max
                    // alignment).
                    let mut v: Vec<u64> = set.into_iter().collect();
                    v.sort_unstable();
                    Some(Arc::new(v))
                };
                blocks.push(BlockEntry { entry, fp_set });
            }
        }
    }

    MetricsTable::new(&bucket, blocks, q.ts_min, q.ts_max)
        .map_err(|e| anyhow::anyhow!("constructing MetricsTable: {e}"))
}

/// Build the table (postings resolve + catalog narrow) and register
/// it on `ctx` under `"metrics"`. Also registers `store` against the
/// table's `ObjectStoreUrl` so DataFusion can route reads.
pub async fn register_metrics_table(
    ctx: &SessionContext,
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &MetricsQuery,
) -> Result<()> {
    let candidates = list_metrics_candidates(catalog, q)?;
    register_metrics_table_from_candidates(ctx, candidates, store, None, q).await
}

/// Same as [`register_metrics_table`] but accepts pre-listed catalog
/// entries — for callers that need to take the catalog lock for the
/// sync `list_metrics_candidates` call themselves (e.g. the Flight
/// daemon, where the `Catalog` lives behind a `Mutex` so the service
/// can be `Sync`).
pub async fn register_metrics_table_from_candidates(
    ctx: &SessionContext,
    candidates: Vec<CatalogEntry>,
    store: Arc<dyn ObjectStore>,
    cache: Option<&PostingsCache>,
    q: &MetricsQuery,
) -> Result<()> {
    let table = build_metrics_table_from_candidates(candidates, store.clone(), cache, q).await?;

    // Register the object store under the URL the table will query.
    // `register_object_store` routes on (scheme, host).
    let url: &url::Url = table.object_store_url().as_ref();
    ctx.runtime_env().register_object_store(url, store);

    ctx.register_table(METRICS_TABLE_NAME, Arc::new(table))
        .map_err(|e| anyhow::anyhow!("register metrics table: {e}"))?;
    Ok(())
}

/// One-shot convenience: build a fresh `SessionContext`, register the
/// metrics table, return the `metrics` `DataFrame`. The caller can
/// `.collect()` for batches, chain `.filter` / `.aggregate`, or use
/// `.show()` for ad-hoc inspection.
///
/// If you need to issue SQL or register additional tables, use
/// [`register_metrics_table`] against your own `SessionContext`.
pub async fn metrics_query(
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &MetricsQuery,
) -> Result<DataFrame> {
    let ctx = SessionContext::new();
    register_metrics_table(&ctx, catalog, store, q).await?;
    ctx.table(METRICS_TABLE_NAME)
        .await
        .with_context(|| format!("looking up table {METRICS_TABLE_NAME}"))
}
