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

mod postings;
mod table;

use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;
use object_store::ObjectStore;
use scry_catalog::Catalog;

pub use postings::resolve_fingerprints;
pub use table::{time_overlaps, MetricsTable};

/// AND of equality matchers over a metrics block. An empty matcher
/// set returns every series in every overlapping block — useful as a
/// "scan everything" sanity primitive. The fingerprint set for the
/// empty-matcher case is derived from `BlockMeta::series_types`
/// (sidecar JSON) rather than from a postings scan, since the
/// postings file is keyed by `(label_name, label_value)` and has no
/// natural "all series" row.
#[derive(Debug, Clone, Default)]
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
    // ── Step 1: catalog plan ──────────────────────────────────────
    let candidates: Vec<_> = catalog
        .list_blocks()
        .context("listing blocks from catalog")?
        .into_iter()
        .filter(|e| e.meta.signal == "metrics")
        .filter(|e| table::time_overlaps(&e.meta, q.ts_min, q.ts_max))
        .collect();

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
            return Ok(MetricsTable::new(
                "",
                Vec::new(),
                None,
                q.ts_min,
                q.ts_max,
            )?);
        }
    };

    // ── Step 2: postings resolve (per block) ──────────────────────
    //
    // Serial for now — at v0.2/v0.3 scale (≤ low hundreds of blocks
    // per query) the dominant cost is the parquet scan, not the
    // postings GETs. If postings start dominating a query, the v0.2
    // querier's `buffer_unordered` is the template — but the right
    // answer is probably to cache the sidecar contents (blocks are
    // immutable).
    let mut blocks = Vec::with_capacity(candidates.len());
    let mut union_fps: Option<std::collections::HashSet<u64>> = None;
    let matchers_empty = q.matchers.is_empty();
    for entry in candidates {
        match postings::resolve_fingerprints(store.clone(), &entry.meta, &q.matchers).await? {
            None => {
                // Postings intersect was empty → block fully pruned;
                // don't even add it to `blocks`. Preserves the v0.2
                // "matched 0 fingerprints (postings pruned)" outcome
                // — DataFusion never opens the parquet.
            }
            Some(set) => {
                blocks.push(entry);
                if !matchers_empty {
                    union_fps.get_or_insert_with(Default::default).extend(set);
                }
                // Empty matcher set deliberately skips accumulating a
                // fingerprint filter — the v0.2 contract is "return
                // every sample in every overlapping block".
            }
        }
    }

    MetricsTable::new(
        &bucket,
        blocks,
        union_fps.map(|set| {
            // Sort once for stable test output and slightly tighter
            // physical-eval (DataFusion's InListExpr handles either
            // shape, but sorted means predictable).
            let mut v: Vec<u64> = set.into_iter().collect();
            v.sort_unstable();
            Arc::new(v)
        }),
        q.ts_min,
        q.ts_max,
    )
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
    let table = build_metrics_table(catalog, store.clone(), q).await?;

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
