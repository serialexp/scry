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
//! - [`Query`] — `Vec<(name, value)>` AND'd-equality + optional
//!   `ts_min`/`ts_max` bounds. Shared across signals because at v0
//!   metrics and logs both want exactly this preselect shape
//!   (postings on AND'd equality matchers, optional time-range).
//!   When a signal eventually diverges (e.g. logs gains
//!   body-substring predicates in the tantivy phase), `Query` either
//!   grows new fields (kept simple, everyone ignores what they
//!   don't need) or splits into a signal-tagged enum — but the
//!   share is honest at v0.
//! - [`register_metrics_table`] / [`register_logs_table`] — async
//!   helpers that do the postings + catalog work *once*, build the
//!   per-signal `TableProvider`, and register it under the table
//!   name `"metrics"` / `"logs"` on the caller's `SessionContext`.
//!   After this returns, the caller can use `ctx.sql(...)` /
//!   `ctx.table("metrics" | "logs").await?` freely.
//! - [`metrics_query`] — convenience that wraps the above and
//!   returns a `DataFrame` for the common metrics shape (no SQL
//!   desired). Logs callers go through [`register_logs_table`] +
//!   `ctx.table(LOGS_TABLE_NAME)`.
//!
//! The v0.2 `Sample` and `BlockHit` structs are intentionally gone —
//! results stream as `RecordBatch`es and pruning stats come from
//! DataFusion's `MetricsSet` on the produced `ExecutionPlan`.

pub mod bloom_cache;
pub mod body_bloom;
pub mod cli;
pub mod evict;
pub mod label_enrich;
pub mod logs;
pub mod metadata;
pub mod postings;
pub mod postings_cache;
pub mod profiles;
pub mod result_cache;
mod table;
pub mod traces;
pub mod wire;

use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;
use object_store::ObjectStore;
use scry_catalog::{Catalog, CatalogEntry};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::label_enrich::{freeze_fp_labels, FpAcc};

pub use bloom_cache::{
    BloomCache, BloomCacheConfig, BloomCacheStats,
    DEFAULT_BUDGET_BYTES as DEFAULT_BLOOM_CACHE_BYTES,
};
pub use evict::EvictOnNotFound;
pub use label_enrich::{FpLabels, LabelEnrichExec, LabelPairs};
pub use metadata::{collect_label_names, collect_label_values, meta_query, MetaError};
pub use postings::resolve_fingerprints;
pub use postings_cache::{
    PostingsCache, PostingsCacheConfig, PostingsCacheStats, PostingsIndex,
    DEFAULT_BUDGET_BYTES as DEFAULT_POSTINGS_CACHE_BYTES,
};
pub use result_cache::{
    hash128, CachedResponse, QueryResultCache, QueryResultCacheStats, DEFAULT_QUERY_CACHE_BYTES,
    DEFAULT_QUERY_CACHE_ENTRY_BYTES,
};
pub use table::{time_overlaps, BlockEntry, MetricsTable};
pub use wire::QueryRequest;
// Logs symmetry: same convenience re-exports the metrics path has, so
// CLI/server callers can `use scry_query::{register_logs_table, ...}`
// without reaching into the submodule. Names are signal-prefixed
// because the metrics symbols above already claim the bare names.
pub use logs::{
    build_logs_table_from_candidates, list_logs_candidates, logs_query, register_logs_table,
    register_logs_table_from_candidates, LogsBlockEntry, LogsTable, LOGS_TABLE_NAME,
};
// Traces + profiles query verticals (v0.5 / v0.6). Same signal-prefixed
// convenience re-exports as logs — these two have no postings sidecar,
// so their `build_*` helpers skip the postings resolve entirely and the
// catalog narrow + per-block scan is the whole story.
pub use profiles::{
    build_profiles_table_from_candidates, list_profiles_candidates, profiles_query,
    register_profiles_table, register_profiles_table_from_candidates, ProfilesBlockEntry,
    ProfilesTable, PROFILES_TABLE_NAME,
};
pub use traces::{
    build_traces_table_from_candidates, list_traces_candidates, promoted_column_for,
    register_traces_table, register_traces_table_from_candidates, traces_query, TracesBlockEntry,
    TracesTable, TRACES_TABLE_NAME, TRACE_PROMOTED_LABELS,
};

/// AND of equality matchers + optional time-range bounds. Shared
/// across signals: at v0 both metrics and logs want exactly this
/// preselect shape (postings-on-AND'd-equality + a window).
///
/// An empty `matchers` set returns every series/stream in every
/// overlapping block — useful as a "scan everything" sanity
/// primitive. The fingerprint set for the empty-matcher case is
/// derived from `BlockMeta::all_fingerprints` (sidecar JSON)
/// rather than from a postings scan, since the postings file is
/// keyed by `(label_name, label_value)` and has no natural
/// "all-fingerprints" row.
///
/// When a signal eventually diverges (logs gaining body-substring
/// predicates in the tantivy phase, traces gaining trace-id
/// lookups), this struct either grows new optional fields
/// (everyone ignores what they don't need) or splits into a
/// signal-tagged enum. Today the share is honest.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Query {
    pub matchers: Vec<(String, String)>,
    pub ts_min: Option<u64>,
    pub ts_max: Option<u64>,
    /// Trace-by-id lookup, meaningful only for the traces signal. When
    /// `Some`, [`crate::traces::TracesTable`] pushes an equality
    /// predicate on the `trace_id` `FixedSizeBinary(16)` column (which
    /// the block is sorted by, so row-group min/max stats prune to the
    /// one block + row-group that holds the trace). Other signals ignore
    /// it. This is the "new optional field everyone ignores" growth the
    /// doc above predicted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<[u8; 16]>,
    /// Body substring search, meaningful only for the logs signal. When
    /// `Some(pat)`, [`crate::logs::LogsTable`] pushes a literal-substring
    /// predicate on the `body` column (so DataFusion still scans surviving
    /// rows exactly), and the planner skips any block whose body bloom
    /// sidecar rules `pat` out (the v0.7 full-text accelerator). Case-
    /// sensitive, matching the `body LIKE` backstop. Other signals ignore
    /// it — another "new optional field everyone ignores" growth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_contains: Option<String>,
    /// Opt-in fingerprint→label join, meaningful only for the metrics
    /// signal. When `true`, [`crate::table::MetricsTable`] exposes a
    /// synthesised `labels` `Map<Utf8,Utf8>` column (each series' resolved
    /// labels, inverted from the postings sidecars) so a series can be named
    /// by its labels instead of its opaque `series_fingerprint`. Off by
    /// default so fingerprint-only metrics queries stay byte-for-byte as
    /// cheap as before; even when `true` the column is only materialised
    /// when the query's projection actually selects it. Logs already carry a
    /// `labels` column unconditionally, so they ignore this flag; traces /
    /// profiles have no postings sidecar and ignore it too.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub with_labels: bool,
}

/// Default query look-back window (seconds) applied when a query specifies no
/// time bounds at all. An unbounded query selects *every* block in the bucket
/// as a scan candidate ([`table::time_overlaps`] passes all blocks when both
/// bounds are `None`), so on a large catalog the work is proportional to total
/// history — enough to blow client timeouts and OOM the daemon. Defaulting to
/// the last hour keeps a boundless query cheap; scanning further back is fine
/// but must be requested explicitly (any explicit bound disables the default).
pub const DEFAULT_QUERY_WINDOW_SECS: u64 = 3600;

/// When a query specifies **neither** time bound, clamp its look-back to the
/// last `window_nanos` (sets `ts_min = now - window`, leaving `ts_max` open so
/// recent data is never excluded by clock skew). An explicit bound of either
/// kind is honored exactly (no-op), and `window_nanos == 0` disables the
/// default entirely (no-op).
///
/// Returns `true` when a default was applied (so callers can log it).
/// `now_unix_nano` is injected rather than read from the clock inline, mirroring
/// the retention engine's determinism pattern, so the behaviour is unit-testable.
pub fn apply_default_window(q: &mut Query, now_unix_nano: u64, window_nanos: u64) -> bool {
    if window_nanos == 0 || q.ts_min.is_some() || q.ts_max.is_some() {
        return false;
    }
    q.ts_min = Some(now_unix_nano.saturating_sub(window_nanos));
    true
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
    q: &Query,
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
pub fn list_metrics_candidates(catalog: &Catalog, q: &Query) -> Result<Vec<CatalogEntry>> {
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
    q: &Query,
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
            // SQL like `SELECT count(*) FROM metrics` still works. When labels
            // were requested we still hand an (empty) `fp_labels` so the table
            // exposes the 4-column schema — a `SELECT *` returns the `labels`
            // column (with zero rows) rather than a schema that flips shape
            // depending on whether any block happened to overlap.
            let fp_labels = q.with_labels.then(|| Arc::new(FpLabels::new()));
            return Ok(MetricsTable::new(
                "",
                Vec::new(),
                q.ts_min,
                q.ts_max,
                fp_labels,
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
    // immutable; v0.3 step 3 does exactly that via `PostingsCache`).
    //
    // v0.3 step 4: we keep each block's fingerprint set *separately*
    // rather than unioning them. `MetricsTable::scan` emits one
    // `DataSourceExec` per block with that block's own fp predicate,
    // which lets row-group pruning fire against the tightest possible
    // set per file.
    let mut blocks: Vec<BlockEntry> = Vec::with_capacity(candidates.len());
    let matchers_empty = q.matchers.is_empty();
    // Opt-in fingerprint→label join (D-058): accumulate `fingerprint → labels`
    // by inverting each surviving block's postings sidecar. `None` unless the
    // query asked for it, so a label-less query never inverts anything.
    let mut fp_acc: Option<FpAcc> = q.with_labels.then(FpAcc::new);
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
                // Labels wanted → invert this surviving block's postings into
                // the accumulator. The matcher resolve above already fetched +
                // cached the parsed index (`PostingsCache::resolve` calls
                // `get_or_fetch` internally), so with a cache present this is a
                // cache hit and adds no object-store I/O. Only the empty-matcher
                // path (which resolves fingerprints from the meta sidecar, not
                // the postings) pays one postings GET per block for the labels.
                // A fetch failure is non-fatal: labels for this block are simply
                // omitted (each row still carries an — empty — `labels` map).
                if let Some(acc) = fp_acc.as_mut() {
                    let index = match cache {
                        Some(c) => c.get_or_fetch(store.clone(), &entry.meta).await,
                        None => postings::fetch_and_parse_postings(store.clone(), &entry.meta)
                            .await
                            .map(Arc::new),
                    };
                    match index {
                        Ok(idx) => idx.invert_into(acc),
                        Err(e) => warn!(
                            block = %entry.meta.uuid,
                            error = %e,
                            "metrics label join: postings fetch failed; labels omitted for this block"
                        ),
                    }
                }
                blocks.push(BlockEntry { entry, fp_set });
            }
        }
    }

    // Freeze the accumulator (when labels were requested) into the shared
    // `fingerprint → labels` map handed to the table.
    let fp_labels = fp_acc.map(|acc| Arc::new(freeze_fp_labels(acc)));

    MetricsTable::new(&bucket, blocks, q.ts_min, q.ts_max, fp_labels)
        .map_err(|e| anyhow::anyhow!("constructing MetricsTable: {e}"))
}

/// Build the table (postings resolve + catalog narrow) and register
/// it on `ctx` under `"metrics"`. Also registers `store` against the
/// table's `ObjectStoreUrl` so DataFusion can route reads.
pub async fn register_metrics_table(
    ctx: &SessionContext,
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
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
    q: &Query,
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
    q: &Query,
) -> Result<DataFrame> {
    let ctx = SessionContext::new();
    register_metrics_table(&ctx, catalog, store, q).await?;
    ctx.table(METRICS_TABLE_NAME)
        .await
        .with_context(|| format!("looking up table {METRICS_TABLE_NAME}"))
}

#[cfg(test)]
mod default_window_tests {
    use super::{apply_default_window, Query};

    const HOUR_NS: u64 = 3600 * 1_000_000_000;
    const NOW: u64 = 10 * HOUR_NS; // an arbitrary "now" well past the epoch

    #[test]
    fn applies_when_both_bounds_absent() {
        let mut q = Query::default();
        assert!(apply_default_window(&mut q, NOW, HOUR_NS));
        assert_eq!(q.ts_min, Some(NOW - HOUR_NS));
        assert_eq!(q.ts_max, None, "ts_max stays open");
    }

    #[test]
    fn noop_when_ts_min_set() {
        let mut q = Query {
            ts_min: Some(123),
            ..Query::default()
        };
        assert!(!apply_default_window(&mut q, NOW, HOUR_NS));
        assert_eq!(q.ts_min, Some(123));
        assert_eq!(q.ts_max, None);
    }

    #[test]
    fn noop_when_ts_max_set() {
        let mut q = Query {
            ts_max: Some(456),
            ..Query::default()
        };
        assert!(!apply_default_window(&mut q, NOW, HOUR_NS));
        assert_eq!(q.ts_min, None, "an explicit ts_max is honored exactly");
        assert_eq!(q.ts_max, Some(456));
    }

    #[test]
    fn noop_when_window_zero() {
        let mut q = Query::default();
        assert!(!apply_default_window(&mut q, NOW, 0));
        assert_eq!(q.ts_min, None);
        assert_eq!(q.ts_max, None);
    }

    #[test]
    fn saturates_when_now_before_window() {
        // now < window must not underflow-panic.
        let mut q = Query::default();
        assert!(apply_default_window(&mut q, 5, HOUR_NS));
        assert_eq!(q.ts_min, Some(0));
    }
}
