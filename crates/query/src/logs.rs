//! DataFusion `TableProvider` over logs blocks, plus the register-
//! helpers that match `lib.rs`'s metrics shape.
//!
//! Logically, the "logs" table is the row-wise union of every logs
//! block currently in the catalog whose time range overlaps the
//! query window. Each block contributes its `<block>.parquet`
//! (schema `(stream_fingerprint UInt64, ts_unix_nano UInt64,
//! severity UInt8, body Utf8, attributes Map<Utf8,Utf8>)`).
//!
//! Same per-block-fingerprint-pushdown shape as
//! [`crate::table::MetricsTable`]: one
//! [`DataSourceExec`](datafusion::datasource::memory::DataSourceExec)
//! per block carrying its own narrow fingerprint set, all wrapped
//! in a [`UnionExec`](datafusion::physical_plan::union::UnionExec)
//! that collapses to the single branch when only one block survives.
//!
//! ## Why not generalise with `MetricsTable`?
//!
//! At v0 the structural overlap is high but the divergence point is
//! real: once logs picks up body-substring pushdown (the tantivy
//! phase), `LogsTable::scan` will route entirely differently — full
//! parquet-only scan vs. tantivy-narrowed RowGroup pushdown. A
//! shared `SignalTable` trait today would have to be retrofitted
//! the moment that lands. The duplication is honest at v0; the
//! shared envelope ([`crate::Query`]) is the part that *is*
//! genuinely identical and gets shared.

use std::any::Any;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, Result as DfResult, ScalarValue};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::lit as physical_lit;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectStore;
use scry_block::block_path;
use scry_catalog::{Catalog, CatalogEntry};

use crate::bloom_cache::BloomCache;
use crate::postings;
use crate::postings_cache::PostingsCache;
use crate::table::{object_store_url_for, time_overlaps};
use crate::Query;

/// Default name the [`LogsTable`] is registered under in a
/// `SessionContext`. The CLI and the query daemon agree on this so
/// users can write `SELECT … FROM logs …` without thinking about it.
pub const LOGS_TABLE_NAME: &str = "logs";

/// The Arrow schema of a logs block's main parquet, as written by
/// `crates/block/src/logs.rs::LogsBlockBuilder::main_schema`.
/// Kept private to this module — callers receive `SchemaRef` via the
/// `TableProvider::schema()` method.
fn logs_schema() -> SchemaRef {
    // `values` nullable: matches the writer schema in
    // `LogsBlockBuilder::main_schema` (which has to mirror Arrow
    // `MapBuilder`'s nullable-by-default StringBuilder for its value
    // column). The parquet file's column type and DataFusion's
    // registered table type must agree exactly or scan time errors.
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    ));
    Arc::new(Schema::new(vec![
        Field::new("stream_fingerprint", DataType::UInt64, false),
        Field::new("ts_unix_nano", DataType::UInt64, false),
        Field::new("severity", DataType::UInt8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new(
            "attributes",
            DataType::Map(entries_field, /*keys_sorted=*/ false),
            false,
        ),
    ]))
}

/// One catalog row plus its postings-resolved fingerprint set.
/// Mirrors `crate::table::BlockEntry` but lives next to `LogsTable`
/// since the two diverge once logs picks up body-search resolution.
#[derive(Debug, Clone)]
pub struct LogsBlockEntry {
    pub entry: CatalogEntry,
    /// Pre-resolved fingerprint set for *this block*. Sorted for
    /// stable test output. `None` means "no label matchers were
    /// given — scan every stream in this block". `Some(empty)` is
    /// avoided upstream (the builder drops blocks whose postings
    /// intersect to nothing before they ever reach here).
    pub fp_set: Option<Arc<Vec<u64>>>,
}

/// `TableProvider` for the logs signal. One instance per call to
/// [`register_logs_table`] — carries a snapshot of catalog rows plus
/// their per-block resolved fingerprint sets, so `scan()` stays
/// pure CPU.
pub struct LogsTable {
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    blocks: Vec<LogsBlockEntry>,
    ts_min: Option<u64>,
    ts_max: Option<u64>,
    /// Literal substring the query requires in `body` (the `--grep` /
    /// `Query::body_contains` surface). Pushed as an exact substring
    /// predicate per block in `scan`; block-level skipping via the bloom
    /// sidecar happens earlier, in `build_logs_table_from_candidates`.
    body_contains: Option<String>,
}

impl std::fmt::Debug for LogsTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogsTable")
            .field("blocks", &self.blocks.len())
            .field(
                "fp_sets",
                &self
                    .blocks
                    .iter()
                    .map(|b| b.fp_set.as_ref().map(|s| s.len()))
                    .collect::<Vec<_>>(),
            )
            .field("ts_min", &self.ts_min)
            .field("ts_max", &self.ts_max)
            .field("body_contains", &self.body_contains)
            .finish()
    }
}

impl LogsTable {
    pub fn new(
        bucket: &str,
        blocks: Vec<LogsBlockEntry>,
        ts_min: Option<u64>,
        ts_max: Option<u64>,
        body_contains: Option<String>,
    ) -> DfResult<Self> {
        Ok(Self {
            schema: logs_schema(),
            object_store_url: object_store_url_for(bucket)?,
            blocks,
            ts_min,
            ts_max,
            body_contains,
        })
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn blocks(&self) -> &[LogsBlockEntry] {
        &self.blocks
    }
}

#[async_trait]
impl TableProvider for LogsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // ── Per-block plan emission ────────────────────────────────
        //
        // Same structural shape as `MetricsTable::scan` — comments
        // there cover the row-group-pruning rationale. The only
        // signal-specific concern here is the column we pin the
        // fingerprint predicate to (`stream_fingerprint` vs
        // metrics' `series_fingerprint`).
        let df_schema = DFSchema::try_from(self.schema())?;

        let make_branch = |fp_set: Option<&Arc<Vec<u64>>>,
                           file_path: String,
                           file_size: u64|
         -> DfResult<Arc<dyn ExecutionPlan>> {
            let mut block_filters: Vec<Expr> = filters.to_vec();
            if let Some(fp_set) = fp_set {
                let lits: Vec<Expr> = fp_set
                    .iter()
                    .map(|fp| Expr::Literal(ScalarValue::UInt64(Some(*fp)), None))
                    .collect();
                block_filters.push(
                    datafusion::logical_expr::col("stream_fingerprint").in_list(lits, false),
                );
            }
            if let Some(min) = self.ts_min {
                block_filters.push(
                    datafusion::logical_expr::col("ts_unix_nano")
                        .gt_eq(Expr::Literal(ScalarValue::UInt64(Some(min)), None)),
                );
            }
            if let Some(max) = self.ts_max {
                block_filters.push(
                    datafusion::logical_expr::col("ts_unix_nano")
                        .lt_eq(Expr::Literal(ScalarValue::UInt64(Some(max)), None)),
                );
            }
            // Body full-text: an exact, case-sensitive substring predicate.
            // `contains(body, pat)` (not `LIKE '%pat%'`) so the search text is
            // matched literally — no `%`/`_` wildcard interpretation — which
            // is exactly the semantics the trigram bloom skip relies on. This
            // is the correctness backstop: even if a block's bloom let it
            // through as a false positive, this filter drops non-matching rows.
            if let Some(pat) = &self.body_contains {
                block_filters.push(datafusion::functions::expr_fn::contains(
                    datafusion::logical_expr::col("body"),
                    Expr::Literal(ScalarValue::Utf8(Some(pat.clone())), None),
                ));
            }

            let predicate = conjunction(block_filters)
                .map(|p| state.create_physical_expr(p, &df_schema))
                .transpose()?
                .unwrap_or_else(|| physical_lit(true));

            let source = Arc::new(
                ParquetSource::new(self.schema())
                    .with_predicate(predicate)
                    .with_pushdown_filters(true),
            );
            let builder = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
                .with_projection_indices(projection.cloned())?
                .with_limit(limit)
                .with_file(PartitionedFile::new(file_path, file_size));

            Ok(DataSourceExec::from_data_source(builder.build()))
        };

        if self.blocks.is_empty() {
            // No overlapping blocks. Emit a single empty
            // `DataSourceExec` so `SELECT count(*) FROM logs`
            // returns 0 cleanly. Mirrors metrics behaviour.
            let source = Arc::new(
                ParquetSource::new(self.schema()).with_pushdown_filters(true),
            );
            let builder = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
                .with_projection_indices(projection.cloned())?
                .with_limit(limit);
            return Ok(DataSourceExec::from_data_source(builder.build()));
        }

        let mut branches: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(self.blocks.len());
        for block in &self.blocks {
            let meta = &block.entry.meta;
            let path = block_path(
                &meta.signal,
                meta.ts_min_unix_nano,
                meta.writer_id,
                meta.uuid,
                "parquet",
            );
            branches.push(make_branch(block.fp_set.as_ref(), path, meta.byte_size)?);
        }

        UnionExec::try_new(branches)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

// ── Catalog + register helpers ────────────────────────────────────

/// Synchronous step: list catalog blocks, filter by `signal="logs"`
/// and the query's time bounds. Pure compute over the connection;
/// callers wrapping the catalog in a mutex (the query daemon) can
/// drop the guard before doing async work.
pub fn list_logs_candidates(catalog: &Catalog, q: &Query) -> Result<Vec<CatalogEntry>> {
    Ok(catalog
        .list_blocks()
        .context("listing blocks from catalog")?
        .into_iter()
        .filter(|e| e.meta.signal == "logs")
        .filter(|e| time_overlaps(&e.meta, q.ts_min, q.ts_max))
        .collect())
}

/// Async step: take the already-narrowed catalog list (per
/// [`list_logs_candidates`]), run postings resolve per block, and
/// produce a ready-to-register [`LogsTable`].
pub async fn build_logs_table_from_candidates(
    candidates: Vec<CatalogEntry>,
    store: Arc<dyn ObjectStore>,
    cache: Option<&PostingsCache>,
    bloom_cache: Option<&BloomCache>,
    q: &Query,
) -> Result<LogsTable> {
    let bucket = match candidates.first() {
        Some(first) => {
            let b = first.bucket.clone();
            anyhow::ensure!(
                candidates.iter().all(|e| e.bucket == b),
                "logs blocks span multiple buckets; multi-bucket queries not yet supported"
            );
            b
        }
        None => {
            // No overlapping blocks. Return an empty table so
            // `SELECT count(*) FROM logs` still works.
            return Ok(LogsTable::new(
                "",
                Vec::new(),
                q.ts_min,
                q.ts_max,
                q.body_contains.clone(),
            )?);
        }
    };

    let mut blocks: Vec<LogsBlockEntry> = Vec::with_capacity(candidates.len());
    let matchers_empty = q.matchers.is_empty();
    for entry in candidates {
        // ── Body-bloom skip (full-text accelerator) ────────────────
        //
        // If the query carries a `body_contains` substring and this
        // block's bloom sidecar authoritatively rules it out, drop the
        // block before any postings or parquet I/O. The exact
        // `contains(body, pat)` predicate added in `LogsTable::scan`
        // remains the correctness backstop for survivors, so a bloom
        // false positive only costs a scan and a missing/bad bloom just
        // means "don't skip" (see `body_bloom::block_excluded_by_bloom`).
        if let Some(pat) = q.body_contains.as_deref() {
            let excluded = match bloom_cache {
                Some(bc) => bc.block_excluded(store.clone(), &entry.meta, pat).await,
                None => {
                    crate::body_bloom::block_excluded_by_bloom(store.clone(), &entry.meta, pat).await
                }
            };
            if excluded {
                continue;
            }
        }

        // PostingsCache is signal-agnostic (keyed by block UUID +
        // matcher set), so the metrics infrastructure carries over
        // unchanged. The empty-matcher fallback inside the cache /
        // resolver fetches `meta.all_fingerprints` from the
        // sidecar — same shape both signals populate.
        let resolved = match cache {
            Some(c) => c.resolve(store.clone(), &entry.meta, &q.matchers).await?,
            None => postings::resolve_fingerprints(store.clone(), &entry.meta, &q.matchers).await?,
        };
        match resolved {
            None => {
                // Postings intersect was empty → block fully pruned.
                // Drop it before it ever reaches DataFusion.
            }
            Some(set) => {
                let fp_set = if matchers_empty {
                    None
                } else {
                    let mut v: Vec<u64> = set.into_iter().collect();
                    v.sort_unstable();
                    Some(Arc::new(v))
                };
                blocks.push(LogsBlockEntry { entry, fp_set });
            }
        }
    }

    LogsTable::new(&bucket, blocks, q.ts_min, q.ts_max, q.body_contains.clone())
        .map_err(|e| anyhow::anyhow!("constructing LogsTable: {e}"))
}

/// Build the table (postings resolve + catalog narrow) and register
/// it on `ctx` under `"logs"`. Also registers `store` against the
/// table's `ObjectStoreUrl` so DataFusion can route reads.
pub async fn register_logs_table(
    ctx: &SessionContext,
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
) -> Result<()> {
    let candidates = list_logs_candidates(catalog, q)?;
    register_logs_table_from_candidates(ctx, candidates, store, None, None, q).await
}

/// Same as [`register_logs_table`] but accepts pre-listed catalog
/// entries — for callers that need to take the catalog lock for the
/// sync `list_logs_candidates` call themselves (the query daemon,
/// where the `Catalog` lives behind a `Mutex`).
pub async fn register_logs_table_from_candidates(
    ctx: &SessionContext,
    candidates: Vec<CatalogEntry>,
    store: Arc<dyn ObjectStore>,
    cache: Option<&PostingsCache>,
    bloom_cache: Option<&BloomCache>,
    q: &Query,
) -> Result<()> {
    let table =
        build_logs_table_from_candidates(candidates, store.clone(), cache, bloom_cache, q).await?;
    let url: &url::Url = table.object_store_url().as_ref();
    ctx.runtime_env().register_object_store(url, store);
    ctx.register_table(LOGS_TABLE_NAME, Arc::new(table))
        .map_err(|e| anyhow::anyhow!("register logs table: {e}"))?;
    Ok(())
}

/// One-shot convenience for the local (no-daemon) path: build a
/// fresh `SessionContext`, register the logs table, return the
/// `logs` `DataFrame`. Mirrors `metrics_query` for callers that
/// only want a DataFrame and don't need to issue SQL.
pub async fn logs_query(
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
) -> Result<datafusion::prelude::DataFrame> {
    let ctx = SessionContext::new();
    register_logs_table(&ctx, catalog, store, q).await?;
    ctx.table(LOGS_TABLE_NAME)
        .await
        .with_context(|| format!("looking up table {LOGS_TABLE_NAME}"))
}

// ── tiny helper: shared time_overlaps lives in table.rs ───────────
//
// The time_overlaps helper is signal-agnostic and lives in
// `crate::table` already; we re-import it here rather than
// duplicating it (`use crate::table::time_overlaps`). This is the
// one piece of `table.rs` that genuinely belongs to "any signal"
// rather than "metrics specifically."
