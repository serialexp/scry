//! DataFusion `TableProvider` over traces blocks, plus the register-
//! helpers that match `lib.rs`'s metrics/logs shape.
//!
//! Logically, the "traces" table is the row-wise union of every traces
//! block currently in the catalog whose time range overlaps the query
//! window. Each block contributes its `<block>.parquet`, whose schema
//! is exactly [`scry_block::TracesBlockBuilder::main_schema`] — one row
//! per span, sorted by `(trace_id, start_unix_nano)`.
//!
//! ## No postings — predicates instead
//!
//! Unlike metrics/logs, traces blocks carry **no postings sidecar**
//! (`has_postings = false`). There is therefore no per-block
//! fingerprint set to push; matcher + time + trace-id filters become
//! ordinary DataFusion **row-filter predicates** pushed into
//! `ParquetSource` (`with_predicate` + `with_pushdown_filters`), and
//! row-group min/max statistics do the pruning. Because the block is
//! sorted by `trace_id`, a `--trace-id` equality lookup prunes to the
//! one row group holding that trace.
//!
//! ## Matcher scope
//!
//! Only the **promoted** resource columns (`service.name`,
//! `service.namespace`, `deployment.environment[.name]`) are first-class
//! `--matcher` targets — they map to the Utf8 promoted columns the
//! block writer fills. Arbitrary span-attribute / resource-label
//! filtering (Map element access) is expressible via `--sql` until the
//! query language lands; an unrecognised matcher key is rejected up
//! front rather than silently ignored (which would over-return rows).

use std::any::Any;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, Result as DfResult, ScalarValue};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{col, Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::lit as physical_lit;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectStore;
use scry_block::{block_path, TracesBlockBuilder};
use scry_catalog::{Catalog, CatalogEntry};

use crate::postings_cache::PostingsCache;
use crate::table::{object_store_url_for, time_overlaps};
use crate::Query;

/// Default name the [`TracesTable`] is registered under in a
/// `SessionContext`. The CLI and the query daemon agree on this so
/// users can write `SELECT … FROM traces …` without thinking about it.
pub const TRACES_TABLE_NAME: &str = "traces";

/// The Arrow schema of a traces block's main parquet. Reused verbatim
/// from the block writer ([`TracesBlockBuilder::main_schema`]) so the
/// registered table type can never drift from the on-disk parquet type
/// (the complex `Map` / `List<Struct>` columns make hand-mirroring
/// error-prone — metrics/logs hand-write theirs only because they're
/// flat).
fn traces_schema() -> SchemaRef {
    TracesBlockBuilder::main_schema()
}

/// Map a matcher key to the promoted Utf8 column it filters, or `None`
/// if the key isn't a promoted resource attribute (in which case the
/// caller rejects it and points the user at `--sql`). Keys mirror
/// `crates/block/src/traces.rs`'s `PROMOTED_*_KEYS`.
fn promoted_column_for(key: &str) -> Option<&'static str> {
    match key {
        "service.name" => Some("service_name"),
        "service.namespace" => Some("service_namespace"),
        "deployment.environment" | "deployment.environment.name" => {
            Some("deployment_environment")
        }
        _ => None,
    }
}

/// One catalog row for a traces block. No postings ⇒ no fingerprint
/// set; the parquet predicate (built in [`TracesTable::scan`] from the
/// shared matcher/trace-id/time state) carries all selectivity.
#[derive(Debug, Clone)]
pub struct TracesBlockEntry {
    pub entry: CatalogEntry,
}

/// `TableProvider` for the traces signal. One instance per call to
/// [`register_traces_table`] — carries a snapshot of catalog rows plus
/// the resolved matcher/trace-id/time predicate inputs, so `scan()`
/// stays pure CPU.
pub struct TracesTable {
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    blocks: Vec<TracesBlockEntry>,
    /// Promoted-column equality predicates, pre-validated from
    /// `q.matchers` at build time (`(column, value)`).
    promoted: Vec<(&'static str, String)>,
    trace_id: Option<[u8; 16]>,
    ts_min: Option<u64>,
    ts_max: Option<u64>,
}

impl std::fmt::Debug for TracesTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TracesTable")
            .field("blocks", &self.blocks.len())
            .field("promoted", &self.promoted)
            .field("trace_id", &self.trace_id)
            .field("ts_min", &self.ts_min)
            .field("ts_max", &self.ts_max)
            .finish()
    }
}

impl TracesTable {
    pub fn new(
        bucket: &str,
        blocks: Vec<TracesBlockEntry>,
        promoted: Vec<(&'static str, String)>,
        trace_id: Option<[u8; 16]>,
        ts_min: Option<u64>,
        ts_max: Option<u64>,
    ) -> DfResult<Self> {
        Ok(Self {
            schema: traces_schema(),
            object_store_url: object_store_url_for(bucket)?,
            blocks,
            promoted,
            trace_id,
            ts_min,
            ts_max,
        })
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn blocks(&self) -> &[TracesBlockEntry] {
        &self.blocks
    }
}

#[async_trait]
impl TableProvider for TracesTable {
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
        // Same structural shape as `LogsTable::scan`, minus the
        // fingerprint predicate (traces have no postings). We push:
        //   * any DataFusion-supplied `filters` (e.g. from --sql),
        //   * promoted-column equality (service_name etc.),
        //   * an optional `trace_id` equality on the sorted
        //     FixedSizeBinary(16) column — row-group min/max prunes,
        //   * the time bounds over `start_unix_nano`.
        let df_schema = DFSchema::try_from(self.schema())?;

        let make_branch = |file_path: String, file_size: u64| -> DfResult<Arc<dyn ExecutionPlan>> {
            let mut block_filters: Vec<Expr> = filters.to_vec();

            for (column, value) in &self.promoted {
                block_filters.push(
                    col(*column).eq(Expr::Literal(
                        ScalarValue::Utf8(Some(value.clone())),
                        None,
                    )),
                );
            }

            if let Some(id) = self.trace_id {
                block_filters.push(col("trace_id").eq(Expr::Literal(
                    ScalarValue::FixedSizeBinary(16, Some(id.to_vec())),
                    None,
                )));
            }

            if let Some(min) = self.ts_min {
                block_filters.push(
                    col("start_unix_nano")
                        .gt_eq(Expr::Literal(ScalarValue::UInt64(Some(min)), None)),
                );
            }
            if let Some(max) = self.ts_max {
                block_filters.push(
                    col("start_unix_nano")
                        .lt_eq(Expr::Literal(ScalarValue::UInt64(Some(max)), None)),
                );
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
            // `DataSourceExec` so `SELECT count(*) FROM traces`
            // returns 0 cleanly. Mirrors metrics/logs behaviour.
            let source =
                Arc::new(ParquetSource::new(self.schema()).with_pushdown_filters(true));
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
            branches.push(make_branch(path, meta.byte_size)?);
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

/// Synchronous step: list catalog blocks, filter by `signal="traces"`
/// and the query's time bounds. Pure compute over the connection;
/// callers wrapping the catalog in a mutex (the query daemon) can drop
/// the guard before doing async work.
pub fn list_traces_candidates(catalog: &Catalog, q: &Query) -> Result<Vec<CatalogEntry>> {
    Ok(catalog
        .list_blocks()
        .context("listing blocks from catalog")?
        .into_iter()
        .filter(|e| e.meta.signal == "traces")
        .filter(|e| time_overlaps(&e.meta, q.ts_min, q.ts_max))
        .collect())
}

/// Validate `q.matchers` against the promoted columns, returning the
/// `(column, value)` predicate inputs. Rejects any matcher whose key
/// isn't a promoted resource attribute — silently ignoring it would
/// over-return rows and break the loss-free round-trip guarantee.
fn promoted_predicates(q: &Query) -> Result<Vec<(&'static str, String)>> {
    q.matchers
        .iter()
        .map(|(k, v)| {
            promoted_column_for(k)
                .map(|col| (col, v.clone()))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "traces matcher `{k}` is not a promoted column \
                         (service.name, service.namespace, deployment.environment); \
                         use --sql for arbitrary attribute filtering"
                    )
                })
        })
        .collect()
}

/// Async step: take the already-narrowed catalog list (per
/// [`list_traces_candidates`]) and produce a ready-to-register
/// [`TracesTable`]. No postings resolve — the `cache` argument is
/// accepted only for signature symmetry with the metrics/logs path
/// (so the query daemon's dispatch stays uniform) and is ignored.
pub async fn build_traces_table_from_candidates(
    candidates: Vec<CatalogEntry>,
    _store: Arc<dyn ObjectStore>,
    _cache: Option<&PostingsCache>,
    q: &Query,
) -> Result<TracesTable> {
    let promoted = promoted_predicates(q)?;
    let bucket = match candidates.first() {
        Some(first) => {
            let b = first.bucket.clone();
            anyhow::ensure!(
                candidates.iter().all(|e| e.bucket == b),
                "traces blocks span multiple buckets; multi-bucket queries not yet supported"
            );
            b
        }
        None => {
            // No overlapping blocks. Return an empty table so
            // `SELECT count(*) FROM traces` still works.
            return Ok(TracesTable::new(
                "",
                Vec::new(),
                promoted,
                q.trace_id,
                q.ts_min,
                q.ts_max,
            )?);
        }
    };

    let blocks: Vec<TracesBlockEntry> = candidates
        .into_iter()
        .map(|entry| TracesBlockEntry { entry })
        .collect();

    TracesTable::new(&bucket, blocks, promoted, q.trace_id, q.ts_min, q.ts_max)
        .map_err(|e| anyhow::anyhow!("constructing TracesTable: {e}"))
}

/// Build the table (catalog narrow only) and register it on `ctx`
/// under `"traces"`. Also registers `store` against the table's
/// `ObjectStoreUrl` so DataFusion can route reads.
pub async fn register_traces_table(
    ctx: &SessionContext,
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
) -> Result<()> {
    let candidates = list_traces_candidates(catalog, q)?;
    register_traces_table_from_candidates(ctx, candidates, store, None, q).await
}

/// Same as [`register_traces_table`] but accepts pre-listed catalog
/// entries — for callers that take the catalog lock for the sync
/// `list_traces_candidates` call themselves (the query daemon, where
/// the `Catalog` lives behind a `Mutex`).
pub async fn register_traces_table_from_candidates(
    ctx: &SessionContext,
    candidates: Vec<CatalogEntry>,
    store: Arc<dyn ObjectStore>,
    cache: Option<&PostingsCache>,
    q: &Query,
) -> Result<()> {
    let table = build_traces_table_from_candidates(candidates, store.clone(), cache, q).await?;
    let url: &url::Url = table.object_store_url().as_ref();
    ctx.runtime_env().register_object_store(url, store);
    ctx.register_table(TRACES_TABLE_NAME, Arc::new(table))
        .map_err(|e| anyhow::anyhow!("register traces table: {e}"))?;
    Ok(())
}

/// One-shot convenience for the local (no-daemon) path: build a fresh
/// `SessionContext`, register the traces table, return the `traces`
/// `DataFrame`. Mirrors `metrics_query`/`logs_query`.
pub async fn traces_query(
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
) -> Result<datafusion::prelude::DataFrame> {
    let ctx = SessionContext::new();
    register_traces_table(&ctx, catalog, store, q).await?;
    ctx.table(TRACES_TABLE_NAME)
        .await
        .with_context(|| format!("looking up table {TRACES_TABLE_NAME}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The query-side schema must be byte-identical to the block
    /// writer's, or DataFusion errors at scan time on a type mismatch.
    /// We reuse `main_schema()` directly, so this guards against the
    /// writer's schema changing without the query path noticing.
    #[test]
    fn schema_matches_block_writer() {
        assert_eq!(traces_schema(), TracesBlockBuilder::main_schema());
        // The trace_id column the --trace-id predicate pins must exist
        // and be FixedSizeBinary(16).
        let f = traces_schema().field_with_name("trace_id").unwrap().clone();
        assert_eq!(
            f.data_type(),
            &datafusion::arrow::datatypes::DataType::FixedSizeBinary(16)
        );
    }

    #[test]
    fn promoted_matchers_map_to_columns() {
        let q = Query {
            matchers: vec![
                ("service.name".into(), "api".into()),
                ("deployment.environment".into(), "prod".into()),
            ],
            ..Default::default()
        };
        let promoted = promoted_predicates(&q).unwrap();
        assert_eq!(
            promoted,
            vec![
                ("service_name", "api".to_string()),
                ("deployment_environment", "prod".to_string()),
            ]
        );
    }

    #[test]
    fn unknown_matcher_is_rejected() {
        let q = Query {
            matchers: vec![("http.method".into(), "GET".into())],
            ..Default::default()
        };
        let err = promoted_predicates(&q).unwrap_err().to_string();
        assert!(err.contains("not a promoted column"), "got: {err}");
        assert!(err.contains("--sql"), "got: {err}");
    }
}
