//! `MetricsTable`: a DataFusion `TableProvider` over metrics blocks.
//!
//! Logically, the "metrics" table is the row-wise union of every
//! metrics block currently in the catalog whose time range overlaps
//! the query window. Each block contributes its
//! `<block>.parquet` (schema `(series_fingerprint UInt64,
//! ts_unix_nano UInt64, value Float64)`), and DataFusion's
//! `ParquetSource` does the actual reading + row-group pruning + row
//! filter pushdown.
//!
//! Where this differs from a plain `ListingTable` of parquet files
//! is the postings layer: callers pass an optional **pre-resolved
//! fingerprint set** at construction time, derived from the postings
//! sidecars of the candidate blocks. That set becomes a
//! `series_fingerprint IN (...)` predicate handed to `ParquetSource`,
//! which combines with row-group min/max stats (sharp because blocks
//! are sorted by `(fp, ts)`) to skip most row groups before any byte
//! is read.
//!
//! The postings resolution itself happens in `register_metrics_table`
//! (see `lib.rs`) before this struct is built — `scan()` is planning
//! and must not do I/O.
//!
//! ## v0.3 limitation: union-of-blocks fingerprint predicate
//!
//! Because the predicate handed to `ParquetSource` is one
//! `PhysicalExpr` shared across every `PartitionedFile`, we pass the
//! **union** of per-block fingerprint sets, not per-block. A block
//! whose postings resolved to `{A, B}` may end up reading a row group
//! whose `[min, max]` fingerprint range overlaps some `C` that belongs
//! to another block's postings. Correctness is preserved (the row
//! filter drops the false positives), but a small amount of extra
//! decode happens. Blocks whose postings resolved to the empty set
//! are still dropped entirely — they never make it into `blocks`.
//!
//! Fix path (v0.3.1): per-partition `PhysicalExpr`, or a custom
//! optimizer rule that splits the scan along fingerprint-set
//! boundaries. Measure with `scripts/profile-query.sh` first.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, Result as DfResult, ScalarValue};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::lit as physical_lit;
use datafusion::physical_plan::ExecutionPlan;
use scry_block::{block_path, BlockMeta};
use scry_catalog::CatalogEntry;

/// The Arrow schema of a metrics block's main parquet, as written by
/// `crates/block/src/metrics.rs::MetricsBlockBuilder::main_schema`.
/// Kept private to this module — callers receive `SchemaRef` via the
/// `TableProvider::schema()` method.
fn metrics_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("series_fingerprint", DataType::UInt64, false),
        Field::new("ts_unix_nano", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

/// Build the `s3://<bucket>` URL we register the object store under
/// in the SessionContext. The path part of each `PartitionedFile` is
/// then `block_path(...)` as written today — no scheme, no bucket.
pub(crate) fn object_store_url_for(bucket: &str) -> DfResult<ObjectStoreUrl> {
    // The catalog stores `bucket` as plain text and the v0.1 dev path
    // uses `scry-dev`. `s3://` is the right scheme for both Garage and
    // AWS S3; DataFusion routes by scheme + host, not by full URL.
    ObjectStoreUrl::parse(format!("s3://{bucket}"))
}

/// `TableProvider` for the metrics signal. One instance per call to
/// `register_metrics_table` — it carries a snapshot of the catalog +
/// resolved fingerprint set, so `scan()` stays pure CPU.
pub struct MetricsTable {
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    /// Pre-narrowed by signal + time-overlap + postings (any block
    /// whose postings resolved to the empty set is already excluded
    /// here). Each becomes one `PartitionedFile` in the scan plan.
    blocks: Vec<CatalogEntry>,
    /// Union-of-blocks fingerprint set, sorted for stable test
    /// output. `None` = "no label matchers were given, return every
    /// fingerprint in every overlapping block". `Some(empty)` is
    /// avoided upstream (caller turns it into "no scan at all").
    fp_filter: Option<Arc<Vec<u64>>>,
    ts_min: Option<u64>,
    ts_max: Option<u64>,
}

impl std::fmt::Debug for MetricsTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsTable")
            .field("blocks", &self.blocks.len())
            .field("fp_filter", &self.fp_filter.as_ref().map(|s| s.len()))
            .field("ts_min", &self.ts_min)
            .field("ts_max", &self.ts_max)
            .finish()
    }
}

impl MetricsTable {
    /// Build a new table from a pre-narrowed block list + optional
    /// fingerprint pre-filter. The caller is responsible for catalog
    /// listing, time-overlap pruning, and postings resolution.
    pub fn new(
        bucket: &str,
        blocks: Vec<CatalogEntry>,
        fp_filter: Option<Arc<Vec<u64>>>,
        ts_min: Option<u64>,
        ts_max: Option<u64>,
    ) -> DfResult<Self> {
        Ok(Self {
            schema: metrics_schema(),
            object_store_url: object_store_url_for(bucket)?,
            blocks,
            fp_filter,
            ts_min,
            ts_max,
        })
    }

    /// The `ObjectStoreUrl` this table will look up at scan time. The
    /// caller must `runtime_env().register_object_store(...)` with the
    /// matching URL before running any query against this table.
    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    /// Snapshot of the catalog rows this table will scan. Useful for
    /// per-block reporting (CLI trailer, tests).
    pub fn blocks(&self) -> &[CatalogEntry] {
        &self.blocks
    }
}

#[async_trait]
impl TableProvider for MetricsTable {
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
        // ── Build the conjunctive predicate ───────────────────────
        //
        // Two predicate sources:
        //   1. `filters` from DataFusion — anything the planner could
        //      lift out of the surrounding query (`WHERE ts > X`,
        //      `WHERE fp = Y`, etc.). Pushed through as-is.
        //   2. Our own pre-resolved fingerprint + time predicates.
        //      These are the *real* selectivity at v0.3 — the
        //      `fp IN (..)` set built from postings, plus the
        //      `BETWEEN` on ts that mirrors the catalog-time prune.
        //
        // `ParquetSource` takes one `PhysicalExpr`; we conjunct
        // everything together with `conjunction(...).create_physical_expr(...)`.
        let df_schema = DFSchema::try_from(self.schema())?;
        let mut all_filters: Vec<Expr> = filters.to_vec();

        if let Some(fp_set) = self.fp_filter.as_ref() {
            // Materialise the IN-list. DataFusion's `InListExpr`
            // physical-eval switches to a hash-based check above a
            // size threshold, so a thousand-element list is fine.
            let lits: Vec<Expr> = fp_set
                .iter()
                .map(|fp| Expr::Literal(ScalarValue::UInt64(Some(*fp)), None))
                .collect();
            all_filters.push(datafusion::logical_expr::col("series_fingerprint").in_list(lits, false));
        }
        if let Some(min) = self.ts_min {
            all_filters.push(
                datafusion::logical_expr::col("ts_unix_nano")
                    .gt_eq(Expr::Literal(ScalarValue::UInt64(Some(min)), None)),
            );
        }
        if let Some(max) = self.ts_max {
            all_filters.push(
                datafusion::logical_expr::col("ts_unix_nano")
                    .lt_eq(Expr::Literal(ScalarValue::UInt64(Some(max)), None)),
            );
        }

        let predicate = conjunction(all_filters)
            .map(|p| state.create_physical_expr(p, &df_schema))
            .transpose()?
            .unwrap_or_else(|| physical_lit(true));

        // ── Build the file scan ───────────────────────────────────
        //
        // One `PartitionedFile` per block. We use the catalog's
        // `byte_size` for `file_size` so ParquetObjectReader can use
        // bounded range requests rather than suffix-range probes.
        // `with_pushdown_filters(true)` makes ParquetSource actually
        // evaluate the predicate against decoded rows (row filter),
        // not just use it for row-group stats. Without this our
        // ts_min/ts_max + fp IN-list would prune row groups but still
        // hand back every row in the surviving groups.
        let source = Arc::new(
            ParquetSource::new(self.schema())
                .with_predicate(predicate)
                .with_pushdown_filters(true),
        );
        let mut builder = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
            .with_projection_indices(projection.cloned())?
            .with_limit(limit);

        for entry in &self.blocks {
            let path = block_path(
                &entry.meta.signal,
                entry.meta.ts_min_unix_nano,
                entry.meta.writer_id,
                entry.meta.uuid,
                "parquet",
            );
            builder = builder.with_file(PartitionedFile::new(path, entry.meta.byte_size));
        }

        Ok(DataSourceExec::from_data_source(builder.build()))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // `Inexact` mirrors the parquet_index example: the parquet
        // pruning we do (row-group stats + row filter) may produce
        // false positives at the row level, so DataFusion keeps the
        // filter at the `FilterExec` layer too as a safety net.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

/// Reusable helper for the `BlockMeta::ts_*` overlap check. Treats
/// `None` bounds as ±∞. Lives here (rather than the orchestrator)
/// because the postings step also wants the same predicate.
pub fn time_overlaps(meta: &BlockMeta, q_min: Option<u64>, q_max: Option<u64>) -> bool {
    if let Some(qmin) = q_min {
        if meta.ts_max_unix_nano < qmin {
            return false;
        }
    }
    if let Some(qmax) = q_max {
        if meta.ts_min_unix_nano > qmax {
            return false;
        }
    }
    true
}
