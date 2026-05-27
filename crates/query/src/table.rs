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
//! is the postings layer: each [`BlockEntry`] carries the
//! pre-resolved fingerprint set derived from *its own* postings
//! sidecar. That set becomes a `series_fingerprint IN (...)`
//! predicate handed to a per-block `ParquetSource`, which combines
//! with row-group min/max stats (sharp because blocks are sorted by
//! `(fp, ts)`) to skip most row groups before any byte is read.
//!
//! The postings resolution itself happens in `register_metrics_table`
//! (see `lib.rs`) before this struct is built — `scan()` is planning
//! and must not do I/O.
//!
//! ## Per-block fingerprint pushdown (v0.3 step 4)
//!
//! `scan()` emits one [`datafusion::datasource::memory::DataSourceExec`]
//! *per block*, each with its own narrow fingerprint predicate, and
//! wraps them in a [`datafusion::physical_plan::union::UnionExec`].
//! `UnionExec::try_new` collapses single-input plans automatically, so
//! the 1-block case has no UnionExec overhead. The win versus the
//! older union-of-fingerprint-sets shape: row groups in block A whose
//! `[min_fp, max_fp]` overlapped a fingerprint that only existed in
//! block B's postings used to survive row-group pruning; now each
//! block's row-group pruning sees only its own fingerprint set.
//! Correctness was already preserved by the row filter in both
//! designs; this just sharpens pruning before any rows are decoded.
//!
//! Blocks whose postings resolved to the empty set are still dropped
//! upstream — they never make it into `blocks` and so contribute no
//! `DataSourceExec`.

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
use datafusion::physical_plan::union::UnionExec;
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

/// One catalog row plus its postings-resolved fingerprint set. The
/// builder in `lib.rs` produces one of these per surviving block;
/// `scan()` turns each into its own `DataSourceExec`.
#[derive(Debug, Clone)]
pub struct BlockEntry {
    /// Catalog row for this block. `entry.meta` carries the path
    /// pieces (`signal`, `ts_min_unix_nano`, `writer_id`, `uuid`) and
    /// `entry.meta.byte_size` for bounded range requests.
    pub entry: CatalogEntry,
    /// Pre-resolved fingerprint set for *this block*. Sorted for
    /// stable test output. `None` means "no label matchers were
    /// given — scan every fingerprint in this block". `Some(empty)`
    /// is avoided upstream (the builder drops blocks whose postings
    /// intersect to nothing before they ever reach here).
    pub fp_set: Option<Arc<Vec<u64>>>,
}

/// `TableProvider` for the metrics signal. One instance per call to
/// `register_metrics_table` — it carries a snapshot of the catalog
/// rows plus their per-block resolved fingerprint sets, so `scan()`
/// stays pure CPU.
pub struct MetricsTable {
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    /// Pre-narrowed by signal + time-overlap + postings (any block
    /// whose postings resolved to the empty set is already excluded
    /// here). Each becomes one `DataSourceExec` in the scan plan,
    /// unioned by `UnionExec::try_new` (which collapses the 1-input
    /// case automatically).
    blocks: Vec<BlockEntry>,
    ts_min: Option<u64>,
    ts_max: Option<u64>,
}

impl std::fmt::Debug for MetricsTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsTable")
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
            .finish()
    }
}

impl MetricsTable {
    /// Build a new table from a pre-narrowed list of `(CatalogEntry,
    /// per-block fingerprint set)` pairs. The caller (
    /// [`crate::build_metrics_table_from_candidates`]) is responsible
    /// for catalog listing, time-overlap pruning, and per-block
    /// postings resolution.
    pub fn new(
        bucket: &str,
        blocks: Vec<BlockEntry>,
        ts_min: Option<u64>,
        ts_max: Option<u64>,
    ) -> DfResult<Self> {
        Ok(Self {
            schema: metrics_schema(),
            object_store_url: object_store_url_for(bucket)?,
            blocks,
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

    /// Snapshot of the per-block entries this table will scan. Useful
    /// for per-block reporting (CLI trailer, tests). Each element
    /// carries its `entry: CatalogEntry` plus the per-block `fp_set`.
    pub fn blocks(&self) -> &[BlockEntry] {
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
        // ── Per-block plan emission ───────────────────────────────
        //
        // We emit one `DataSourceExec` per block, each with a
        // predicate AND-ing:
        //   1. `filters` from DataFusion — anything the planner could
        //      lift out of the surrounding query (`WHERE ts > X`,
        //      `WHERE fp = Y`, etc.). Pushed through as-is to every
        //      branch.
        //   2. *This block's* pre-resolved fingerprint set as
        //      `fp IN (...)`. v0.3 step 4: each branch sees only its
        //      own fingerprints, so row-group min/max stats prune
        //      against the tightest set possible (rather than the
        //      union across all blocks, which let cross-block false
        //      positives survive pruning).
        //   3. The table-wide `ts_min`/`ts_max` bounds.
        //
        // The branches are wrapped in `UnionExec::try_new`, which
        // collapses the single-branch case automatically — the 1-block
        // query pays no UnionExec overhead.
        //
        // Empty-`blocks` case: keep a single empty `DataSourceExec`
        // with the schema preserved. `UnionExec::try_new` rejects an
        // empty input list, and "no blocks overlap" is a legitimate
        // result that needs to keep the schema for downstream
        // operators (e.g. `SELECT count(*) FROM metrics` returns 0).
        let df_schema = DFSchema::try_from(self.schema())?;

        let make_branch = |fp_set: Option<&Arc<Vec<u64>>>,
                           file_path: String,
                           file_size: u64|
         -> DfResult<Arc<dyn ExecutionPlan>> {
            let mut block_filters: Vec<Expr> = filters.to_vec();
            if let Some(fp_set) = fp_set {
                // Materialise the IN-list. DataFusion's `InListExpr`
                // physical-eval switches to a hash-based check above a
                // size threshold, so a thousand-element list is fine.
                let lits: Vec<Expr> = fp_set
                    .iter()
                    .map(|fp| Expr::Literal(ScalarValue::UInt64(Some(*fp)), None))
                    .collect();
                block_filters.push(
                    datafusion::logical_expr::col("series_fingerprint").in_list(lits, false),
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

            let predicate = conjunction(block_filters)
                .map(|p| state.create_physical_expr(p, &df_schema))
                .transpose()?
                .unwrap_or_else(|| physical_lit(true));

            // `with_pushdown_filters(true)` makes ParquetSource
            // evaluate the predicate against decoded rows (row
            // filter), not just use it for row-group stats. Without
            // this our ts bounds + fp IN-list would prune row groups
            // but still hand back every row in the surviving groups.
            let source = Arc::new(
                ParquetSource::new(self.schema())
                    .with_predicate(predicate)
                    .with_pushdown_filters(true),
            );
            // One file per branch. We use the catalog's `byte_size`
            // for `file_size` so ParquetObjectReader can use bounded
            // range requests rather than suffix-range probes. Limit
            // is propagated to every branch — DataFusion's planner
            // adds a `GlobalLimitExec` above the union that terminates
            // the stream early once enough rows arrive.
            let builder = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
                .with_projection_indices(projection.cloned())?
                .with_limit(limit)
                .with_file(PartitionedFile::new(file_path, file_size));

            Ok(DataSourceExec::from_data_source(builder.build()))
        };

        if self.blocks.is_empty() {
            // No overlapping blocks. Emit a single empty
            // `DataSourceExec` (no files) with the schema preserved
            // so downstream operators have a typed input —
            // `SELECT count(*) FROM metrics` with no blocks returns
            // 0 cleanly. We bypass `make_branch` here since there's
            // no fingerprint set to apply and no file to attach.
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

        // `try_new` returns the single branch directly when there's
        // only one, so we don't gain a UnionExec layer for 1-block
        // queries. For ≥2 branches the planner can later parallelise
        // them across partitions if it chooses.
        UnionExec::try_new(branches)
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
