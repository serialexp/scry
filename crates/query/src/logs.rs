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
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, MapBuilder, MapFieldNames, StringBuilder, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, Result as DfResult, ScalarValue};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::{lit as physical_lit, Column};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use object_store::ObjectStore;
use scry_block::block_path;
use scry_catalog::{Catalog, CatalogEntry};
use tracing::warn;

use crate::bloom_cache::BloomCache;
use crate::postings;
use crate::postings_cache::PostingsCache;
use crate::table::{object_store_url_for, time_overlaps};
use crate::Query;

/// A stream's resolved labels: `(name, value)` pairs, deduplicated and
/// sorted (the `BTreeSet` build order is preserved on freeze). `Arc<str>`
/// so the same label strings are shared across fingerprints without
/// re-allocating per row at scan time.
pub type LabelPairs = Arc<Vec<(Arc<str>, Arc<str>)>>;

/// `fingerprint → labels` map for a query's candidate blocks, built by
/// inverting their postings sidecars. Shared (`Arc`) into the per-block
/// scan branches so [`LabelEnrichExec`] can attach labels to each row.
pub type FpLabels = HashMap<u64, LabelPairs>;

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

/// The `labels` column appended to the logs table by the query-side label
/// join. A `Map<Utf8,Utf8>` carrying the stream's resolved labels
/// (namespace / pod / container / node / `k8s_*`), which live only in the
/// per-block postings sidecar (keyed by `stream_fingerprint`) and so are
/// otherwise invisible in query results.
///
/// The field shape mirrors `attributes` exactly so it matches the
/// `MapArray` produced by Arrow's [`MapBuilder`] (entry struct non-null,
/// `keys` non-null, `values` nullable) — a mismatch would fail
/// `RecordBatch::try_new` at scan time. The column itself is nullable.
fn labels_field() -> Field {
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    ));
    Field::new(
        "labels",
        DataType::Map(entries_field, /*keys_sorted=*/ false),
        true,
    )
}

/// The Arrow field names [`MapBuilder`] must use so the `labels`
/// `MapArray`'s type matches [`labels_field`] (and the parquet `attributes`
/// column) exactly.
fn labels_map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "keys".to_string(),
        value: "values".to_string(),
    }
}

/// The logs *table* schema as exposed to queries: the physical parquet
/// columns from [`logs_schema`] plus the synthesised `labels` column. The
/// physical columns keep their indices; `labels` is appended last.
fn logs_table_schema() -> SchemaRef {
    let mut fields: Vec<Field> = logs_schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(labels_field());
    Arc::new(Schema::new(fields))
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
    /// `fingerprint → labels` for every stream across the candidate
    /// blocks, inverted from their postings sidecars. Drives the
    /// synthesised `labels` column (see [`LabelEnrichExec`]).
    fp_labels: Arc<FpLabels>,
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
        fp_labels: Arc<FpLabels>,
    ) -> DfResult<Self> {
        Ok(Self {
            // The *table* schema (physical columns + synthesised `labels`).
            // The parquet scans below read the physical schema only.
            schema: logs_table_schema(),
            object_store_url: object_store_url_for(bucket)?,
            blocks,
            ts_min,
            ts_max,
            body_contains,
            fp_labels,
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
        //
        // The parquet files carry only the *physical* schema; `labels` is
        // synthesised on top by [`LabelEnrichExec`]. So the predicate and
        // the `ParquetSource` use `physical_schema`, while `projection`
        // (which indexes into the *table* schema, `labels` last) is
        // translated below.
        let physical_schema = logs_schema();
        let table_schema = self.schema();
        let labels_idx = physical_schema.fields().len();
        let df_schema = DFSchema::try_from(physical_schema.clone())?;

        // Is the synthesised `labels` column actually requested? `None`
        // projection = "all columns" = yes. When it isn't, we take the
        // exact pre-existing code path: every requested index is a physical
        // column (< `labels_idx`), so `projection` passes straight through
        // to the parquet scan and no enrichment plan is added (keeps e.g.
        // `SELECT count(*)` and label-free SQL byte-for-byte as before).
        let want_labels = projection.map_or(true, |p| p.contains(&labels_idx));

        // Physical projection pushed into the parquet scan. When labels are
        // wanted we read the full physical schema (so `stream_fingerprint`
        // is present for the join) and let the enrich + a final projection
        // shape the output; otherwise the requested indices are already
        // physical and map 1:1.
        let phys_projection: Option<Vec<usize>> = if want_labels {
            None
        } else {
            projection.cloned()
        };

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
                ParquetSource::new(physical_schema.clone())
                    .with_predicate(predicate)
                    .with_pushdown_filters(true),
            );
            let builder = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
                .with_projection_indices(phys_projection.clone())?
                .with_limit(limit)
                .with_file(PartitionedFile::new(file_path, file_size));

            Ok(DataSourceExec::from_data_source(builder.build()))
        };

        // ── Build the physical (parquet) scan ──────────────────────
        let scan_plan: Arc<dyn ExecutionPlan> = if self.blocks.is_empty() {
            // No overlapping blocks. Emit a single empty `DataSourceExec`
            // so `SELECT count(*) FROM logs` returns 0 cleanly. Mirrors
            // metrics behaviour.
            let source =
                Arc::new(ParquetSource::new(physical_schema.clone()).with_pushdown_filters(true));
            let builder = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
                .with_projection_indices(phys_projection.clone())?
                .with_limit(limit);
            DataSourceExec::from_data_source(builder.build())
        } else {
            let mut branches: Vec<Arc<dyn ExecutionPlan>> =
                Vec::with_capacity(self.blocks.len());
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
            UnionExec::try_new(branches)?
        };

        // ── Label join (synthesised `labels` column) ───────────────
        //
        // When `labels` isn't requested the parquet scan already produces
        // exactly the requested physical columns — return it untouched.
        if !want_labels {
            return Ok(scan_plan);
        }

        // Otherwise the scan produces the full physical schema; append
        // `labels` (a `Map<Utf8,Utf8>` joined from `stream_fingerprint`),
        // then project to the exact requested column set/order. `None`
        // projection means "all columns" → the enriched plan is already it.
        let enriched: Arc<dyn ExecutionPlan> = Arc::new(LabelEnrichExec::try_new(
            scan_plan,
            table_schema.clone(),
            self.fp_labels.clone(),
        )?);

        match projection {
            None => Ok(enriched),
            Some(proj) => {
                let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = proj
                    .iter()
                    .map(|&i| {
                        let f = table_schema.field(i);
                        (
                            Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
                            f.name().to_string(),
                        )
                    })
                    .collect();
                Ok(Arc::new(ProjectionExec::try_new(exprs, enriched)?))
            }
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Filters that reference the synthesised `labels` column cannot be
        // pushed into the parquet scan (no such physical column), so report
        // them `Unsupported` — DataFusion keeps them in a `FilterExec`
        // above our scan, where `labels` exists. Everything else stays
        // `Inexact` (pushed as a predicate *and* re-checked above), exactly
        // as before.
        Ok(filters
            .iter()
            .map(|f| {
                if expr_references_labels(f) {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }
}

/// Does `expr` reference the synthesised `labels` column? Such filters
/// can't be pushed into the parquet scan (no physical `labels` column).
fn expr_references_labels(expr: &Expr) -> bool {
    expr.column_refs().iter().any(|c| c.name == "labels")
}

// ── Label-enrich execution plan ───────────────────────────────────

/// Appends a synthesised `labels` `Map<Utf8,Utf8>` column to its child's
/// batches, joining each row's `stream_fingerprint` against a precomputed
/// `fingerprint → labels` map.
///
/// Stream labels (namespace / pod / container / node / `k8s_*`) live only
/// in the per-block postings sidecar, keyed by fingerprint — they never
/// appear in the main parquet. This node is the query-side join that makes
/// them a first-class result column without re-ingesting any data. The
/// child must expose `stream_fingerprint`; its index is resolved once at
/// construction.
struct LabelEnrichExec {
    input: Arc<dyn ExecutionPlan>,
    /// Output schema: the child's columns plus `labels` last.
    schema: SchemaRef,
    fp_labels: Arc<FpLabels>,
    /// Index of `stream_fingerprint` within the child's output.
    fp_idx: usize,
    props: Arc<PlanProperties>,
}

impl LabelEnrichExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        fp_labels: Arc<FpLabels>,
    ) -> DfResult<Self> {
        let fp_idx = input.schema().index_of("stream_fingerprint").map_err(|_| {
            DataFusionError::Internal(
                "LabelEnrichExec child is missing the stream_fingerprint column".to_string(),
            )
        })?;
        let child = input.properties();
        let props = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            child.partitioning.clone(),
            child.emission_type,
            child.boundedness,
        );
        Ok(Self {
            input,
            schema,
            fp_labels,
            fp_idx,
            props: Arc::new(props),
        })
    }
}

impl std::fmt::Debug for LabelEnrichExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabelEnrichExec")
            .field("fp_idx", &self.fp_idx)
            .field("known_fingerprints", &self.fp_labels.len())
            .finish()
    }
}

impl DisplayAs for LabelEnrichExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LabelEnrichExec: labels<-stream_fingerprint, known_fps={}",
            self.fp_labels.len()
        )
    }
}

impl ExecutionPlan for LabelEnrichExec {
    fn name(&self) -> &str {
        "LabelEnrichExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let child = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("LabelEnrichExec expects exactly one child".to_string())
        })?;
        Ok(Arc::new(LabelEnrichExec::try_new(
            child,
            self.schema.clone(),
            self.fp_labels.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let out_schema = self.schema.clone();
        let fp_labels = self.fp_labels.clone();
        let fp_idx = self.fp_idx;
        let stream = input.map(move |batch| {
            let batch = batch?;
            enrich_batch(&batch, fp_idx, &fp_labels, &out_schema)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Append the joined `labels` column to one batch.
fn enrich_batch(
    batch: &RecordBatch,
    fp_idx: usize,
    fp_labels: &FpLabels,
    out_schema: &SchemaRef,
) -> DfResult<RecordBatch> {
    let fps = batch
        .column(fp_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("stream_fingerprint column is not UInt64".to_string())
        })?;

    let mut mb = MapBuilder::new(
        Some(labels_map_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    for i in 0..batch.num_rows() {
        if !fps.is_null(i) {
            if let Some(pairs) = fp_labels.get(&fps.value(i)) {
                for (k, v) in pairs.iter() {
                    mb.keys().append_value(k.as_ref());
                    mb.values().append_value(v.as_ref());
                }
            }
        }
        // One map per row (empty when the fingerprint has no resolved
        // labels). Non-null map; the column field is nullable but we never
        // emit a null entry.
        mb.append(true)?;
    }
    let labels = mb.finish();

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(labels));
    RecordBatch::try_new(out_schema.clone(), columns).map_err(DataFusionError::from)
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
                Arc::new(FpLabels::new()),
            )?);
        }
    };

    let mut blocks: Vec<LogsBlockEntry> = Vec::with_capacity(candidates.len());
    // Accumulates `fingerprint → {(name, value)}` across every surviving
    // block's postings sidecar; frozen into the table's `fp_labels` map
    // below. Fingerprints are global, so a stream resolves to the same
    // labels in every block — duplicates collapse in the `BTreeSet`.
    let mut fp_acc: HashMap<u64, BTreeSet<(String, String)>> = HashMap::new();
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

                // Invert this block's full postings index into the
                // `fingerprint → labels` accumulator so the synthesised
                // `labels` column can be populated at scan time. For
                // matcher queries the index is already cached (resolve
                // fetched it); the empty-matcher path resolves via
                // `meta.json`, so this is the one extra GET that surfaces
                // labels — and it's cached for subsequent queries. A
                // missing/bad sidecar is non-fatal: labels just don't
                // appear for that block's streams.
                let index = match cache {
                    Some(c) => c.get_or_fetch(store.clone(), &entry.meta).await,
                    None => postings::fetch_and_parse_postings(store.clone(), &entry.meta)
                        .await
                        .map(Arc::new),
                };
                match index {
                    Ok(index) => index.invert_into(&mut fp_acc),
                    Err(e) => warn!(
                        uuid = %entry.meta.uuid,
                        error = %e,
                        "logs label join: postings fetch failed; labels omitted for this block"
                    ),
                }

                blocks.push(LogsBlockEntry { entry, fp_set });
            }
        }
    }

    // Freeze the accumulator into the shared `fingerprint → labels` map,
    // interning each string once via `Arc<str>`.
    let fp_labels: FpLabels = fp_acc
        .into_iter()
        .map(|(fp, pairs)| {
            let v: Vec<(Arc<str>, Arc<str>)> = pairs
                .into_iter()
                .map(|(k, val)| (Arc::from(k.as_str()), Arc::from(val.as_str())))
                .collect();
            (fp, Arc::new(v))
        })
        .collect();

    LogsTable::new(
        &bucket,
        blocks,
        q.ts_min,
        q.ts_max,
        q.body_contains.clone(),
        Arc::new(fp_labels),
    )
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
