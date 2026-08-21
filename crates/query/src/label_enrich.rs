//! Shared query-side label join: attach a synthesised `labels`
//! `Map<Utf8,Utf8>` column to a signal's scan rows by joining each row's
//! fingerprint against a precomputed `fingerprint → labels` map inverted
//! from the per-block postings sidecars.
//!
//! Both the logs (`stream_fingerprint`) and metrics (`series_fingerprint`)
//! query paths use this — the only signal-specific input is the name of the
//! fingerprint column, passed to [`LabelEnrichExec::try_new`]. Labels live
//! only in the postings sidecar (keyed by fingerprint) and never appear in
//! the main parquet, so this node is the query-side join that makes them a
//! first-class result column without re-ingesting any data.

use std::any::Any;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, MapBuilder, MapFieldNames, StringBuilder, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

/// A stream/series' resolved labels: `(name, value)` pairs, deduplicated and
/// sorted (the `BTreeSet` build order is preserved on freeze). `Arc<str>`
/// so the same label strings are shared across fingerprints without
/// re-allocating per row at scan time.
pub type LabelPairs = Arc<Vec<(Arc<str>, Arc<str>)>>;

/// `fingerprint → labels` map for a query's candidate blocks, built by
/// inverting their postings sidecars. Shared (`Arc`) into the scan plan so
/// [`LabelEnrichExec`] can attach labels to each row.
pub type FpLabels = HashMap<u64, LabelPairs>;

/// The mutable accumulator that a block's `PostingsIndex::invert_into`
/// merges into; frozen into an [`FpLabels`] by [`freeze_fp_labels`].
pub type FpAcc = HashMap<u64, BTreeSet<(String, String)>>;

/// The `labels` column appended to a table by the query-side label join. A
/// `Map<Utf8,Utf8>` carrying a stream/series' resolved labels, which live
/// only in the per-block postings sidecar (keyed by fingerprint) and so are
/// otherwise invisible in query results.
///
/// The field shape mirrors the parquet `attributes` map exactly so it
/// matches the `MapArray` produced by Arrow's [`MapBuilder`] (entry struct
/// non-null, `keys` non-null, `values` nullable) — a mismatch would fail
/// `RecordBatch::try_new` at scan time. The column itself is nullable.
pub fn labels_field() -> Field {
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
pub fn labels_map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "keys".to_string(),
        value: "values".to_string(),
    }
}

/// Freeze a per-block accumulator into the shared `fingerprint → labels`
/// map, interning each string once via `Arc<str>`.
pub fn freeze_fp_labels(fp_acc: FpAcc) -> FpLabels {
    fp_acc
        .into_iter()
        .map(|(fp, pairs)| {
            let v: Vec<(Arc<str>, Arc<str>)> = pairs
                .into_iter()
                .map(|(k, val)| (Arc::from(k.as_str()), Arc::from(val.as_str())))
                .collect();
            (fp, Arc::new(v))
        })
        .collect()
}

/// Does `expr` reference the synthesised `labels` column? Such filters can't
/// be pushed into the parquet scan (no physical `labels` column), so a
/// `TableProvider` should report them `Unsupported`.
pub fn expr_references_labels(expr: &Expr) -> bool {
    expr.column_refs().iter().any(|c| c.name == "labels")
}

// ── Label-enrich execution plan ───────────────────────────────────

/// Appends a synthesised `labels` `Map<Utf8,Utf8>` column to its child's
/// batches, joining each row's fingerprint against a precomputed
/// `fingerprint → labels` map. The child must expose the fingerprint column
/// named `fp_col`; its index is resolved once at construction.
pub struct LabelEnrichExec {
    input: Arc<dyn ExecutionPlan>,
    /// Output schema: the child's columns plus `labels` last.
    schema: SchemaRef,
    fp_labels: Arc<FpLabels>,
    /// Name of the fingerprint column in the child's output
    /// (`stream_fingerprint` for logs, `series_fingerprint` for metrics).
    fp_col: &'static str,
    /// Index of `fp_col` within the child's output.
    fp_idx: usize,
    props: Arc<PlanProperties>,
}

impl LabelEnrichExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        fp_labels: Arc<FpLabels>,
        fp_col: &'static str,
    ) -> DfResult<Self> {
        let fp_idx = input.schema().index_of(fp_col).map_err(|_| {
            DataFusionError::Internal(format!(
                "LabelEnrichExec child is missing the {fp_col} column"
            ))
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
            fp_col,
            fp_idx,
            props: Arc::new(props),
        })
    }
}

impl std::fmt::Debug for LabelEnrichExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabelEnrichExec")
            .field("fp_col", &self.fp_col)
            .field("fp_idx", &self.fp_idx)
            .field("known_fingerprints", &self.fp_labels.len())
            .finish()
    }
}

impl DisplayAs for LabelEnrichExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LabelEnrichExec: labels<-{}, known_fps={}",
            self.fp_col,
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
            self.fp_col,
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
        let fp_col = self.fp_col;
        let stream = input.map(move |batch| {
            let batch = batch?;
            enrich_batch(&batch, fp_idx, fp_col, &fp_labels, &out_schema)
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
    fp_col: &str,
    fp_labels: &FpLabels,
    out_schema: &SchemaRef,
) -> DfResult<RecordBatch> {
    let fps = batch
        .column(fp_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| DataFusionError::Internal(format!("{fp_col} column is not UInt64")))?;

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
