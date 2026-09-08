//! Normalization of versioned logs parquet batches into the stable v2 query schema.
use std::{any::Any, sync::Arc};

use arrow::array::new_null_array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

pub(crate) fn physical_schema(version: u32) -> DfResult<SchemaRef> {
    match version {
        1 => Ok(scry_block::logs_physical_schema_v1()),
        2 => Ok(scry_block::logs_physical_schema_v2()),
        _ => Err(DataFusionError::Plan(format!(
            "unsupported logs block schema version {version}"
        ))),
    }
}

fn normalize_batch(
    batch: RecordBatch,
    version: u32,
    input_indices: &[usize],
    output_indices: &[usize],
    schema: &SchemaRef,
) -> DfResult<RecordBatch> {
    let available = physical_schema(version)?.fields().len();
    let columns = output_indices
        .iter()
        .map(|output_idx| {
            if *output_idx >= available {
                new_null_array(schema.field(*output_idx).data_type(), batch.num_rows())
            } else {
                let input_idx = input_indices
                    .iter()
                    .position(|idx| idx == output_idx)
                    .expect("normalizer output column included in parquet projection");
                batch.column(input_idx).clone()
            }
        })
        .collect();
    let projected = Arc::new(schema.project(output_indices)?);
    RecordBatch::try_new_with_options(
        projected,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(Into::into)
}

pub(crate) struct LogsNormalizeExec {
    input: Arc<dyn ExecutionPlan>,
    version: u32,
    input_indices: Arc<Vec<usize>>,
    output_indices: Arc<Vec<usize>>,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl LogsNormalizeExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        version: u32,
        input_indices: Vec<usize>,
        output_indices: Vec<usize>,
    ) -> Self {
        let full_schema = scry_block::logs_physical_schema_v2();
        let schema = Arc::new(
            full_schema
                .project(&output_indices)
                .expect("valid logs projection"),
        );
        let child = input.properties();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            child.partitioning.clone(),
            child.emission_type,
            child.boundedness,
        ));
        Self {
            input,
            version,
            input_indices: Arc::new(input_indices),
            output_indices: Arc::new(output_indices),
            schema,
            props,
        }
    }
}

impl std::fmt::Debug for LogsNormalizeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogsNormalizeExec")
            .field("version", &self.version)
            .finish()
    }
}

impl DisplayAs for LogsNormalizeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogsNormalizeExec: v{} -> v2", self.version)
    }
}

impl ExecutionPlan for LogsNormalizeExec {
    fn name(&self) -> &str {
        "LogsNormalizeExec"
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
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "LogsNormalizeExec expects one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.version,
            self.input_indices.as_ref().clone(),
            self.output_indices.as_ref().clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let schema = scry_block::logs_physical_schema_v2();
        let output_schema = self.schema.clone();
        let version = self.version;
        let input_indices = self.input_indices.clone();
        let output_indices = self.output_indices.clone();
        let stream = self.input.execute(partition, context)?.map(move |batch| {
            normalize_batch(batch?, version, &input_indices, &output_indices, &schema)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray, UInt64Array, UInt8Array};

    #[test]
    fn v1_appends_typed_null_fidelity_columns() {
        let v1 = scry_block::logs_physical_schema_v1();
        let mut attrs = arrow::array::MapBuilder::new(
            None,
            arrow::array::StringBuilder::new(),
            arrow::array::StringBuilder::new(),
        );
        attrs.append(true).unwrap();
        let attrs = attrs.finish();
        let batch = RecordBatch::try_new(
            v1,
            vec![
                Arc::new(UInt64Array::from(vec![7])),
                Arc::new(UInt64Array::from(vec![11])),
                Arc::new(UInt8Array::from(vec![17])),
                Arc::new(StringArray::from(vec!["boom"])),
                Arc::new(attrs),
            ],
        )
        .unwrap();
        let schema = scry_block::logs_physical_schema_v2();
        let indices: Vec<usize> = (0..13).collect();
        let normalized =
            normalize_batch(batch, 1, &(0..5).collect::<Vec<_>>(), &indices, &schema).unwrap();
        assert_eq!(normalized.schema(), schema);
        assert_eq!(normalized.num_columns(), 13);
        for column in normalized.columns().iter().skip(5) {
            assert_eq!(column.null_count(), 1);
        }
    }
}
