// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Worker-side write sink for a distributed IVM refresh.
//!
//! The sink consumes one output partition, writes its batches through the
//! normal LakeSoul writer and returns a small `TaskOutput` row.  It never
//! commits: the coordinator collects every task output and publishes the files
//! with the usual `IvmTable::commit_files` path.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::StreamExt;

use crate::table::{IVM_ROW_KINDS_COLUMN, IvmTable};

/// One output row of a worker write task.
pub const IVM_TASK_OUTPUT_SCHEMA_MARKER: &str = "__ivm_task_output";

/// The schema returned by [`IvmRefreshSinkExec`].
pub fn task_output_schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("task_id", DataType::UInt32, false),
            Field::new("table_id", DataType::Utf8, false),
            Field::new("epoch", DataType::Int64, false),
            Field::new("files", DataType::Utf8, false),
            Field::new("rows", DataType::UInt64, false),
            Field::new("inserts", DataType::UInt64, false),
            Field::new("deletes", DataType::UInt64, false),
            Field::new("bytes", DataType::UInt64, false),
        ],
        std::collections::HashMap::from([(
            IVM_TASK_OUTPUT_SCHEMA_MARKER.to_string(),
            "true".to_string(),
        )]),
    ))
}

/// One table write a worker must perform.
#[derive(Debug, Clone)]
pub struct IvmSinkSpec {
    pub table: IvmTable,
    pub epoch: Option<i64>,
}

tokio::task_local! {
    /// Set by `write_dataframe` while a distributed refresh plans its output.
    pub static IVM_SINK_SPEC: IvmSinkSpec;
    /// Marks a nested plan (a lookup contribution, for example) that must not
    /// be wrapped by the root write sink.
    static IVM_SINK_DISABLED: bool;
}

/// The sink spec of the current refresh task, if any.
pub fn current_sink_spec() -> Option<IvmSinkSpec> {
    IVM_SINK_SPEC.try_with(Clone::clone).ok()
}

fn sink_disabled() -> bool {
    IVM_SINK_DISABLED
        .try_with(|disabled| *disabled)
        .unwrap_or(false)
}

/// Plan a nested contribution without letting the root write sink wrap it.
pub async fn without_sink<F: std::future::Future>(future: F) -> F::Output {
    IVM_SINK_DISABLED.scope(true, future).await
}

/// Wrap `plan` so its output partitions are written on workers.
pub fn sink_plan(
    plan: Arc<dyn ExecutionPlan>,
    spec: IvmSinkSpec,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
            Arc::new(IvmRefreshSinkExec::new(plan, spec)),
        ),
    )
}

/// A physical write sink: one task output row per input partition.
pub struct IvmRefreshSinkExec {
    input: Arc<dyn ExecutionPlan>,
    table: IvmTable,
    epoch: Option<i64>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl IvmRefreshSinkExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, spec: IvmSinkSpec) -> Self {
        let partitions = input.output_partitioning().partition_count().max(1);
        let schema = task_output_schema();
        Self {
            input,
            table: spec.table,
            epoch: spec.epoch,
            schema: schema.clone(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }

    pub(crate) fn table(&self) -> &IvmTable {
        &self.table
    }

    pub(crate) fn epoch(&self) -> Option<i64> {
        self.epoch
    }
}

impl std::fmt::Debug for IvmRefreshSinkExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IvmRefreshSinkExec")
    }
}

impl DisplayAs for IvmRefreshSinkExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "IvmRefreshSinkExec table={}", self.table.table_name)
    }
}

impl ExecutionPlanProperties for IvmRefreshSinkExec {
    fn output_partitioning(&self) -> &Partitioning {
        &self.properties.partitioning
    }

    fn output_ordering(&self) -> Option<&datafusion::physical_expr::LexOrdering> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        &self.properties.eq_properties
    }
}

impl ExecutionPlan for IvmRefreshSinkExec {
    fn name(&self) -> &str {
        "IvmRefreshSinkExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )
            -> DFResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return datafusion::common::internal_err!(
                "IvmRefreshSinkExec expects exactly one child"
            );
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            IvmSinkSpec {
                table: self.table.clone(),
                epoch: self.epoch,
            },
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let table = self.table.clone();
        let epoch = self.epoch;
        let schema = self.schema.clone();
        let task_id = partition as u32;
        let output = futures::stream::once(async move {
            write_partition(table, epoch, task_id, schema, input).await
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            output,
        )))
    }
}

async fn write_partition(
    table: IvmTable,
    epoch: Option<i64>,
    task_id: u32,
    schema: SchemaRef,
    mut input: SendableRecordBatchStream,
) -> DFResult<RecordBatch> {
    let mut writer = table.writer().await.map_err(|error| {
        datafusion::error::DataFusionError::Execution(error.to_string())
    })?;
    let mut rows = 0u64;
    let mut inserts = 0u64;
    let mut deletes = 0u64;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        rows += batch.num_rows() as u64;
        if let Ok(index) = batch.schema().index_of(IVM_ROW_KINDS_COLUMN)
            && let Some(kinds) =
                batch.column(index).as_any().downcast_ref::<StringArray>()
        {
            for row in 0..kinds.len() {
                match kinds.value(row) {
                    "insert" => inserts += 1,
                    "delete" => deletes += 1,
                    _ => {}
                }
            }
        }
        let batch = table.cast_batch(batch).map_err(|error| {
            datafusion::error::DataFusionError::Execution(error.to_string())
        })?;
        writer.write_record_batch(batch).await.map_err(|error| {
            datafusion::error::DataFusionError::Execution(error.to_string())
        })?;
    }
    let outputs = writer.flush_and_close().await.map_err(|error| {
        datafusion::error::DataFusionError::Execution(error.to_string())
    })?;
    let files = table.output_files(outputs, rows);
    let bytes = files
        .iter()
        .map(|file| file.size.max(0) as u64)
        .sum::<u64>();
    let encoded = serde_json::to_string(&files).map_err(|error| {
        datafusion::error::DataFusionError::Execution(format!(
            "encode IVM task files: {error}"
        ))
    })?;
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt32Array::from(vec![task_id])),
            Arc::new(StringArray::from(vec![table.table_id.as_str()])),
            Arc::new(Int64Array::from(vec![epoch.unwrap_or(-1)])),
            Arc::new(StringArray::from(vec![encoded.as_str()])),
            Arc::new(UInt64Array::from(vec![rows])),
            Arc::new(UInt64Array::from(vec![inserts])),
            Arc::new(UInt64Array::from(vec![deletes])),
            Arc::new(UInt64Array::from(vec![bytes])),
        ],
    )
    .map_err(Into::into)
}

/// Physical optimizer rule that wraps a refresh output with [`IvmRefreshSinkExec`].
///
/// `write_dataframe` sets [`IVM_SINK_SPEC`] only while it plans an IVM write,
/// so the rule is a no-op for ordinary queries and reads.  It runs in the local
/// planner before the distributed planner inserts stage boundaries, which is
/// what makes the sink part of a worker stage.
#[derive(Debug, Default)]
pub struct IvmSinkRule;

impl PhysicalOptimizerRule for IvmSinkRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if sink_disabled() {
            return Ok(plan);
        }
        let Some(spec) = current_sink_spec() else {
            return Ok(plan);
        };
        // Physical optimizer passes can revisit the root; never nest sinks.
        if plan.downcast_ref::<IvmRefreshSinkExec>().is_some()
            || plan
                .downcast_ref::<datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec>()
                .and_then(|coalesce| coalesce.input().downcast_ref::<IvmRefreshSinkExec>())
                .is_some()
        {
            return Ok(plan);
        }
        Ok(sink_plan(plan, spec))
    }

    fn name(&self) -> &str {
        "ivm_refresh_sink"
    }

    fn schema_check(&self) -> bool {
        // The sink intentionally changes the output schema to task descriptors.
        false
    }
}
