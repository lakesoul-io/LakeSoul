// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Primary-key merge through the LakeSoul merge-on-read pipeline.
//!
//! The compaction planner must agree with the reader about which row survives
//! for every key: the same `UseLast` rule, the same per-file deduplication for
//! non-compacted inputs, the same handling of partition/default columns.  This
//! module therefore runs the real [`crate::physical_plan::merge::MergeParquetExec`]
//! over a primary-key-only projection and tags every winner with the source
//! file and source row.
//!
//! The tag is appended *before* the merge by [`SourceTagExec`], so the merge's
//! own deduplication and winner selection carry it along.  The result is one
//! `(file, row)` pair per surviving key, in merge output key order.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalExpr};
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_session::Session;
use futures::{Stream, StreamExt};
use rootcause::report;

use crate::Result;
use crate::compaction::analyze::InputSummary;
use crate::config::{IOSchema, LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use crate::physical_plan::merge::{MergeParquetExec, merge_input_scan};
use crate::session::LakeSoulIOSession;

/// Reserved column carrying the source file index of a merged row.
pub const PROVENANCE_FILE_COLUMN: &str = "__lakesoul_compaction_source_file";
/// Reserved column carrying the source row index within that file.
pub const PROVENANCE_ROW_COLUMN: &str = "__lakesoul_compaction_source_row";

/// One surviving row and where it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WinnerRow {
    /// Index into the input file list.
    pub file: usize,
    /// Row index within that file.
    pub row: u64,
}

/// The merged winner rows in primary-key order.
#[derive(Debug, Clone, Default)]
pub struct PkMergeResult {
    pub winners: Vec<WinnerRow>,
    /// Physical rows in all input files before merging.
    pub total_rows: u64,
}

/// Run the merge-on-read pipeline over the primary keys only and return the
/// surviving source row of every key.
///
/// `reader_config` supplies object-store credentials, partition defaults, range
/// partitions and tuning options; its file list, primary keys and target
/// schema are replaced by the planner.  Files must be in version order
/// (oldest first), which is the order the caller already passes to the native
/// compaction reader.
pub async fn merge_pk_winners(
    summary: &InputSummary,
    primary_keys: &[String],
    reader_config: Option<&LakeSoulIOConfig>,
) -> Result<PkMergeResult> {
    if primary_keys.is_empty() {
        return Err(report!(
            "compaction planning requires at least one primary key"
        ));
    }
    if summary.files.is_empty() {
        return Ok(PkMergeResult::default());
    }
    let pk_types = summary
        .pk_types
        .as_ref()
        .ok_or_else(|| report!("primary key types differ across input files"))?;
    if let Some(missing) =
        primary_keys
            .iter()
            .zip(pk_types)
            .find_map(|(name, data_type)| {
                (!is_mergeable_key_type(data_type)).then_some(name.clone())
            })
    {
        return Err(report!(
            "primary key {missing} cannot be merged by the native reader"
        ));
    }

    let pk_fields = primary_keys
        .iter()
        .zip(pk_types)
        .map(|(name, data_type)| Field::new(name, data_type.clone(), false))
        .collect::<Vec<_>>();
    let pk_schema = Arc::new(Schema::new(pk_fields));

    let mut config = match reader_config {
        Some(config) => config.clone(),
        None => LakeSoulIOConfigBuilder::new().build(),
    };
    config.files = summary.files.iter().map(|file| file.path.clone()).collect();
    config.primary_keys = primary_keys.to_vec();
    config.target_schema = IOSchema(pk_schema.clone());
    // Planning reads primary keys only: filters, CDC columns, range
    // partitions and merge-skipping are caller concerns of the full read.
    // A CDC column would be projected by the scan but is absent from the
    // key-only merged schema, and range partitions may not be part of the
    // key schema at all.
    config.options.remove(crate::config::OPTION_KEY_CDC_COLUMN);
    config
        .options
        .remove(crate::config::OPTION_KEY_SKIP_MERGE_ON_READ);
    config.range_partitions.clear();

    let session = LakeSoulIOSession::try_new(config.clone())
        .map_err(|error| report!("build compaction merge session: {error}"))?;
    let scan_configs = session
        .scan_configs(pk_schema.clone())
        .await
        .map_err(|error| report!("build compaction primary-key scans: {error}"))?;
    if scan_configs.len() != summary.files.len() {
        return Err(report!(
            "primary-key merge planned {} scans for {} files",
            scan_configs.len(),
            summary.files.len()
        ));
    }

    let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(scan_configs.len());
    for (file, scan_config) in scan_configs.into_iter().enumerate() {
        let input = merge_input_scan(scan_config, primary_keys);
        inputs.push(Arc::new(SourceTagExec::new(input, file as i64)));
    }

    let mut merged_fields = pk_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    merged_fields.push(Field::new(PROVENANCE_FILE_COLUMN, DataType::Int64, false));
    merged_fields.push(Field::new(PROVENANCE_ROW_COLUMN, DataType::Int64, false));
    let merged_schema = Arc::new(Schema::new(merged_fields));

    let merge = MergeParquetExec::new_with_inputs(
        merged_schema,
        inputs,
        config.clone(),
        Arc::new(config.default_column_value.clone()),
    )
    .map_err(|error| report!("build compaction primary-key merge: {error}"))?;
    let mut stream = execute_stream(Arc::new(merge), session.task_ctx())
        .map_err(|error| report!("execute compaction primary-key merge: {error}"))?;

    let mut winners = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| report!("read primary-key merge: {error}"))?;
        let file_index = batch
            .schema()
            .index_of(PROVENANCE_FILE_COLUMN)
            .map_err(|error| report!("provenance file column: {error}"))?;
        let row_index = batch
            .schema()
            .index_of(PROVENANCE_ROW_COLUMN)
            .map_err(|error| report!("provenance row column: {error}"))?;
        let files = batch
            .column(file_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| report!("provenance file column is not Int64"))?;
        let rows = batch
            .column(row_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| report!("provenance row column is not Int64"))?;
        for index in 0..batch.num_rows() {
            let file = usize::try_from(files.value(index))
                .map_err(|_| report!("negative provenance file index"))?;
            let row = u64::try_from(rows.value(index))
                .map_err(|_| report!("negative provenance row index"))?;
            if file >= summary.files.len() {
                return Err(report!(
                    "provenance file index {file} out of range for {} files",
                    summary.files.len()
                ));
            }
            if row >= summary.files[file].rows {
                return Err(report!(
                    "provenance row {row} out of range for {} rows in {}",
                    summary.files[file].rows,
                    summary.files[file].path
                ));
            }
            winners.push(WinnerRow { file, row });
        }
    }

    Ok(PkMergeResult {
        winners,
        total_rows: summary.files.iter().map(|file| file.rows).sum(),
    })
}

fn is_mergeable_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// Appends the source file index and a per-file row counter to every batch.
///
/// The merge sees these as ordinary payload columns, so its `UseLast` winner
/// selection returns the tag of the row that actually survived.
#[derive(Debug)]
struct SourceTagExec {
    input: Arc<dyn ExecutionPlan>,
    file_id: i64,
    target_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SourceTagExec {
    fn new(input: Arc<dyn ExecutionPlan>, file_id: i64) -> Self {
        let mut fields = input
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new(PROVENANCE_FILE_COLUMN, DataType::Int64, false));
        fields.push(Field::new(PROVENANCE_ROW_COLUMN, DataType::Int64, false));
        let target_schema = Arc::new(Schema::new(fields));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(target_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            file_id,
            target_schema,
            properties,
        }
    }
}

impl DisplayAs for SourceTagExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "SourceTagExec(file_id={})", self.file_id)
    }
}

impl ExecutionPlanProperties for SourceTagExec {
    fn output_partitioning(&self) -> &Partitioning {
        &self.properties.partitioning
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
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

impl ExecutionPlan for SourceTagExec {
    fn name(&self) -> &str {
        "SourceTagExec"
    }

    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.file_id)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "SourceTagExec requires a single input partition, got {partition}"
            )));
        }
        if self.input.output_partitioning().partition_count() != 1 {
            return Err(DataFusionError::Internal(format!(
                "SourceTagExec input has {} partitions; one file must be one partition",
                self.input.output_partitioning().partition_count()
            )));
        }
        let input = self.input.execute(0, context)?;
        Ok(Box::pin(SourceTagStream {
            input,
            schema: self.target_schema.clone(),
            file_id: self.file_id,
            next_row: 0,
        }))
    }
}

struct SourceTagStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    file_id: i64,
    next_row: i64,
}

impl Stream for SourceTagStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let batch = match futures::ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => batch,
            Some(Err(error)) => return Poll::Ready(Some(Err(error))),
            None => return Poll::Ready(None),
        };
        let rows = batch.num_rows();
        let files: ArrayRef = Arc::new(Int64Array::from(vec![self.file_id; rows]));
        let row_ids: ArrayRef = Arc::new(Int64Array::from(
            (self.next_row..self.next_row + rows as i64).collect::<Vec<_>>(),
        ));
        self.next_row += rows as i64;

        let mut columns = batch.columns().to_vec();
        columns.push(files);
        columns.push(row_ids);
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        Poll::Ready(Some(
            RecordBatch::try_new_with_options(self.schema.clone(), columns, &options)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
        ))
    }
}

impl RecordBatchStream for SourceTagStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
