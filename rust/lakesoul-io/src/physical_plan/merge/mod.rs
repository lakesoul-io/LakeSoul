// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the merge on read execution plan.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef, SortOptions};
use async_stream::try_stream;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::physical_plan::FileScanConfigBuilder;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlanProperties, Partitioning, PlanProperties};
use datafusion::prelude::SessionContext;
use datafusion::{
    datasource::physical_plan::FileScanConfig,
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
    },
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DFSchemaRef, DataFusionError, Result as DFResult};
use datafusion_substrait::substrait::proto::Plan;
use futures::StreamExt;

use self::sorted::merge_operator::MergeOperator;
use self::sorted::sorted_stream_merger::{SortedStream, build_sorted_stream_merger};
use crate::Result;
use crate::config::LakeSoulIOConfig;
use crate::execution_trace::instrument_record_batch_stream;
use crate::file_format::PhysicalFormat;
use crate::filter::parser::{FilterContainer, Parser as FilterParser};
use crate::stream::default_column::DefaultColumnStream;
use crate::stream::empty_schema::EmptySchemaStream;

pub mod sorted;

/// Builds the scan feeding one file into a [`MergeParquetExec`].
///
/// The k-way merge consumes each input as one stream sorted by the primary
/// keys: it only merges rows with equal keys when they surface adjacently
/// across streams, and `execute` rejects an input with more than one
/// partition. The scan therefore declares
///
/// - the primary key ordering, so file sources keep file order — the vortex
///   reader otherwise scans splits concurrently and yields batches out of
///   order, silently breaking merge-on-read deduplication;
/// - its file groups as its output partitioning, which makes
///   `FileScanConfig::repartitioned` refuse to byte-split the file:
///   `EnforceDistribution` splits every file scan of at least
///   `repartition_file_min_size` bytes into `target_partitions` ranges,
///   whatever its parent consumes.
pub fn merge_input_scan(
    config: FileScanConfig,
    primary_keys: &[String],
) -> Arc<dyn ExecutionPlan> {
    let partitions = config.file_groups.len();
    let ordering = primary_key_ordering(&config, primary_keys);
    let mut builder = FileScanConfigBuilder::from(config)
        .with_output_partitioning(Some(Partitioning::UnknownPartitioning(partitions)));
    if let Some(ordering) = ordering {
        builder = builder.with_output_ordering(vec![ordering]);
    }
    DataSourceExec::from_data_source(builder.build())
}

fn primary_key_ordering(
    config: &FileScanConfig,
    primary_keys: &[String],
) -> Option<LexOrdering> {
    let table_schema = config.file_source.table_schema().table_schema();
    LexOrdering::new(
        primary_keys
            .iter()
            .filter_map(|pk| {
                Column::new_with_schema(pk, table_schema).ok().map(|col| {
                    PhysicalSortExpr::new(Arc::new(col), SortOptions::default())
                })
            })
            .collect::<Vec<_>>(),
    )
}

/// One atomic MOR merge. Child order is file/version order within this unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeWorkUnit {
    pub input_range: Range<usize>,
    pub files: Vec<String>,
    pub default_column_value: Arc<HashMap<String, String>>,
    pub lane: usize,
}

/// [`ExecutionPlan`] implementation for the merge on read operation.
#[derive(Debug)]
pub struct MergeParquetExec {
    /// The schema of the merge on read operation.
    merged_schema: SchemaRef,
    /// The primary keys of the merge on read operation, which is the sorted columns.
    primary_keys: Arc<Vec<String>>,
    /// The merge operators of the merge on read operation.
    merge_operators: Arc<HashMap<String, String>>,
    /// The input execution plans of the merge on read operation.
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// The io config of the merge on read operation.
    io_config: LakeSoulIOConfig, // try use arc
    /// Atomic bucket merges, assigned to bounded, sequential execution lanes.
    work_units: Vec<MergeWorkUnit>,
    /// The properties of the merge on read operation.
    properties: Arc<PlanProperties>,
}

impl MergeParquetExec {
    /// Align the advertised merged schema with the values this plan can emit.
    ///
    /// A value field absent from an input may have no contribution for some
    /// keys and therefore produce NULL. Nullable input fields can likewise
    /// contribute NULL. Neither rule applies to declared primary keys or
    /// injected partition constants: retain their contract and let Arrow
    /// reject actual NULLs under a non-null declaration.
    fn merged_schema_with_input_nullability(
        merged_schema: SchemaRef,
        inputs: &[Arc<dyn ExecutionPlan>],
        partition_columns: &[String],
        primary_keys: &[String],
    ) -> SchemaRef {
        let mut fields = None;
        for (index, field) in merged_schema.fields().iter().enumerate() {
            if !field.is_nullable()
                && !partition_columns.contains(field.name())
                && !primary_keys.contains(field.name())
                && inputs.iter().any(|plan| {
                    plan.schema()
                        .column_with_name(field.name())
                        .is_none_or(|(_, input_field)| input_field.is_nullable())
                })
            {
                fields.get_or_insert_with(|| merged_schema.fields().to_vec())[index] =
                    Arc::new(field.as_ref().clone().with_nullable(true));
            }
        }
        match fields {
            Some(fields) => Arc::new(Schema::new_with_metadata(
                fields,
                merged_schema.metadata().clone(),
            )),
            None => merged_schema,
        }
    }

    /// Create a new Parquet reader execution plan provided file list and schema.
    pub fn new(
        merged_schema: SchemaRef,
        flatten_configs: Vec<FileScanConfig>,
        io_config: LakeSoulIOConfig,
    ) -> Result<Self> {
        let inputs = flatten_configs
            .into_iter()
            .map(|config| merge_input_scan(config, io_config.primary_keys_slice()))
            .collect();
        let defaults = Arc::new(io_config.default_column_value.clone());
        Self::new_with_inputs(merged_schema, inputs, io_config, defaults)
    }

    pub fn new_with_inputs(
        schema: SchemaRef,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        io_config: LakeSoulIOConfig,
        default_column_value: Arc<HashMap<String, String>>,
    ) -> Result<Self> {
        let work_units = vec![MergeWorkUnit {
            input_range: 0..inputs.len(),
            files: io_config.files_slice().to_vec(),
            default_column_value,
            lane: 0,
        }];
        Self::new_with_work_units(schema, inputs, io_config, work_units, 1)
    }

    /// Build bucket-local merges sharing one schema and a bounded set of lanes.
    pub fn new_with_work_units(
        schema: SchemaRef,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        io_config: LakeSoulIOConfig,
        work_units: Vec<MergeWorkUnit>,
        partition_count: usize,
    ) -> Result<Self> {
        let partition_columns: Vec<String> = work_units
            .iter()
            .flat_map(|unit| unit.default_column_value.keys().cloned())
            .collect();
        let schema = Self::merged_schema_with_input_nullability(
            schema,
            &inputs,
            &partition_columns,
            io_config.primary_keys_slice(),
        );
        Ok(Self::from_parts(
            schema,
            Arc::new(io_config.primary_keys.clone()),
            Arc::new(io_config.merge_operators.clone()),
            inputs,
            io_config,
            work_units,
            partition_count,
        )?)
    }
    /// Rebuild a [`MergeParquetExec`] from its encoded parts.
    ///
    /// Unlike [`Self::new_with_inputs`], the provided `merged_schema` is used
    /// as-is: input-driven nullability refinement must be identical to what the
    /// original (coordinator-side) instance computed, so the schema of every
    /// per-task variant stays consistent for plan serialization.
    pub fn from_parts(
        merged_schema: SchemaRef,
        primary_keys: Arc<Vec<String>>,
        merge_operators: Arc<HashMap<String, String>>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        io_config: LakeSoulIOConfig,
        work_units: Vec<MergeWorkUnit>,
        partition_count: usize,
    ) -> DFResult<Self> {
        if partition_count == 0 || work_units.is_empty() {
            return Err(DataFusionError::Internal(
                "MergeParquetExec requires at least one lane and work unit".into(),
            ));
        }
        let mut end = 0;
        for unit in &work_units {
            if unit.input_range.start != end
                || unit.input_range.end < end
                || unit.input_range.end > inputs.len()
                || unit.lane >= partition_count
            {
                return Err(DataFusionError::Internal(
                    "MergeParquetExec has invalid work-unit spans or lane assignment"
                        .into(),
                ));
            }
            end = unit.input_range.end;
        }
        if end != inputs.len() {
            return Err(DataFusionError::Internal(
                "MergeParquetExec work units do not cover all children".into(),
            ));
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&merged_schema)),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            merged_schema,
            primary_keys,
            merge_operators,
            inputs,
            io_config,
            work_units,
            properties,
        })
    }

    /// The IO configuration backing this merge (files, options, merge flags).
    pub fn io_config(&self) -> &LakeSoulIOConfig {
        &self.io_config
    }

    /// The schema of the merge on read operation.
    pub fn merged_schema(&self) -> SchemaRef {
        Arc::clone(&self.merged_schema)
    }

    pub fn primary_keys(&self) -> Arc<Vec<String>> {
        self.primary_keys.clone()
    }

    pub fn work_units(&self) -> &[MergeWorkUnit] {
        &self.work_units
    }

    pub fn merge_operators(&self) -> Arc<HashMap<String, String>> {
        self.merge_operators.clone()
    }
}

impl DisplayAs for MergeParquetExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "MergeParquetExec: lanes={}, units={}",
            self.output_partitioning().partition_count(),
            self.work_units.len()
        )
    }
}

impl ExecutionPlanProperties for MergeParquetExec {
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

impl ExecutionPlan for MergeParquetExec {
    fn name(&self) -> &str {
        "MergeParquetExec"
    }

    fn schema(&self) -> SchemaRef {
        self.merged_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if inputs.len() != self.inputs.len() {
            return Err(DataFusionError::Internal(
                "MergeParquetExec child replacement must preserve work-unit spans".into(),
            ));
        }
        Ok(Arc::new(Self {
            merged_schema: self.schema(),
            inputs,
            primary_keys: self.primary_keys(),
            merge_operators: self.merge_operators(),
            io_config: self.io_config.clone(),
            work_units: self.work_units.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let partition_count = self.output_partitioning().partition_count();
        if partition >= partition_count {
            return Err(DataFusionError::Internal(format!(
                "Invalid merge lane {partition}; expected fewer than {partition_count}"
            )));
        }
        let schema = self.schema();
        let inputs = self.inputs.clone();
        let units: Vec<_> = self
            .work_units
            .iter()
            .filter(|unit| unit.lane == partition)
            .cloned()
            .collect();
        let primary_keys = self.primary_keys();
        let merge_operators = self.merge_operators();
        let io_config = self.io_config.clone();
        let merged_schema = schema.clone();
        let stream = try_stream! {
            for unit in units {
                // No child of a later unit is executed until this merged stream
                // has been exhausted and dropped, including its retained buffers.
                let mut streams = Vec::with_capacity(unit.input_range.len());
                for input in &inputs[unit.input_range.clone()] {
                    let input_partition_count = input.output_partitioning().partition_count();
                    if input_partition_count != 1 {
                        Err(DataFusionError::Internal(format!(
                            "Merge input requires one partition, got {input_partition_count}"
                        )))?;
                    }
                    let stream = input.execute(0, context.clone())?;
                    streams.push(match input_files(input) {
                        Some(files) => instrument_record_batch_stream(
                            stream,
                            info_span!(
                                "file_scan",
                                file = files.first,
                                file_format = files.format.name(),
                                file_count = files.count,
                                outcome = tracing::field::Empty,
                                output_batches = tracing::field::Empty,
                                output_rows = tracing::field::Empty,
                                error = tracing::field::Empty,
                            ),
                            "file_scan",
                        ),
                        None => stream,
                    });
                }
                let reservation = MemoryConsumer::new(format!("LakeSoulMerge[{partition}]"))
                    .with_can_spill(false)
                    .register(&context.runtime_env().memory_pool);
                let mut config = io_config.clone();
                config.set_files(unit.files);
                let mut merged = merge_stream(
                    streams,
                    merged_schema.clone(),
                    primary_keys.clone(),
                    unit.default_column_value,
                    merge_operators.clone(),
                    context.session_config().batch_size(),
                    config,
                    reservation,
                ).map_err(crate::stream::merge_memory::merge_error)?;
                while let Some(batch) = merged.next().await {
                    yield batch?;
                }
                drop(merged);
            }
        };
        Ok(instrument_record_batch_stream(
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            info_span!(
                "merge_parquet_execute",
                partition,
                input_count = self.inputs.len(),
                unit_count = self.work_units.len(),
                primary_key_count = self.primary_keys.len(),
                outcome = tracing::field::Empty,
                output_batches = tracing::field::Empty,
                output_rows = tracing::field::Empty,
                error = tracing::field::Empty,
            ),
            "merge_parquet",
        ))
    }
}

/// The files one merge input reads.
struct InputFiles<'a> {
    /// Store-relative path of the input's first file.
    first: &'a str,
    /// How many files the input reads.
    count: usize,
    /// Physical format the input's files are stored in.
    format: PhysicalFormat,
}

/// Describes `input` when it is a plain DataFusion file scan.
///
/// Merge inputs are not always scans — a primary-key locator or an
/// already-distributed stage feeds the merge too — so `None` only means the
/// files are unknown, never that the input is broken.
fn input_files(input: &Arc<dyn ExecutionPlan>) -> Option<InputFiles<'_>> {
    let config = input
        .downcast_ref::<DataSourceExec>()?
        .data_source()
        .downcast_ref::<FileScanConfig>()?;
    let mut files = config.file_groups.iter().flat_map(|group| group.files());
    let first = files.next()?.path().as_ref();
    Some(InputFiles {
        first,
        count: 1 + files.count(),
        format: PhysicalFormat::from_extension(first).ok()?,
    })
}

/// Merge the streams into a single stream.
fn merge_stream(
    streams: Vec<SendableRecordBatchStream>,
    merged_schema: SchemaRef,
    primary_keys: Arc<Vec<String>>,
    default_column_value: Arc<HashMap<String, String>>,
    merge_operators: Arc<HashMap<String, String>>,
    batch_size: usize,
    config: LakeSoulIOConfig,
    reservation: MemoryReservation,
) -> Result<SendableRecordBatchStream> {
    debug!(
        "merge_stream with config= {:?}, {:?}",
        &config, default_column_value
    );
    let merge_on_read = if config.skip_merge_on_read() || config.primary_keys.is_empty() {
        info!("skip merge on read");
        false
    } else {
        !(config.files.len() == 1
            && config.merge_operators.is_empty()
            && config.is_compacted())
    };
    let merge_stream = if !merge_on_read {
        info!("not merge on read use DefaultColumnStream");
        Box::pin(DefaultColumnStream::new_from_streams_with_default(
            streams,
            merged_schema,
            default_column_value,
        ))
    } else {
        info!("use sorted merger");
        let physical_schema = Arc::new(Schema::new(
            merged_schema
                .fields
                .iter()
                .filter_map(|field| {
                    if default_column_value.get(field.name()).is_none() {
                        // phycial reserve
                        Some(field.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
        ));

        // A primary key may contain a constant a data file does not store (a
        // partition column injected on read): the merge key then only resolves
        // when the merge runs on the merged schema, with the constants part of
        // the batches.  The constant is fixed per stream (one partition per
        // read), so it cannot change the merged order.  When the primary keys
        // are all physically stored, the previous physical-schema merge is
        // kept.
        let keys_need_defaults = primary_keys
            .iter()
            .any(|key| default_column_value.contains_key(key));
        let merge_schema = if keys_need_defaults {
            merged_schema.clone()
        } else {
            physical_schema.clone()
        };

        let merge_ops = merge_schema
            .fields()
            .iter()
            .map(|field| {
                MergeOperator::from_name(
                    merge_operators
                        .get(field.name())
                        .unwrap_or(&String::from("UseLast")),
                )
            })
            .collect::<Vec<_>>();

        let streams = streams
            .into_iter()
            .map(|s| {
                let stream: SendableRecordBatchStream = if keys_need_defaults {
                    Box::pin(DefaultColumnStream::new_from_streams_with_default(
                        vec![s],
                        merged_schema.clone(),
                        default_column_value.clone(),
                    ))
                } else {
                    Box::pin(DefaultColumnStream::new_from_stream(
                        s,
                        physical_schema.clone(),
                    ))
                };
                SortedStream::new(stream)
            })
            .collect();
        let is_compacted = config
            .files
            .iter()
            .map(|f| f.contains("/compactdir"))
            .collect::<Vec<_>>();
        info!("is_compacted: {:?}", is_compacted);
        build_sorted_stream_merger(
            streams,
            primary_keys,
            merge_schema,
            merged_schema.clone(),
            batch_size,
            default_column_value.clone(),
            merge_ops,
            reservation,
            is_compacted,
        )?
    };
    Ok(merge_stream)
}

/// Compute the intersection of the dataframe schema and the request schema.
fn schema_intersection(df_schema: DFSchemaRef, request_schema: SchemaRef) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for field in request_schema.fields() {
        if df_schema.field_with_unqualified_name(field.name()).is_ok() {
            exprs.push(Expr::Column(datafusion::common::Column::new_unqualified(
                field.name(),
            )));
        }
    }
    exprs
}

/// Convert the filter string from Java or [`datafusion_substrait::substrait::proto::Plan`] to the [`datafusion::logical_expr::Expr`].
pub async fn convert_filter(
    ctx: &SessionContext,
    df: &DataFrame,
    filter_str: Vec<String>,
    filter_protos: Vec<Plan>,
    filter_bufs: Vec<Vec<u8>>,
) -> Result<Vec<Expr>> {
    let iter = filter_str
        .into_iter()
        .map(FilterContainer::String)
        .chain(filter_protos.into_iter().map(FilterContainer::Plan))
        .chain(filter_bufs.into_iter().map(FilterContainer::RawBuf));

    let mut exprs: Vec<Expr> = Vec::new();

    for container in iter {
        exprs.extend(
            FilterParser::parse_filter_container(ctx, df.schema(), container).await?,
        );
    }
    Ok(exprs)
}

/// Execute the dataframe by colum pruning and row filtering.
pub async fn prune_filter_and_execute(
    df: DataFrame,
    request_schema: SchemaRef,
    filters: Vec<Expr>,
    batch_size: usize,
) -> Result<SendableRecordBatchStream> {
    debug!("filters: {:?}", filters);
    let df_schema = df.schema().clone();
    // find columns requested and prune otherPlans
    let cols = schema_intersection(Arc::new(df_schema.clone()), request_schema.clone());
    debug!("cols: {:?}", cols);
    if cols.is_empty() {
        let stream: SendableRecordBatchStream =
            Box::pin(EmptySchemaStream::new(batch_size, df.count().await?));
        return Ok(stream);
    }
    // row filtering should go first since filter column may not in the selected cols
    let df = filters.into_iter().try_fold(df, |df, f| df.filter(f))?;
    // column pruning
    let df = df.select(cols)?;
    // return a stream
    Ok(df.execute_stream().await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    use crate::config::LakeSoulIOConfigBuilder;

    #[test]
    fn new_with_inputs_marks_missing_requested_fields_nullable() {
        // Simulate schema evolution: the old file was written before `evolved`
        // existed, while the table/requested schema now includes it.
        let merged_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("evolved", DataType::Utf8, false),
        ]));
        let old_file_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let new_file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("evolved", DataType::Utf8, false),
        ]));
        let inputs = vec![
            Arc::new(EmptyExec::new(old_file_schema)) as Arc<dyn ExecutionPlan>,
            Arc::new(EmptyExec::new(new_file_schema)) as Arc<dyn ExecutionPlan>,
        ];

        let exec = MergeParquetExec::new_with_inputs(
            merged_schema,
            inputs,
            LakeSoulIOConfigBuilder::new().build(),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        assert!(
            exec.schema()
                .field_with_name("evolved")
                .unwrap()
                .is_nullable(),
            "fields missing from any input schema must be nullable because default-column projection synthesizes nulls"
        );
        assert!(
            !exec.schema().field_with_name("id").unwrap().is_nullable(),
            "fields present and non-nullable in every input should keep their nullability"
        );
    }
}
