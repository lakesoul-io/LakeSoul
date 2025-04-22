// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the merge on read execution plan.

use std::sync::Arc;
use std::{any::Any, collections::HashMap};

use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlanProperties, Partitioning, PlanProperties};
use datafusion::{
    datasource::physical_plan::{FileScanConfig, ParquetExec},
    execution::TaskContext,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, SendableRecordBatchStream},
};
use datafusion_common::{DFSchemaRef, DataFusionError, Result};
use datafusion_substrait::substrait::proto::Plan;

use crate::default_column_stream::empty_schema_stream::EmptySchemaStream;
use crate::default_column_stream::DefaultColumnStream;
use crate::filter::parser::Parser as FilterParser;
use crate::lakesoul_io_config::LakeSoulIOConfig;
use crate::sorted_merge::merge_operator::MergeOperator;
use crate::sorted_merge::sorted_stream_merger::{SortedStream, SortedStreamMerger};

/// [`ExecutionPlan`] implementation for the merge on read operation.
#[derive(Debug)]
pub struct MergeParquetExec {
    /// The schema of the merge on read operation.
    schema: SchemaRef,
    /// The primary keys of the merge on read operation, which is the sorted columns.
    primary_keys: Arc<Vec<String>>,
    /// The default column value of the merge on read operation, which is the default value for the partition columns.
    default_column_value: Arc<HashMap<String, String>>,
    /// The merge operators of the merge on read operation.
    merge_operators: Arc<HashMap<String, String>>,
    /// The input execution plans of the merge on read operation.
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// The io config of the merge on read operation.
    io_config: LakeSoulIOConfig,
    /// The properties of the merge on read operation.
    properties: PlanProperties,
}

impl MergeParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    pub fn new(
        schema: SchemaRef,
        flatten_configs: Vec<FileScanConfig>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metadata_size_hint: Option<usize>,
        io_config: LakeSoulIOConfig,
    ) -> Result<Self> {
        // source file parquet scan
        let mut inputs = Vec::<Arc<dyn ExecutionPlan>>::new();
        for config in flatten_configs {
            let single_exec = Arc::new({
                let mut builder = ParquetExec::builder(config);
                if let Some(predicate) = predicate.clone() {
                    builder = builder.with_predicate(predicate.clone());
                }
                if let Some(metadata_size_hint) = metadata_size_hint {
                    builder = builder.with_metadata_size_hint(metadata_size_hint);
                }
                builder.build()
            });
            inputs.push(single_exec);
        }
        // O(nml), n = number of schema fields, m = number of file schema fields, l = number of files
        let schema = SchemaRef::new(Schema::new(
            schema
                .fields()
                .iter()
                .map(|field| {
                    Field::new(
                        field.name(),
                        field.data_type().clone(),
                        field.is_nullable()
                            | inputs.iter().any(|plan| {
                                if let Some((_, plan_field)) = plan.schema().column_with_name(field.name()) {
                                    plan_field.is_nullable()
                                } else {
                                    true
                                }
                            }),
                    )
                })
                .collect::<Vec<_>>(),
        ));

        let config = io_config.clone();
        let primary_keys = Arc::new(io_config.primary_keys);
        let default_column_value = Arc::new(io_config.default_column_value);
        let merge_operators: Arc<HashMap<String, String>> = Arc::new(io_config.merge_operators);

        Ok(Self {
            schema: schema.clone(),
            inputs,
            primary_keys,
            default_column_value,
            merge_operators,
            io_config: config,
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        })
    }

    pub fn new_with_inputs(
        schema: SchemaRef,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        io_config: LakeSoulIOConfig,
        default_column_value: Arc<HashMap<String, String>>,
    ) -> Result<Self> {
        info!(
            "MergeParquetExec::new_with_inputs: {:?}, {:?}, {:?}",
            schema, io_config, default_column_value
        );
        let config = io_config.clone();
        let primary_keys = Arc::new(io_config.primary_keys);
        let merge_operators = Arc::new(io_config.merge_operators);

        Ok(Self {
            schema: schema.clone(),
            inputs,
            primary_keys,
            default_column_value,
            merge_operators,
            io_config: config,
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        })
    }

    pub fn primary_keys(&self) -> Arc<Vec<String>> {
        self.primary_keys.clone()
    }

    pub fn default_column_value(&self) -> Arc<HashMap<String, String>> {
        self.default_column_value.clone()
    }

    pub fn merge_operators(&self) -> Arc<HashMap<String, String>> {
        self.merge_operators.clone()
    }
}

impl DisplayAs for MergeParquetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeParquetExec")
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().map(|p| p).collect()
    }

    fn with_new_children(self: Arc<Self>, inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            schema: self.schema(),
            inputs,
            primary_keys: self.primary_keys(),
            default_column_value: self.default_column_value(),
            merge_operators: self.merge_operators(),
            io_config: self.io_config.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "Invalid requested partition {partition}. InsertExec requires a single input partition."
            )));
        }

        let mut stream_init_futs = Vec::with_capacity(self.inputs.len());
        for i in 0..self.inputs.len() {
            let input = &self.inputs[i];
            // dbg!(&input);
            let input_partition_count = input.output_partitioning().partition_count();
            if input_partition_count != 1 {
                return Err(DataFusionError::Internal(format!(
                    "Invalid input partition count {input_partition_count}. \
                                InsertExec needs only a single partition."
                )));
            }
            let stream = input.execute(partition, context.clone())?;
            stream_init_futs.push(stream);
        }

        let merged_stream = merge_stream(
            stream_init_futs,
            self.schema(),
            self.primary_keys(),
            self.default_column_value(),
            self.merge_operators(),
            context.session_config().batch_size(),
            self.io_config.clone(),
        )?;

        Ok(merged_stream)
    }
}

/// Merge the streams into a single stream.
pub fn merge_stream(
    streams: Vec<SendableRecordBatchStream>,
    schema: SchemaRef,
    primary_keys: Arc<Vec<String>>,
    default_column_value: Arc<HashMap<String, String>>,
    merge_operators: Arc<HashMap<String, String>>,
    batch_size: usize,
    config: LakeSoulIOConfig,
) -> Result<SendableRecordBatchStream> {
    debug!("merge_stream with config= {:?}", &config);
    let merge_on_read = if config.skip_merge_on_read() {
        false
    } else if config.primary_keys.is_empty() {
        false
    } else {
        !(config.files.len() == 1 && config.merge_operators.is_empty() && config.is_compacted())
    };
    let merge_stream = if !merge_on_read {
        Box::pin(DefaultColumnStream::new_from_streams_with_default(
            streams,
            schema,
            default_column_value,
        ))
    } else {
        let merge_schema = Arc::new(Schema::new(
            schema
                .fields
                .iter()
                .filter_map(|field| {
                    if default_column_value.get(field.name()).is_none() {
                        Some(field.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
        )); // merge_schema
        let merge_ops = schema
            .fields()
            .iter()
            .map(|field| {
                MergeOperator::from_name(merge_operators.get(field.name()).unwrap_or(&String::from("UseLast")))
            })
            .collect::<Vec<_>>();

        let streams = streams
            .into_iter()
            .map(|s| SortedStream::new(Box::pin(DefaultColumnStream::new_from_stream(s, merge_schema.clone()))))
            .collect();
        let merge_stream = SortedStreamMerger::new_from_streams(
            streams,
            merge_schema,
            primary_keys.iter().cloned().collect(),
            batch_size,
            merge_ops,
        )?;
        Box::pin(DefaultColumnStream::new_from_streams_with_default(
            vec![Box::pin(merge_stream)],
            schema,
            default_column_value,
        ))
    };
    Ok(merge_stream)
}

/// Compute the intersection of the dataframe schema and the request schema.
fn schema_intersection(df_schema: DFSchemaRef, request_schema: SchemaRef) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for field in request_schema.fields() {
        if df_schema.field_with_unqualified_name(field.name()).is_ok() {
            exprs.push(Expr::Column(datafusion::common::Column::new_unqualified(field.name())));
        }
    }
    exprs
}

/// Convert the filter string from Java or [`datafusion_substrait::substrait::proto::Plan`] to the [`datafusion::logical_expr::Expr`].
pub fn convert_filter(df: &DataFrame, filter_str: Vec<String>, filter_protos: Vec<Plan>) -> Result<Vec<Expr>> {
    let arrow_schema = Arc::new(Schema::from(df.schema()));
    debug!("schema:{:?}", arrow_schema);
    let mut str_filters = vec![];
    for f in &filter_str {
        let filter = FilterParser::parse(f.clone(), arrow_schema.clone())?;
        str_filters.push(filter);
    }
    let proto_filters = filter_protos
        .into_iter()
        .map(|plan| FilterParser::parse_substrait_plan(plan, df.schema()))
        .collect::<Result<Vec<_>>>()?;
    debug!("str filters: {:#?}", str_filters);
    debug!("proto filters: {:#?}", proto_filters);
    if proto_filters.is_empty() {
        Ok(str_filters)
    } else {
        Ok(proto_filters)
    }
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
        return Ok(Box::pin(EmptySchemaStream::new(batch_size, df.count().await?)));
    }
    // row filtering should go first since filter column may not in the selected cols
    let df = filters.into_iter().try_fold(df, |df, f| df.filter(f))?;
    // column pruning
    let df = df.select(cols)?;
    // return a stream
    df.execute_stream().await
}
