// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the merge on read execution plan.

use std::sync::Arc;
use std::{any::Any, collections::HashMap};

use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::dataframe::DataFrame;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlanProperties, Partitioning, PlanProperties};
use datafusion::prelude::SessionContext;
use datafusion::{
    datasource::physical_plan::FileScanConfig,
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
    },
};
use datafusion_common::{DFSchemaRef, DataFusionError, Result as DFResult};
use datafusion_substrait::substrait::proto::Plan;
use rootcause::compat::boxed_error::IntoBoxedError;

use self::sorted::merge_operator::MergeOperator;
use self::sorted::sorted_stream_merger::{SortedStream, build_sorted_stream_merger};
use crate::Result;
use crate::config::LakeSoulIOConfig;
use crate::filter::parser::{FilterContainer, Parser as FilterParser};
use crate::stream::default_column::DefaultColumnStream;
use crate::stream::empty_schema::EmptySchemaStream;

pub mod sorted;

/// [`ExecutionPlan`] implementation for the merge on read operation.
#[derive(Debug)]
pub struct MergeParquetExec {
    /// The schema of the merge on read operation.
    merged_schema: SchemaRef,
    /// The primary keys of the merge on read operation, which is the sorted columns.
    primary_keys: Arc<Vec<String>>,
    /// The default column value of the merge on read operation, which is the default value for the partition columns.
    default_column_value: Arc<HashMap<String, String>>,
    /// The merge operators of the merge on read operation.
    merge_operators: Arc<HashMap<String, String>>,
    /// The input execution plans of the merge on read operation.
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// The io config of the merge on read operation.
    io_config: LakeSoulIOConfig, // try use arc
    /// The properties of the merge on read operation.
    properties: PlanProperties,
}

impl MergeParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    pub fn new(
        merged_schema: SchemaRef,
        flatten_configs: Vec<FileScanConfig>,
        io_config: LakeSoulIOConfig,
    ) -> Result<Self> {
        // source file parquet scan
        let mut inputs = Vec::<Arc<dyn ExecutionPlan>>::new();
        for (i, config) in flatten_configs.into_iter().enumerate() {
            debug!("flatten_configs[{i}]:{:?}", config);
            let single_exec = DataSourceExec::from_data_source(config);
            inputs.push(single_exec);
        }
        // Compute Nullability
        // O(nml), n = number of schema fields, m = number of file schema fields, l = number of files
        let merged_schema = SchemaRef::new(Schema::new(
            merged_schema
                .fields()
                .iter()
                .map(|field| {
                    Field::new(
                        field.name(),
                        field.data_type().clone(),
                        field.is_nullable()
                            | inputs.iter().any(|plan| {
                                // see all inputs
                                if let Some((_, plan_field)) =
                                    plan.schema().column_with_name(field.name())
                                {
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
        debug!("default_column_values:{:?}", default_column_value);
        let merge_operators: Arc<HashMap<String, String>> =
            Arc::new(io_config.merge_operators);

        Ok(Self {
            merged_schema: merged_schema.clone(),
            inputs,
            primary_keys,
            default_column_value,
            merge_operators,
            io_config: config,
            properties: PlanProperties::new(
                EquivalenceProperties::new(merged_schema),
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
            merged_schema: schema.clone(),
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
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
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
        self.merged_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            merged_schema: self.schema(),
            inputs,
            primary_keys: self.primary_keys(),
            default_column_value: self.default_column_value(),
            merge_operators: self.merge_operators(),
            io_config: self.io_config.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "Invalid requested partition {partition}. InsertExec requires a single input partition."
            )));
        }

        let mut stream_init_futs = Vec::with_capacity(self.inputs.len());
        for i in 0..self.inputs.len() {
            debug!("inputs[{i}]: {} -> stream", self.inputs[i].name());
            let input = &self.inputs[i];
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

        let reservation = MemoryConsumer::new(format!("LakeSoulMerge[{partition}]"))
            .register(&context.runtime_env().memory_pool);

        let merged_stream = merge_stream(
            stream_init_futs,
            self.schema(),
            self.primary_keys(),
            self.default_column_value(),
            self.merge_operators(),
            context.session_config().batch_size(),
            self.io_config.clone(),
            reservation,
        )
        .map_err(|e| {
            error!("{e}");
            e.into_boxed_error()
        })?;

        Ok(merged_stream)
    }
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
        // is file schema?
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

        let merge_ops = merged_schema
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
                SortedStream::new(Box::pin(DefaultColumnStream::new_from_stream(
                    s,
                    physical_schema.clone(),
                )))
            })
            .collect();
        build_sorted_stream_merger(
            streams,
            primary_keys,
            physical_schema,
            merged_schema,
            batch_size,
            default_column_value,
            merge_ops,
            reservation,
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
        return Ok(Box::pin(EmptySchemaStream::new(
            batch_size,
            df.count().await?,
        )));
    }
    // row filtering should go first since filter column may not in the selected cols
    let df = filters.into_iter().try_fold(df, |df, f| df.filter(f))?;
    // column pruning
    let df = df.select(cols)?;
    // return a stream
    Ok(df.execute_stream().await?)
}
