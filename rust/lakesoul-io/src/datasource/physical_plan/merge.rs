// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::{any::Any, collections::HashMap};

use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::Expr;
use datafusion::{
    datasource::physical_plan::{FileScanConfig, ParquetExec},
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, SendableRecordBatchStream},
};
use datafusion_common::{DFSchemaRef, DataFusionError, Result};

use crate::filter::parser::Parser as FilterParser;
use crate::default_column_stream::empty_schema_stream::EmptySchemaStream;
use crate::default_column_stream::DefaultColumnStream;
use crate::lakesoul_io_config::LakeSoulIOConfig;
use crate::sorted_merge::merge_operator::MergeOperator;
use crate::sorted_merge::sorted_stream_merger::{SortedStream, SortedStreamMerger};

#[derive(Debug)]
pub struct MergeParquetExec {
    schema: SchemaRef,
    primary_keys: Arc<Vec<String>>,
    default_column_value: Arc<HashMap<String, String>>,
    merge_operators: Arc<HashMap<String, String>>,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
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
            let single_exec = Arc::new(ParquetExec::new(config, predicate.clone(), metadata_size_hint));
            inputs.push(single_exec);
        }
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

        let primary_keys = Arc::new(io_config.primary_keys);
        let default_column_value = Arc::new(io_config.default_column_value);
        let merge_operators: Arc<HashMap<String, String>> = Arc::new(io_config.merge_operators);

        Ok(Self {
            schema,
            inputs,
            primary_keys,
            default_column_value,
            merge_operators,
        })
    }

    pub fn new_with_inputs(
        schema: SchemaRef,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        io_config: LakeSoulIOConfig,
        default_column_value: Arc<HashMap<String, String>>,
    ) -> Result<Self> {

        let primary_keys = Arc::new(io_config.primary_keys);
        let merge_operators = Arc::new(io_config.merge_operators);

        Ok(Self {
            schema,
            inputs,
            primary_keys,
            default_column_value,
            merge_operators,
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

impl ExecutionPlan for MergeParquetExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inputs.clone()
    }

    fn with_new_children(self: Arc<Self>, inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            schema: self.schema(),
            inputs,
            primary_keys: self.primary_keys(),
            default_column_value: self.default_column_value(),
            merge_operators: self.merge_operators(),
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
        )?;

        Ok(merged_stream)
    }
}

pub fn merge_stream(
    streams: Vec<SendableRecordBatchStream>,
    schema: SchemaRef,
    primary_keys: Arc<Vec<String>>,
    default_column_value: Arc<HashMap<String, String>>,
    merge_operators: Arc<HashMap<String, String>>,
    batch_size: usize,
) -> Result<SendableRecordBatchStream> {
    let merge_stream = if primary_keys.is_empty() {
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

fn schema_intersection(df_schema: DFSchemaRef, request_schema: SchemaRef) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for field in request_schema.fields() {
        if df_schema.field_with_unqualified_name(field.name()).is_ok() {
            exprs.push(Expr::Column(datafusion::common::Column::new_unqualified(field.name())));
        }
    }
    exprs
}

pub async fn prune_filter_and_execute(
    df: DataFrame,
    request_schema: SchemaRef,
    filter_str: Vec<String>,
    batch_size: usize,
) -> Result<SendableRecordBatchStream> {
    let df_schema = df.schema().clone();
    // find columns requested and prune others
    let cols = schema_intersection(Arc::new(df_schema.clone()), request_schema.clone());
    if cols.is_empty() {
        Ok(Box::pin(EmptySchemaStream::new(batch_size, df.count().await?)))
    } else {
        // row filtering should go first since filter column may not in the selected cols
        let arrow_schema = Arc::new(Schema::from(df_schema));
        let df = filter_str.iter().try_fold(df, |df, f| {
            let filter = FilterParser::parse(f.clone(), arrow_schema.clone())?;
            df.filter(filter)
        })?;
        // column pruning
        let df = df.select(cols)?;
        // return a stream
        df.execute_stream().await
    }
}
