// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::{any::Any, collections::HashMap};

use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::{
    datasource::physical_plan::{FileScanConfig, ParquetExec},
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, SendableRecordBatchStream},
};
use datafusion_common::{DataFusionError, Result};

use crate::datasource::parquet_source::merge_stream;
use crate::lakesoul_io_config::LakeSoulIOConfig;

#[derive(Debug)]
pub struct MergeParquetExec {
    schema: SchemaRef,
    primary_keys: Arc<Vec<String>>,
    default_column_value: Arc<HashMap<String, String>>,
    merge_operators: Arc<HashMap<String, String>>,
    config: FileScanConfig,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
}

impl MergeParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    pub fn new(
        schema: SchemaRef,
        config: FileScanConfig,
        flatten_configs: Vec<FileScanConfig>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metadata_size_hint: Option<usize>,
        io_config: LakeSoulIOConfig,
    ) -> Self {
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
        let merge_operators = Arc::new(io_config.merge_operators);

        Self {
            schema,
            inputs,
            config,
            primary_keys,
            default_column_value,
            merge_operators,
        }
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
            config: self.config.clone(),
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
            let input = self.inputs.get(i).unwrap();
            let input_partition_count = input.output_partitioning().partition_count();
            if input_partition_count != 1 {
                return Err(DataFusionError::Internal(format!(
                    "Invalid input partition count {input_partition_count}. \
                                InsertExec needs only a single partition."
                )));
            }
            let stream = input.execute(partition, context.clone()).unwrap();
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
