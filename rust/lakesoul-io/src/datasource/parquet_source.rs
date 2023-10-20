// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;

use datafusion::common::{DFSchemaRef, Statistics, ToDFSchema};
use datafusion::datasource::{provider_as_source, TableProvider};
use datafusion::error::Result;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, Expr::Column, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use datafusion::prelude::{DataFrame, SessionContext};

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};

use crate::default_column_stream::empty_schema_stream::EmptySchemaStream;
use crate::default_column_stream::DefaultColumnStream;
use crate::filter::parser::Parser as FilterParser;
use crate::lakesoul_io_config::LakeSoulIOConfig;
use crate::projection::ProjectionStream;
use crate::sorted_merge::merge_operator::MergeOperator;
use crate::sorted_merge::sorted_stream_merger::{SortedStream, SortedStreamMerger};
use crate::transform::uniform_schema;

#[derive(Clone, Debug)]
pub struct EmptySchemaProvider {
    count: usize,
    empty_schema: SchemaRef,
}

impl EmptySchemaProvider {
    pub fn new(count: usize) -> Self {
        EmptySchemaProvider {
            count,
            empty_schema: SchemaRef::new(Schema::new(Vec::<Field>::new())),
        }
    }
}

#[async_trait]
impl TableProvider for EmptySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.empty_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projections: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptySchemaScanExec {
            count: self.count,
            empty_schema: self.empty_schema.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct EmptySchemaScanExec {
    count: usize,
    empty_schema: SchemaRef,
}

impl ExecutionPlan for EmptySchemaScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.empty_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(EmptySchemaStream::new(
            _context.session_config().batch_size(),
            self.count,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[derive(Clone, Debug)]
pub struct LakeSoulParquetProvider {
    config: LakeSoulIOConfig,
    plans: Vec<LogicalPlan>,
    full_schema: SchemaRef,
}

impl LakeSoulParquetProvider {
    pub fn from_config(config: LakeSoulIOConfig) -> Self {
        Self {
            config,
            plans: vec![],
            full_schema: SchemaRef::new(Schema::empty()),
        }
    }

    pub(crate) async fn build_with_context(&self, context: &SessionContext) -> Result<Self> {
        let mut plans = vec![];
        let mut full_schema = uniform_schema(self.config.schema.0.clone()).to_dfschema().unwrap();
        for i in 0..self.config.files.len() {
            let file = self.config.files[i].clone();
            let df = context.read_parquet(file, Default::default()).await.unwrap();
            full_schema.merge(&Schema::try_from(df.schema()).unwrap().to_dfschema().unwrap());
            let plan = df.into_unoptimized_plan();
            plans.push(plan);
        }
        Ok(Self {
            config: self.config.clone(),
            plans,
            full_schema: SchemaRef::new(Schema::try_from(full_schema).unwrap()),
        })
    }

    pub(crate) fn get_full_schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        full_schema: SchemaRef,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LakeSoulParquetScanExec::new(
            projections,
            full_schema,
            inputs,
            Arc::new(self.config.default_column_value.clone()),
            Arc::new(self.config.merge_operators.clone()),
            Arc::new(self.config.primary_keys.clone()),
        )))
    }
}

#[async_trait]
impl TableProvider for LakeSoulParquetProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        if self.config.primary_keys.is_empty() {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        } else {
            filters
                .iter()
                .map(|f| {
                    if let Ok(cols) = f.to_columns() {
                        if cols.iter().all(|col| self.config.primary_keys.contains(&col.name)) {
                            Ok(TableProviderFilterPushDown::Inexact)
                        } else {
                            Ok(TableProviderFilterPushDown::Unsupported)
                        }
                    } else {
                        Ok(TableProviderFilterPushDown::Unsupported)
                    }
                })
                .collect()
        }
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projections: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.get_full_schema(), projections).unwrap();
        let mut inputs = vec![];
        for i in 0..self.plans.len() {
            let df = DataFrame::new(_state.clone(), self.plans[i].clone());
            let df = _filters
                .iter()
                .fold(df, |df, f| df.clone().filter(f.clone()).unwrap_or(df));
            let df_schema = Arc::new(df.schema().clone());
            let projected_cols = schema_intersection(df_schema, projected_schema.clone(), &self.config.primary_keys);
            let df = if projected_cols.is_empty() {
                let plan = LogicalPlanBuilder::scan(
                    "empty",
                    provider_as_source(Arc::new(EmptySchemaProvider::new(df.count().await?))),
                    None,
                )?
                .build()?;
                DataFrame::new(_state.clone(), plan)
            } else {
                df.select(projected_cols)?
            };

            let phycical_plan = df.create_physical_plan().await.unwrap();
            inputs.push(phycical_plan);
        }

        let full_schema = SchemaRef::new(Schema::new(
            self.get_full_schema()
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

        self.create_physical_plan(projections, full_schema, inputs).await
    }
}

#[derive(Debug, Clone)]
struct LakeSoulParquetScanExec {
    projections: Vec<usize>,
    origin_schema: SchemaRef,
    target_schema_with_pks: SchemaRef,
    target_schema: SchemaRef,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    default_column_value: Arc<HashMap<String, String>>,
    merge_operators: Arc<HashMap<String, String>>,
    primary_keys: Arc<Vec<String>>,
}

impl LakeSoulParquetScanExec {
    fn new(
        projections: Option<&Vec<usize>>,
        full_schema: SchemaRef,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        default_column_value: Arc<HashMap<String, String>>,
        merge_operators: Arc<HashMap<String, String>>,
        primary_keys: Arc<Vec<String>>,
    ) -> Self {
         let target_schema_with_pks = if let Some(proj) = projections {
            let mut proj_with_pks = proj.clone();
            for idx in 0..primary_keys.len() {
                let field_idx = full_schema.index_of(primary_keys[idx].as_str()).unwrap();
                if !projections.unwrap().contains(&field_idx) {
                    proj_with_pks.push(field_idx);
                }
            }
            project_schema(&full_schema, Some(&proj_with_pks)).unwrap()
        } else {
            full_schema.clone()
        };
        
        Self {
            projections: projections.unwrap().clone(),
            origin_schema: full_schema.clone(),
            target_schema_with_pks,
            target_schema: project_schema(&full_schema, projections).unwrap(),
            inputs,
            default_column_value,
            merge_operators,
            primary_keys,
        }
    }

    fn origin_schema(&self) -> SchemaRef {
        self.origin_schema.clone()
    }
}

impl DisplayAs for LakeSoulParquetScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "LakeSoulParquetScanExec")
    }
}

impl ExecutionPlan for LakeSoulParquetScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
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

    fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let mut stream_init_futs = Vec::with_capacity(self.inputs.len());
        for i in 0..self.inputs.len() {
            let plan = self.inputs.get(i).unwrap();
            let stream = plan.execute(_partition, _context.clone()).unwrap();
            stream_init_futs.push(stream);
        }
        let merged_stream = merge_stream(
            stream_init_futs,
            self.target_schema_with_pks.clone(),
            self.primary_keys.clone(),
            self.default_column_value.clone(),
            self.merge_operators.clone(),
            _context.session_config().batch_size(),
        )?;

        let result = ProjectionStream {
            expr: self
                .projections
                .iter()
                .map(|&idx| {
                    datafusion::physical_expr::expressions::col(
                        self.origin_schema().field(idx).name(),
                        &self.schema(),
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>(),
            schema: self.target_schema.clone(),
            input: merged_stream,
        };

        Ok(Box::pin(result))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
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
        let merge_schema: SchemaRef = Arc::new(Schema::new(
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
        )); //merge_schema
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
            primary_keys.iter().map(String::clone).collect(),
            batch_size,
            merge_ops,
        )
        .unwrap();
        Box::pin(DefaultColumnStream::new_from_streams_with_default(
            vec![Box::pin(merge_stream)],
            schema,
            default_column_value,
        ))
    };
    Ok(merge_stream)
}

fn schema_intersection(df_schema: DFSchemaRef, schema: SchemaRef, primary_keys: &[String]) -> Vec<Expr> {
    df_schema
        .fields()
        .iter()
        .filter_map(|df_field| match schema.field_with_name(df_field.name()) {
                // datafusion's select is case sensitive, but col will transform field name to lower case
                // so we use Column::new_unqualified instead
                Ok(_) => Some(Column(datafusion::common::Column::new_unqualified(df_field.name()))),
                _ if primary_keys.contains(df_field.name()) => Some(Column(datafusion::common::Column::new_unqualified(df_field.name()))),
                _ => None
        })
        .collect()
}

pub async fn prune_filter_and_execute(
    df: DataFrame,
    request_schema: SchemaRef,
    filter_str: Vec<String>,
    batch_size: usize,
) -> Result<SendableRecordBatchStream> {
    let df_schema = df.schema().clone();
    // find columns requested and prune others
    let cols = schema_intersection(Arc::new(df_schema.clone()), request_schema.clone(), &[]);
    if cols.is_empty() {
        Ok(Box::pin(EmptySchemaStream::new(batch_size, df.count().await?)))
    } else {
        // row filtering should go first since filter column may not in the selected cols
        let arrow_schema = Arc::new(Schema::from(df_schema));
        let df = filter_str.iter().try_fold(df, |df, f| {
            let filter = FilterParser::parse(f.clone(), arrow_schema.clone());
            df.filter(filter)
        })?;
        // column pruning
        let df = df.select(cols)?;
        df.execute_stream().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use arrow::util::pretty::print_batches;

    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::*;
    use datafusion::scalar::ScalarValue;

    use crate::filter::parser::Parser;
    use crate::lakesoul_io_config::LakeSoulIOConfigBuilder;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_lakesoul_parquet_source_with_pk() -> Result<()> {
        // create our custom datasource and adding some users
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("hash", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
            Field::new("name", DataType::Int32, true),
            Field::new("range", DataType::Int32, true),
        ]));
        let builder = LakeSoulIOConfigBuilder::new()
            .with_file("/Users/ceng/Desktop/test/range=20201101/2.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-before.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-after.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/4.parquet".to_owned())
            .with_schema(schema.clone())
            .with_default_column_value("range".to_string(), "20201101".to_string())
            .with_primary_keys(vec!["hash".to_string()]);

        query(
            LakeSoulParquetProvider::from_config(builder.build()),
            Some(Parser::parse("gt(value,0)".to_string(), schema.clone())),
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_lakesoul_parquet_source_exclude_pk() -> Result<()> {
        let schema = SchemaRef::new(Schema::new(vec![
            // Field::new("hash", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
            Field::new("name", DataType::Int32, true),
            Field::new("range", DataType::Int32, true),
        ]));
        let builder = LakeSoulIOConfigBuilder::new()
            .with_file("/Users/ceng/Desktop/test/range=20201101/2.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-before.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-after.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/4.parquet".to_owned())
            .with_schema(schema.clone())
            .with_default_column_value("range".to_string(), "20201101".to_string())
            .with_primary_keys(vec!["hash".to_string()]);

        query(
            LakeSoulParquetProvider::from_config(builder.build()),
            Some(Parser::parse("gt(hash,0)".to_string(), schema.clone())),
        )
        .await?;

        Ok(())
    }

    async fn query(db: LakeSoulParquetProvider, filter: Option<Expr>) -> Result<()> {
        // create local execution context
        let config = SessionConfig::default();
        let ctx = SessionContext::with_config(config);

        let db = db.build_with_context(&ctx).await.unwrap();

        // create logical plan composed of a single TableScan
        let logical_plan =
            LogicalPlanBuilder::scan_with_filters("name", provider_as_source(Arc::new(db)), None, vec![])?.build()?;

        let mut dataframe = DataFrame::new(ctx.state(), logical_plan);

        if let Some(f) = filter {
            dataframe = dataframe.filter(f)?;
        }
        dataframe = dataframe.select_columns(&["hash", "value", "name", "range"])?;
        dataframe = dataframe.explain(true, false)?;

        timeout(Duration::from_secs(10), async move {
            let result = dataframe.collect().await.unwrap();
            // let record_batch = result.get(0).unwrap();

            // assert_eq!(expected_result_length, record_batch.column(1).len());
            let _ = print_batches(&result);
        })
        .await
        .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_lakesoul_parquet_source_by_select_from_sql_and_filter_api() -> Result<()> {
        let ctx = SessionContext::new();
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("hash", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
            Field::new("name", DataType::Int32, true),
            Field::new("range", DataType::Int32, true),
        ]));
        let builder = LakeSoulIOConfigBuilder::new()
            .with_file("/Users/ceng/Desktop/test/range=20201101/2.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-before.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-after.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/4.parquet".to_owned())
            .with_schema(schema.clone())
            .with_default_column_value("range".to_string(), "20201101".to_string())
            .with_primary_keys(vec!["hash".to_string()]);

        let provider = LakeSoulParquetProvider::from_config(builder.build())
            .build_with_context(&ctx)
            .await?;
        ctx.register_table("lakesoul", Arc::new(provider))?;

        let results = ctx
            .sql("SELECT * FROM lakesoul")
            .await?
            .filter(col("value").gt(datafusion::prelude::Expr::Literal(ScalarValue::Int32(Some(1)))))?
            .select(vec![col("hash")])?
            .explain(true, false)?
            .collect()
            .await?;

        let _ = print_batches(&results);

        Ok(())
    }

    #[tokio::test]
    async fn test_lakesoul_parquet_source_by_select_from_where_sql() -> Result<()> {
        let ctx = SessionContext::new();
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("hash", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
            Field::new("name", DataType::Int32, true),
            Field::new("range", DataType::Int32, true),
        ]));
        let builder = LakeSoulIOConfigBuilder::new()
            .with_file("/Users/ceng/Desktop/test/range=20201101/2.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-before.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-after.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/4.parquet".to_owned())
            .with_schema(schema.clone())
            .with_default_column_value("range".to_string(), "20201101".to_string())
            .with_primary_keys(vec!["hash".to_string()]);

        let provider = LakeSoulParquetProvider::from_config(builder.build())
            .build_with_context(&ctx)
            .await?;
        ctx.register_table("lakesoul", Arc::new(provider))?;

        let results = ctx
            .sql("SELECT hash FROM lakesoul where value > 1")
            .await?
            .explain(true, false)?
            .collect()
            .await?;

        let _ = print_batches(&results);

        Ok(())
    }
}
