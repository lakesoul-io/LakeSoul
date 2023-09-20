// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::fmt::{self, Debug};
use std::any::Any;

use async_trait::async_trait;

use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::common::{Statistics, ToDFSchema};
use datafusion::logical_expr::{Expr, TableType, LogicalPlan, Expr::Column};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionPlan,
    SendableRecordBatchStream
};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::error::Result;


use datafusion::arrow::datatypes::{Schema, SchemaRef};

use crate::default_column_stream::DefaultColumnStream;
use crate::lakesoul_io_config::LakeSoulIOConfig;
use crate::sorted_merge::sorted_stream_merger::{SortedStreamMerger, SortedStream};
use crate::sorted_merge::merge_operator::MergeOperator;
use crate::projection::ProjectionStream;
use crate::default_column_stream::empty_schema_stream::EmptySchemaStream;
use crate::filter::parser::Parser as FilterParser;


/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone, Debug)]
pub struct LakeSoulParquetSource {
    config: LakeSoulIOConfig,
    plans: Vec<LogicalPlan>,
    full_schema: SchemaRef,
}


impl LakeSoulParquetSource {
    pub fn from_config(config: LakeSoulIOConfig) -> Self{
        Self { 
            config, 
            plans: vec![],
            full_schema: SchemaRef::new(Schema::empty())
        }
    }

    pub(crate) async fn build_with_context(&self, context:&SessionContext) -> Result<Self>{
        let mut plans = vec![];
        let mut full_schema = self.config.schema.0.clone().to_dfschema().unwrap();
        for i in 0..self.config.files.len() {
            let file = self.config.files[i].clone();
            let df = context.read_parquet(file, Default::default()).await.unwrap();
            println!("file schema: {:?}", df.schema());
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
        schema: SchemaRef,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LakeSoulParquetScanExec::new(
            projections, 
            schema, 
            inputs, 
            Arc::new(self.config.default_column_value.clone()),
            Arc::new(self.config.merge_operators.clone()),
            Arc::new(self.config.primary_keys.clone()), 
            self.config.batch_size)))
    }

}

#[async_trait]
impl TableProvider for LakeSoulParquetSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("fn scan _filters={:?}", _filters);
        let mut inputs = vec![];
        for i in 0..self.plans.len() {
            let df = DataFrame::new(_state.clone(), self.plans[i].clone());
            let phycical_plan = df.create_physical_plan().await.unwrap();
            inputs.push(phycical_plan);
        }
        self.create_physical_plan(
            projection,  
            self.get_full_schema(), 
            inputs).await
        
    }
}

#[derive(Debug, Clone)]
struct LakeSoulParquetScanExec {
    projections: Vec<usize>,
    projected_schema: SchemaRef,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    default_column_value: Arc<HashMap<String, String>>,
    merge_operators: Arc<HashMap<String, String>>,
    primary_keys: Arc<Vec<String>>,
    batch_size: usize
}

impl LakeSoulParquetScanExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        default_column_value: Arc<HashMap<String, String>>,
        merge_operators: Arc<HashMap<String, String>>,
        primary_keys: Arc<Vec<String>>,
        batch_size: usize,
    ) -> Self {
        Self {
            projections: projections.unwrap().clone(),
            projected_schema: project_schema(&schema.clone(), projections).unwrap(),
            inputs,
            default_column_value,
            merge_operators,
            primary_keys,
            batch_size
        }
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
        self.projected_schema.clone()
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

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut stream_init_futs = Vec::with_capacity(self.inputs.len());
        for i in 0..self.inputs.len() {
            let plan = self.inputs.get(i).unwrap();
            let stream = plan.execute(_partition, _context.clone()).unwrap();
            stream_init_futs.push(stream);
        }
        let merged_stream = merge_stream(
            stream_init_futs, self.schema(),
            self.primary_keys.clone(), 
            self.default_column_value.clone(), 
            self.merge_operators.clone(), 
            self.batch_size)?;

        let result = ProjectionStream {
            expr: self.projections.iter().map(|&idx| datafusion::physical_expr::expressions::col(self.schema().field(idx).name(), &self.schema().clone()).unwrap()).collect::<Vec<_>>(),
            schema: self.projected_schema.clone(),
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
            schema.clone(),
            default_column_value.clone(),
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
                MergeOperator::from_name(
                    merge_operators.get(field.name()).unwrap_or(&String::from("UseLast")),
                )
            })
            .collect::<Vec<_>>();
        

        let streams = streams
                .into_iter()
                .map(|s| SortedStream::new(Box::pin(DefaultColumnStream::new_from_stream(s, merge_schema.clone()))))
                .collect();
        let merge_stream = SortedStreamMerger::new_from_streams(
            streams,
            merge_schema.clone(),
            primary_keys.iter().map(String::clone).collect(),
            batch_size,
            merge_ops,
        ).unwrap();
        Box::pin(DefaultColumnStream::new_from_streams_with_default(
            vec![Box::pin(merge_stream)],
            schema.clone(),
            default_column_value.clone(),
        ))
    };
    Ok(merge_stream)
}

pub async fn prune_filter_and_execute(
    df: DataFrame,
    request_schema: SchemaRef,
    filter_str: Vec<String>,
    batch_size: usize,
) -> Result<SendableRecordBatchStream> {
    
    let file_schema = df.schema().clone();
    // find columns requested and prune others
    let cols = request_schema
        .fields()
        .iter()
        .filter_map(|field| match file_schema.field_with_name(None, field.name()) {
            // datafusion's select is case sensitive, but col will transform field name to lower case
            // so we use Column::new_unqualified instead
            Ok(file_field) => Some(Column(datafusion::common::Column::new_unqualified(file_field.name()))),
            _ => None,
        })
        .collect::<Vec<_>>();
    if cols.is_empty() {
        Ok(Box::pin(EmptySchemaStream::new(batch_size, df.count().await?)))
    } else {
        // row filtering should go first since filter column may not in the selected cols
        let arrow_schema = Arc::new(Schema::from(file_schema));
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

    use arrow::util::pretty::print_batches;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::*;
    use datafusion::logical_expr::Expr;
    use datafusion::scalar::ScalarValue;

    use datafusion::physical_plan::{
        project_schema, DisplayAs, DisplayFormatType, ExecutionPlan,
        SendableRecordBatchStream, Statistics,
    };

    
    use std::time::Duration;
    use tokio::time::timeout;
    use crate::lakesoul_io_config::{LakeSoulIOConfigBuilder, LakeSoulIOConfig};
    use crate::filter::parser::Parser;


    #[tokio::test]
    async fn test_lakesoul_parquet_source_with_pk() -> Result<()> {
        // create our custom datasource and adding some users
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("hash", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
            Field::new("name", DataType::Int32, true),
            Field::new("range", DataType::Int32, true)]));
        let builder = LakeSoulIOConfigBuilder::new()
            .with_file("/Users/ceng/Desktop/test/range=20201101/2.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-before.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-after.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/4.parquet".to_owned())
            .with_schema(schema.clone())
            .with_default_column_value("range".to_string(), "20201101".to_string())
            .with_primary_keys(vec!["hash".to_string()]);
        
        
    
        query(LakeSoulParquetSource::from_config(builder.build()), Some(Parser::parse("gt(value,0)".to_string(),schema.clone())), 1).await?;
    
        Ok(())
    }

    #[tokio::test]
    async fn test_lakesoul_parquet_source_exclude_pk() -> Result<()> {
        let schema = SchemaRef::new(Schema::new(vec![
            // Field::new("hash", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
            Field::new("name", DataType::Int32, true),
            Field::new("range", DataType::Int32, true)]));
        let builder = LakeSoulIOConfigBuilder::new()
            .with_file("/Users/ceng/Desktop/test/range=20201101/2.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-before.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-after.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/4.parquet".to_owned())
            .with_schema(schema.clone())
            .with_default_column_value("range".to_string(), "20201101".to_string())
            .with_primary_keys(vec!["hash".to_string()]);
        
    
        query(LakeSoulParquetSource::from_config(builder.build()), Some(Parser::parse("gt(hash,0)".to_string(),schema.clone())), 1).await?;
    
        Ok(())
    }


    async fn query(
        db: LakeSoulParquetSource,
        filter: Option<Expr>,
        expected_result_length: usize,
    ) -> Result<()> {
        // create local execution context
        let config = SessionConfig::default();
        let ctx = SessionContext::with_config(config);
        
        let db = db.build_with_context(&ctx).await.unwrap();
    
        // create logical plan composed of a single TableScan
        let logical_plan = LogicalPlanBuilder::scan_with_filters(
            "name",
            provider_as_source(Arc::new(db)),
            None,
            vec![],
        )?
        .build()?;
    
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
        let mut ctx = SessionContext::new();
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("hash", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
            Field::new("name", DataType::Int32, true),
            Field::new("range", DataType::Int32, true)]));
        let builder = LakeSoulIOConfigBuilder::new()
            .with_file("/Users/ceng/Desktop/test/range=20201101/2.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-before.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-after.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/4.parquet".to_owned())
            .with_schema(schema.clone())
            .with_default_column_value("range".to_string(), "20201101".to_string())
            .with_primary_keys(vec!["hash".to_string()]);

        let provider =  LakeSoulParquetSource::from_config(builder.build()).build_with_context(&ctx).await?;
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
        let mut ctx = SessionContext::new();
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("hash", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
            Field::new("name", DataType::Int32, true),
            Field::new("range", DataType::Int32, true)]));
        let builder = LakeSoulIOConfigBuilder::new()
            .with_file("/Users/ceng/Desktop/test/range=20201101/2.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-before.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/1-3-after.parquet".to_owned())
            .with_file("/Users/ceng/Desktop/test/range=20201101/4.parquet".to_owned())
            .with_schema(schema.clone())
            .with_default_column_value("range".to_string(), "20201101".to_string())
            .with_primary_keys(vec!["hash".to_string()]);

        let provider =  LakeSoulParquetSource::from_config(builder.build()).build_with_context(&ctx).await?;
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