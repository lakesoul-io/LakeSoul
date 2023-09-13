// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::fmt::{self, Debug, Formatter};
use std::any::Any;

use async_trait::async_trait;

use datafusion::common::Statistics;
use datafusion::logical_expr::{Expr, TableType, LogicalPlan};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::datasource::physical_plan::{ParquetExec, FileScanConfig};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionPlan,
    SendableRecordBatchStream
};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::error::Result;


use datafusion::arrow::array::{UInt64Builder, UInt8Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::execution::context::DataFilePaths;
use datafusion::datasource::listing::{ListingTableConfig, ListingTable};
use datafusion::prelude::DataFrame;


/// A User, with an id and a bank account
#[derive(Clone, Debug)]
struct User {
    id: u8,
    bank_account: u64,
}

/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone)]
pub struct CustomDataSource {
    inner: Arc<Mutex<CustomDataSourceInner>>,
    files: Vec<String>,
    plans: Vec<LogicalPlan>,
}


struct CustomDataSourceInner {
    data: HashMap<u8, User>,
    bank_account_index: BTreeMap<u64, u8>,
}

impl Debug for CustomDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("custom_db")
    }
}

impl CustomDataSource {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("{:?}", self.files);
        Ok(Arc::new(CustomExec::new(projections, schema, inputs, self.clone())))
    }

    pub(crate) fn populate_users(&self) {
        self.add_user(User {
            id: 1,
            bank_account: 9_000,
        });
        self.add_user(User {
            id: 2,
            bank_account: 100,
        });
        self.add_user(User {
            id: 3,
            bank_account: 1_000,
        });
    }

    fn add_user(&self, user: User) {
        let mut inner = self.inner.lock().unwrap();
        inner.bank_account_index.insert(user.bank_account, user.id);
        inner.data.insert(user.id, user);
    }

    fn add_file(&mut self, path: String) {
        self.files.push(path);
    }

    fn add_plan(&mut self, plan: LogicalPlan) {
        self.plans.push(plan);
    }
}

impl Default for CustomDataSource {
    fn default() -> Self {
        CustomDataSource {
            inner: Arc::new(Mutex::new(CustomDataSourceInner {
                data: Default::default(),
                bank_account_index: Default::default()
            })),
            files: vec![],
            plans: vec![],
        }
    }
}

#[async_trait]
impl TableProvider for CustomDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("id", DataType::UInt8, false),
            Field::new("bank_account", DataType::UInt64, true),
        ]))
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
        println!("{:?}", self.files);
        println!("{:?}", self.plans);
        self.create_physical_plan(projection,  self.schema(), vec![DataFrame::new(_state.clone(), self.plans[0].clone()).create_physical_plan().await.unwrap()]).await
        
    }
}

#[derive(Debug, Clone)]
struct CustomExec {
    db: CustomDataSource,
    projected_schema: SchemaRef,
    inputs: Vec<Arc<dyn ExecutionPlan>>
}

impl CustomExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        db: CustomDataSource,
    ) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        Self {
            db,
            projected_schema,
            inputs,
        }
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "CustomExec")
    }
}

impl ExecutionPlan for CustomExec {
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
        let mut input = self.inputs[0].execute(_partition, _context.clone())?;

        Ok(input)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::*;
    use datafusion::logical_expr::Expr;
    use datafusion::physical_plan::{
        project_schema, DisplayAs, DisplayFormatType, ExecutionPlan,
        SendableRecordBatchStream, Statistics,
    };

    
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_lakesoul_parquet_source() -> Result<()> {
        // create our custom datasource and adding some users
        let mut db = CustomDataSource::default();
        db.populate_users();
        db.add_file("".to_owned());
    
        query(db.clone(), None, 1).await?;
        // query(db.clone(), Some(col("bank_account").gt(lit(8000u64))), 1).await?;
        // query(db.clone(), Some(col("bank_account").gt(lit(200u64))), 2).await?;
    
        Ok(())
    }

    async fn test_lakesoul_parquet_source_with_pk() -> Result<()> {
        // create our custom datasource and adding some users
        let mut db = CustomDataSource::default();
        db.populate_users();
        db.add_file("".to_owned());
    
        query(db.clone(), None, 1).await?;
        // query(db.clone(), Some(col("bank_account").gt(lit(8000u64))), 1).await?;
        // query(db.clone(), Some(col("bank_account").gt(lit(200u64))), 2).await?;
    
        Ok(())
    }

    async fn test_lakesoul_parquet_source_without_pk() -> Result<()> {
        // create our custom datasource and adding some users
        let mut db = CustomDataSource::default();
        db.populate_users();
        db.add_file("".to_owned());
    
        query(db.clone(), None, 1).await?;
        // query(db.clone(), Some(col("bank_account").gt(lit(8000u64))), 1).await?;
        // query(db.clone(), Some(col("bank_account").gt(lit(200u64))), 2).await?;
    
        Ok(())
    }


    async fn query(
        mut db: CustomDataSource,
        filter: Option<Expr>,
        expected_result_length: usize,
    ) -> Result<()> {
        // create local execution context
        let ctx = SessionContext::new();
        let plan = ctx.read_parquet("/Users/ceng/Desktop/test/range=20201101/2.parquet", Default::default()).await.unwrap().into_unoptimized_plan();
        db.add_plan(plan);
    
        // create logical plan composed of a single TableScan
        let logical_plan = LogicalPlanBuilder::scan_with_filters(
            "name",
            provider_as_source(Arc::new(db)),
            None,
            vec![],
        )?
        .build()?;
    
        let mut dataframe = DataFrame::new(ctx.state(), logical_plan)
            .select_columns(&["id", "bank_account"])?;
    
        if let Some(f) = filter {
            dataframe = dataframe.filter(f)?;
        }
    
        timeout(Duration::from_secs(10), async move {
            let result = dataframe.collect().await.unwrap();
            let record_batch = result.get(0).unwrap();
    
            assert_eq!(expected_result_length, record_batch.column(1).len());
            dbg!(record_batch.columns());
        })
        .await
        .unwrap();
    
        Ok(())
    }
    
}