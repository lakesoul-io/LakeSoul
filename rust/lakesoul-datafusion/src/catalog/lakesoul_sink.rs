// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0


use std::any::Any;
use std::fmt::{self, Debug};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use lakesoul_io::lakesoul_reader::ArrowError;
use lakesoul_io::{arrow, datafusion};

use arrow::array::{UInt64Array, ArrayRef};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Field, DataType};
use async_trait::async_trait;

use datafusion::common::{Statistics, DataFusionError};

use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, TableType,
};
use datafusion::physical_expr::{PhysicalSortExpr, PhysicalSortRequirement};
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, Distribution, stream::RecordBatchStreamAdapter
};
use datafusion::arrow::datatypes::{Schema, SchemaRef};

use futures::StreamExt;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_io::lakesoul_writer::{MultiPartAsyncWriter, AsyncBatchWriter};

use super::{commit_data, create_io_config_builder};

#[derive(Debug, Clone)]
pub struct LakeSoulSinkProvider {
    table_name: String,
    schema: SchemaRef,
    io_config: LakeSoulIOConfig,
    postgres_config: Option<String>,
}

impl LakeSoulSinkProvider {
    pub async fn new(table_name: &str) -> crate::error::Result<Self> {
        Self::create_provider(table_name, Arc::new(MetaDataClient::from_env().await?), None).await
    }

    pub async fn new_with_postgres_config(table_name: &str, postgres_config: &str) -> crate::error::Result<Self> {
        Self::create_provider(table_name, Arc::new(MetaDataClient::from_config(postgres_config.to_string()).await?), Some(postgres_config.to_string())).await
    }

    pub async fn create_provider(table_name: &str, client: MetaDataClientRef, postgres_config: Option<String>) -> crate::error::Result<Self> {
        let io_config = create_io_config_builder(client, Some(table_name))
            .await?
            .build();
        let schema  = io_config.schema();
        let table_name = table_name.to_string();
        Ok(Self {
            table_name,
            io_config,
            schema,
            postgres_config,
        })
    }
}

#[async_trait]
impl TableProvider for LakeSoulSinkProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        let msg = "Scan not implemented for LakeSoulParquetSinkProvider".to_owned();
        Err(DataFusionError::NotImplemented(msg))
    }


    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let client = Arc::new(
            if let Some(postgres_config) = &self.postgres_config {
                match MetaDataClient::from_config(postgres_config.clone()).await {
                    Ok(client) => client,
                    Err(e) => return Err(DataFusionError::External(Box::new(e)))
                }
            } else {
                match MetaDataClient::from_env().await {
                    Ok(client) => client,
                    Err(e) => return Err(DataFusionError::External(Box::new(e)))
                }
            }
        );
        Ok(Arc::new(
            LakeSoulParquetSinkExec::new(
                input, 
                Arc::new(
                    LakeSoulParquekSink::new(client, self.table_name.clone(), self.io_config.clone().into())
                    )
                )    
        ))
    }

}

#[derive(Debug, Clone)]
struct LakeSoulParquetSinkExec {
        /// Input plan that produces the record batches to be written.
        input: Arc<dyn ExecutionPlan>,
        /// Sink to which to write
        sink: Arc<dyn DataSink>,
        /// Schema describing the structure of the data.
        schema: SchemaRef,
    
}

impl LakeSoulParquetSinkExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        sink: Arc<dyn DataSink>,
    ) -> Self {
        Self {
            input,
            sink,
            schema: Arc::new(Schema::empty())
        }
    }
}

impl DisplayAs for LakeSoulParquetSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "LakeSoulParquetSinkExec")
    }
}

impl ExecutionPlan for LakeSoulParquetSinkExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        // Require that the InsertExec gets the data in the order the
        // input produced it (otherwise the optimizer may chose to reorder
        // the input which could result in unintended / poor UX)
        //
        // More rationale:
        // https://github.com/apache/arrow-datafusion/pull/6354#discussion_r1195284178
        vec![self
            .input
            .output_ordering()
            .map(PhysicalSortRequirement::from_sort_exprs)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            sink: self.sink.clone(),
            schema: self.schema.clone(),
        }))
    }

    /// Execute the plan and return a stream of `RecordBatch`es for
    /// the specified partition.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                format!("Invalid requested partition {partition}. InsertExec requires a single input partition."
                )));
        }

        // Execute each of our own input's partitions and pass them to the sink
        let input_partition_count = self.input.output_partitioning().partition_count();
        if input_partition_count != 1 {
            return Err(DataFusionError::Internal(format!(
                "Invalid input partition count {input_partition_count}. \
                         InsertExec needs only a single partition."
            )));
        }

        let data = self.input.execute(0, context.clone())?;
        let schema = self.schema.clone();
        let sink = self.sink.clone();

        let stream = futures::stream::once(async move {
            sink.write_all(data, &context).await.map(make_count_batch)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }


    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

/// Create a output record batch with a count
///
/// ```text
/// +-------+,
/// | count |,
/// +-------+,
/// | 6     |,
/// +-------+,
/// ```
fn make_count_batch(count: u64) -> RecordBatch {
    let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;

    RecordBatch::try_from_iter_with_nullable(vec![("row_count", array, false)]).unwrap()
}

fn make_count_schema() -> SchemaRef {
    // define a schema.
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// Implements for writing to a [`MemTable`]
struct LakeSoulParquekSink {
    table_name: String,
    client: MetaDataClientRef,
    config_builder: LakeSoulIOConfigBuilder,
    schema: SchemaRef,
}

impl Debug for LakeSoulParquekSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LakeSoulParquekSink")
            .finish()
    }
}

impl DisplayAs for LakeSoulParquekSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MemoryTable (partitions=)")
            }
        }
    }
}

impl LakeSoulParquekSink {
    fn new(client: MetaDataClientRef, table_name: String, config_builder: LakeSoulIOConfigBuilder) -> Self {
        let schema = config_builder.schema();
        Self { client, table_name, config_builder, schema }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl DataSink for LakeSoulParquekSink {
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let sink_schema = self.schema();
        if self.config_builder.primary_keys_slice().is_empty() {
            if sink_schema != data.schema() {
                return Err(DataFusionError::ArrowError(ArrowError::SchemaError(format!("Schema mismatch: to-write record_batch[{:?}] v.s. parquet writer[{:?}]", data.schema(), sink_schema))))
            }  
        } else if !data.schema().fields().iter().all(|f| sink_schema.field_with_name(f.name()).is_ok()) {
            return Err(DataFusionError::ArrowError(ArrowError::SchemaError(format!("Extra column is present at to-write record_batch[{:?}], sink schema is {:?}", data.schema(), self.schema()))));
            
        }
        let mut row_count = 0;
        let table_name = self.table_name.as_str();
        let file = [std::env::temp_dir().to_str().unwrap(), table_name, format!("{}.parquet", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().to_string()).as_str()].iter().collect::<PathBuf>().to_str().unwrap().to_string();
        let builder = self.config_builder.clone()
            .with_files(vec![file])
            .with_schema(data.schema());
        let mut async_writer = MultiPartAsyncWriter::try_new(builder.clone().build()).await?;
        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();
            async_writer.write_record_batch(batch).await?;
        }
        Box::new(async_writer).flush_and_close().await?;

        match commit_data(self.client.clone(), table_name, builder.build()).await {
            Ok(_) => Ok(row_count as u64),
            Err(e) => Err(DataFusionError::External(Box::new(e)))
        }
    }
}