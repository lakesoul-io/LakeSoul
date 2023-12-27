use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{FileType, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::common::AbortOnDropSingle;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, Distribution, Partitioning, SendableRecordBatchStream};
use datafusion::scalar::ScalarValue;
use datafusion::{
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        physical_plan::{FileScanConfig, FileSinkConfig},
    },
    error::Result,
    execution::context::SessionState,
    physical_expr::PhysicalSortRequirement,
    physical_plan::{ExecutionPlan, PhysicalExpr},
};
use futures::StreamExt;
use lakesoul_io::lakesoul_writer::{AsyncBatchWriter, MultiPartAsyncWriter};
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use object_store::{ObjectMeta, ObjectStore};
use proto::proto::entity::TableInfo;
use rand::distributions::DistString;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::catalog::commit_data;
use crate::lakesoul_table::helpers::{create_io_config_builder_from_table_info, get_columnar_value};

pub struct LakeSoulMetaDataParquetFormat {
    parquet_format: Arc<ParquetFormat>,
    client: MetaDataClientRef,
    table_info: Arc<TableInfo>,
}

impl Debug for LakeSoulMetaDataParquetFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LakeSoulMetaDataParquetFormat").finish()
    }
}

impl LakeSoulMetaDataParquetFormat {
    pub async fn new(parquet_format: Arc<ParquetFormat>, table_info: Arc<TableInfo>) -> crate::error::Result<Self> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        Ok(Self {
            parquet_format,
            client,
            table_info,
        })
    }

    fn client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

    pub fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }
}

#[async_trait]
impl FileFormat for LakeSoulMetaDataParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.parquet_format.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.parquet_format
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.parquet_format.create_physical_plan(state, conf, filters).await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &SessionState,
        conf: FileSinkConfig,
        order_requirements: Option<Vec<PhysicalSortRequirement>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented yet for Parquet".to_string(),
            ));
        }

        Ok(
            Arc::new(LakeSoulHashSinkExec::new(input, order_requirements, self.table_info(), self.client()).await?)
                as _,
        )
    }

    fn file_type(&self) -> FileType {
        FileType::PARQUET
    }
}

// /// Execution plan for writing record batches to a [`LakeSoulParquetSink`]
// ///
// /// Returns a single row with the number of values written
pub struct LakeSoulHashSinkExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,

    /// Schema describing the structure of the output data.
    count_schema: SchemaRef,
    /// Optional required sort order for output data.
    sort_order: Option<Vec<PhysicalSortRequirement>>,

    table_info: Arc<TableInfo>,

    metadata_client: MetaDataClientRef,
}

impl fmt::Debug for LakeSoulHashSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LakeSoulHashSinkExec schema: {:?}", self.count_schema)
    }
}

impl LakeSoulHashSinkExec {
    /// Create a plan to write to `sink`
    pub async fn new(
        input: Arc<dyn ExecutionPlan>,
        sort_order: Option<Vec<PhysicalSortRequirement>>,
        table_info: Arc<TableInfo>,
        metadata_client: MetaDataClientRef,
    ) -> Result<Self> {
        Ok(Self {
            input,
            count_schema: make_count_schema(),
            sort_order,
            table_info,
            metadata_client,
        })
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Optional sort order for output data
    pub fn sort_order(&self) -> &Option<Vec<PhysicalSortRequirement>> {
        &self.sort_order
    }

    pub fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }

    pub fn metadata_client(&self) -> MetaDataClientRef {
        self.metadata_client.clone()
    }

    async fn pull_and_sink(
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
        table_info: Arc<TableInfo>,
        write_id: String,
        partitioned_file_path_and_row_count: Arc<Mutex<HashMap<Vec<(String, ScalarValue)>, (Vec<String>, u64)>>>,
    ) -> Result<u64> {
        let mut data = input.execute(partition, context.clone())?;

        let mut row_count = 0;
        // let mut async_writer = MultiPartAsyncWriter::try_new(lakesoul_io_config).await?;
        let mut partitioned_writer = HashMap::<Vec<(String, ScalarValue)>, Box<MultiPartAsyncWriter>>::new();
        let mut partitioned_file_path_and_row_count_locked = partitioned_file_path_and_row_count.lock().await;
        while let Some(batch) = data.next().await.transpose()? {
            // dbg!(&batch);
            let columnar_value = get_columnar_value(&batch);
            let file_absolute_path = format!("{}/part-{}_{:0>4}.parquet", table_info.table_path, write_id, partition);
            if !partitioned_writer.contains_key(&columnar_value) {
                let mut config = create_io_config_builder_from_table_info(table_info.clone())
                    .with_files(vec![file_absolute_path.clone()])
                    .with_schema(batch.schema())
                    .build();

                let writer = MultiPartAsyncWriter::try_new_with_context(&mut config, context.clone()).await?;
                partitioned_writer.insert(columnar_value.clone(), Box::new(writer));
            }

            if let Some(async_writer) = partitioned_writer.get_mut(&columnar_value) {
                if let Some(file_path_and_row_count) =
                    partitioned_file_path_and_row_count_locked.get_mut(&columnar_value)
                {
                    file_path_and_row_count.0.push(file_absolute_path);
                    file_path_and_row_count.1 += batch.num_rows() as u64;
                } else {
                    partitioned_file_path_and_row_count_locked.insert(
                        columnar_value.clone(),
                        (vec![file_absolute_path], batch.num_rows() as u64),
                    );
                }
                row_count += batch.num_rows();
                async_writer.write_record_batch(batch).await?;
            }
        }

        let partitioned_writer = partitioned_writer.into_values().collect::<Vec<_>>();
        for writer in partitioned_writer {
            writer.flush_and_close().await?;
        }

        Ok(row_count as u64)
    }

    async fn wait_for_commit(
        join_handles: Vec<JoinHandle<Result<u64>>>,
        client: MetaDataClientRef,
        table_name: String,
        partitioned_file_path_and_row_count: Arc<Mutex<HashMap<Vec<(String, ScalarValue)>, (Vec<String>, u64)>>>,
    ) -> Result<u64> {
        let count =
            futures::future::join_all(join_handles)
                .await
                .iter()
                .try_fold(0u64, |counter, result| match &result {
                    Ok(Ok(count)) => Ok(counter + count),
                    Ok(Err(e)) => Err(DataFusionError::Execution(format!("{}", e))),
                    Err(e) => Err(DataFusionError::Execution(format!("{}", e))),
                })?;
        let partitioned_file_path_and_row_count = partitioned_file_path_and_row_count.lock().await;

        for (columnar_value, (files, _)) in partitioned_file_path_and_row_count.iter() {
            let partition_desc = columnar_value
                .iter()
                .map(|(column, value)| (column.to_string(), value.to_string()))
                .collect::<Vec<_>>();
            commit_data(client.clone(), &table_name, partition_desc, &files)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        Ok(count)
    }
}

impl DisplayAs for LakeSoulHashSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LakeSoulHashSinkExec")
    }
}

impl ExecutionPlan for LakeSoulHashSinkExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.count_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time.
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time, and so requires a single input partition.
        vec![Distribution::SinglePartition; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        // The input order is either exlicitly set (such as by a ListingTable),
        // or require that the [FileSinkExec] gets the data in the order the
        // input produced it (otherwise the optimizer may chose to reorder
        // the input which could result in unintended / poor UX)
        //
        // More rationale:
        // https://github.com/apache/arrow-datafusion/pull/6354#discussion_r1195284178
        match &self.sort_order {
            Some(requirements) => vec![Some(requirements.clone())],
            None => vec![self
                .input
                .output_ordering()
                .map(PhysicalSortRequirement::from_sort_exprs)],
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            count_schema: self.count_schema.clone(),
            sort_order: self.sort_order.clone(),
            table_info: self.table_info.clone(),
            metadata_client: self.metadata_client.clone(),
        }))
    }

    fn unbounded_output(&self, _children: &[bool]) -> Result<bool> {
        Ok(_children[0])
    }

    /// Execute the plan and return a stream of `RecordBatch`es for
    /// the specified partition.
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::NotImplemented(format!(
                "FileSinkExec can only be called on partition 0!"
            )));
        }
        let num_input_partitions = self.input.output_partitioning().partition_count();

        // launch one async task per *input* partition
        let mut join_handles = vec![];

        let write_id = rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

        let partitioned_file_path_and_row_count = Arc::new(Mutex::new(HashMap::<
            Vec<(String, ScalarValue)>,
            (Vec<String>, u64),
        >::new()));
        for i in 0..num_input_partitions {
            let sink_task = tokio::spawn(Self::pull_and_sink(
                self.input().clone(),
                i,
                context.clone(),
                self.table_info(),
                write_id.clone(),
                partitioned_file_path_and_row_count.clone(),
            ));
            // // In a separate task, wait for each input to be done
            // // (and pass along any errors, including panic!s)
            join_handles.push(sink_task);
        }

        let join_handle = AbortOnDropSingle::new(tokio::spawn(Self::wait_for_commit(
            join_handles,
            self.metadata_client(),
            self.table_info().table_name.clone(),
            partitioned_file_path_and_row_count,
        )));

        // });

        // let abort_helper = Arc::new(AbortOnDropMany(join_handles));

        let count_schema = self.count_schema.clone();
        // let count = futures::future::join_all(join_handles).await;
        // for (columnar_values, result) in partitioned_file_path_and_row_count.lock().await.iter() {
        //     match commit_data(self.metadata_client(), self.table_info().table_name.as_str(), &result.0).await {
        //         Ok(()) => todo!(),
        //         Err(_) => todo!(),
        //     }
        // }

        let stream = futures::stream::once(async move {
            match join_handle.await {
                Ok(Ok(count)) => Ok(make_count_batch(count)),
                other => Ok(make_count_batch(u64::MAX)),
            }
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(count_schema, stream)))
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

    RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)]).unwrap()
}

fn make_count_schema() -> SchemaRef {
    // define a schema.
    Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]))
}
