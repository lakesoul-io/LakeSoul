use std::fmt::{self, Debug};
use std::sync::Arc;
use std::any::Any;

use async_trait::async_trait;

use arrow::datatypes::{SchemaRef, Schema};
use datafusion::common::{Statistics, FileType};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion::physical_plan::insert::{FileSinkExec, DataSink};
use datafusion::{datasource::{file_format::{parquet::ParquetFormat, FileFormat}, physical_plan::{FileSinkConfig, FileScanConfig}}, physical_plan::{ExecutionPlan, PhysicalExpr}, execution::context::SessionState, physical_expr::PhysicalSortRequirement, error::Result};
use lakesoul_io::lakesoul_writer::{MultiPartAsyncWriter, AsyncBatchWriter};
use lakesoul_metadata::{MetaDataClientRef, MetaDataClient};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectMeta};
use proto::proto::entity::TableInfo;
use rand::distributions::DistString;
use futures::StreamExt;

use crate::catalog::commit_data;
use crate::lakesoul_table::helpers::create_io_config_builder_from_table_info;

pub struct LakeSoulMetaDataParquetFormat {
    parquet_format: Arc<ParquetFormat>,
    client: MetaDataClientRef,
    table_info: Arc<TableInfo>
}

impl Debug for LakeSoulMetaDataParquetFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LakeSoulMetaDataParquetFormat")
            .finish()
    }
}

impl LakeSoulMetaDataParquetFormat {
    pub async fn new(parquet_format: Arc<ParquetFormat>, table_info: Arc<TableInfo>) -> crate::error::Result<Self> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        Ok(Self { parquet_format, client, table_info })
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
    fn as_any(&self) ->  &dyn Any {
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
        self.parquet_format.infer_stats(state, store, table_schema, object).await
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
        state: &SessionState,
        conf: FileSinkConfig,
        order_requirements: Option<Vec<PhysicalSortRequirement>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.overwrite {
            return Err(DataFusionError::NotImplemented("Overwrites are not implemented yet for Parquet".to_string()));
        }

        let sink_schema = input.schema();
        let sink = Arc::new(LakeSoulParquetSink::new(conf, self.client(), self.table_info()));


        Ok(Arc::new(FileSinkExec::new(
            input,
            sink,
            sink_schema,
            order_requirements,
        )) as _)
    }

    fn file_type(&self) -> FileType {
        FileType::PARQUET
    }

}

struct LakeSoulParquetSink {
    /// Config options for writing data
    config: FileSinkConfig,
    client: MetaDataClientRef,
    table_info: Arc<TableInfo>,
    bucket_id: usize,
    do_commit: bool,
}

impl Debug for LakeSoulParquetSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LakeSoulParquetSink")
            .finish()
    }
}

impl DisplayAs for LakeSoulParquetSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LakeSoulParquetSink (partitions=)")
            }
        }
    }
}

impl LakeSoulParquetSink {
    fn new(config: FileSinkConfig, client: MetaDataClientRef, table_info: Arc<TableInfo>) -> Self {
        Self { config, client, table_info, bucket_id: 0, do_commit: true }
    }

    fn with_bucket_id(self, bucket_id: usize) -> Self {
        Self { config: self.config, client: self.client, table_info: self.table_info, do_commit: self.do_commit, bucket_id}
    }

    fn client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

    fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }

}

#[async_trait]
impl DataSink for LakeSoulParquetSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let parquet_props = self
            .config
            .file_type_writer_options
            .try_into_parquet()?
            .writer_options();

        let object_store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;
        let parquet_opts = &context.session_config().options().execution.parquet;

        let part_col = if !self.config.table_partition_cols.is_empty() {
            Some(self.config.table_partition_cols.clone())
        } else {
            None
        };

        let write_id =
            rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let file_absolute_path = vec![format!("{}/part-{}_{:0>4}.parquet", self.table_info().table_path, write_id, self.bucket_id)];
        let lakesoul_io_config = create_io_config_builder_from_table_info(self.table_info())
            .with_files(file_absolute_path.clone())
            .with_schema(data.schema())
            .build();

        let mut row_count = 0;
        let mut async_writer = MultiPartAsyncWriter::try_new(lakesoul_io_config).await?;
        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();
            async_writer.write_record_batch(batch).await?;
        }
        Box::new(async_writer).flush_and_close().await?;

        if self.do_commit {
            match commit_data(self.client(), &self.table_info().table_name, &file_absolute_path).await {
                Ok(_) => Ok(row_count as u64),
                Err(e) => Err(DataFusionError::External(Box::new(e)))
            }
        } else {
            Ok(row_count as u64)
        }
    }
}

