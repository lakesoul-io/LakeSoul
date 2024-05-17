// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::{FileFormat, parquet::ParquetFormat},
        physical_plan::{FileScanConfig, FileSinkConfig},
    },
    error::Result,
    execution::context::SessionState,
    physical_expr::PhysicalSortRequirement,
    physical_plan::{ExecutionPlan, PhysicalExpr},
};
use datafusion::common::{FileType, project_schema, Statistics};
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, Distribution, Partitioning, SendableRecordBatchStream};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::sql::TableReference;
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore};
use rand::distributions::DistString;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::debug;

use lakesoul_io::datasource::file_format::{compute_project_column_indices/*, flatten_file_scan_config*/};
use lakesoul_io::datasource::physical_plan::MergeParquetExec;
use lakesoul_io::helpers::{columnar_values_to_partition_desc, columnar_values_to_sub_path, get_columnar_values};
// use lakesoul_io::datasource::physical_plan::MergeParquetExec;
// use lakesoul_io::helpers::partition_desc_from_file_scan_config;
use lakesoul_io::lakesoul_io_config::LakeSoulIOConfig;
use lakesoul_io::lakesoul_writer::{AsyncBatchWriter, MultiPartAsyncWriter};
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata::transfusion::SplitDesc;
use proto::proto::entity::TableInfo;

use crate::catalog::{commit_data, parse_table_info_partitions};
use crate::lakesoul_table::helpers::create_io_config_builder_from_table_info;

pub struct LakeSoulMetaDataParquetFormat {
    parquet_format: Arc<ParquetFormat>,
    client: MetaDataClientRef,
    table_info: Arc<TableInfo>,
    conf: LakeSoulIOConfig,
}

impl Debug for LakeSoulMetaDataParquetFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LakeSoulMetaDataParquetFormat").finish()
    }
}

impl LakeSoulMetaDataParquetFormat {
    pub async fn new(client: MetaDataClientRef, parquet_format: Arc<ParquetFormat>, table_info: Arc<TableInfo>, conf: LakeSoulIOConfig) -> crate::error::Result<Self> {
        Ok(Self {
            parquet_format,
            client,
            table_info,
            conf,
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

    #[tracing::instrument(skip_all)]
    async fn create_physical_plan(
        &self,
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // If enable pruning then combine the filters to build the predicate.
        // If disable pruning then set the predicate to None, thus readers
        // will not prune data based on the statistics.
        let predicate = self
            .parquet_format
            .enable_pruning(state.config_options())
            .then(|| filters.cloned())
            .flatten();

        let file_schema = conf.file_schema.clone();
        let mut builder = SchemaBuilder::from(file_schema.fields());
        for field in &conf.table_partition_cols {
            builder.push(Field::new(field.name(), field.data_type().clone(), false));
        }

        let table_schema = Arc::new(builder.finish());

        let projection = conf.projection.clone();
        let target_schema = project_schema(&table_schema, projection.as_ref())?;

        let merged_projection = compute_project_column_indices(table_schema.clone(), target_schema.clone(), self.conf.primary_keys_slice());
        let merged_schema = project_schema(&table_schema, merged_projection.as_ref())?;

        let splits = splits_from_file_config(&conf, Arc::clone(&self.parquet_format), state, self.conf.primary_keys_slice()).await?;
        debug!(?splits);
        let execs = execs_from_splits(&conf, Arc::clone(&self.parquet_format), state, &splits, Arc::clone(&target_schema), predicate.clone()).await?;
        debug!(?execs);
        let column_nullable = execs
            .iter()
            .flat_map(|e| e.schema().fields.to_vec())
            .filter(|f| f.is_nullable())
            .map(|f| f.to_string())
            .collect::<HashSet<String>>();


        let merged_schema = SchemaRef::new(
            Schema::new(
                merged_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        Field::new(
                            field.name(),
                            field.data_type().clone(),
                            field.is_nullable() | column_nullable.contains(field.name()),
                        )
                    })
                    .collect::<Vec<_>>()
            )
        );

        let mut partitioned_exec = Vec::new();
        let mut exec_iter = execs.into_iter();
        for split in splits {
            let inputs = (0..split.file_paths.len())
                .map(|_| exec_iter.next().ok_or(DataFusionError::External(anyhow!("exec does not match").into())))
                .collect::<Result<Vec<_>>>()?;
            let merge_exec = Arc::new(MergeParquetExec::new_with_inputs(
                merged_schema.clone(),
                inputs,
                self.conf.clone(),
                Arc::new(split.partition_desc),
            )?) as Arc<dyn ExecutionPlan>;
            partitioned_exec.push(merge_exec);
        }

        let exec = if partitioned_exec.len() > 1 {
            Arc::new(UnionExec::new(partitioned_exec)) as Arc<dyn ExecutionPlan>
        } else {
            partitioned_exec.first().unwrap().clone()
        };

        if target_schema.fields().len() < merged_schema.fields().len() {
            let mut projection_expr = vec![];
            for field in target_schema.fields() {
                projection_expr.push((
                    datafusion::physical_expr::expressions::col(field.name(), &merged_schema)?,
                    field.name().clone(),
                ));
            }
            Ok(Arc::new(ProjectionExec::try_new(projection_expr, exec)?))
        } else {
            Ok(exec)
        }
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

/// Execution plan for writing record batches to a [`LakeSoulParquetSink`]
///
/// Returns a single row with the number of values written
pub struct LakeSoulHashSinkExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,

    /// Schema describing the structure of the output data.
    sink_schema: SchemaRef,

    /// Optional required sort order for output data.
    sort_order: Option<Vec<PhysicalSortRequirement>>,

    table_info: Arc<TableInfo>,

    metadata_client: MetaDataClientRef,

    range_partitions: Arc<Vec<String>>,
}

impl Debug for LakeSoulHashSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LakeSoulHashSinkExec schema: {:?}", self.sink_schema)
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
        let (range_partitions, _) = parse_table_info_partitions(table_info.partitions.clone()).map_err(|_| DataFusionError::External("parse table_info.partitions failed".into()))?;
        let range_partitions = Arc::new(range_partitions);
        Ok(Self {
            input,
            sink_schema: make_sink_schema(),
            sort_order,
            table_info,
            metadata_client,
            range_partitions,
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
        range_partitions: Arc<Vec<String>>,
        write_id: String,
        partitioned_file_path_and_row_count: Arc<Mutex<HashMap<String, (Vec<String>, u64)>>>,
    ) -> Result<u64> {
        let mut data = input.execute(partition, context.clone())?;
        let schema_projection_excluding_range =
            data.schema()
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(idx, field)|
                    match range_partitions.contains(field.name()) {
                        true => None,
                        false => Some(idx)
                    })
                .collect::<Vec<_>>();

        let mut row_count = 0;
        // let mut async_writer = MultiPartAsyncWriter::try_new(lakesoul_io_config).await?;
        let mut partitioned_writer = HashMap::<String, Box<MultiPartAsyncWriter>>::new();
        let mut partitioned_file_path_and_row_count_locked = partitioned_file_path_and_row_count.lock().await;
        while let Some(batch) = data.next().await.transpose()? {
            debug!("write record_batch with {} rows", batch.num_rows());
            let columnar_values = get_columnar_values(&batch, range_partitions.clone())?;
            let partition_desc = columnar_values_to_partition_desc(&columnar_values);
            let batch_excluding_range = batch.project(&schema_projection_excluding_range)?;
            let file_absolute_path = format!("{}{}part-{}_{:0>4}.parquet", table_info.table_path, columnar_values_to_sub_path(&columnar_values), write_id, partition);

            if !partitioned_writer.contains_key(&partition_desc) {
                let mut config = create_io_config_builder_from_table_info(table_info.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .with_files(vec![file_absolute_path])
                    .with_schema(batch_excluding_range.schema())
                    .build();

                let writer = MultiPartAsyncWriter::try_new_with_context(&mut config, context.clone()).await?;
                partitioned_writer.insert(partition_desc.clone(), Box::new(writer));
            }

            if let Some(async_writer) = partitioned_writer.get_mut(&partition_desc) {
                row_count += batch_excluding_range.num_rows();
                async_writer.write_record_batch(batch_excluding_range).await?;
            }
        }

        // TODO: apply rolling strategy
        for (partition_desc, writer) in partitioned_writer.into_iter() {
            let file_absolute_path = writer.absolute_path();
            let num_rows = writer.nun_rows();
            if let Some(file_path_and_row_count) =
                partitioned_file_path_and_row_count_locked.get_mut(&partition_desc)
            {
                file_path_and_row_count.0.push(file_absolute_path);
                file_path_and_row_count.1 += num_rows;
            } else {
                partitioned_file_path_and_row_count_locked.insert(
                    partition_desc.clone(),
                    (vec![file_absolute_path], num_rows),
                );
            }
            writer.flush_and_close().await?;
        }

        Ok(row_count as u64)
    }

    async fn wait_for_commit(
        join_handles: Vec<JoinHandle<Result<u64>>>,
        client: MetaDataClientRef,
        table_name: String,
        partitioned_file_path_and_row_count: Arc<Mutex<HashMap<String, (Vec<String>, u64)>>>,
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

        for (partition_desc, (files, _)) in partitioned_file_path_and_row_count.iter() {
            commit_data(client.clone(), &table_name, partition_desc.clone(), files)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            debug!(
                "table: {} insert success at {:?}",
                &table_name,
                std::time::SystemTime::now()
            )
        }
        Ok(count)
    }
}

impl DisplayAs for LakeSoulHashSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
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
        self.sink_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn unbounded_output(&self, _children: &[bool]) -> Result<bool> {
        Ok(_children[0])
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time, and so requires a single input partition.
        vec![Distribution::SinglePartition; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        // The input order is either explicitly set (such as by a ListingTable),
        // or require that the [FileSinkExec] gets the data in the order the
        // input produced it (otherwise the optimizer may choose to reorder
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

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time.
        vec![false]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            sink_schema: self.sink_schema.clone(),
            sort_order: self.sort_order.clone(),
            table_info: self.table_info.clone(),
            range_partitions: self.range_partitions.clone(),
            metadata_client: self.metadata_client.clone(),
        }))
    }

    /// Execute the plan and return a stream of `RecordBatch`es for
    /// the specified partition.
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::NotImplemented(
                "FileSinkExec can only be called on partition 0!".to_string(),
            ));
        }
        let num_input_partitions = self.input.output_partitioning().partition_count();

        // launch one async task per *input* partition
        let mut join_handles = vec![];

        let write_id = rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

        let partitioned_file_path_and_row_count =
            Arc::new(
                Mutex::new(
                    HashMap::<String, (Vec<String>, u64)>::new()
                ));
        for i in 0..num_input_partitions {
            let sink_task = tokio::spawn(Self::pull_and_sink(
                self.input().clone(),
                i,
                context.clone(),
                self.table_info(),
                self.range_partitions.clone(),
                write_id.clone(),
                partitioned_file_path_and_row_count.clone(),
            ));
            // // In a separate task, wait for each input to be done
            // // (and pass along any errors, including panic!s)
            join_handles.push(sink_task);
        }

        let table_ref = TableReference::Partial {
            schema: self.table_info().table_namespace.clone().into(),
            table: self.table_info().table_name.clone().into(),
        };
        let join_handle = tokio::spawn(Self::wait_for_commit(
            join_handles,
            self.metadata_client(),
            table_ref.to_string(),
            partitioned_file_path_and_row_count,
        ));

        // });

        // let abort_helper = Arc::new(AbortOnDropMany(join_handles));

        let sink_schema = self.sink_schema.clone();
        // let count = futures::future::join_all(join_handles).await;
        // for (columnar_values, result) in partitioned_file_path_and_row_count.lock().await.iter() {
        //     match commit_data(self.metadata_client(), self.table_info().table_name.as_str(), &result.0).await {
        //         Ok(()) => todo!(),
        //         Err(_) => todo!(),
        //     }
        // }

        let stream = futures::stream::once(async move {
            match join_handle.await {
                Ok(Ok(count)) => Ok(make_sink_batch(count, String::from(""))),
                Ok(Err(e)) => {
                    debug!("{}", e.to_string());
                    Ok(make_sink_batch(u64::MAX, e.to_string()))
                }
                Err(e) => {
                    debug!("{}", e.to_string());
                    Ok(make_sink_batch(u64::MAX, e.to_string()))
                }
            }
        })
            .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(sink_schema, stream)))
    }
}

fn make_sink_batch(count: u64, msg: String) -> RecordBatch {
    let count_array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
    let msg_array = Arc::new(StringArray::from(vec![msg])) as ArrayRef;
    RecordBatch::try_from_iter_with_nullable(vec![("count", count_array, false), ("msg", msg_array, false)]).unwrap()
}

fn make_sink_schema() -> SchemaRef {
    // define a schema.
    Arc::new(Schema::new(vec![
        Field::new("count", DataType::UInt64, false),
        Field::new("msg", DataType::Utf8, false),
    ]))
}


/// use file config to create splits
async fn splits_from_file_config(
    conf: &FileScanConfig,
    format: Arc<ParquetFormat>,
    state: &SessionState,
    pks: &[String],
) -> Result<Vec<SplitDesc>> {
    let url = &conf.object_store_url;
    let store = state.runtime_env().object_store(url)?;
    let mut ret = vec![];
    for i in 0..conf.file_groups.len() {
        let mut objects = vec![];
        let file_group = &conf.file_groups[i];
        let mut partition_desc = HashMap::new();
        for file in file_group {
            let table_partition_cols = conf.table_partition_cols.clone();
            for (idx, f) in table_partition_cols.iter().enumerate() {
                partition_desc.insert(f.name().clone(), file.partition_values[idx].to_string());
            }
            objects.push(file.object_meta.clone());
        }
        let locations: Vec<String> = objects.iter().map(|o| o.location.to_string()).collect();
        let schema = format.infer_schema(state, &store, &objects).await?.to_string();
        let sd = SplitDesc {
            file_paths: locations,
            primary_keys: pks.to_vec(),
            partition_desc,
            table_schema: schema,
        };
        ret.push(sd);
    }
    Ok(ret)
}

/// use splits to create execs
async fn execs_from_splits(
    conf: &FileScanConfig,
    format: Arc<ParquetFormat>,
    state: &SessionState,
    splits: &[SplitDesc],
    target_schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
    let url = &conf.object_store_url;
    let store = state.runtime_env().object_store(url)?;
    let mut ret = vec![];
    for (split, files) in splits.iter().zip(conf.file_groups.iter()) {
        for file in files {
            let objs = vec![file.object_meta.clone()];
            let file_groups = vec![vec![file.clone()]];
            let file_schema = format.infer_schema(state, &store, &objs).await?;
            let statistics = format.infer_stats(state, &store, file_schema.clone(), &file.object_meta).await?;
            let projection = compute_project_column_indices(file_schema.clone(), target_schema.clone(), &split.primary_keys);
            let limit = conf.limit;
            let table_partition_cols = conf.table_partition_cols.clone();
            let infinite_source = conf.infinite_source;
            let output_ordering = conf.output_ordering.clone();
            let config = FileScanConfig {
                object_store_url: url.clone(),
                file_schema,
                file_groups,
                statistics,
                projection,
                limit,
                table_partition_cols,
                output_ordering,
                infinite_source,
            };
            let exec = Arc::new(ParquetExec::new(config, predicate.clone(), format.metadata_size_hint(state.config_options()))) as Arc<dyn ExecutionPlan>;
            ret.push(exec);
        }
    }
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::runtime::Runtime;

    use lakesoul_io::lakesoul_io_config::{create_session_context, LakeSoulIOConfigBuilder};
    use lakesoul_metadata::MetaDataClient;

    // #[test]
    fn simple_test() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            tracing::debug!("test");
            let client = Arc::new(MetaDataClient::from_env().await.unwrap());
            let mut cfg = LakeSoulIOConfigBuilder::new().build();
            let sc = Arc::new(create_session_context(&mut cfg).unwrap());
            let catalog = Arc::new(crate::catalog::LakeSoulCatalog::new(client.clone(), sc.clone()));
            sc.register_catalog("lk", catalog.clone());
            // let sma = catalog.schema("default").unwrap();
            // let vec = sma.table_names();
            // let arc = sma.table("KAtG7Fh6hB").await.unwrap();
            // println!("{:?}",.);
            let test_sql = "select * from lk.default.fv22cscz03;";
            let df = sc.sql(test_sql).await.unwrap();
            df.show().await.unwrap()
        });
    }
}