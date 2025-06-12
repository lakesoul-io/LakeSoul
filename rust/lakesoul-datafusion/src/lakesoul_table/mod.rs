// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The interface of LakeSoul table.

pub mod helpers;

use std::sync::Arc;

use crate::LakeSoulError;
use crate::datasource::file_format::LakeSoulMetaDataParquetFormat;
use crate::serialize::arrow_java::schema_from_metadata_str;
use crate::{
    catalog::{
        LakeSoulTableProperty, create_io_config_builder, parse_table_info_partitions,
    },
    error::Result,
    planner::query_planner::LakeSoulQueryPlanner,
};
use arrow::datatypes::SchemaRef;
use arrow_cast::pretty::pretty_format_batches;
use chrono::Utc;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::sql::TableReference;
use datafusion::{
    arrow::record_batch::RecordBatch,
    dataframe::DataFrame,
    datasource::TableProvider,
    execution::context::{SessionContext, SessionState},
    logical_expr::LogicalPlanBuilder,
};
use helpers::case_fold_table_name;
use lakesoul_io::async_writer::{
    AsyncBatchWriter, AsyncSendableMutableLakeSoulWriter, WriterFlushResult,
};
use lakesoul_io::lakesoul_io_config::OPTION_KEY_MEM_LIMIT;
use lakesoul_io::lakesoul_io_config::create_session_context_with_planner;
use lakesoul_metadata::{LakeSoulMetaDataError, MetaDataClient, MetaDataClientRef};
use proto::proto::entity::{CommitOp, DataCommitInfo, DataFileOp, FileOp, TableInfo};
use std::collections::HashMap;
use uuid::Uuid;

use crate::datasource::table_provider::LakeSoulTableProvider;

#[derive(Debug)]
pub struct LakeSoulTable {
    client: MetaDataClientRef,
    table_info: Arc<TableInfo>,
    table_name: String,
    table_schema: SchemaRef,
    primary_keys: Vec<String>,
    range_partitions: Vec<String>,
    properties: LakeSoulTableProperty,
}

impl LakeSoulTable {
    pub async fn for_path(_path: String) -> Result<Self> {
        todo!()
    }

    pub async fn for_path_snapshot(path: String) -> Result<Self> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_info = client.get_table_info_by_table_path(&path).await?;
        if let Some(table_info) = table_info {
            Self::try_new_with_client_and_table_info(client, table_info).await
        } else {
            Err(LakeSoulError::MetaDataError(
                LakeSoulMetaDataError::NotFound(format!("Table '{}' not found", path)),
            ))
        }
    }

    pub async fn for_name(table_name: &str) -> Result<Self> {
        Self::for_namespace_and_name("default", table_name, None).await
    }

    pub async fn for_table_reference(
        table_ref: &TableReference,
        client: Option<MetaDataClientRef>,
    ) -> Result<Self> {
        let schema = table_ref.schema().unwrap_or("default");
        let table_name = case_fold_table_name(table_ref.table());
        info!("for_table_reference: {:?}, {:?}", schema, table_name);
        Self::for_namespace_and_name(schema, &table_name, client).await
    }

    pub async fn for_namespace_and_name(
        namespace: &str,
        table_name: &str,
        client: Option<MetaDataClientRef>,
    ) -> Result<Self> {
        let client = match client {
            Some(client) => client,
            None => Arc::new(MetaDataClient::from_env().await?),
        };
        let table_info = client
            .get_table_info_by_table_name(table_name, namespace)
            .await?;
        if let Some(table_info) = table_info {
            Self::try_new_with_client_and_table_info(client, table_info).await
        } else {
            Err(LakeSoulError::MetaDataError(
                LakeSoulMetaDataError::NotFound(format!(
                    "Table '{}' in '{}' not found",
                    table_name, namespace
                )),
            ))
        }
    }

    /// Create a new LakeSoulTable from a MetaDataClient and TableInfo.
    pub async fn try_new_with_client_and_table_info(
        client: MetaDataClientRef,
        table_info: TableInfo,
    ) -> Result<Self> {
        let table_schema = schema_from_metadata_str(&table_info.table_schema);

        let table_name = table_info.table_name.clone();
        let properties =
            serde_json::from_str::<LakeSoulTableProperty>(&table_info.properties)?;
        let (range_partitions, hash_partitions) =
            parse_table_info_partitions(&table_info.partitions)?;

        Ok(Self {
            client,
            table_info: Arc::new(table_info),
            table_name,
            table_schema,
            primary_keys: hash_partitions,
            range_partitions,
            properties,
        })
    }

    pub async fn upsert_dataframe(&self, dataframe: DataFrame) -> Result<()> {
        let builder = create_io_config_builder(
            self.client.clone(),
            None,
            false,
            self.table_namespace(),
            Default::default(),
            Default::default(),
        )
        .await?;
        let sess_ctx = create_session_context_with_planner(
            &mut builder.clone().build(),
            Some(LakeSoulQueryPlanner::new_ref()),
        )?;

        let logical_plan = LogicalPlanBuilder::insert_into(
            dataframe.into_unoptimized_plan(),
            TableReference::partial(
                self.table_namespace().to_string(),
                self.table_name().to_string(),
            ),
            provider_as_source(self.as_provider().await?),
            InsertOp::Replace,
        )?
        .build()?;
        let dataframe = DataFrame::new(sess_ctx.state(), logical_plan);

        dataframe.collect().await?;

        Ok(())
    }

    pub async fn execute_upsert(&self, record_batch: RecordBatch) -> Result<()> {
        let builder = create_io_config_builder(
            self.client.clone(),
            None,
            false,
            self.table_namespace(),
            Default::default(),
            Default::default(),
        )
        .await?;
        let sess_ctx = create_session_context_with_planner(
            &mut builder.clone().build(),
            Some(LakeSoulQueryPlanner::new_ref()),
        )?;

        let logical_plan = LogicalPlanBuilder::insert_into(
            sess_ctx.read_batch(record_batch)?.into_unoptimized_plan(),
            TableReference::partial(
                self.table_namespace().to_string(),
                self.table_name().to_string(),
            ),
            provider_as_source(self.as_provider().await?),
            InsertOp::Replace,
        )?
        .build()?;
        let dataframe = DataFrame::new(sess_ctx.state(), logical_plan);

        let results = dataframe
            // .explain(true, false)?
            .collect()
            .await?;

        info!(
            "execute_upsert result: {}",
            pretty_format_batches(&results)?
        );
        Ok(())
    }

    pub async fn get_writer(
        &self,
        object_store_options: HashMap<String, String>,
    ) -> Result<Box<dyn AsyncBatchWriter + Send>> {
        let builder = create_io_config_builder(
            self.client.clone(),
            Some(self.table_name()),
            false,
            self.table_namespace(),
            HashMap::from([(
                OPTION_KEY_MEM_LIMIT.to_string(),
                format!("{}", 1024 * 1024 * 1024),
            )]),
            object_store_options,
        )
        .await?
        .clone();

        let config = builder.build();
        let writer = AsyncSendableMutableLakeSoulWriter::try_new(config).await?;
        Ok(Box::new(writer))
    }

    pub async fn to_dataframe(&self, context: &SessionContext) -> Result<DataFrame> {
        let config_builder = create_io_config_builder(
            self.client(),
            Some(self.table_name()),
            true,
            self.table_namespace(),
            HashMap::new(),
            HashMap::new(),
        )
        .await?;
        let provider = Arc::new(
            LakeSoulTableProvider::try_new(
                &context.state(),
                self.client(),
                config_builder.build(),
                self.table_info(),
                false,
            )
            .await?,
        );
        Ok(context.read_table(provider)?)
    }

    pub async fn as_sink_provider(
        &self,
        session_state: &SessionState,
    ) -> Result<Arc<dyn TableProvider>> {
        let config_builder = create_io_config_builder(
            self.client(),
            Some(self.table_name()),
            false,
            self.table_namespace(),
            HashMap::new(),
            HashMap::new(),
        )
        .await?
        .with_prefix(self.table_info.table_path.clone());
        Ok(Arc::new(
            LakeSoulTableProvider::try_new(
                session_state,
                self.client(),
                config_builder.build(),
                self.table_info(),
                true,
            )
            .await?,
        ))
    }

    pub async fn as_provider(&self) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(LakeSoulTableProvider {
            listing_options: LakeSoulMetaDataParquetFormat::default_listing_options()
                .await?,
            listing_table_paths: vec![],
            client: self.client(),
            table_info: self.table_info(),
            table_schema: self.schema(),
            file_schema: self.schema(),
            primary_keys: self.primary_keys().to_vec(),
            range_partitions: self.range_partitions().to_vec(),
        }))
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

    pub fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }

    pub fn primary_keys(&self) -> &Vec<String> {
        &self.primary_keys
    }

    pub fn range_partitions(&self) -> &Vec<String> {
        &self.range_partitions
    }

    pub fn hash_bucket_num(&self) -> usize {
        self.properties.hash_bucket_num.unwrap_or(1)
    }

    pub fn table_namespace(&self) -> &str {
        &self.table_info.table_namespace
    }

    pub fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    #[instrument(skip_all)]
    pub async fn commit_flush_result(&self, result: WriterFlushResult) -> Result<()> {
        // 创建data commit info列表
        let mut data_commit_info_list = Vec::new();

        let partition_desc_and_files_map =
            partitioned_files_from_writer_flush_result(&result)?;

        // 处理分区文件映射
        for (partition_desc, file_list) in partition_desc_and_files_map {
            // 创建DataCommitInfo
            let data_commit_info = DataCommitInfo {
                table_id: self.table_info.table_id.clone(),
                partition_desc,
                commit_id: {
                    let (high, low) = Uuid::new_v4().as_u64_pair();
                    Some(proto::proto::entity::Uuid { high, low })
                },
                file_ops: {
                    file_list
                        .into_iter()
                        .map(|res| DataFileOp {
                            file_op: FileOp::Add.into(),
                            path: res.file_path,
                            size: res.file_size,
                            file_exist_cols: res.file_exist_cols,
                        })
                        .collect()
                },
                commit_op: CommitOp::AppendCommit.into(),
                timestamp: Utc::now().timestamp_millis(),
                committed: false,
                domain: String::from("public"),
            };
            data_commit_info_list.push(data_commit_info);
        }

        // 提交所有DataCommitInfo
        debug!("Committing DataCommitInfo={:?}", data_commit_info_list);
        for commit_info in data_commit_info_list {
            let commit_id = commit_info.commit_id;
            self.client.commit_data_commit_info(commit_info).await?;
            debug!("Commit done for commit_id={:?}", commit_id);
        }

        Ok(())
    }
}

/// The result of the flush operation.
/// todo: maybe define in unproper place.
#[derive(Debug)]
struct FlushResult {
    /// The path of the file.
    file_path: String,
    /// The size of the file.
    file_size: i64,
    /// The columns of the file.
    file_exist_cols: String,
}

/// Get the partition description and the files from the writer flush result.
fn partitioned_files_from_writer_flush_result(
    flush_result: &WriterFlushResult,
) -> Result<HashMap<String, Vec<FlushResult>>> {
    let mut partition_desc_and_files_map = HashMap::new();

    for (partition_desc, file_path, meta, file_meta) in flush_result {
        let file_exist_cols = file_meta
            .schema
            .iter()
            .map(|s| s.name.clone())
            .collect::<Vec<String>>()
            .join(",");
        let flush_result = FlushResult {
            file_path: file_path.clone(),
            file_size: meta.size as i64,
            file_exist_cols,
        };

        partition_desc_and_files_map
            .entry(partition_desc.to_string())
            .or_insert_with(Vec::new)
            .push(flush_result);
    }

    Ok(partition_desc_and_files_map)
}
