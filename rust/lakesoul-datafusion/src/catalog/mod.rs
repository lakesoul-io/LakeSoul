// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0


use std::sync::{Arc};
use std::time::SystemTime;
use std::{env};
use std::any::Any;
use std::fmt::{Debug, Formatter, Pointer};


use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use futures::future::try_maybe_done;
use tokio::runtime::Runtime;
use tracing::debug;
use tracing::field::debug;
use lakesoul_io::datasource::file_format::LakeSoulParquetFormat;
use lakesoul_io::datasource::listing::LakeSoulListingTable;

use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_metadata::{MetaDataClientRef};
use proto::proto::entity::{CommitOp, DataCommitInfo, DataFileOp, FileOp, Namespace, TableInfo, Uuid};

use crate::lakesoul_table::helpers::create_io_config_builder_from_table_info;
use crate::serialize::arrow_java::{ArrowJavaSchema};
// use crate::transaction::TransactionMetaInfo;
use crate::error::Result;

// pub mod lakesoul_sink;
// pub mod lakesoul_source;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LakeSoulTableProperty {
    pub hash_bucket_num: Option<usize>,
}

pub(crate) async fn create_table(client: MetaDataClientRef, table_name: &str, config: LakeSoulIOConfig) -> Result<()> {
    client
        .create_table(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_name: table_name.to_string(),
            table_path: format!("file://{}default/{}", env::temp_dir().to_str().unwrap(), table_name),
            table_schema: serde_json::to_string::<ArrowJavaSchema>(&config.schema().into()).unwrap(),
            table_namespace: "default".to_string(),
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: Some(4),
            })?,
            partitions: format!(
                "{};{}",
                "",
                config
                    .primary_keys_slice()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            domain: "public".to_string(),
        })
        .await?;
    Ok(())
}

pub(crate) async fn create_io_config_builder(
    client: MetaDataClientRef,
    table_name: Option<&str>,
    fetch_files: bool,
) -> Result<LakeSoulIOConfigBuilder> {
    if let Some(table_name) = table_name {
        let table_info = client.get_table_info_by_table_name(table_name, "default").await?;
        let data_files = if fetch_files {
            client
                .get_data_files_by_table_name(table_name, vec![], "default")
                .await?
        } else {
            vec![]
        };
        Ok(create_io_config_builder_from_table_info(Arc::new(table_info)).with_files(data_files))
    } else {
        Ok(LakeSoulIOConfigBuilder::new())
    }
}

pub(crate) fn parse_table_info_partitions(partitions: String) -> (Vec<String>, Vec<String>) {
    let (range_keys, hash_keys) = partitions.split_at(partitions.find(';').unwrap());
    let hash_keys = &hash_keys[1..];
    (
        range_keys
            .split(',')
            .collect::<Vec<&str>>()
            .iter()
            .filter_map(|str| if str.is_empty() { None } else { Some(str.to_string()) })
            .collect::<Vec<String>>(),
        hash_keys
            .split(',')
            .collect::<Vec<&str>>()
            .iter()
            .filter_map(|str| if str.is_empty() { None } else { Some(str.to_string()) })
            .collect::<Vec<String>>(),
    )
}

pub(crate) async fn commit_data(
    client: MetaDataClientRef,
    table_name: &str,
    partitions: Vec<(String, String)>,
    files: &[String],
) -> Result<()> {
    let table_name_id = client.get_table_name_id_by_table_name(table_name, "default").await?;
    client
        .commit_data_commit_info(DataCommitInfo {
            table_id: table_name_id.table_id,
            partition_desc: if partitions.is_empty() {
                "-5".to_string()
            } else {
                partitions
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(",")
            },
            file_ops: files
                .iter()
                .map(|file| DataFileOp {
                    file_op: FileOp::Add as i32,
                    path: file.clone(),
                    ..Default::default()
                })
                .collect(),
            commit_op: CommitOp::AppendCommit as i32,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            commit_id: {
                let (high, low) = uuid::Uuid::new_v4().as_u64_pair();
                Some(Uuid { high, low })
            },
            committed: false,
            domain: "public".to_string(),
        })
        .await?;
    Ok(())
}


/// A metadata wrapper
#[derive(Debug)]
struct LakeSoulCatalog {
    // metadata_client: MetaDataClientRef,
}


impl LakeSoulCatalog {
    pub fn new() -> Self {
        Self {}
    }
}

impl CatalogProvider for LakeSoulCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        todo!()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        todo!()
    }
}


/// A ['SchemaProvider`] that query pg to automatically discover tables
pub struct LakeSoulNamespace {
    metadata_client: MetaDataClientRef,
    runtime: Arc<Runtime>,
    context: Arc<SessionContext>,
    // primary key
    namespace: String,
}


impl LakeSoulNamespace {
    pub fn new(
        meta_data_client_ref: MetaDataClientRef,
        runtime: Arc<Runtime>,
        context: Arc<SessionContext>,
        namespace: &str) -> Self {
        Self {
            metadata_client: meta_data_client_ref,
            runtime,
            context,
            namespace: namespace.to_string(),
        }
    }


    pub fn metadata_client(&self) -> MetaDataClientRef {
        self.metadata_client.clone()
    }
    pub fn runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }
    pub fn context(&self) -> Arc<SessionContext> {
        self.context.clone()
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl Debug for LakeSoulNamespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSoulNamespace{...}").finish()
    }
}


#[async_trait]
impl SchemaProvider for LakeSoulNamespace {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// query table_name_id by namespace
    /// ref: https://www.modb.pro/db/618126
    fn table_names(&self) -> Vec<String> {
        let client = self.metadata_client.clone();
        let np = self.namespace.clone();
        futures::executor::block_on(
            async move {
                self.runtime.spawn(async move {
                    client.get_all_table_name_id_by_namespace(&np).await.unwrap()
                }).await.unwrap().into_iter().map(|t| t.table_name).collect()
            }
        )
    }

    /// Search table by name
    /// return LakesoulListing table
    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        if let Ok(t) = self.metadata_client.get_table_info_by_table_name(name, &self.namespace).await
        {
            let config = create_io_config_builder_from_table_info(Arc::new(t)).build();
            let file_format = Arc::new(LakeSoulParquetFormat::new(
                Arc::new(ParquetFormat::new()),
                config.clone(),
            ));
            if let Ok(table_provider) = LakeSoulListingTable::new_with_config_and_format(
                &self.context.state(),
                config,
                file_format,
                false,
            ).await {
                debug!("create table provider success");
                return Some(Arc::new(table_provider));
            }
            debug("create table provider fail");
            return None;
        } else {
            debug("create table provider fail");
            None
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        let client = self.metadata_client.clone();
        let name = name.to_string();
        let np = self.namespace.clone();
        futures::executor::block_on(
            async move {
                self.runtime.spawn(async move {
                    if let Ok(t) = client.get_table_name_id_by_table_name(&name, &np).await {
                        &t.table_name == &name
                    } else {
                        false
                    }
                }).await.unwrap()
            }
        )
    }
}