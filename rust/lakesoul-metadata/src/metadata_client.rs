// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Formatter};
use std::ops::DerefMut;
use std::sync::Arc;
use std::{collections::HashMap, env, fs, vec};

use log::{debug, info};
use postgres::Config;
use prost::Message;
use tokio::sync::Mutex;
use url::Url;

use proto::proto::entity::{
    self, CommitOp, DataCommitInfo, JniWrapper, MetaInfo, Namespace, PartitionInfo, TableInfo, TableNameId, TablePathId,
};

use crate::error::{LakeSoulMetaDataError, Result};
use crate::pooled_client::PooledClient;
use crate::{
    clean_meta_for_test, create_connection, execute_insert, execute_query, execute_update, DaoType, PARAM_DELIM,
    PARTITION_DESC_DELIM,
};

pub struct MetaDataClient {
    client: Arc<Mutex<PooledClient>>,
    max_retry: usize,
    secret: String,
}

impl Debug for MetaDataClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaDataClient")
            .field("client", &"{pg_client}")
            .field("max_retry", &self.max_retry)
            .finish()
    }
}

pub type MetaDataClientRef = Arc<MetaDataClient>;

impl MetaDataClient {
    pub async fn from_env() -> Result<Self> {
        match env::var("lakesoul_home") {
            Ok(config_path) => {
                let config = fs::read_to_string(&config_path)
                    .unwrap_or_else(|_| panic!("Fails at reading config file {}", &config_path));
                let config_map = config
                    .split('\n')
                    .filter_map(|property| property.find('=').map(|idx| property.split_at(idx + 1)))
                    .collect::<HashMap<_, _>>();
                let url = Url::parse(
                    &config_map
                        .get("lakesoul.pg.url=")
                        .unwrap_or(&"jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified")[5..],
                )?;
                Self::from_config(format!(
                    "host={} port={} dbname={} user={} password={}",
                    url.host_str()
                        .ok_or(LakeSoulMetaDataError::Internal("url host missing".to_string()))?,
                    url.port()
                        .ok_or(LakeSoulMetaDataError::Internal("url port missing".to_string()))?,
                    url.path_segments()
                        .ok_or(LakeSoulMetaDataError::Internal("url path missing".to_string()))?
                        .next()
                        .ok_or(LakeSoulMetaDataError::Internal("url path missing".to_string()))?,
                    config_map.get("lakesoul.pg.username=").unwrap_or(&"lakesoul_test"),
                    config_map.get("lakesoul.pg.password=").unwrap_or(&"lakesoul_test")
                ))
                .await
            }
            Err(_) => match env::var("LAKESOUL_PG_URL") {
                Ok(pg_url) => {
                    info!("create metadata client from env LAKESOUL_PG_URL= {}", pg_url);
                    let url = Url::parse(&pg_url[5..])?;
                    Self::from_config(format!(
                        "host={} port={} dbname={} user={} password={}",
                        url.host_str()
                            .ok_or(LakeSoulMetaDataError::Internal("url host missing".to_string()))?,
                        url.port()
                            .ok_or(LakeSoulMetaDataError::Internal("url port missing".to_string()))?,
                        url.path_segments()
                            .ok_or(LakeSoulMetaDataError::Internal("url path missing".to_string()))?
                            .next()
                            .ok_or(LakeSoulMetaDataError::Internal("url path missing".to_string()))?,
                        env::var("LAKESOUL_PG_USERNAME").unwrap_or_else(|_| "lakesoul_test".to_string()),
                        env::var("LAKESOUL_PG_PASSWORD").unwrap_or_else(|_| "lakesoul_test".to_string())
                    ))
                    .await
                }
                Err(_) => {
                    Self::from_config(
                        "host=127.0.0.1 port=5432 dbname=lakesoul_test user=lakesoul_test password=lakesoul_test"
                            .to_string(),
                    )
                    .await
                }
            },
        }
    }

    pub async fn from_config(config: String) -> Result<Self> {
        Self::from_config_and_max_retry(config, 3).await
    }

    pub async fn from_config_and_max_retry(config: String, max_retry: usize) -> Result<Self> {
        let client = Arc::new(Mutex::new(create_connection(config.clone()).await?));
        let config = config.parse::<Config>()?;
        info!("Metadata client connected to: {:?}", config);
        Ok(Self {
            client,
            max_retry,
            secret: format!(
                "{:x}",
                md5::compute(
                    format!(
                        "!@{}#${:?}&*",
                        config.get_user().unwrap(),
                        config.get_password().unwrap()
                    )
                    .as_bytes()
                )
            ),
        })
    }

    pub async fn create_namespace(&self, namespace: Namespace) -> Result<()> {
        self.insert_namespace(&namespace).await?;
        Ok(())
    }

    pub async fn create_table(&self, table_info: TableInfo) -> Result<()> {
        info!("create_table: {:?}", &table_info);
        self.insert_table_path_id(&table_path_id_from_table_info(&table_info))
            .await?;
        self.insert_table_name_id(&table_name_id_from_table_info(&table_info))
            .await?;
        self.insert_table_info(&table_info).await?;
        Ok(())
    }

    pub async fn delete_namespace_by_namespace(&self, namespace: &str) -> Result<()> {
        debug!("delete namespace {}", namespace);
        self.execute_update(
            DaoType::DeleteNamespaceByNamespace as i32,
            [namespace].join(PARAM_DELIM),
        )
        .await?;
        Ok(())
    }

    // Use transaction?
    pub async fn delete_table_by_table_info_cascade(&self, table_info: &TableInfo) -> Result<()> {
        self.delete_table_by_table_id_cascade(table_info.table_id.as_str(),
                                              table_info.table_path.as_str()).await
    }

    pub async fn delete_table_by_table_id_cascade(&self, table_id: &str, table_path: &str) -> Result<()> {
        self.delete_table_name_id_by_table_id(table_id).await?;
        self.delete_table_path_id_by_table_id(table_id).await?;
        self.delete_partition_info_by_table_id(table_id).await?;
        self.delete_data_commit_info_by_table_id(table_id).await?;
        self.delete_table_info_by_id_and_path(table_id, table_path)
            .await?;
        Ok(())
    }

    pub async fn delete_table_path_id_by_table_id(&self, table_id: &str) -> Result<i32> {
        self.execute_update(DaoType::DeleteTablePathIdByTableId as i32, [table_id].join(PARAM_DELIM))
            .await
    }

    pub async fn delete_table_name_id_by_table_id(&self, table_id: &str) -> Result<i32> {
        self.execute_update(DaoType::DeleteTableNameIdByTableId as i32, [table_id].join(PARAM_DELIM))
            .await
    }

    pub async fn delete_partition_info_by_table_id(&self, table_id: &str) -> Result<i32> {
        self.execute_update(
            DaoType::DeletePartitionInfoByTableId as i32,
            [table_id].join(PARAM_DELIM),
        )
        .await
    }
    pub async fn delete_data_commit_info_by_table_id(&self, table_id: &str) -> Result<i32> {
        self.execute_update(
            DaoType::DeleteDataCommitInfoByTableId as i32,
            [table_id].join(PARAM_DELIM),
        )
        .await
    }

    pub async fn delete_table_info_by_id_and_path(&self, id: &str, path: &str) -> Result<i32> {
        self.execute_update(DaoType::DeleteTableInfoByIdAndPath as i32, [id, path].join(PARAM_DELIM))
            .await
    }

    async fn execute_insert(&self, insert_type: i32, wrapper: JniWrapper) -> Result<i32> {
        for times in 0..self.max_retry as i64 {
            match execute_insert(self.client.lock().await.deref_mut(), insert_type, wrapper.clone()).await {
                Ok(count) => return Ok(count),
                Err(_) if times < self.max_retry as i64 - 1 => continue,
                Err(e) => return Err(e),
            };
        }
        Err(LakeSoulMetaDataError::Internal("unreachable".to_string()))
    }

    async fn execute_update(&self, update_type: i32, joined_string: String) -> Result<i32> {
        for times in 0..self.max_retry as i64 {
            match execute_update(self.client.lock().await.deref_mut(), update_type, joined_string.clone()).await {
                Ok(count) => return Ok(count),
                Err(_) if times < self.max_retry as i64 - 1 => continue,
                Err(e) => return Err(e),
            };
        }
        Err(LakeSoulMetaDataError::Internal("unreachable".to_string()))
    }

    async fn execute_query(&self, query_type: i32, joined_string: String) -> Result<JniWrapper> {
        for times in 0..self.max_retry as i64 {
            match execute_query(self.client.lock().await.deref_mut(), query_type, joined_string.clone()).await {
                Ok(encoded) => return Ok(JniWrapper::decode(prost::bytes::Bytes::from(encoded))?),
                Err(_) if times < self.max_retry as i64 - 1 => continue,
                Err(e) => return Err(e),
            };
        }
        Err(LakeSoulMetaDataError::Internal("unreachable".to_string()))
    }

    async fn insert_namespace(&self, namespace: &Namespace) -> Result<i32> {
        self.execute_insert(
            DaoType::InsertNamespace as i32,
            JniWrapper {
                namespace: vec![namespace.clone()],
                ..Default::default()
            },
        )
        .await
    }

    async fn insert_table_info(&self, table_info: &TableInfo) -> Result<i32> {
        info!("insert_table_info: {:?}", &table_info);
        self.execute_insert(
            DaoType::InsertTableInfo as i32,
            JniWrapper {
                table_info: vec![table_info.clone()],
                ..Default::default()
            },
        )
        .await
    }

    async fn insert_table_name_id(&self, table_name_id: &TableNameId) -> Result<i32> {
        self.execute_insert(
            DaoType::InsertTableNameId as i32,
            JniWrapper {
                table_name_id: vec![table_name_id.clone()],
                ..Default::default()
            },
        )
        .await
    }

    async fn insert_table_path_id(&self, table_path_id: &TablePathId) -> Result<i32> {
        self.execute_insert(
            DaoType::InsertTablePathId as i32,
            JniWrapper {
                table_path_id: vec![table_path_id.clone()],
                ..Default::default()
            },
        )
        .await
    }

    async fn insert_data_commit_info(&self, data_commit_info: &DataCommitInfo) -> Result<i32> {
        self.execute_insert(
            DaoType::InsertDataCommitInfo as i32,
            JniWrapper {
                data_commit_info: vec![data_commit_info.clone()],
                ..Default::default()
            },
        )
        .await
    }

    async fn transaction_insert_partition_info(&self, partition_info_list: Vec<PartitionInfo>) -> Result<i32> {
        self.execute_insert(
            DaoType::TransactionInsertPartitionInfo as i32,
            JniWrapper {
                partition_info: partition_info_list,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn meta_cleanup(&self) -> Result<i32> {
        clean_meta_for_test(self.client.lock().await.deref_mut()).await?;
        self.insert_namespace(&Namespace {
            namespace: "default".to_string(),
            properties: "{}".to_string(),
            comment: "".to_string(),
            domain: "public".to_string(),
        })
        .await
    }

    pub async fn commit_data(&self, meta_info: MetaInfo, commit_op: CommitOp) -> Result<()> {
        let table_info = meta_info
            .table_info
            .ok_or(LakeSoulMetaDataError::Internal("table info missing".to_string()))?;

        // if !table_info.table_name.is_empty() {
        //     self.update_table_short_name(&table_info.table_path, &table_info.table_id,
        //         &table_info.table_name, &table_info.table_namespace).await?;
        // }

        // self.update_table_properties(&table_info.table_id, &table_info.properties).await?;

        let partition_desc_list = meta_info
            .list_partition
            .iter()
            .map(|partition_info| partition_info.partition_desc.clone())
            .collect::<Vec<String>>();

        let _snapshot_list = meta_info
            .list_partition
            .iter()
            .flat_map(|partition_info| partition_info.snapshot.clone())
            .collect::<Vec<entity::Uuid>>();

        let cur_map = self
            .get_cur_partition_map(&table_info.table_id, &partition_desc_list)
            .await?;
        let domain = self.get_table_domain(table_info.table_id.as_str()).await?.domain;

        match commit_op {
            CommitOp::AppendCommit | CommitOp::MergeCommit => {
                let mut new_partition_list = meta_info
                    .list_partition
                    .iter()
                    .map(|partition_info| {
                        let partition_desc = &partition_info.partition_desc;
                        match cur_map.get(partition_desc) {
                            Some(cur_partition_info) => {
                                let mut cur_partition_info = cur_partition_info.clone();
                                cur_partition_info.domain = domain.clone();
                                cur_partition_info
                                    .snapshot
                                    .extend_from_slice(&partition_info.snapshot[..]);
                                cur_partition_info.version += 1;
                                cur_partition_info.commit_op = commit_op as i32;
                                cur_partition_info.expression = partition_info.expression.clone();
                                Ok(cur_partition_info)
                            }
                            None => Ok(PartitionInfo {
                                table_id: table_info.table_id.clone(),
                                partition_desc: partition_desc.clone(),
                                version: 0,
                                snapshot: Vec::from(&partition_info.snapshot[..]),
                                domain: domain.clone(),
                                commit_op: commit_op as i32,
                                expression: partition_info.expression.clone(),
                                ..Default::default()
                            }),
                        }
                    })
                    .collect::<Result<Vec<PartitionInfo>>>()?;
                new_partition_list.push(PartitionInfo { ..Default::default() });
                let partition_version = new_partition_list.iter().map(|p| p.version).max().unwrap_or(0);
                self.transaction_insert_partition_info(new_partition_list).await?;
                info!(
                    "Commit Done for {:?}, partition_version={:?}",
                    commit_op, partition_version
                );
                Ok(())
            }

            CommitOp::CompactionCommit | CommitOp::UpdateCommit => {
                let read_partition_map: HashMap<String, PartitionInfo> = meta_info
                    .read_partition_info
                    .iter()
                    .map(|p| (p.partition_desc.clone(), p.clone()))
                    .collect();

                let mut new_partition_list = Vec::new();

                for partition_info in &meta_info.list_partition {
                    let partition_desc = &partition_info.partition_desc;
                    let mut cur_partition_info = match cur_map.get(partition_desc) {
                        Some(info) => info.clone(),
                        None => PartitionInfo {
                            table_id: table_info.table_id.clone(),
                            partition_desc: partition_desc.clone(),
                            version: 0,
                            domain: self.get_table_domain(&table_info.table_id).await?.domain,
                            ..Default::default()
                        },
                    };

                    let read_version = read_partition_map.get(partition_desc).map(|p| p.version).unwrap_or(0);

                    if read_version == cur_partition_info.version {
                        cur_partition_info.snapshot = partition_info.snapshot.clone();
                    } else {
                        // 处理版本冲突
                        // TODO: 实现版本冲突检查逻辑
                    }

                    cur_partition_info.version += 1;
                    cur_partition_info.commit_op = commit_op as i32;
                    cur_partition_info.expression = partition_info.expression.clone();

                    new_partition_list.push(cur_partition_info);
                }

                self.transaction_insert_partition_info(new_partition_list).await?;
                Ok(())
            }

            CommitOp::DeleteCommit => {
                let read_partition_map: HashMap<String, PartitionInfo> = meta_info
                    .read_partition_info
                    .iter()
                    .map(|p| (p.partition_desc.clone(), p.clone()))
                    .collect();

                let mut new_partition_list = Vec::new();

                for partition_info in &meta_info.list_partition {
                    let partition_desc = &partition_info.partition_desc;

                    if !read_partition_map.contains_key(partition_desc) {
                        continue;
                    }

                    let mut cur_partition_info = match cur_map.get(partition_desc) {
                        Some(info) => info.clone(),
                        None => continue,
                    };

                    cur_partition_info.version += 1;
                    cur_partition_info.commit_op = commit_op as i32;
                    cur_partition_info.expression = partition_info.expression.clone();
                    cur_partition_info.snapshot.clear();

                    new_partition_list.push(cur_partition_info);
                }

                self.transaction_insert_partition_info(new_partition_list).await?;
                Ok(())
            }
        }
    }

    async fn get_cur_partition_map(
        &self,
        table_id: &str,
        partition_desc_list: &[String],
    ) -> Result<HashMap<String, PartitionInfo>> {
        Ok(self
            .get_partition_info_by_table_id_and_partition_list(table_id, partition_desc_list)
            .await?
            .iter()
            .map(|partition_info| (partition_info.partition_desc.clone(), partition_info.clone()))
            .collect())
    }

    pub async fn commit_data_commit_info(&self, data_commit_info: DataCommitInfo) -> Result<()> {
        let table_id = &data_commit_info.table_id;
        let partition_desc = &data_commit_info.partition_desc;
        let commit_op = data_commit_info.commit_op;
        let commit_id = &data_commit_info
            .commit_id
            .clone()
            .ok_or(LakeSoulMetaDataError::Internal("commit_id missing".to_string()))?;
        let commit_id_str = uuid::Uuid::from_u64_pair(commit_id.high, commit_id.low).to_string();
        match self
            .get_single_data_commit_info(table_id, partition_desc, &commit_id_str)
            .await?
        {
            Some(data_commit_info) if data_commit_info.committed => {
                return Ok(());
            }
            None => {
                self.insert_data_commit_info(&data_commit_info).await?;
            }
            _ => {}
        };
        let table_info = self.get_table_info_by_table_id(table_id).await?;
        let domain = self.get_table_domain(table_id).await?.domain;
        self.commit_data(
            MetaInfo {
                table_info,
                list_partition: vec![PartitionInfo {
                    table_id: table_id.clone(),
                    partition_desc: partition_desc.clone(),
                    commit_op,
                    domain,
                    snapshot: vec![commit_id.clone()],
                    ..Default::default()
                }],
                ..Default::default()
            },
            CommitOp::try_from(commit_op)
                .map_err(|_| LakeSoulMetaDataError::Internal("unknown commit_op".to_string()))?,
        )
        .await
    }

    pub async fn get_table_domain(&self, table_id: &str) -> Result<TableNameId> {
        match self
            .execute_query(DaoType::SelectTableDomainById as i32, [table_id].join(PARAM_DELIM))
            .await
        {
            Ok(wrapper) => Ok(wrapper.table_name_id[0].clone()),
            Err(err) => Err(err),
        }
    }

    pub async fn get_all_table_name_id_by_namespace(&self, namespace: &str) -> Result<Vec<TableNameId>> {
        match self
            .execute_query(DaoType::ListTableNameByNamespace as i32, namespace.to_string())
            .await
        {
            Ok(wrapper) => Ok(wrapper.table_name_id),
            Err(e) => Err(e),
        }
    }

    pub async fn get_all_namespace(&self) -> Result<Vec<Namespace>> {
        self.execute_query(DaoType::ListNamespaces as i32, String::new())
            .await
            .map(|wrapper| wrapper.namespace)
    }

    pub async fn get_namespace_by_namespace(&self, namespace: &str) -> Result<Namespace> {
        self.execute_query(
            DaoType::SelectNamespaceByNamespace as i32,
            [namespace].join(PARAM_DELIM),
        )
        .await
        .map(|wrapper| wrapper.namespace[0].clone())
    }

    pub async fn get_table_name_id_by_table_name(&self, table_name: &str, namespace: &str) -> Result<TableNameId> {
        match self
            .execute_query(
                DaoType::SelectTableNameIdByTableName as i32,
                [table_name, namespace].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) => Ok(wrapper.table_name_id[0].clone()),
            Err(err) => Err(err),
        }
    }

    pub async fn get_table_path_id_by_table_path(&self, table_path: &str) -> Result<TablePathId> {
        match self
            .execute_query(
                DaoType::SelectTablePathIdByTablePath as i32,
                [table_path].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) => Ok(wrapper.table_path_id[0].clone()),
            Err(err) => Err(err),
        }
    }

    pub async fn get_table_info_by_table_name(&self, table_name: &str, namespace: &str) -> Result<Option<TableInfo>> {
        match self
            .execute_query(
                DaoType::SelectTableInfoByTableNameAndNameSpace as i32,
                [table_name, namespace].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) if wrapper.table_info.is_empty() => Ok(None),
            Ok(wrapper) => Ok(Some(wrapper.table_info[0].clone())),
            Err(err) => Err(err),
        }
    }

    pub async fn get_table_info_by_table_path(&self, table_path: &str) -> Result<Option<TableInfo>> {
        match self
            .execute_query(DaoType::SelectTablePathIdByTablePath as i32, table_path.to_string())
            .await
        {
            Ok(wrapper) if wrapper.table_info.is_empty() => Ok(None),
            Ok(wrapper) => Ok(Some(wrapper.table_info[0].clone())),
            Err(err) => Err(err),
        }
    }

    pub async fn get_table_info_by_table_id(&self, table_id: &str) -> Result<Option<TableInfo>> {
        match self
            .execute_query(DaoType::SelectTableInfoByTableId as i32, table_id.to_string())
            .await
        {
            Ok(wrapper) if wrapper.table_info.is_empty() => Ok(None),
            Ok(wrapper) => Ok(Some(wrapper.table_info[0].clone())),
            Err(err) => Err(err),
        }
    }

    pub async fn get_data_files_by_table_name(&self, table_name: &str, namespace: &str) -> Result<Vec<String>> {
        let table_info = self.get_table_info_by_table_name(table_name, namespace).await?;
        if let Some(table_info) = table_info {
            let partition_list = self.get_all_partition_info(table_info.table_id.as_str()).await?;
            debug!(
                "{} 's partition_list: {:?}",
                table_info.table_id.as_str(),
                partition_list
            );
            self.get_data_files_of_partitions(partition_list).await
        } else {
            Err(LakeSoulMetaDataError::NotFound(format!(
                "Table '{}' not found",
                table_name
            )))
        }
    }

    pub async fn get_data_files_of_partitions(&self, partition_list: Vec<PartitionInfo>) -> Result<Vec<String>> {
        let mut data_files = Vec::<String>::new();
        for partition_info in &partition_list {
            let _data_file_list = self.get_data_files_of_single_partition(partition_info).await?;
            data_files.extend_from_slice(&_data_file_list);
        }
        Ok(data_files)
    }

    pub async fn get_data_files_of_single_partition(&self, partition_info: &PartitionInfo) -> Result<Vec<String>> {
        let data_commit_info_list = self.get_data_commit_info_of_single_partition(partition_info).await?;
        // let data_commit_info_list = Vec::<DataCommitInfo>::new();
        let data_file_list = data_commit_info_list
            .iter()
            .flat_map(|data_commit_info| {
                data_commit_info
                    .file_ops
                    .iter()
                    .map(|file_op| file_op.path.clone())
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<String>>();
        Ok(data_file_list)
    }

    async fn get_data_commit_info_of_single_partition(
        &self,
        partition_info: &PartitionInfo,
    ) -> Result<Vec<DataCommitInfo>> {
        if partition_info.snapshot.is_empty() {
            return Ok(Vec::new());
        }
        let table_id = &partition_info.table_id;
        let partition_desc = &partition_info.partition_desc;
        let joined_commit_id = &partition_info
            .snapshot
            .iter()
            .map(|commit_id| format!("{:0>16x}{:0>16x}", commit_id.high, commit_id.low))
            .collect::<Vec<String>>()
            .join("");
        let joined_string = [table_id.as_str(), partition_desc.as_str(), joined_commit_id.as_str()].join(PARAM_DELIM);
        match self
            .execute_query(
                DaoType::ListDataCommitInfoByTableIdAndPartitionDescAndCommitList as i32,
                joined_string,
            )
            .await
        {
            Ok(wrapper) => Ok(wrapper.data_commit_info),
            Err(e) => Err(e),
        }
    }

    pub async fn get_schema_by_table_name(&self, table_name: &str, namespace: &str) -> Result<String> {
        let table_info = self.get_table_info_by_table_name(table_name, namespace).await?;
        if let Some(table_info) = table_info {
            Ok(table_info.table_schema)
        } else {
            Err(LakeSoulMetaDataError::NotFound(format!(
                "Table '{}' not found",
                table_name
            )))
        }
    }

    pub async fn get_all_partition_info(&self, table_id: &str) -> Result<Vec<PartitionInfo>> {
        match self
            .execute_query(DaoType::ListPartitionByTableId as i32, table_id.to_string())
            .await
        {
            Ok(wrapper) => Ok(wrapper.partition_info),
            Err(e) => Err(e),
        }
    }

    pub async fn get_single_data_commit_info(
        &self,
        table_id: &str,
        partition_desc: &str,
        commit_id: &str,
    ) -> Result<Option<DataCommitInfo>> {
        match self
            .execute_query(
                DaoType::SelectOneDataCommitInfoByTableIdAndPartitionDescAndCommitId as i32,
                [table_id, partition_desc, commit_id].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) => Ok(if wrapper.data_commit_info.is_empty() {
                None
            } else {
                Some(wrapper.data_commit_info[0].clone())
            }),
            Err(e) => Err(e),
        }
    }

    pub async fn get_partition_info_by_table_id_and_partition_list(
        &self,
        table_id: &str,
        partition_desc_list: &[String],
    ) -> Result<Vec<PartitionInfo>> {
        match self
            .execute_query(
                DaoType::ListPartitionDescByTableIdAndParList as i32,
                [table_id, partition_desc_list.join(PARTITION_DESC_DELIM).as_str()].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) => Ok(wrapper.partition_info),
            Err(e) => Err(e),
        }
    }

    pub fn get_client_secret(&self) -> &String {
        &self.secret
    }

    pub async fn update_table_properties(&self, table_id: &str, properties: &str) -> Result<i32> {
        // 获取现有表信息
        let table_info = self.get_table_info_by_table_id(table_id).await?;

        if let Some(table_info) = table_info {
            
            // 解析新的和原始的properties
            let new_properties: serde_json::Value = serde_json::from_str(properties)?;
            let mut new_properties = new_properties.as_object()
                .ok_or(LakeSoulMetaDataError::Internal("Invalid properties format".to_string()))?
                .clone();

            if let Ok(origin_properties) = serde_json::from_str::<serde_json::Value>(&table_info.properties) {
                if let Some(origin_obj) = origin_properties.as_object() {
                    // 如果原始properties中包含domain,保留它
                    if let Some(domain) = origin_obj.get("domain") {
                        new_properties.insert("domain".to_string(), domain.clone());
                    }
                }
            }

            // 更新properties
            self.execute_update(
                DaoType::UpdateTableInfoById as i32,
                [table_id, &serde_json::to_string(&new_properties)?].join(PARAM_DELIM),
            )
            .await
        } else {
            Err(LakeSoulMetaDataError::NotFound(format!(
                "Table '{}' not found",
                table_id
            )))
        }
    }

    pub async fn update_table_short_name(
        &self,
        table_path: &str,
        table_id: &str,
        table_name: &str,
        table_namespace: &str,
    ) -> Result<()> {
        let table_info = self.get_table_info_by_table_id(table_id).await?;
        if let Some(table_info) = table_info {
            // 检查现有表名
            if !table_info.table_name.is_empty() {
                if table_info.table_name != table_name {
                    return Err(LakeSoulMetaDataError::Internal(format!(
                        "Table name already exists {} for table id {}",
                        table_info.table_name, table_id
                    )));
                }
                return Ok(());
            }

            // 更新表信息
            self.execute_update(
                DaoType::UpdateTableInfoById as i32,
                [table_id, table_name, table_path, ""].join(PARAM_DELIM),
            )
                .await?;

            // 插入新的表名ID映射
            self.insert_table_name_id(&TableNameId {
                table_name: table_name.to_string(),
                table_id: table_id.to_string(),
                table_namespace: table_namespace.to_string(),
                domain: table_info.domain,
            })
                .await?;
            Ok(())
        } else {
            Err(LakeSoulMetaDataError::NotFound(format!(
                "Table '{}' not found",
                table_id
            )))
        }
    }
}

pub fn table_path_id_from_table_info(table_info: &TableInfo) -> TablePathId {
    TablePathId {
        table_path: table_info.table_path.clone(),
        table_id: table_info.table_id.clone(),
        table_namespace: table_info.table_namespace.clone(),
        domain: table_info.domain.clone(),
    }
}

pub fn table_name_id_from_table_info(table_info: &TableInfo) -> TableNameId {
    TableNameId {
        table_name: table_info.table_name.clone(),
        table_id: table_info.table_id.clone(),
        table_namespace: table_info.table_namespace.clone(),
        domain: table_info.domain.clone(),
    }
}
