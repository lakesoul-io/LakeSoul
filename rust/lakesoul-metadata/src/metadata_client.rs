// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Formatter};
use std::ops::DerefMut;
use std::sync::Arc;
use std::{collections::HashMap, env, fs, vec};

use prost::Message;
use tokio::sync::Mutex;
use tracing::debug;
use url::Url;

use proto::proto::entity::{
    self, CommitOp, DataCommitInfo, JniWrapper, MetaInfo, Namespace, PartitionInfo, TableInfo, TableNameId, TablePathId,
};

use crate::error::{LakeSoulMetaDataError, Result};
use crate::pooled_client::PooledClient;
use crate::{
    clean_meta_for_test, create_connection, execute_insert, execute_query, execute_update, DaoType,
    PARAM_DELIM, PARTITION_DESC_DELIM,
};

pub struct MetaDataClient {
    client: Arc<Mutex<PooledClient>>,
    max_retry: usize,
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
            Err(_) => {
                Self::from_config(
                    "host=127.0.0.1 port=5432 dbname=lakesoul_test user=lakesoul_test password=lakesoul_test"
                        .to_string(),
                )
                .await
            }
        }
    }

    pub async fn from_config(config: String) -> Result<Self> {
        Self::from_config_and_max_retry(config, 3).await
    }

    pub async fn from_config_and_max_retry(config: String, max_retry: usize) -> Result<Self> {
        let client = Arc::new(Mutex::new(create_connection(config).await?));
        Ok(Self {
            client,
            max_retry,
        })
    }

    pub async fn create_namespace(&self, namespace: Namespace) -> Result<()> {
        self.insert_namespace(&namespace).await?;
        Ok(())
    }

    pub async fn create_table(&self, table_info: TableInfo) -> Result<()> {
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
        self.delete_table_name_id_by_table_id(&table_info.table_id).await?;
        self.delete_table_path_id_by_table_id(&table_info.table_id).await?;
        self.delete_partition_info_by_table_id(&table_info.table_id).await?;
        self.delete_data_commit_info_by_table_id(&table_info.table_id).await?;
        self.delete_table_info_by_id_and_path(&table_info.table_id, &table_info.table_path)
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
            match execute_insert(
                self.client.lock().await.deref_mut(),
                insert_type,
                wrapper.clone(),
            )
            .await
            {
                Ok(count) => return Ok(count),
                Err(_) if times < self.max_retry as i64 - 1 => continue,
                Err(e) => return Err(e),
            };
        }
        Err(LakeSoulMetaDataError::Internal("unreachable".to_string()))
    }

    async fn execute_update(&self, update_type: i32, joined_string: String) -> Result<i32> {
        for times in 0..self.max_retry as i64 {
            match execute_update(
                self.client.lock().await.deref_mut(),
                update_type,
                joined_string.clone(),
            )
            .await
            {
                Ok(count) => return Ok(count),
                Err(_) if times < self.max_retry as i64 - 1 => continue,
                Err(e) => return Err(e),
            };
        }
        Err(LakeSoulMetaDataError::Internal("unreachable".to_string()))
    }

    async fn execute_query(&self, query_type: i32, joined_string: String) -> Result<JniWrapper> {
        for times in 0..self.max_retry as i64 {
            match execute_query(
                self.client.lock().await.deref_mut(),
                query_type,
                joined_string.clone(),
            )
            .await
            {
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

        if !table_info.table_name.is_empty() {
            // todo: updateTableShortName
        }
        // todo: updateTableProperties

        // conflict handling
        let _raw_map = meta_info
            .list_partition
            .iter()
            .map(|partition_info| (partition_info.partition_desc.clone(), partition_info.clone()))
            .collect::<HashMap<String, PartitionInfo>>();

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

        // conflict handling
        let cur_map = self
            .get_cur_partition_map(&table_info.table_id, &partition_desc_list)
            .await?;

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
                                cur_partition_info.domain = self.get_table_domain(&table_info.table_id)?;
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
                                domain: self.get_table_domain(&table_info.table_id)?,
                                commit_op: commit_op as i32,
                                expression: partition_info.expression.clone(),
                                ..Default::default()
                            }),
                        }
                    })
                    .collect::<Result<Vec<PartitionInfo>>>()?;
                new_partition_list.push(PartitionInfo { ..Default::default() });
                let val = self.transaction_insert_partition_info(new_partition_list).await?;
                let vec = self.get_all_partition_info(table_info.table_id.as_str()).await?;
                debug!("val = {val} ,get partition list after finished: {:?}", vec);
                Ok(())
            }
            _ => {
                todo!()
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
        let table_info = Some(self.get_table_info_by_table_id(table_id).await?);
        let domain = self.get_table_domain(table_id)?;
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

    pub fn get_table_domain(&self, _table_id: &str) -> Result<String> {
        // todo: get property table_domain
        Ok("public".to_string())
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

    pub async fn get_table_info_by_table_name(&self, table_name: &str, namespace: &str) -> Result<TableInfo> {
        match self
            .execute_query(
                DaoType::SelectTableInfoByTableNameAndNameSpace as i32,
                [table_name, namespace].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) if wrapper.table_info.is_empty() => Err(LakeSoulMetaDataError::NotFound(format!(
                "Table '{}' not found",
                table_name
            ))),
            Ok(wrapper) => Ok(wrapper.table_info[0].clone()),
            Err(err) => Err(err),
        }
    }

    pub async fn get_table_info_by_table_path(&self, table_path: &str) -> Result<TableInfo> {
        match self
            .execute_query(DaoType::SelectTablePathIdByTablePath as i32, table_path.to_string())
            .await
        {
            Ok(wrapper) if wrapper.table_info.is_empty() => Err(LakeSoulMetaDataError::NotFound(format!(
                "Table '{}' not found",
                table_path
            ))),
            Ok(wrapper) => Ok(wrapper.table_info[0].clone()),
            Err(err) => Err(err),
        }
    }

    pub async fn get_table_info_by_table_id(&self, table_id: &str) -> Result<TableInfo> {
        match self
            .execute_query(DaoType::SelectTableInfoByTableId as i32, table_id.to_string())
            .await
        {
            Ok(wrapper) => Ok(wrapper.table_info[0].clone()),
            Err(err) => Err(err),
        }
    }

    pub async fn get_data_files_by_table_name(&self, table_name: &str, namespace: &str) -> Result<Vec<String>> {
        let table_info = self.get_table_info_by_table_name(table_name, namespace).await?;
        debug!("table_info: {:?}", table_info);
        let partition_list = self.get_all_partition_info(table_info.table_id.as_str()).await?;
        debug!(
            "{} 's partition_list: {:?}",
            table_info.table_id.as_str(),
            partition_list
        );
        self.get_data_files_of_partitions(partition_list).await
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
        Ok(table_info.table_schema)
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
