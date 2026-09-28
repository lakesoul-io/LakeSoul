// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The LakeSoul metadata client for the postgres database.

use std::fmt::{Debug, Formatter};
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::{HashMap, HashSet},
    env, fs, vec,
};

use postgres::Config;
use prost::Message;
use tokio::sync::Mutex;
use url::Url;

use lakesoul_metadata_proto::entity::{
    self, CommitOp, DataCommitInfo, JniWrapper, MetaInfo, Namespace, PartitionInfo,
    TableInfo, TableNameId, TablePathId,
};

use crate::error::{LakeSoulMetaDataError, Result};
use crate::pooled_client::PooledClient;
use crate::transfusion::{DataFileInfo, parse_table_info_partitions};
use crate::{
    DaoType, PARAM_DELIM, PARTITION_DESC_DELIM, clean_meta_for_test, create_connection,
    execute_insert, execute_query, execute_update,
};

/// The metadata client for the postgres database.
pub struct MetaDataClient {
    /// The pooled client for the postgres database.
    client: Arc<Mutex<PooledClient>>,
    /// The maximum number of retries for the postgres database.
    max_retry: usize,
    /// The encoded secret for the postgres database.
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

/// The changelog of one partition over the version window
/// `(from_version_exclusive, to_version_inclusive]`.
///
/// The window is expressed in partition versions, not wall-clock time, so that
/// same-millisecond commits cannot be skipped or read twice. See
/// [`MetaDataClient::get_partition_changelog`] for the exact file semantics.
#[derive(Debug, Clone, Default)]
pub struct PartitionChangelog {
    /// The partition this changelog belongs to.
    pub partition_desc: String,
    /// Files added by append/merge commits in commit order.
    ///
    /// Compaction output files are excluded (compaction is not changelog) and
    /// `del` operations only suppress an earlier `add` of the same path.
    pub added_files: Vec<DataFileInfo>,
    /// Whether the partition was dropped by a delete commit inside the window.
    pub partition_deleted: bool,
    /// Whether the window contains an update commit, or the baseline snapshot is
    /// missing; the caller must rebuild the partition from a snapshot instead of
    /// applying `added_files` (which is empty in that case).
    pub requires_rebuild: bool,
    /// The last version included in the window. Equals the requested lower bound
    /// when the window is empty.
    pub to_version: i64,
    /// The `partition_info.timestamp` of `to_version`, or of the baseline when
    /// the window is empty.
    pub to_timestamp: i64,
}

/// The changelog of one or several partitions of a table, as returned by
/// [`MetaDataClient::get_incremental_files`].
#[derive(Debug, Clone, Default)]
pub struct IncrementalWindow {
    /// Per-partition changelogs, ordered by `partition_desc` for table-wide reads.
    pub partitions: Vec<PartitionChangelog>,
    /// All added files of `partitions`, flattened in the same order.
    pub added_files: Vec<DataFileInfo>,
    /// Partitions dropped by a delete commit inside the window.
    pub deleted_partitions: Vec<String>,
    /// Whether any partition requires a rebuild.
    pub requires_rebuild: bool,
}

impl IncrementalWindow {
    fn from_partitions(partitions: Vec<PartitionChangelog>) -> Self {
        let added_files = partitions
            .iter()
            .flat_map(|partition| partition.added_files.iter().cloned())
            .collect();
        let deleted_partitions = partitions
            .iter()
            .filter(|partition| partition.partition_deleted)
            .map(|partition| partition.partition_desc.clone())
            .collect();
        let requires_rebuild = partitions
            .iter()
            .any(|partition| partition.requires_rebuild);
        Self {
            partitions,
            added_files,
            deleted_partitions,
            requires_rebuild,
        }
    }
}

pub const PRIMARY_URL_PROP_KEY: &str = "lakesoul.pg.url=";
pub const PRIMARY_URL_ENV_KEY: &str = "LAKESOUL_PG_URL";
pub const SECONDARY_URL_PROP_KEY: &str = "lakesoul.pg.secondary.url=";
pub const SECONDARY_URL_ENV_KEY: &str = "LAKESOUL_PG_SECONDARY_URL";

const DEFAULT_PG_URL: &str =
    "jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified";
const DEFAULT_PG_USERNAME: &str = "lakesoul_test";
const DEFAULT_PG_PASSWORD: &str = "lakesoul_test";

/// Maximum number of commit attempts when concurrent writers touch the same
/// partition (mirrors `DBConfig.MAX_COMMIT_ATTEMPTS`).
const MAX_COMMIT_ATTEMPTS: usize = 5;

/// Whether a PostgreSQL error is a retryable concurrency conflict
/// (`serialization failure` / `deadlock detected`). The docker-compose test
/// environment runs `serializable`, where concurrent commits abort each
/// other's statements.
fn is_retryable_conflict(error: &LakeSoulMetaDataError) -> bool {
    let code = match error {
        LakeSoulMetaDataError::PostgresError(error) => error.code(),
        _ => None,
    };
    matches!(
        code,
        Some(&tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE)
            | Some(&tokio_postgres::error::SqlState::T_R_DEADLOCK_DETECTED)
    )
}

/// Exponential backoff with jitter for one commit retry attempt.
async fn commit_backoff(attempt: usize) {
    let base = 1u64 << (attempt as u32).min(5);
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos() as u64)
        .unwrap_or_default()
        % (base + 1);
    tokio::time::sleep(std::time::Duration::from_millis(base + jitter)).await;
}

fn secondary_url_not_found() -> LakeSoulMetaDataError {
    LakeSoulMetaDataError::NotFound("Secondary url not found".to_string())
}

fn parse_pg_config(
    pg_url: &str,
    username: &str,
    password: &str,
) -> Result<String, LakeSoulMetaDataError> {
    let pg_url = pg_url.strip_prefix("jdbc:").ok_or_else(|| {
        LakeSoulMetaDataError::Internal(
            "PostgreSQL URL must start with jdbc:".to_string(),
        )
    })?;
    let url = Url::parse(pg_url)?;
    let database = url.path().trim_start_matches('/');
    if database.is_empty() {
        return Err(LakeSoulMetaDataError::Internal(
            "url database name missing".to_string(),
        ));
    }
    Ok(format!(
        "host={} port={} dbname={} user={} password={}",
        url.host_str()
            .ok_or_else(|| LakeSoulMetaDataError::Internal(
                "url host missing".to_string()
            ))?,
        url.port().ok_or_else(|| LakeSoulMetaDataError::Internal(
            "url port missing".to_string()
        ))?,
        database,
        username,
        password
    ))
}

fn pg_config_from_properties(
    config: &str,
    url_prop: &str,
    url_env: &str,
) -> Result<String, LakeSoulMetaDataError> {
    let config_map = config
        .lines()
        .filter_map(|property| {
            property.find('=').map(|index| property.split_at(index + 1))
        })
        .collect::<HashMap<_, _>>();
    let pg_url = match config_map
        .get(url_prop)
        .copied()
        .filter(|value| !value.trim().is_empty())
    {
        Some(value) => value,
        None if url_env == SECONDARY_URL_ENV_KEY => return Err(secondary_url_not_found()),
        None => DEFAULT_PG_URL,
    };
    parse_pg_config(
        pg_url,
        config_map
            .get("lakesoul.pg.username=")
            .copied()
            .unwrap_or(DEFAULT_PG_USERNAME),
        config_map
            .get("lakesoul.pg.password=")
            .copied()
            .unwrap_or(DEFAULT_PG_PASSWORD),
    )
}

/// Generate a PostgreSQL connection config from LakeSoul properties and environment variables.
pub fn pg_config_from_env(
    url_prop: &str,
    url_env: &str,
) -> Result<String, LakeSoulMetaDataError> {
    if let Ok(config_path) = std::env::var("lakesoul_home") {
        trace!("get config from lakesoul_home: {}", config_path);
        let config = fs::read_to_string(&config_path)
            .unwrap_or_else(|_| panic!("Fails at reading a config file {}", config_path));
        return pg_config_from_properties(&config, url_prop, url_env);
    }
    if let Ok(pg_url) = std::env::var(url_env) {
        if pg_url.trim().is_empty() && url_env == SECONDARY_URL_ENV_KEY {
            return Err(secondary_url_not_found());
        }
        trace!("get config from env {}={}", url_env, pg_url);
        return parse_pg_config(
            &pg_url,
            &env::var("LAKESOUL_PG_USERNAME")
                .unwrap_or_else(|_| DEFAULT_PG_USERNAME.to_string()),
            &env::var("LAKESOUL_PG_PASSWORD")
                .unwrap_or_else(|_| DEFAULT_PG_PASSWORD.to_string()),
        );
    }
    if url_env == SECONDARY_URL_ENV_KEY {
        Err(secondary_url_not_found())
    } else {
        parse_pg_config(DEFAULT_PG_URL, DEFAULT_PG_USERNAME, DEFAULT_PG_PASSWORD)
    }
}

impl MetaDataClient {
    pub async fn from_env() -> Result<Self> {
        let config = pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY)?;
        let secondary_config =
            match pg_config_from_env(SECONDARY_URL_PROP_KEY, SECONDARY_URL_ENV_KEY) {
                Ok(config) => Some(config),
                Err(LakeSoulMetaDataError::NotFound(_)) => None,
                Err(error) => return Err(error),
            };
        Self::from_config(config, secondary_config).await
    }

    pub async fn from_config(
        config: String,
        secondary_config: Option<String>,
    ) -> Result<Self> {
        Self::from_config_and_max_retry(config, secondary_config, 3).await
    }

    #[instrument]
    pub async fn from_config_and_max_retry(
        config: String,
        secondary_config: Option<String>,
        max_retry: usize,
    ) -> Result<Self> {
        let client = Arc::new(Mutex::new(
            create_connection(config.clone(), secondary_config).await?,
        ));
        let config = config.parse::<Config>()?;
        Ok(Self {
            client,
            max_retry,
            secret: format!(
                "{:x}",
                md5::compute(
                    format!(
                        "!@{}#${}&*",
                        config.get_user().unwrap(),
                        String::from_utf8_lossy(config.get_password().unwrap())
                    )
                    .as_bytes()
                )
            ),
        })
    }

    /// The shared connection pool backing this client.
    pub(crate) fn pooled_client(&self) -> Arc<Mutex<PooledClient>> {
        self.client.clone()
    }

    /// The configured retry budget.
    pub(crate) fn max_retry(&self) -> usize {
        self.max_retry
    }

    pub async fn create_namespace(&self, namespace: Namespace) -> Result<()> {
        self.insert_namespace(&namespace).await?;
        Ok(())
    }

    /// Atomically creates all metadata rows for a table.
    pub async fn create_table(&self, table_info: TableInfo) -> Result<()> {
        info!("create_table: {:?}", &table_info);
        let inserted = self.insert_table_atomic(&table_info, false).await?;
        if inserted != 1 {
            return Err(LakeSoulMetaDataError::Internal(format!(
                "expected to insert one table, inserted {inserted}"
            )));
        }
        Ok(())
    }

    /// Atomically creates a table, or returns `false` when its name already exists.
    ///
    /// PostgreSQL's unique constraint on `(table_name, table_namespace)` is the
    /// arbiter, so concurrent callers cannot both decide to create the table.
    pub async fn create_table_if_not_exists(
        &self,
        table_info: TableInfo,
    ) -> Result<bool> {
        info!("create_table_if_not_exists: {:?}", &table_info);
        match self.insert_table_atomic(&table_info, true).await? {
            0 => Ok(false),
            1 => Ok(true),
            inserted => Err(LakeSoulMetaDataError::Internal(format!(
                "expected to insert at most one table, inserted {inserted}"
            ))),
        }
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
    pub async fn delete_table_by_table_info_cascade(
        &self,
        table_info: &TableInfo,
    ) -> Result<()> {
        self.delete_table_by_table_id_cascade(
            table_info.table_id.as_str(),
            table_info.table_path.as_str(),
        )
        .await
    }

    pub async fn drop_table(&self, table_name: &str, namespace: &str) -> Result<()> {
        let table_info = self
            .get_table_info_by_table_name(table_name, namespace)
            .await?
            .ok_or_else(|| {
                LakeSoulMetaDataError::NotFound(format!(
                    "not found {} in namespace {}",
                    table_name, namespace
                ))
            })?;
        self.delete_table_by_table_info_cascade(&table_info).await?;
        Ok(())
    }

    pub async fn delete_table_by_table_id_cascade(
        &self,
        table_id: &str,
        table_path: &str,
    ) -> Result<()> {
        self.delete_table_name_id_by_table_id(table_id).await?;
        self.delete_table_path_id_by_table_id(table_id).await?;
        self.delete_partition_info_by_table_id(table_id).await?;
        self.delete_data_commit_info_by_table_id(table_id).await?;
        self.delete_table_info_by_id_and_path(table_id, table_path)
            .await?;
        Ok(())
    }

    pub async fn delete_table_path_id_by_table_id(&self, table_id: &str) -> Result<i32> {
        self.execute_update(
            DaoType::DeleteTablePathIdByTableId as i32,
            [table_id].join(PARAM_DELIM),
        )
        .await
    }

    pub async fn delete_table_name_id_by_table_id(&self, table_id: &str) -> Result<i32> {
        self.execute_update(
            DaoType::DeleteTableNameIdByTableId as i32,
            [table_id].join(PARAM_DELIM),
        )
        .await
    }

    pub async fn delete_partition_info_by_table_id(&self, table_id: &str) -> Result<i32> {
        self.execute_update(
            DaoType::DeletePartitionInfoByTableId as i32,
            [table_id].join(PARAM_DELIM),
        )
        .await
    }
    pub async fn delete_data_commit_info_by_table_id(
        &self,
        table_id: &str,
    ) -> Result<i32> {
        self.execute_update(
            DaoType::DeleteDataCommitInfoByTableId as i32,
            [table_id].join(PARAM_DELIM),
        )
        .await
    }

    pub async fn delete_table_info_by_id_and_path(
        &self,
        id: &str,
        path: &str,
    ) -> Result<i32> {
        self.execute_update(
            DaoType::DeleteTableInfoByIdAndPath as i32,
            [id, path].join(PARAM_DELIM),
        )
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

    async fn execute_update(
        &self,
        update_type: i32,
        joined_string: String,
    ) -> Result<i32> {
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

    pub async fn execute_query_raw(
        &self,
        query_type: i32,
        joined_string: String,
    ) -> Result<Vec<u8>> {
        for times in 0..self.max_retry as i64 {
            match execute_query(
                self.client.lock().await.deref_mut(),
                query_type,
                joined_string.clone(),
            )
            .await
            {
                Ok(encoded) => {
                    return Ok(encoded);
                }
                Err(_) if times < self.max_retry as i64 - 1 => continue,
                Err(e) => return Err(e),
            };
        }
        Err(LakeSoulMetaDataError::Internal("unreachable".to_string()))
    }

    pub async fn execute_query(
        &self,
        query_type: i32,
        joined_string: String,
    ) -> Result<JniWrapper> {
        let bytes = self.execute_query_raw(query_type, joined_string).await?;
        Ok(JniWrapper::decode(prost::bytes::Bytes::from(bytes))?)
    }

    pub async fn execute_update_raw(
        &self,
        update_type: i32,
        joined_string: String,
    ) -> Result<i32> {
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

    async fn insert_table_atomic(
        &self,
        table_info: &TableInfo,
        if_not_exists: bool,
    ) -> Result<i32> {
        let dao_type = if if_not_exists {
            DaoType::InsertTableIfNotExistsAtomic
        } else {
            DaoType::InsertTableAtomic
        };
        self.execute_insert(
            dao_type as i32,
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

    async fn insert_data_commit_info(
        &self,
        data_commit_info: &DataCommitInfo,
    ) -> Result<i32> {
        self.execute_insert(
            DaoType::InsertDataCommitInfo as i32,
            JniWrapper {
                data_commit_info: vec![data_commit_info.clone()],
                ..Default::default()
            },
        )
        .await
    }

    async fn transaction_insert_partition_info(
        &self,
        partition_info_list: Vec<PartitionInfo>,
    ) -> Result<i32> {
        self.execute_insert(
            DaoType::TransactionInsertPartitionInfo as i32,
            JniWrapper {
                partition_info: partition_info_list,
                ..Default::default()
            },
        )
        .await
    }

    async fn transaction_insert_data_commit_info(
        &self,
        data_commit_info_list: Vec<DataCommitInfo>,
    ) -> Result<i32> {
        let expected_count = data_commit_info_list.len();
        let inserted_count = self
            .execute_insert(
                DaoType::TransactionInsertDataCommitInfo as i32,
                JniWrapper {
                    data_commit_info: data_commit_info_list,
                    ..Default::default()
                },
            )
            .await?;
        if inserted_count as usize != expected_count {
            return Err(LakeSoulMetaDataError::Internal(format!(
                "expected to insert {expected_count} data commits, inserted {inserted_count}"
            )));
        }
        Ok(inserted_count)
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

    pub async fn commit_data(
        &self,
        meta_info: MetaInfo,
        commit_op: CommitOp,
    ) -> Result<()> {
        let table_info = meta_info.table_info.ok_or(LakeSoulMetaDataError::Internal(
            "table info missing".to_string(),
        ))?;

        // if !table_info.table_name.is_empty() {
        //     self.update_table_short_name(&table_info.table_path, &table_info.table_id,
        //         &table_info.table_name, &table_info.table_namespace).await?;
        // }

        // self.update_table_properties(&table_info.table_id, &table_info.properties).await?;

        let domain = self
            .get_table_domain(table_info.table_id.as_str())
            .await?
            .domain;
        let partition_desc_list = meta_info
            .list_partition
            .iter()
            .map(|partition_info| partition_info.partition_desc.clone())
            .collect::<Vec<String>>();
        let read_partition_map = meta_info
            .read_partition_info
            .iter()
            .map(|partition_info| {
                (
                    partition_info.partition_desc.clone(),
                    partition_info.clone(),
                )
            })
            .collect::<HashMap<String, PartitionInfo>>();

        // Optimistic concurrency: a stale read of the current version is
        // resolved against the commits that landed in between, and the insert
        // is retried with a fresh version. Mirrors `DBManager.commitData`.
        let mut planned = HashMap::<String, PartitionInfo>::new();
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            let cur_map = match self
                .get_cur_partition_map(&table_info.table_id, &partition_desc_list)
                .await
            {
                Ok(cur_map) => cur_map,
                Err(error)
                    if is_retryable_conflict(&error) && attempt < MAX_COMMIT_ATTEMPTS =>
                {
                    commit_backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let new_partition_list = match self
                .plan_partition_commit(
                    &table_info,
                    &domain,
                    commit_op,
                    &meta_info.list_partition,
                    &read_partition_map,
                    &cur_map,
                    &mut planned,
                )
                .await
            {
                Ok(new_partition_list) => new_partition_list,
                Err(error)
                    if is_retryable_conflict(&error) && attempt < MAX_COMMIT_ATTEMPTS =>
                {
                    commit_backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };

            if new_partition_list.is_empty() {
                return Ok(());
            }

            let expected = new_partition_list.len();
            let mut partition_info_list = new_partition_list;
            partition_info_list.push(PartitionInfo::default());
            let inserted = match self
                .transaction_insert_partition_info(partition_info_list)
                .await
            {
                Ok(inserted) => inserted,
                Err(error)
                    if is_retryable_conflict(&error) && attempt < MAX_COMMIT_ATTEMPTS =>
                {
                    commit_backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if inserted as usize == expected {
                return Ok(());
            }
            debug!(
                "commit of {:?} conflicted on attempt {} (expected {} partition rows, inserted {})",
                commit_op, attempt, expected, inserted
            );
            if attempt < MAX_COMMIT_ATTEMPTS {
                commit_backoff(attempt).await;
            }
        }

        Err(LakeSoulMetaDataError::Internal(format!(
            "commit of {commit_op:?} failed after {MAX_COMMIT_ATTEMPTS} attempts because of concurrent writers on table {}",
            table_info.table_id
        )))
    }

    /// Plan the partition rows inserted by one commit attempt.
    ///
    /// Mirrors `DBManager.commitData` with its `appendConflict` /
    /// `mergeConflict` / `updateConflict` / `compactionConflict` retries folded
    /// in: when the version the caller read is stale, the commits that landed
    /// in between decide whether this commit can be merged, must be rejected or
    /// must skip the partition. `planned` carries rows computed by a previous
    /// attempt so an append that only conflicted on another partition is not
    /// applied twice.
    #[allow(clippy::too_many_arguments)]
    async fn plan_partition_commit(
        &self,
        table_info: &TableInfo,
        domain: &str,
        commit_op: CommitOp,
        list_partition: &[PartitionInfo],
        read_partition_map: &HashMap<String, PartitionInfo>,
        cur_map: &HashMap<String, PartitionInfo>,
        planned: &mut HashMap<String, PartitionInfo>,
    ) -> Result<Vec<PartitionInfo>> {
        match commit_op {
            CommitOp::AppendCommit | CommitOp::MergeCommit => {
                let mut new_partition_list = Vec::with_capacity(list_partition.len());
                for partition_info in list_partition {
                    let partition_desc = &partition_info.partition_desc;
                    let cur_version = cur_map
                        .get(partition_desc)
                        .map(|info| i64::from(info.version))
                        .unwrap_or(-1);

                    if let Some(previous) = planned.get(partition_desc)
                        && cur_version + 1 == i64::from(previous.version)
                    {
                        new_partition_list.push(previous.clone());
                        continue;
                    }

                    let mut cur_partition_info = match cur_map.get(partition_desc) {
                        Some(info) => info.clone(),
                        None => PartitionInfo {
                            table_id: table_info.table_id.clone(),
                            partition_desc: partition_desc.clone(),
                            version: -1,
                            domain: domain.to_string(),
                            ..Default::default()
                        },
                    };
                    let cur_op = cur_partition_info.commit_op();
                    let compatible = if commit_op == CommitOp::AppendCommit {
                        matches!(
                            cur_op,
                            CommitOp::AppendCommit
                                | CommitOp::MergeCommit
                                | CommitOp::CompactionCommit
                                | CommitOp::UpdateCommit
                        )
                    } else {
                        matches!(
                            cur_op,
                            CommitOp::MergeCommit
                                | CommitOp::CompactionCommit
                                | CommitOp::UpdateCommit
                        )
                    };
                    if !compatible {
                        return Err(LakeSoulMetaDataError::Internal(format!(
                            "commit of {commit_op:?} conflicts with {:?} on table {} partition {}",
                            cur_op, table_info.table_id, partition_desc
                        )));
                    }

                    cur_partition_info
                        .snapshot
                        .extend_from_slice(&partition_info.snapshot);
                    cur_partition_info.version += 1;
                    cur_partition_info.commit_op = commit_op as i32;
                    cur_partition_info.expression = partition_info.expression.clone();
                    planned.insert(partition_desc.clone(), cur_partition_info.clone());
                    new_partition_list.push(cur_partition_info);
                }
                Ok(new_partition_list)
            }

            CommitOp::CompactionCommit | CommitOp::UpdateCommit => {
                let mut new_partition_list = Vec::new();
                for partition_info in list_partition {
                    let partition_desc = &partition_info.partition_desc;
                    let mut cur_partition_info = match cur_map.get(partition_desc) {
                        Some(info) => info.clone(),
                        None => PartitionInfo {
                            table_id: table_info.table_id.clone(),
                            partition_desc: partition_desc.clone(),
                            version: -1,
                            domain: domain.to_string(),
                            ..Default::default()
                        },
                    };

                    let read_version = read_partition_map
                        .get(partition_desc)
                        .map(|info| i64::from(info.version))
                        .unwrap_or(0);
                    let cur_version = i64::from(cur_partition_info.version);

                    if read_version == cur_version {
                        cur_partition_info.snapshot = partition_info.snapshot.clone();
                    } else {
                        let middle_ops = self
                            .get_commit_ops_between_versions(
                                &table_info.table_id,
                                partition_desc,
                                read_version + 1,
                                cur_version,
                            )
                            .await?;
                        let has_update = middle_ops.contains(&CommitOp::UpdateCommit);
                        let has_compaction =
                            middle_ops.contains(&CommitOp::CompactionCommit);

                        if commit_op == CommitOp::UpdateCommit {
                            if read_version > 0
                                && (has_update
                                    || (middle_ops.len() > 1 && has_compaction))
                            {
                                return Err(LakeSoulMetaDataError::Internal(format!(
                                    "update commit conflicts with concurrent writes on table {} partition {} (read version {read_version}, current version {cur_version}, middle commits {middle_ops:?})",
                                    table_info.table_id, partition_desc
                                )));
                            }
                            if middle_ops.len() == 1 && has_compaction {
                                let middle_versions = self
                                    .get_partition_versions_in_range(
                                        &table_info.table_id,
                                        partition_desc,
                                        read_version + 1,
                                        cur_version,
                                    )
                                    .await?;
                                let compaction_has_concurrent_appends =
                                    middle_versions.iter().any(|version| {
                                        version.commit_op() == CommitOp::CompactionCommit
                                            && version.snapshot.len() > 1
                                    });
                                if compaction_has_concurrent_appends {
                                    return Err(LakeSoulMetaDataError::Internal(
                                        format!(
                                            "update commit conflicts with a compaction on table {} partition {}",
                                            table_info.table_id, partition_desc
                                        ),
                                    ));
                                }
                                cur_partition_info.snapshot =
                                    partition_info.snapshot.clone();
                            } else {
                                merge_submitted_snapshot(
                                    &mut cur_partition_info,
                                    partition_info,
                                    read_partition_map.get(partition_desc),
                                );
                            }
                        } else {
                            // A compaction only folds the files it read; a
                            // concurrent append is preserved in the merged
                            // snapshot. If a compaction or update landed in
                            // between, drop this partition from the commit:
                            // the concurrent writer already owns a valid
                            // snapshot.
                            if has_update || has_compaction {
                                continue;
                            }
                            merge_submitted_snapshot(
                                &mut cur_partition_info,
                                partition_info,
                                read_partition_map.get(partition_desc),
                            );
                        }
                    }

                    cur_partition_info.version += 1;
                    cur_partition_info.commit_op = commit_op as i32;
                    cur_partition_info.expression = partition_info.expression.clone();
                    new_partition_list.push(cur_partition_info);
                }
                Ok(new_partition_list)
            }

            CommitOp::DeleteCommit => {
                let mut new_partition_list = Vec::new();
                for partition_info in list_partition {
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
                Ok(new_partition_list)
            }
        }
    }

    /// The distinct commit ops of the partition versions in
    /// `[from_version, to_version]`.
    async fn get_commit_ops_between_versions(
        &self,
        table_id: &str,
        partition_desc: &str,
        from_version: i64,
        to_version: i64,
    ) -> Result<Vec<CommitOp>> {
        if to_version < from_version {
            return Ok(Vec::new());
        }
        let wrapper = self
            .execute_query(
                DaoType::ListCommitOpsBetweenVersions as i32,
                [
                    table_id,
                    partition_desc,
                    &clamp_version(from_version).to_string(),
                    &clamp_version(to_version).to_string(),
                ]
                .join(PARAM_DELIM),
            )
            .await?;
        Ok(wrapper
            .partition_info
            .iter()
            .map(|info| info.commit_op())
            .collect())
    }

    async fn get_cur_partition_map(
        &self,
        table_id: &str,
        partition_desc_list: &[String],
    ) -> Result<HashMap<String, PartitionInfo>> {
        Ok(self
            .get_partition_info_by_table_id_and_partition_list(
                table_id,
                partition_desc_list,
            )
            .await?
            .iter()
            .map(|partition_info| {
                (
                    partition_info.partition_desc.clone(),
                    partition_info.clone(),
                )
            })
            .collect())
    }

    pub async fn commit_data_commit_info(
        &self,
        data_commit_info: DataCommitInfo,
    ) -> Result<()> {
        let table_id = &data_commit_info.table_id;
        let partition_desc = &data_commit_info.partition_desc;
        let commit_op = data_commit_info.commit_op;
        let commit_id =
            &data_commit_info
                .commit_id
                .ok_or(LakeSoulMetaDataError::Internal(
                    "commit_id missing".to_string(),
                ))?;
        let commit_id_str =
            uuid::Uuid::from_u64_pair(commit_id.high, commit_id.low).to_string();
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
                    snapshot: vec![*commit_id],
                    ..Default::default()
                }],
                ..Default::default()
            },
            CommitOp::try_from(commit_op).map_err(|_| {
                LakeSoulMetaDataError::Internal("unknown commit_op".to_string())
            })?,
        )
        .await
    }

    /// Commit data files for one table and publish all affected partitions together.
    /// Commit the given files and return the LakeSoul commit ids that were
    /// created (one per partition).
    pub async fn commit_data_files(
        &self,
        table_name: &str,
        namespace: &str,
        files: Vec<DataFileInfo>,
    ) -> Result<Vec<String>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }

        let table_info = self
            .get_table_info_by_table_name(table_name, namespace)
            .await?
            .ok_or_else(|| {
                LakeSoulMetaDataError::NotFound(format!(
                    "table {table_name} is not found in namespace {namespace}"
                ))
            })?;
        let (_, primary_keys) = parse_table_info_partitions(&table_info.partitions);
        let commit_op = if primary_keys.is_empty() {
            CommitOp::AppendCommit
        } else {
            CommitOp::MergeCommit
        };
        self.commit_data_files_with_commit_op(table_name, namespace, files, commit_op)
            .await
    }

    /// Commit data files for one table using an explicit commit operation.
    /// Commit the given files with an explicit commit op and return the
    /// LakeSoul commit ids that were created (one per partition).
    pub async fn commit_data_files_with_commit_op(
        &self,
        table_name: &str,
        namespace: &str,
        files: Vec<DataFileInfo>,
        commit_op: CommitOp,
    ) -> Result<Vec<String>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }

        let table_info = self
            .get_table_info_by_table_name(table_name, namespace)
            .await?
            .ok_or_else(|| {
                LakeSoulMetaDataError::NotFound(format!(
                    "table {table_name} is not found in namespace {namespace}"
                ))
            })?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| LakeSoulMetaDataError::Internal(error.to_string()))?
            .as_millis() as i64;
        let domain = self.get_table_domain(&table_info.table_id).await?.domain;

        let data_commit_info_list = data_commit_info_list_from_files(
            &table_info,
            files,
            commit_op,
            timestamp,
            &domain,
        );
        let commit_ids = data_commit_info_list
            .iter()
            .filter_map(|info| info.commit_id)
            .map(|id| uuid::Uuid::from_u64_pair(id.high, id.low).to_string())
            .collect::<Vec<_>>();

        self.transaction_insert_data_commit_info(data_commit_info_list.clone())
            .await?;
        self.commit_data(
            MetaInfo {
                table_info: Some(table_info.clone()),
                list_partition: data_commit_info_list
                    .iter()
                    .map(|data_commit_info| PartitionInfo {
                        table_id: table_info.table_id.clone(),
                        partition_desc: data_commit_info.partition_desc.clone(),
                        commit_op: commit_op.into(),
                        domain: domain.clone(),
                        snapshot: data_commit_info.commit_id.into_iter().collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            commit_op,
        )
        .await?;
        Ok(commit_ids)
    }

    pub async fn get_table_domain(&self, table_id: &str) -> Result<TableNameId> {
        match self
            .execute_query(
                DaoType::SelectTableDomainById as i32,
                [table_id].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) => wrapper.table_name_id.into_iter().next().ok_or_else(|| {
                LakeSoulMetaDataError::NotFound(format!(
                    "table {table_id} has no domain row"
                ))
            }),
            Err(err) => Err(err),
        }
    }

    pub async fn get_all_table_name_id_by_namespace(
        &self,
        namespace: &str,
    ) -> Result<Vec<TableNameId>> {
        match self
            .execute_query(
                DaoType::ListTableNameByNamespace as i32,
                namespace.to_string(),
            )
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

    pub async fn get_namespace_by_namespace(
        &self,
        namespace: &str,
    ) -> Result<Option<Namespace>> {
        match self
            .execute_query(
                DaoType::SelectNamespaceByNamespace as i32,
                [namespace].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) if wrapper.namespace.is_empty() => Ok(None),
            Ok(wrapper) => Ok(Some(wrapper.namespace[0].clone())),
            Err(err) => Err(err),
        }
    }

    pub async fn get_table_name_id_by_table_name(
        &self,
        table_name: &str,
        namespace: &str,
    ) -> Result<Option<TableNameId>> {
        match self
            .execute_query(
                DaoType::SelectTableNameIdByTableName as i32,
                [table_name, namespace].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) if wrapper.table_name_id.is_empty() => Ok(None),
            Ok(wrapper) => Ok(Some(wrapper.table_name_id[0].clone())),
            Err(err) => Err(err),
        }
    }

    pub async fn get_table_path_id_by_table_path(
        &self,
        table_path: &str,
    ) -> Result<Option<TablePathId>> {
        match self
            .execute_query(
                DaoType::SelectTablePathIdByTablePath as i32,
                [table_path].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) => {
                if wrapper.table_path_id.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(wrapper.table_path_id[0].clone()))
                }
            }
            Err(err) => Err(err),
        }
    }

    pub async fn get_table_info_by_table_name(
        &self,
        table_name: &str,
        namespace: &str,
    ) -> Result<Option<TableInfo>> {
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

    pub async fn get_table_info_by_table_path(
        &self,
        table_path: &str,
    ) -> Result<Option<TableInfo>> {
        match self
            .execute_query(
                DaoType::SelectTableInfoByTablePath as i32,
                table_path.to_string(),
            )
            .await
        {
            Ok(wrapper) if wrapper.table_info.is_empty() => Ok(None),
            Ok(wrapper) => Ok(Some(wrapper.table_info[0].clone())),
            Err(err) => Err(err),
        }
    }

    pub async fn get_table_info_by_table_id(
        &self,
        table_id: &str,
    ) -> Result<Option<TableInfo>> {
        match self
            .execute_query(
                DaoType::SelectTableInfoByTableId as i32,
                table_id.to_string(),
            )
            .await
        {
            Ok(wrapper) if wrapper.table_info.is_empty() => Ok(None),
            Ok(wrapper) => Ok(Some(wrapper.table_info[0].clone())),
            Err(err) => Err(err),
        }
    }

    pub async fn get_data_files_by_table_name(
        &self,
        table_name: &str,
        namespace: &str,
    ) -> Result<Vec<String>> {
        let table_info = self
            .get_table_info_by_table_name(table_name, namespace)
            .await?;
        if let Some(table_info) = table_info {
            let partition_list = self
                .get_all_partition_info(table_info.table_id.as_str())
                .await?;
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

    pub async fn get_data_files_of_partitions(
        &self,
        partition_list: Vec<PartitionInfo>,
    ) -> Result<Vec<String>> {
        let mut data_files = Vec::<String>::new();
        for partition_info in &partition_list {
            let _data_file_list = self
                .get_data_files_of_single_partition(partition_info)
                .await?;
            data_files.extend_from_slice(&_data_file_list);
        }
        Ok(data_files)
    }

    pub async fn get_data_files_of_single_partition(
        &self,
        partition_info: &PartitionInfo,
    ) -> Result<Vec<String>> {
        let data_commit_info_list = self
            .get_data_commit_info_of_single_partition(partition_info)
            .await?;
        Ok(active_data_files(&data_commit_info_list))
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
        let joined_string = [
            table_id.as_str(),
            partition_desc.as_str(),
            joined_commit_id.as_str(),
        ]
        .join(PARAM_DELIM);
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

    pub async fn get_schema_by_table_name(
        &self,
        table_name: &str,
        namespace: &str,
    ) -> Result<String> {
        let table_info = self
            .get_table_info_by_table_name(table_name, namespace)
            .await?;
        if let Some(table_info) = table_info {
            Ok(table_info.table_schema)
        } else {
            Err(LakeSoulMetaDataError::NotFound(format!(
                "Table '{}' not found",
                table_name
            )))
        }
    }

    pub async fn get_all_partition_info(
        &self,
        table_id: &str,
    ) -> Result<Vec<PartitionInfo>> {
        match self
            .execute_query(DaoType::ListPartitionByTableId as i32, table_id.to_string())
            .await
        {
            Ok(wrapper) => Ok(wrapper.partition_info),
            Err(e) => Err(e),
        }
    }

    /// Get the latest version of every partition of the table at or before `as_of_ms`.
    ///
    /// `as_of_ms` is compared against `partition_info.timestamp`, which is the
    /// PostgreSQL wall clock (milliseconds) at which the partition version row was
    /// inserted. Partitions created after `as_of_ms` are not returned.
    pub async fn get_all_partition_info_as_of(
        &self,
        table_id: &str,
        as_of_ms: i64,
    ) -> Result<Vec<PartitionInfo>> {
        match self
            .execute_query(
                DaoType::ListPartitionByTableIdAndTimestamp as i32,
                [table_id, &as_of_ms.to_string()].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) => Ok(wrapper.partition_info),
            Err(e) => Err(e),
        }
    }

    /// Get the latest version of one partition at or before `as_of_ms`.
    ///
    /// Returns `None` when the partition has no version committed at or before
    /// `as_of_ms`. See [`MetaDataClient::get_all_partition_info_as_of`] for the
    /// meaning of `as_of_ms`.
    pub async fn get_partition_info_as_of(
        &self,
        table_id: &str,
        partition_desc: &str,
        as_of_ms: i64,
    ) -> Result<Option<PartitionInfo>> {
        match self
            .execute_query(
                DaoType::SelectOnePartitionVersionByTableIdAndDescAndTimestamp as i32,
                [table_id, partition_desc, &as_of_ms.to_string()].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) => Ok(wrapper.partition_info.into_iter().next()),
            Err(e) => Err(e),
        }
    }

    /// Get one exact historical version of a partition.
    ///
    /// Returns `None` when there is no row for `(table_id, partition_desc, version)`.
    pub async fn get_partition_info_by_version(
        &self,
        table_id: &str,
        partition_desc: &str,
        version: i32,
    ) -> Result<Option<PartitionInfo>> {
        match self
            .execute_query(
                DaoType::SelectPartitionVersionByTableIdAndDescAndVersion as i32,
                [table_id, partition_desc, &version.to_string()].join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) => Ok(wrapper.partition_info.into_iter().next()),
            Err(e) => Err(e),
        }
    }

    async fn get_partition_versions_in_range(
        &self,
        table_id: &str,
        partition_desc: &str,
        from_version_inclusive: i64,
        to_version_inclusive: i64,
    ) -> Result<Vec<PartitionInfo>> {
        if to_version_inclusive < from_version_inclusive {
            return Ok(Vec::new());
        }
        match self
            .execute_query(
                DaoType::ListPartitionVersionByTableIdAndPartitionDescAndVersionRange
                    as i32,
                [
                    table_id,
                    partition_desc,
                    &clamp_version(from_version_inclusive).to_string(),
                    &clamp_version(to_version_inclusive).to_string(),
                ]
                .join(PARAM_DELIM),
            )
            .await
        {
            Ok(wrapper) => Ok(wrapper.partition_info),
            Err(e) => Err(e),
        }
    }

    /// Read the changelog of one partition over the version window
    /// `(from_version_exclusive, to_version_inclusive]`.
    ///
    /// The window is consumed by partition version instead of wall-clock time so
    /// that millisecond timestamp collisions cannot skip or duplicate commits.
    /// The returned files mirror the JVM incremental read
    /// (`DataOperation.getSinglePartitionIncrementalDataInfos`):
    ///
    /// * append/merge snapshots are accumulated and subtracted against the
    ///   baseline snapshot at `from_version_exclusive`;
    /// * a compaction output (`snapshot[0]` of a compaction version) is excluded
    ///   because compaction is not changelog, while `snapshot[1..]` (concurrent
    ///   appends) is kept;
    /// * `del` file operations only suppress an earlier `add` of the same path.
    ///
    /// Two behaviours deliberately differ from the JVM implementation: an update
    /// commit inside the window (or a missing baseline) sets
    /// [`PartitionChangelog::requires_rebuild`] instead of silently returning no
    /// files, and a delete commit is surfaced through
    /// [`PartitionChangelog::partition_deleted`].
    pub async fn get_partition_changelog(
        &self,
        table_id: &str,
        partition_desc: &str,
        from_version_exclusive: i64,
        to_version_inclusive: i64,
    ) -> Result<PartitionChangelog> {
        let mut changelog = PartitionChangelog {
            partition_desc: partition_desc.to_string(),
            to_version: from_version_exclusive,
            ..Default::default()
        };

        let baseline = if from_version_exclusive >= 0 {
            self.get_partition_info_by_version(
                table_id,
                partition_desc,
                clamp_version(from_version_exclusive),
            )
            .await?
        } else {
            None
        };
        if let Some(baseline) = &baseline {
            changelog.to_timestamp = baseline.timestamp;
        }

        let lower = from_version_exclusive.saturating_add(1).max(0);
        let window = self
            .get_partition_versions_in_range(
                table_id,
                partition_desc,
                lower,
                to_version_inclusive,
            )
            .await?;

        if window.is_empty() {
            if baseline.is_none() && from_version_exclusive >= 0 {
                let latest = self
                    .get_partition_info_by_table_id_and_partition_list(
                        table_id,
                        &[partition_desc.to_string()],
                    )
                    .await?
                    .into_iter()
                    .next();
                match latest {
                    Some(latest) => {
                        // History below the cursor is gone: the diff cannot be trusted.
                        changelog.requires_rebuild = true;
                        changelog.to_timestamp = latest.timestamp;
                    }
                    None => changelog.partition_deleted = true,
                }
            }
            return Ok(changelog);
        }

        if baseline.is_none() && from_version_exclusive >= 0 {
            // Report the window bound for observability, then ask for a rebuild.
            let last = window.last().expect("window is not empty");
            changelog.to_version = i64::from(last.version);
            changelog.to_timestamp = last.timestamp;
            changelog.requires_rebuild = true;
            return Ok(changelog);
        }

        let mut added_ids: Vec<entity::Uuid> = Vec::new();
        let mut seen_ids: HashSet<(u64, u64)> = HashSet::new();
        for row in &window {
            let commit_op = row.commit_op();
            if commit_op == CommitOp::UpdateCommit {
                changelog.requires_rebuild = true;
            }
            if commit_op == CommitOp::DeleteCommit && row.snapshot.is_empty() {
                changelog.partition_deleted = true;
            }
            let snapshot_ids = if commit_op == CommitOp::CompactionCommit {
                // snapshot[0] is the compaction output, everything after it is a
                // concurrently appended commit that must stay in the changelog.
                row.snapshot.get(1..).unwrap_or_default()
            } else {
                row.snapshot.as_slice()
            };
            for id in snapshot_ids {
                push_unique_uuid(&mut added_ids, &mut seen_ids, id);
            }
            changelog.to_version = i64::from(row.version);
            changelog.to_timestamp = row.timestamp;
        }

        if changelog.requires_rebuild || changelog.partition_deleted {
            // A partial window must never be applied as a changelog.
            return Ok(changelog);
        }

        if let Some(baseline) = &baseline {
            let baseline_ids = baseline
                .snapshot
                .iter()
                .map(|id| (id.high, id.low))
                .collect::<HashSet<_>>();
            added_ids.retain(|id| !baseline_ids.contains(&(id.high, id.low)));
        }

        if !added_ids.is_empty() {
            let snapshot = PartitionInfo {
                table_id: table_id.to_string(),
                partition_desc: partition_desc.to_string(),
                snapshot: added_ids,
                ..Default::default()
            };
            let commits = self
                .get_data_commit_info_of_single_partition(&snapshot)
                .await?;
            changelog.added_files = active_added_files(&commits);
        }

        Ok(changelog)
    }

    /// Read the changelog of one partition, or of every current partition when
    /// `partition_desc` is `None`.
    ///
    /// When `partition_desc` is `None` the same `from_version_exclusive` is used
    /// for every partition and each partition's window is capped at its latest
    /// version. Partitions dropped through a delete commit are only reported when
    /// their `partition_desc` is requested explicitly.
    pub async fn get_incremental_files(
        &self,
        table_id: &str,
        partition_desc: Option<&str>,
        from_version_exclusive: i64,
        to_version_inclusive: i64,
    ) -> Result<IncrementalWindow> {
        let changelogs = match partition_desc {
            Some(partition_desc) => vec![
                self.get_partition_changelog(
                    table_id,
                    partition_desc,
                    from_version_exclusive,
                    to_version_inclusive,
                )
                .await?,
            ],
            None => {
                let latest_partitions = self.get_all_partition_info(table_id).await?;
                let mut changelogs = Vec::with_capacity(latest_partitions.len());
                for partition in latest_partitions {
                    changelogs.push(
                        self.get_partition_changelog(
                            table_id,
                            &partition.partition_desc,
                            from_version_exclusive,
                            to_version_inclusive.min(i64::from(partition.version)),
                        )
                        .await?,
                    );
                }
                changelogs
            }
        };
        Ok(IncrementalWindow::from_partitions(changelogs))
    }

    pub async fn get_single_data_commit_info(
        &self,
        table_id: &str,
        partition_desc: &str,
        commit_id: &str,
    ) -> Result<Option<DataCommitInfo>> {
        match self
            .execute_query(
                DaoType::SelectOneDataCommitInfoByTableIdAndPartitionDescAndCommitId
                    as i32,
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
                [
                    table_id,
                    partition_desc_list.join(PARTITION_DESC_DELIM).as_str(),
                ]
                .join(PARAM_DELIM),
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

    pub async fn update_table_properties(
        &self,
        table_id: &str,
        properties: &str,
    ) -> Result<i32> {
        // 获取现有表信息
        let table_info = self.get_table_info_by_table_id(table_id).await?;

        if let Some(table_info) = table_info {
            // 解析新的和原始的properties
            let new_properties: serde_json::Value = serde_json::from_str(properties)?;
            let mut new_properties = new_properties
                .as_object()
                .ok_or(LakeSoulMetaDataError::Internal(
                    "Invalid properties format".to_string(),
                ))?
                .clone();

            if let Ok(origin_properties) =
                serde_json::from_str::<serde_json::Value>(&table_info.properties)
                && let Some(origin_obj) = origin_properties.as_object()
            {
                // 如果原始properties中包含domain,保留它
                if let Some(domain) = origin_obj.get("domain") {
                    new_properties.insert("domain".to_string(), domain.clone());
                }
            }

            // 更新properties
            self.execute_update(
                DaoType::UpdateTableInfoPropertiesById as i32,
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

fn active_data_files(commits: &[DataCommitInfo]) -> Vec<String> {
    let mut deleted = HashSet::new();
    let mut active = Vec::new();

    for file_op in commits
        .iter()
        .flat_map(|commit| commit.file_ops.iter())
        .rev()
    {
        match file_op.file_op() {
            entity::FileOp::Del => {
                deleted.insert(&file_op.path);
            }
            entity::FileOp::Add if !deleted.contains(&file_op.path) => {
                active.push(file_op.path.clone());
            }
            entity::FileOp::Add => {}
        }
    }
    active.reverse();
    active
}

/// Turn resolved commits into added files, mirroring the JVM `filterFiles`
/// behaviour: the operations are walked backwards so a `del` suppresses an
/// earlier `add` of the same path, then the commit order is restored. Only
/// `add` operations are returned.
fn active_added_files(commits: &[DataCommitInfo]) -> Vec<DataFileInfo> {
    let operations = commits
        .iter()
        .flat_map(|commit| commit.file_ops.iter().map(move |file_op| (commit, file_op)))
        .collect::<Vec<_>>();

    let mut deleted_paths = HashSet::new();
    let mut active = Vec::new();
    for (commit, file_op) in operations.iter().rev() {
        match file_op.file_op() {
            entity::FileOp::Del => {
                deleted_paths.insert(file_op.path.as_str());
            }
            entity::FileOp::Add if !deleted_paths.contains(file_op.path.as_str()) => {
                active.push(DataFileInfo {
                    partition_desc: commit.partition_desc.clone(),
                    path: file_op.path.clone(),
                    file_op: "add".to_string(),
                    size: file_op.size,
                    bucket_id: None,
                    modification_time: commit.timestamp,
                    file_exist_cols: file_op.file_exist_cols.clone(),
                });
            }
            entity::FileOp::Add => {}
        }
    }
    active.reverse();
    active
}

/// Clamp a partition version into the `i32` range used by the metadata tables.
fn clamp_version(version: i64) -> i32 {
    version.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32
}

fn push_unique_uuid(
    ids: &mut Vec<entity::Uuid>,
    seen: &mut HashSet<(u64, u64)>,
    id: &entity::Uuid,
) {
    if seen.insert((id.high, id.low)) {
        ids.push(*id);
    }
}

/// Merge the snapshot submitted by a stale commit with the commits that landed
/// concurrently: `submitted ++ (current − read)`, mirroring the JVM
/// `updateSubmitPartitionSnapshot`.
fn merge_submitted_snapshot(
    current: &mut PartitionInfo,
    submitted: &PartitionInfo,
    read: Option<&PartitionInfo>,
) {
    let mut snapshot = submitted.snapshot.clone();
    let read_ids = read
        .map(|info| {
            info.snapshot
                .iter()
                .map(|id| (id.high, id.low))
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let mut concurrent = std::mem::take(&mut current.snapshot);
    concurrent.retain(|id| !read_ids.contains(&(id.high, id.low)));
    snapshot.extend(concurrent);
    current.snapshot = snapshot;
}

fn data_commit_info_list_from_files(
    table_info: &TableInfo,
    files: Vec<DataFileInfo>,
    commit_op: CommitOp,
    timestamp: i64,
    domain: &str,
) -> Vec<DataCommitInfo> {
    let mut partition_files: HashMap<String, Vec<DataFileInfo>> = HashMap::new();
    for file in files {
        partition_files
            .entry(file.partition_desc.clone())
            .or_default()
            .push(file);
    }

    partition_files
        .into_iter()
        .map(|(partition_desc, files)| {
            let (high, low) = uuid::Uuid::new_v4().as_u64_pair();
            DataCommitInfo {
                table_id: table_info.table_id.clone(),
                partition_desc,
                commit_id: Some(entity::Uuid { high, low }),
                pinned: false,
                file_ops: files
                    .into_iter()
                    .map(|file| entity::DataFileOp {
                        path: file.path,
                        file_op: entity::FileOp::Add.into(),
                        size: file.size,
                        file_exist_cols: file.file_exist_cols,
                    })
                    .collect(),
                commit_op: commit_op.into(),
                timestamp,
                committed: false,
                domain: domain.to_string(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_secondary_url_in_properties_does_not_use_primary_default() {
        let config = "lakesoul.pg.url=jdbc:postgresql://primary.example:5432/production\n\
                      lakesoul.pg.username=production_user\n\
                      lakesoul.pg.password=secret\n";

        let error = pg_config_from_properties(
            config,
            SECONDARY_URL_PROP_KEY,
            SECONDARY_URL_ENV_KEY,
        )
        .unwrap_err();

        assert!(matches!(error, LakeSoulMetaDataError::NotFound(_)));
        assert!(!error.to_string().contains("127.0.0.1"));
        assert!(!error.to_string().contains("lakesoul_test"));
    }

    #[test]
    fn configured_secondary_url_is_used() {
        let config = "lakesoul.pg.secondary.url=jdbc:postgresql://secondary.example:5433/production\n\
             lakesoul.pg.username=production_user\n\
             lakesoul.pg.password=secret\n";

        let secondary = pg_config_from_properties(
            config,
            SECONDARY_URL_PROP_KEY,
            SECONDARY_URL_ENV_KEY,
        )
        .unwrap();

        assert_eq!(
            secondary,
            "host=secondary.example port=5433 dbname=production \
             user=production_user password=secret"
        );
    }

    #[test]
    fn data_files_are_grouped_into_partition_commits() {
        let table_info = TableInfo {
            table_id: "table-id".to_string(),
            ..Default::default()
        };
        let files = vec![
            DataFileInfo {
                partition_desc: "part=a".to_string(),
                path: "file-a-1.parquet".to_string(),
                size: 10,
                file_exist_cols: "id,value".to_string(),
                ..Default::default()
            },
            DataFileInfo {
                partition_desc: "part=a".to_string(),
                path: "file-a-2.parquet".to_string(),
                size: 20,
                file_exist_cols: "id,value".to_string(),
                ..Default::default()
            },
            DataFileInfo {
                partition_desc: "part=b".to_string(),
                path: "file-b.parquet".to_string(),
                size: 30,
                file_exist_cols: "id,value".to_string(),
                ..Default::default()
            },
        ];

        let mut commits = data_commit_info_list_from_files(
            &table_info,
            files,
            CommitOp::MergeCommit,
            123,
            "public",
        );
        commits.sort_by(|left, right| left.partition_desc.cmp(&right.partition_desc));

        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].partition_desc, "part=a");
        assert_eq!(commits[0].file_ops.len(), 2);
        assert_eq!(commits[1].partition_desc, "part=b");
        assert_eq!(commits[1].file_ops[0].size, 30);
        assert_eq!(commits[0].commit_op(), CommitOp::MergeCommit);
        assert_eq!(commits[0].timestamp, 123);
        assert_eq!(commits[0].domain, "public");
        assert!(commits.iter().all(|commit| commit.commit_id.is_some()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_create_table_if_not_exists_has_one_winner() {
        // Separate clients exercise PostgreSQL arbitration rather than the
        // per-client mutex.
        let first_client = MetaDataClient::from_env().await.unwrap();
        let second_client = MetaDataClient::from_env().await.unwrap();
        let suffix = uuid::Uuid::new_v4();
        let table_name = format!("concurrent_create_{suffix}");
        let first = TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_namespace: "default".to_string(),
            table_name: table_name.clone(),
            table_path: format!("file:///tmp/{table_name}_first"),
            properties: "{}".to_string(),
            domain: "public".to_string(),
            ..Default::default()
        };
        let second = TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_path: format!("file:///tmp/{table_name}_second"),
            ..first.clone()
        };

        let (first_result, second_result) = tokio::join!(
            first_client.create_table_if_not_exists(first.clone()),
            second_client.create_table_if_not_exists(second.clone()),
        );
        let outcomes = [first_result.unwrap(), second_result.unwrap()];
        assert_eq!(outcomes.into_iter().filter(|created| *created).count(), 1);

        let stored = first_client
            .get_table_info_by_table_name(&table_name, "default")
            .await
            .unwrap()
            .expect("the winning table must be complete");
        let winner = if stored.table_id == first.table_id {
            &first
        } else {
            assert_eq!(stored.table_id, second.table_id);
            &second
        };
        let loser = if winner.table_id == first.table_id {
            &second
        } else {
            &first
        };
        assert_eq!(stored, *winner);
        assert_eq!(
            first_client
                .get_table_path_id_by_table_path(&winner.table_path)
                .await
                .unwrap()
                .unwrap()
                .table_id,
            winner.table_id
        );
        assert!(
            first_client
                .get_table_path_id_by_table_path(&loser.table_path)
                .await
                .unwrap()
                .is_none(),
            "the losing statement must not leave partial metadata"
        );

        first_client
            .delete_table_by_table_info_cascade(&stored)
            .await
            .unwrap();
    }

    #[test]
    fn active_data_files_apply_operations_in_snapshot_order() {
        let file_op = |path: &str, operation: entity::FileOp| entity::DataFileOp {
            path: path.to_string(),
            file_op: operation.into(),
            ..Default::default()
        };
        let commits = vec![
            DataCommitInfo {
                file_ops: vec![
                    file_op("replaced.parquet", entity::FileOp::Add),
                    file_op("readded.parquet", entity::FileOp::Add),
                    file_op("active.parquet", entity::FileOp::Add),
                ],
                ..Default::default()
            },
            DataCommitInfo {
                file_ops: vec![
                    file_op("replaced.parquet", entity::FileOp::Del),
                    file_op("readded.parquet", entity::FileOp::Del),
                ],
                ..Default::default()
            },
            DataCommitInfo {
                file_ops: vec![
                    file_op("readded.parquet", entity::FileOp::Add),
                    file_op("new.parquet", entity::FileOp::Add),
                ],
                ..Default::default()
            },
        ];

        assert_eq!(
            active_data_files(&commits),
            vec!["active.parquet", "readded.parquet", "new.parquet"]
        );
    }

    #[test]
    fn added_files_drop_earlier_adds_of_deleted_paths() {
        let file_op = |path: &str, operation: entity::FileOp| entity::DataFileOp {
            path: path.to_string(),
            file_op: operation.into(),
            size: 7,
            file_exist_cols: "id".to_string(),
        };
        let commit = |file_ops: Vec<entity::DataFileOp>| DataCommitInfo {
            partition_desc: "part=a".to_string(),
            timestamp: 42,
            file_ops,
            ..Default::default()
        };
        let commits = vec![
            commit(vec![
                file_op("replaced.parquet", entity::FileOp::Add),
                file_op("readded.parquet", entity::FileOp::Add),
                file_op("kept.parquet", entity::FileOp::Add),
            ]),
            commit(vec![
                file_op("replaced.parquet", entity::FileOp::Del),
                file_op("readded.parquet", entity::FileOp::Del),
                file_op("readded.parquet", entity::FileOp::Add),
            ]),
        ];

        let files = active_added_files(&commits);
        let paths = files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>();
        // `replaced.parquet` is deleted after it was added; `readded.parquet` is
        // added again after its delete and must survive.
        assert_eq!(paths, vec!["kept.parquet", "readded.parquet"]);
        assert!(files.iter().all(|file| file.file_op == "add"));
        assert!(files.iter().all(|file| file.partition_desc == "part=a"));
        assert!(files.iter().all(|file| file.modification_time == 42));
        assert!(files.iter().all(|file| file.size == 7));
    }

    #[test]
    fn clamp_version_saturates_at_i32_bounds() {
        assert_eq!(clamp_version(-1), -1);
        assert_eq!(clamp_version(i64::from(i32::MAX) + 10), i32::MAX);
        assert_eq!(clamp_version(i64::from(i32::MIN) - 10), i32::MIN);
    }
}
