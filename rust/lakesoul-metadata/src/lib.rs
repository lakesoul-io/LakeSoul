// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The Metadata Interface of LakeSoul.

#[macro_use]
extern crate tracing;
use std::io::ErrorKind;
use std::str::FromStr;

use chrono::NaiveDate;
use postgres_types::{FromSql, ToSql};
use prost::Message;
pub use tokio::runtime::{Builder, Runtime};
pub use tokio_postgres::{Client, NoTls, Statement};
use tokio_postgres::{Error, Row};

pub use crate::pooled_client::PooledClient;
use crate::pooled_client::{PgConnection, QueryType};
pub use error::{LakeSoulMetaDataError, Result};
pub use metadata_client::{MetaDataClient, MetaDataClientRef, pg_config_from_env};
use proto::proto::entity;

pub mod transfusion;

pub mod error;
mod jwt;
use crate::pooled_client::QueryType::{RO, RW};
pub use jwt::{Claims, JwtServer};

mod metadata_client;
mod pooled_client;
pub mod rbac;
pub mod utils;

/// The offset of code for the Data Access Object type for query one.
pub const DAO_TYPE_QUERY_ONE_OFFSET: i32 = 0;
/// The offset of code for the Data Access Object type for query list.
pub const DAO_TYPE_QUERY_LIST_OFFSET: i32 = 100;
/// The offset of code for the Data Access Object type for insert one.
pub const DAO_TYPE_INSERT_ONE_OFFSET: i32 = 200;
/// The offset of code for the Data Access Object type for transaction insert list.
pub const DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET: i32 = 300;
/// The offset of code for the Data Access Object type for query scalar.
pub const DAO_TYPE_QUERY_SCALAR_OFFSET: i32 = 400;
/// The offset of code for the Data Access Object type for update.
pub const DAO_TYPE_UPDATE_OFFSET: i32 = 500;

/// The delimiter for the Data Access Object parameters.
pub const PARAM_DELIM: &str = "__DELIM__";
/// The delimiter for the Data Access Object partition description.
pub const PARTITION_DESC_DELIM: &str = "_DELIM_";

/// The result type for the Data Access Object.
enum ResultType {
    /// The result type for the namespace.
    Namespace,
    /// The result type for the table_info.
    TableInfo,
    /// The result type for the table_name_id.
    TableNameId,
    /// The result type for the table_path_id.
    TablePathId,
    /// The result type for the partition_info.
    PartitionInfo,
    /// The result type for the data_commit_info.
    DataCommitInfo,
    /// The result type for the table_path_id with only table path.
    TablePathIdWithOnlyPath,
    /// The result type for the partition_info with only commit_op.
    PartitionInfoWithOnlyCommitOp,
    /// The result type for the discard_compressed_file_info.
    DiscardCompressedFileInfo,
}

/// The Data File Operation type, which is corresponding to the user defined type `data_file_op` in PostgreSQL.
#[derive(FromSql, ToSql, Debug, PartialEq)]
#[postgres(name = "data_file_op")]
struct DataFileOp {
    path: String,
    file_op: String,
    size: i64,
    file_exist_cols: String,
}

impl DataFileOp {
    fn from_proto_data_file_op(data_file_op: &entity::DataFileOp) -> Result<Self> {
        Ok(DataFileOp {
            path: data_file_op.path.clone(),
            file_op: entity::FileOp::try_from(data_file_op.file_op)
                .map_err(|e| LakeSoulMetaDataError::Internal(e.to_string()))?
                .as_str_name()
                .to_string(),
            size: data_file_op.size,
            file_exist_cols: data_file_op.file_exist_cols.clone(),
        })
    }

    fn as_proto_data_file_op(&self) -> Result<entity::DataFileOp> {
        Ok(entity::DataFileOp {
            path: self.path.clone(),
            file_op: entity::FileOp::from_str_name(self.file_op.as_str())
                .ok_or(LakeSoulMetaDataError::Internal("unknown file_op".into()))?
                as i32,
            size: self.size,
            file_exist_cols: self.file_exist_cols.clone(),
        })
    }
}

/// The coded type for the Data Access Object.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, num_enum::TryFromPrimitive,
)]
#[repr(i32)]
pub enum DaoType {
    // ==== Coded Query One ====
    /// The coded type for the Data Access Object for select namespace by namespace.
    SelectNamespaceByNamespace = DAO_TYPE_QUERY_ONE_OFFSET,
    /// The coded type for the Data Access Object for select table path id by table path.
    SelectTablePathIdByTablePath = DAO_TYPE_QUERY_ONE_OFFSET + 1,
    /// The coded type for the Data Access Object for select table info by table id.
    SelectTableInfoByTableId = DAO_TYPE_QUERY_ONE_OFFSET + 2,
    /// The coded type for the Data Access Object for select table name id by table name.
    SelectTableNameIdByTableName = DAO_TYPE_QUERY_ONE_OFFSET + 3,
    /// The coded type for the Data Access Object for select table info by table name and namespace.
    SelectTableInfoByTableNameAndNameSpace = DAO_TYPE_QUERY_ONE_OFFSET + 4,
    /// The coded type for the Data Access Object for select table info by table path.
    SelectTableInfoByTablePath = DAO_TYPE_QUERY_ONE_OFFSET + 5,
    /// The coded type for the Data Access Object for select table info by table id and table path.
    SelectTableInfoByIdAndTablePath = DAO_TYPE_QUERY_ONE_OFFSET + 6,

    /// The coded type for the Data Access Object for select one partition version by table id and partition description.
    SelectOnePartitionVersionByTableIdAndDesc = DAO_TYPE_QUERY_ONE_OFFSET + 7,
    /// The coded type for the Data Access Object for select partition version by table id and partition description and version.
    SelectPartitionVersionByTableIdAndDescAndVersion = DAO_TYPE_QUERY_ONE_OFFSET + 8,
    /// The coded type for the Data Access Object for select one data commit info by table id and partition description and commit id.
    SelectOneDataCommitInfoByTableIdAndPartitionDescAndCommitId =
        DAO_TYPE_QUERY_ONE_OFFSET + 9,
    /// The coded type for the Data Access Object for select table domain by table id.
    SelectTableDomainById = DAO_TYPE_QUERY_ONE_OFFSET + 10,
    /// The coded type for the Data Access Object for select discard compressed file info by file path.
    SelectDiscardCompressedFileInfoByFilePath = DAO_TYPE_QUERY_ONE_OFFSET + 11,

    // ==== Coded Table List ====
    /// The coded type for the Data Access Object for list namespaces.
    ListNamespaces = DAO_TYPE_QUERY_LIST_OFFSET,
    /// The coded type for the Data Access Object for list table name by namespace.
    ListTableNameByNamespace = DAO_TYPE_QUERY_LIST_OFFSET + 1,
    /// The coded type for the Data Access Object for list all table path.
    ListAllTablePath = DAO_TYPE_QUERY_LIST_OFFSET + 2,
    /// The coded type for the Data Access Object for list all path table path by namespace.
    ListAllPathTablePathByNamespace = DAO_TYPE_QUERY_LIST_OFFSET + 3,

    // ==== Coded Query Partition List ====
    /// The coded type for the Data Access Object for list partition by table id.
    ListPartitionByTableId = DAO_TYPE_QUERY_LIST_OFFSET + 4,
    /// The coded type for the Data Access Object for list partition description by table id and partition description.
    ListPartitionDescByTableIdAndParList = DAO_TYPE_QUERY_LIST_OFFSET + 5,
    /// The coded type for the Data Access Object for list partition by table id and partition description.
    ListPartitionByTableIdAndDesc = DAO_TYPE_QUERY_LIST_OFFSET + 6,
    /// The coded type for the Data Access Object for list partition version by table id and partition description and version range.
    ListPartitionVersionByTableIdAndPartitionDescAndVersionRange =
        DAO_TYPE_QUERY_LIST_OFFSET + 7,
    /// The coded type for the Data Access Object for list partition version by table id and partition description and timestamp range.
    ListPartitionVersionByTableIdAndPartitionDescAndTimestampRange =
        DAO_TYPE_QUERY_LIST_OFFSET + 8,
    /// The coded type for the Data Access Object for list commit ops between versions.
    ListCommitOpsBetweenVersions = DAO_TYPE_QUERY_LIST_OFFSET + 9,

    // ==== Coded Query DataCommitInfo List ====
    /// The coded type for the Data Access Object for list data commit info by table id and partition description and commit id list.
    ListDataCommitInfoByTableIdAndPartitionDescAndCommitList =
        DAO_TYPE_QUERY_LIST_OFFSET + 10,
    /// The coded type for the Data Access Object for list all discard compressed file info.
    ListAllDiscardCompressedFileInfo = DAO_TYPE_QUERY_LIST_OFFSET + 11,
    /// The coded type for the Data Access Object for list discard compressed file info before timestamp.
    ListDiscardCompressedFileInfoBeforeTimestamp = DAO_TYPE_QUERY_LIST_OFFSET + 12,
    /// The coded type for the Data Access Object for list discard compressed file by filter condition.
    ListDiscardCompressedFileByFilterCondition = DAO_TYPE_QUERY_LIST_OFFSET + 13,

    /// The coded type for the Data Access Object for list namespaces by domain.
    ListNamespacesByDomain = DAO_TYPE_QUERY_LIST_OFFSET + 14,
    /// The coded type for the Data Access Object for list table name by domain.
    ListTableNamesByDomain = DAO_TYPE_QUERY_LIST_OFFSET + 15,

    // ==== Coded Insert One ====
    /// The coded type for the Data Access Object for insert namespace.
    InsertNamespace = DAO_TYPE_INSERT_ONE_OFFSET,
    /// The coded type for the Data Access Object for insert table path id.
    InsertTablePathId = DAO_TYPE_INSERT_ONE_OFFSET + 1,
    /// The coded type for the Data Access Object for insert table name id.
    InsertTableNameId = DAO_TYPE_INSERT_ONE_OFFSET + 2,
    /// The coded type for the Data Access Object for insert table info.
    InsertTableInfo = DAO_TYPE_INSERT_ONE_OFFSET + 3,
    /// The coded type for the Data Access Object for insert partition info.
    InsertPartitionInfo = DAO_TYPE_INSERT_ONE_OFFSET + 4,
    /// The coded type for the Data Access Object for insert data commit info.
    InsertDataCommitInfo = DAO_TYPE_INSERT_ONE_OFFSET + 5,
    /// The coded type for the Data Access Object for insert discard compressed file info.
    InsertDiscardCompressedFileInfo = DAO_TYPE_INSERT_ONE_OFFSET + 6,

    // ==== Coded Transaction Insert List ====
    /// The coded type for the Data Access Object for transaction insert partition info.
    TransactionInsertPartitionInfo = DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET,
    /// The coded type for the Data Access Object for transaction insert data commit info.
    TransactionInsertDataCommitInfo = DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET + 1,
    /// The coded type for the Data Access Object for transaction insert discard compressed file.
    TransactionInsertDiscardCompressedFile = DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET + 2,

    // ==== Coded Query Scalar ====
    /// The coded type for the Data Access Object for get latest timestamp from partition info.
    GetLatestTimestampFromPartitionInfo = DAO_TYPE_QUERY_SCALAR_OFFSET,
    /// The coded type for the Data Access Object for get latest timestamp from partition info without partition description.
    GetLatestTimestampFromPartitionInfoWithoutPartitionDesc =
        DAO_TYPE_QUERY_SCALAR_OFFSET + 1,
    /// The coded type for the Data Access Object for get latest version up to time from partition info.
    GetLatestVersionUpToTimeFromPartitionInfo = DAO_TYPE_QUERY_SCALAR_OFFSET + 2,
    /// The coded type for the Data Access Object for get latest version timestamp up to time from partition info.
    GetLatestVersionTimestampUpToTimeFromPartitionInfo = DAO_TYPE_QUERY_SCALAR_OFFSET + 3,

    // ==== Coded Update Type ====
    /// The coded type for the Data Access Object for delete namespace by namespace.
    DeleteNamespaceByNamespace = DAO_TYPE_UPDATE_OFFSET,
    /// The coded type for the Data Access Object for update namespace properties by namespace.
    UpdateNamespacePropertiesByNamespace = DAO_TYPE_UPDATE_OFFSET + 1,

    // ==== Coded Update TableInfo ====
    /// The coded type for the Data Access Object for delete table info by table id and table path.
    DeleteTableInfoByIdAndPath = DAO_TYPE_UPDATE_OFFSET + 2,
    /// The coded type for the Data Access Object for update table info properties by table id.
    UpdateTableInfoPropertiesById = DAO_TYPE_UPDATE_OFFSET + 3,
    /// The coded type for the Data Access Object for update table info by table id.
    UpdateTableInfoById = DAO_TYPE_UPDATE_OFFSET + 4,

    // ==== Coded Update TablePathId ====
    /// The coded type for the Data Access Object for delete table path id by table path.
    DeleteTablePathIdByTablePath = DAO_TYPE_UPDATE_OFFSET + 5,
    /// The coded type for the Data Access Object for delete table path id by table id.
    DeleteTablePathIdByTableId = DAO_TYPE_UPDATE_OFFSET + 6,

    // ==== Coded Update TableNameId ====
    /// The coded type for the Data Access Object for delete table name id by table name and namespace.
    DeleteTableNameIdByTableNameAndNamespace = DAO_TYPE_UPDATE_OFFSET + 7,
    /// The coded type for the Data Access Object for delete table name id by table id.
    DeleteTableNameIdByTableId = DAO_TYPE_UPDATE_OFFSET + 8,

    // ==== Coded Update PartitionInfo ====
    /// The coded type for the Data Access Object for delete partition info by table id and partition description.
    DeletePartitionInfoByTableIdAndPartitionDesc = DAO_TYPE_UPDATE_OFFSET + 9,
    /// The coded type for the Data Access Object for delete partition info by table id.
    DeletePartitionInfoByTableId = DAO_TYPE_UPDATE_OFFSET + 10,
    /// The coded type for the Data Access Object for delete previous version partition.
    DeletePreviousVersionPartition = DAO_TYPE_UPDATE_OFFSET + 11,

    // ==== Coded Update DataCommitInfo ====
    /// The coded type for the Data Access Object for delete one data commit info by table id and partition description and commit id.
    DeleteOneDataCommitInfoByTableIdAndPartitionDescAndCommitId =
        DAO_TYPE_UPDATE_OFFSET + 12,
    /// The coded type for the Data Access Object for delete data commit info by table id and partition description and commit id list.
    DeleteDataCommitInfoByTableIdAndPartitionDescAndCommitIdList =
        DAO_TYPE_UPDATE_OFFSET + 13,
    /// The coded type for the Data Access Object for delete data commit info by table id and partition description.
    DeleteDataCommitInfoByTableIdAndPartitionDesc = DAO_TYPE_UPDATE_OFFSET + 14,
    /// The coded type for the Data Access Object for delete data commit info by table id.
    DeleteDataCommitInfoByTableId = DAO_TYPE_UPDATE_OFFSET + 15,

    // ==== Coded Update DiscardCompressedFileInfo ====
    /// The coded type for the Data Access Object for delete discard compressed file info by file path.
    DeleteDiscardCompressedFileInfoByFilePath = DAO_TYPE_UPDATE_OFFSET + 16,

    /// The coded type for the Data Access Object for delete discard compressed file by filter condition.
    DeleteDiscardCompressedFileByFilterCondition = DAO_TYPE_UPDATE_OFFSET + 17,
    DeleteDiscardCompressedFileInfoByTablePath = DAO_TYPE_UPDATE_OFFSET + 18,
}

fn get_query_type(dao_type: DaoType) -> QueryType {
    let dao_type = dao_type as i32;
    if dao_type <= DAO_TYPE_INSERT_ONE_OFFSET
        || (dao_type >= DAO_TYPE_QUERY_SCALAR_OFFSET && dao_type < DAO_TYPE_UPDATE_OFFSET)
    {
        RO
    } else {
        RW
    }
}

/// Get the prepared statement for the coded Data Access Object.
async fn get_prepared_statement<'a>(
    client: &'a PooledClient,
    dao_type: &DaoType,
) -> Result<(PgConnection<'a>, Statement)> {
    let statement = match dao_type {
        // Select Namespace
        DaoType::SelectNamespaceByNamespace =>
            "select namespace, properties, comment, domain
            from namespace
            where namespace = $1::TEXT",
        DaoType::ListNamespaces =>
            "select namespace, properties, comment, domain
            from namespace",
        DaoType::ListNamespacesByDomain =>
            "select namespace, properties, comment, domain
            from namespace
            where domain = $1::TEXT",

        // Select TablePathId
        DaoType::SelectTablePathIdByTablePath =>
            "select table_path, table_id, table_namespace, domain
            from table_path_id
            where table_path = $1::TEXT",
        DaoType::ListAllTablePath =>
            "select table_path, table_id, table_namespace, domain
            from table_path_id",
        DaoType::ListAllPathTablePathByNamespace =>
            "select table_path
            from table_path_id
            where table_namespace = $1::TEXT ",

        // Select TableNameId
        DaoType::SelectTableNameIdByTableName =>
            "select table_name, table_id, table_namespace, domain
            from table_name_id
            where table_name = $1::TEXT and table_namespace = $2::TEXT",
        DaoType::ListTableNameByNamespace =>
            "select table_name, table_id, table_namespace, domain
            from table_name_id
            where table_namespace = $1::TEXT",
        DaoType::ListTableNamesByDomain =>
            "select table_name, table_id, table_namespace, domain
            from table_name_id
            where domain = $1::TEXT",

        // Select TableInfo
        DaoType::SelectTableInfoByTableId =>
            "select table_id, table_name, table_path, table_schema, properties, partitions, table_namespace, domain
            from table_info
            where table_id = $1::TEXT",
        DaoType::SelectTableInfoByTableNameAndNameSpace =>
            "select table_id, table_name, table_path, table_schema, properties, partitions, table_namespace, domain
            from table_info
            where table_name = $1::TEXT and table_namespace=$2::TEXT",
        DaoType::SelectTableInfoByTablePath =>
            "select table_id, table_name, table_path, table_schema, properties, partitions, table_namespace, domain
            from table_info
            where table_path = $1::TEXT",
        DaoType::SelectTableInfoByIdAndTablePath =>
            "select table_id, table_name, table_path, table_schema, properties, partitions, table_namespace, domain
            from table_info
            where table_id = $1::TEXT and table_path=$2::TEXT",

        // Select PartitionInfo
        DaoType::SelectPartitionVersionByTableIdAndDescAndVersion =>
            "select table_id, partition_desc, version, commit_op, snapshot, timestamp, expression, domain
            from partition_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT and version = $3::INT",
        DaoType::SelectOnePartitionVersionByTableIdAndDesc =>
            "select m.table_id, t.partition_desc, m.version, m.commit_op, m.snapshot, m.timestamp, m.expression, m.domain from (
                select table_id,partition_desc,max(version) from partition_info
                where table_id = $1::TEXT and partition_desc = $2::TEXT group by table_id, partition_desc) t
                left join partition_info m on t.table_id = m.table_id
                and t.partition_desc = m.partition_desc and t.max = m.version",
        DaoType::ListPartitionByTableIdAndDesc =>
            "select table_id, partition_desc, version, commit_op, snapshot, timestamp, expression, domain
            from partition_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT ",
        DaoType::ListPartitionByTableId =>
            "select m.table_id, t.partition_desc, m.version, m.commit_op, m.snapshot, m.timestamp, m.expression, m.domain
            from (select table_id, partition_desc,max(version)
                from partition_info
                where table_id = $1::TEXT
                group by table_id, partition_desc) t
            left join partition_info m
            on t.table_id = m.table_id and t.partition_desc = m.partition_desc and t.max = m.version",
        DaoType::ListPartitionVersionByTableIdAndPartitionDescAndTimestampRange =>
            "select table_id, partition_desc, version, commit_op, snapshot, timestamp, expression, domain
            from partition_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT and timestamp >= $3::BIGINT and timestamp < $4::BIGINT",
        DaoType::ListCommitOpsBetweenVersions =>
            "select distinct(commit_op)
            from partition_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT and version between $3::INT and $4::INT",
        DaoType::ListPartitionVersionByTableIdAndPartitionDescAndVersionRange =>
            "select table_id, partition_desc, version, commit_op, snapshot, timestamp, expression, domain
            from partition_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT and version >= $3::INT and version <= $4::INT",

        // Select DataCommitInfo
        DaoType::SelectOneDataCommitInfoByTableIdAndPartitionDescAndCommitId =>
            "select table_id, partition_desc, commit_id, file_ops, commit_op, timestamp, committed, domain
            from data_commit_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT and commit_id = $3::UUID",

        // Select DiscardCompressedFileInfo
        DaoType::SelectDiscardCompressedFileInfoByFilePath =>
            "select file_path, table_path, partition_desc, timestamp, t_date
            from discard_compressed_file_info
            where file_path = $1::TEXT",
        DaoType::ListAllDiscardCompressedFileInfo =>
            "select file_path, table_path, partition_desc, timestamp, t_date
            from discard_compressed_file_info",
        DaoType::ListDiscardCompressedFileInfoBeforeTimestamp =>
            "select file_path, table_path, partition_desc, timestamp, t_date
            from discard_compressed_file_info
            where timestamp < $1::BIGINT",
        DaoType::ListDiscardCompressedFileByFilterCondition =>
            "select file_path, table_path, partition_desc, timestamp, t_date
            from discard_compressed_file_info
            where table_path = $1::TEXT and partition_desc = $2::TEXT and timestamp < $3::BIGINT",

        // Select Table Domain by id
        DaoType::SelectTableDomainById =>
            "select table_name, table_id, table_namespace, domain
             from table_name_id where table_id = $1::TEXT",

        // Insert
        DaoType::InsertNamespace =>
            "insert into namespace(
                namespace,
                properties,
                comment,
                domain)
            values($1::TEXT, $2::JSON, $3::TEXT, $4::TEXT)",
        DaoType::InsertTableInfo =>
            "insert into table_info(
                table_id,
                table_name,
                table_path,
                table_schema,
                properties,
                partitions,
                table_namespace,
                domain)
            values($1::TEXT, $2::TEXT, $3::TEXT, $4::TEXT, $5::JSON, $6::TEXT, $7::TEXT, $8::TEXT)",
        DaoType::InsertTableNameId =>
            "insert into table_name_id(
                table_id,
                table_name,
                table_namespace,
                domain)
            values($1::TEXT, $2::TEXT, $3::TEXT, $4::TEXT)",
        DaoType::InsertTablePathId =>
            "insert into table_path_id(
                table_id,
                table_path,
                table_namespace,
                domain)
            values($1::TEXT, $2::TEXT, $3::TEXT, $4::TEXT)",
        DaoType::InsertPartitionInfo =>
            "insert into partition_info(
                table_id,
                partition_desc,
                version,
                commit_op,
                snapshot,
                expression,
                domain)
            values($1::TEXT, $2::TEXT, $3::INT, $4::TEXT, $5::_UUID, $6::TEXT, $7::TEXT)",
        DaoType::InsertDataCommitInfo =>
            "insert into data_commit_info(
                table_id,
                partition_desc,
                commit_id,
                file_ops,
                commit_op,
                timestamp,
                committed,
                domain
            )
            values($1::TEXT, $2::TEXT, $3::UUID, $4::_data_file_op, $5::TEXT, $6::BIGINT, $7::BOOL, $8::TEXT)",

        DaoType::InsertDiscardCompressedFileInfo =>
            "insert into discard_compressed_file_info(
                file_path,
                table_path,
                partition_desc,
                timestamp,
                t_date
            )
            values($1::TEXT, $2::TEXT, $3::TEXT, $4::BIGINT, $5::DATE)",

        // Query Scalar
        DaoType::GetLatestTimestampFromPartitionInfo =>
            "select max(timestamp) as timestamp
            from partition_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT",
        DaoType::GetLatestTimestampFromPartitionInfoWithoutPartitionDesc =>
            "select max(timestamp) as timestamp
            from partition_info
            where table_id = $1::TEXT",
        DaoType::GetLatestVersionUpToTimeFromPartitionInfo =>
            "select max(version) as version
            from partition_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT and timestamp < $3::BIGINT",
        DaoType::GetLatestVersionTimestampUpToTimeFromPartitionInfo =>
            "select max(timestamp) as timestamp
            from partition_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT and timestamp < $3::BIGINT",

        // Update / Delete
        DaoType::DeleteNamespaceByNamespace =>
            "delete from namespace
            where namespace = $1::TEXT ",
        DaoType::UpdateNamespacePropertiesByNamespace =>
            "update namespace
            set properties = $2::JSON where namespace = $1::TEXT",

        DaoType::DeleteTableNameIdByTableNameAndNamespace =>
            "delete from table_name_id
            where table_name = $1::TEXT and table_namespace = $2::TEXT",
        DaoType::DeleteTableNameIdByTableId =>
            "delete from table_name_id
            where table_id = $1::TEXT",

        DaoType::DeleteTableInfoByIdAndPath =>
            "delete from table_info
            where table_id = $1::TEXT and table_path = $2::TEXT",
        DaoType::UpdateTableInfoPropertiesById =>
            "update table_info
            set properties = $2::JSON where table_id = $1::TEXT",

        DaoType::DeleteTablePathIdByTablePath =>
            "delete from table_path_id
            where table_path = $1::TEXT ",
        DaoType::DeleteTablePathIdByTableId =>
            "delete from table_path_id
            where table_id = $1::TEXT ",

        DaoType::DeleteOneDataCommitInfoByTableIdAndPartitionDescAndCommitId =>
            "delete from data_commit_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT and commit_id = $3::UUID ",
        DaoType::DeleteDataCommitInfoByTableIdAndPartitionDesc =>
            "delete from data_commit_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT",
        DaoType::DeleteDataCommitInfoByTableId =>
            "delete from data_commit_info
            where table_id = $1::TEXT",

        DaoType::DeletePartitionInfoByTableId =>
            "delete from partition_info
            where table_id = $1::TEXT",
        DaoType::DeletePartitionInfoByTableIdAndPartitionDesc =>
            "delete from partition_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT",
        DaoType::DeletePreviousVersionPartition =>
            "delete from partition_info
            where table_id = $1::TEXT and partition_desc = $2::TEXT and timestamp <= $3::BIGINT",

        DaoType::DeleteDiscardCompressedFileInfoByFilePath =>
            "delete from discard_compressed_file_info
            where file_path = $1::TEXT",
        DaoType::DeleteDiscardCompressedFileInfoByTablePath =>
            "delete from discard_compressed_file_info
            where table_path = $1::TEXT",
        DaoType::DeleteDiscardCompressedFileByFilterCondition =>
            "delete from discard_compressed_file_info
            where table_path = $1::TEXT and partition_desc = $2::TEXT and timestamp <= $3::BIGINT",

        // not prepared
        DaoType::UpdateTableInfoById |
        DaoType::TransactionInsertDataCommitInfo |
        DaoType::TransactionInsertPartitionInfo |
        DaoType::TransactionInsertDiscardCompressedFile |
        DaoType::ListDataCommitInfoByTableIdAndPartitionDescAndCommitList |
        DaoType::DeleteDataCommitInfoByTableIdAndPartitionDescAndCommitIdList |
        DaoType::ListPartitionDescByTableIdAndParList => "",

        /* _ => todo!(), */
    };
    client
        .prepare_cached(statement, get_query_type(*dao_type))
        .await
}

/// Parse the joined string to the parameters.
fn get_params(joined_string: String) -> Vec<String> {
    joined_string
        .split(PARAM_DELIM)
        .collect::<Vec<&str>>()
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>()
}

/// Separate the concatenated uuid radix string to the uuid string list.
fn separate_uuid(concatenated_uuid: &str) -> Result<Vec<String>> {
    let uuid_num = concatenated_uuid.len() / 32;
    let mut uuid_list = Vec::<String>::with_capacity(uuid_num);
    let mut idx = 0;
    for _ in 0..uuid_num {
        let high = u64::from_str_radix(&concatenated_uuid[idx..idx + 16], 16)?;
        let low = u64::from_str_radix(&concatenated_uuid[idx + 16..idx + 32], 16)?;
        uuid_list.push(uuid::Uuid::from_u64_pair(high, low).to_string());
        idx += 32;
    }
    Ok(uuid_list)
}

/// Execute the query for the coded Data Access Object.
#[instrument(level = "debug")]
pub async fn execute_query(
    client: &PooledClient,
    query_type: i32,
    joined_string: String,
) -> Result<Vec<u8>> {
    if query_type >= DAO_TYPE_INSERT_ONE_OFFSET {
        eprintln!("Invalid query_type_index: {:?}", query_type);
        return Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput));
    }
    let query_type = DaoType::try_from(query_type)
        .map_err(|e| LakeSoulMetaDataError::Other(Box::new(e)))?;
    let (client, statement) = get_prepared_statement(client, &query_type).await?;

    let params = get_params(joined_string);

    let rows = match query_type {
        DaoType::ListNamespaces
        | DaoType::ListAllTablePath
        | DaoType::ListAllDiscardCompressedFileInfo
            if params.len() == 1 && params[0].is_empty() =>
        {
            let result = client.query(&statement, &[]).await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::ListNamespacesByDomain if params.len() == 1 => {
            let result = client.query(&statement, &[&params[0]]).await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::ListTableNamesByDomain if params.len() == 1 => {
            let result = client.query(&statement, &[&params[0]]).await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::ListTableNameByNamespace if params.len() == 1 => {
            let result = client.query(&statement, &[&params[0]]).await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::ListDiscardCompressedFileInfoBeforeTimestamp if params.len() == 1 => {
            let result = client
                .query(&statement, &[&i64::from_str(&params[0])?])
                .await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::ListDiscardCompressedFileByFilterCondition if params.len() == 3 => {
            let result = client
                .query(
                    &statement,
                    &[&params[0], &params[1], &i64::from_str(&params[2])?],
                )
                .await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::SelectNamespaceByNamespace
        | DaoType::SelectTableInfoByTableId
        | DaoType::SelectTablePathIdByTablePath
        | DaoType::SelectTableInfoByTablePath
        | DaoType::SelectDiscardCompressedFileInfoByFilePath
            if params.len() == 1 =>
        {
            let result = client.query_opt(&statement, &[&params[0]]).await;
            match result {
                Ok(Some(row)) => vec![row],
                Ok(None) => vec![],
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::ListPartitionByTableId | DaoType::ListAllPathTablePathByNamespace
            if params.len() == 1 =>
        {
            let result = client.query(&statement, &[&params[0]]).await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::SelectOnePartitionVersionByTableIdAndDesc
        | DaoType::ListPartitionByTableIdAndDesc
            if params.len() == 2 =>
        {
            let result = client.query(&statement, &[&params[0], &params[1]]).await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::SelectTableNameIdByTableName
        | DaoType::SelectTableInfoByTableNameAndNameSpace
        | DaoType::SelectTableInfoByIdAndTablePath
            if params.len() == 2 =>
        {
            let result = client
                .query_opt(&statement, &[&params[0], &params[1]])
                .await;
            match result {
                Ok(Some(row)) => vec![row],
                Ok(None) => vec![],
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::SelectOneDataCommitInfoByTableIdAndPartitionDescAndCommitId
            if params.len() == 3 =>
        {
            let result = client
                .query_opt(
                    &statement,
                    &[&params[0], &params[1], &uuid::Uuid::from_str(&params[2])?],
                )
                .await;
            match result {
                Ok(Some(row)) => vec![row],
                Ok(None) => vec![],
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::SelectPartitionVersionByTableIdAndDescAndVersion
            if params.len() == 3 =>
        {
            let result = client
                .query(
                    &statement,
                    &[&params[0], &params[1], &i32::from_str(&params[2])?],
                )
                .await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::SelectTableDomainById if params.len() == 1 => {
            let result = client.query(&statement, &[&params[0]]).await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::ListCommitOpsBetweenVersions
        | DaoType::ListPartitionVersionByTableIdAndPartitionDescAndVersionRange
            if params.len() == 4 =>
        {
            let result = client
                .query(
                    &statement,
                    &[
                        &params[0],
                        &params[1],
                        &i32::from_str(&params[2])?,
                        &i32::from_str(&params[3])?,
                    ],
                )
                .await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::ListPartitionDescByTableIdAndParList if params.len() == 2 => {
            let statement = "\
                select
                    m.table_id,
                    m.partition_desc,
                    m.version,
                    m.commit_op,
                    m.snapshot,
                    m.timestamp,
                    m.expression,
                    m.domain
                from
                    (
                        select
                            max(version)
                        from
                            partition_info
                        where
                            table_id = $1::text
                            and partition_desc = $2::text
                    ) t
                    left join partition_info m on t.max = m.version
                where
                    m.table_id = $1::text
                    and m.partition_desc = $2::text;
            ";
            let partitions = params[1].to_owned();
            let partitions = partitions
                .split(PARTITION_DESC_DELIM)
                .collect::<Vec<&str>>();
            let statement = client.prepare_cached(statement).await?;
            let mut all_rows: Vec<Row> = vec![];
            for part in partitions {
                let result = client.query(&statement, &[&params[0], &part]).await;
                match result {
                    Ok(mut rows) => all_rows.append(&mut rows),
                    Err(e) => return Err(LakeSoulMetaDataError::from(e)),
                }
            }
            all_rows
        }
        DaoType::ListPartitionVersionByTableIdAndPartitionDescAndTimestampRange
            if params.len() == 4 =>
        {
            let result = client
                .query(
                    &statement,
                    &[
                        &params[0],
                        &params[1],
                        &i64::from_str(&params[2])?,
                        &i64::from_str(&params[3])?,
                    ],
                )
                .await;
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::ListDataCommitInfoByTableIdAndPartitionDescAndCommitList
            if params.len() == 3 =>
        {
            let concated_uuid = &params[2];
            if !concated_uuid.len().is_multiple_of(32) {
                eprintln!(
                    "Invalid params of query_type={:?}, params={:?}",
                    query_type, params
                );
                return Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput));
            }

            let uuid_list = separate_uuid(concated_uuid)?;

            let uuid_str_list = "'".to_owned() + &uuid_list.join("','") + "'";

            let uuid_list_str = uuid_list.join("");

            let statement = format!(
                "select table_id, partition_desc, commit_id, file_ops, commit_op, timestamp, committed, domain
                from data_commit_info
                where table_id = $1::TEXT and partition_desc = $2::TEXT
                and commit_id in ({})
                order by position(commit_id::text in '{}')",
                uuid_str_list, uuid_list_str
            );

            let result = {
                let statement = client.prepare(&statement).await?;
                client.query(&statement, &[&params[0], &params[1]]).await
            };
            match result {
                Ok(rows) => rows,
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        _ => {
            eprintln!(
                "Invalid params num of query_type={:?}, params={:?}",
                query_type, params
            );
            return Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput));
        }
    };

    let result_type = match query_type {
        DaoType::SelectNamespaceByNamespace
        | DaoType::ListNamespaces
        | DaoType::ListNamespacesByDomain => ResultType::Namespace,

        DaoType::SelectTableInfoByTableId
        | DaoType::SelectTableInfoByTableNameAndNameSpace
        | DaoType::SelectTableInfoByTablePath
        | DaoType::SelectTableInfoByIdAndTablePath => ResultType::TableInfo,

        DaoType::SelectTablePathIdByTablePath | DaoType::ListAllTablePath => {
            ResultType::TablePathId
        }

        DaoType::SelectTableNameIdByTableName
        | DaoType::ListTableNameByNamespace
        | DaoType::SelectTableDomainById
        | DaoType::ListTableNamesByDomain => ResultType::TableNameId,

        DaoType::ListPartitionByTableId
        | DaoType::ListPartitionDescByTableIdAndParList
        | DaoType::SelectPartitionVersionByTableIdAndDescAndVersion
        | DaoType::SelectOnePartitionVersionByTableIdAndDesc
        | DaoType::ListPartitionByTableIdAndDesc
        | DaoType::ListPartitionVersionByTableIdAndPartitionDescAndTimestampRange
        | DaoType::ListPartitionVersionByTableIdAndPartitionDescAndVersionRange => {
            ResultType::PartitionInfo
        }

        DaoType::SelectOneDataCommitInfoByTableIdAndPartitionDescAndCommitId
        | DaoType::ListDataCommitInfoByTableIdAndPartitionDescAndCommitList => {
            ResultType::DataCommitInfo
        }

        DaoType::ListAllPathTablePathByNamespace => ResultType::TablePathIdWithOnlyPath,

        DaoType::ListCommitOpsBetweenVersions => {
            ResultType::PartitionInfoWithOnlyCommitOp
        }

        DaoType::SelectDiscardCompressedFileInfoByFilePath
        | DaoType::ListAllDiscardCompressedFileInfo
        | DaoType::ListDiscardCompressedFileInfoBeforeTimestamp
        | DaoType::ListDiscardCompressedFileByFilterCondition => {
            ResultType::DiscardCompressedFileInfo
        }
        _ => {
            eprintln!(
                "Invalid query_type={:?} when parsing query result type",
                query_type
            );
            return Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput));
        }
    };

    let wrapper = match result_type {
        ResultType::TableNameId => {
            let table_name_id: Vec<entity::TableNameId> = rows
                .iter()
                .map(|row| entity::TableNameId {
                    table_name: row.get(0),
                    table_id: row.get(1),
                    table_namespace: row.get(2),
                    domain: row.get(3),
                })
                .collect();
            entity::JniWrapper {
                table_name_id,
                ..Default::default()
            }
        }
        ResultType::TablePathId => {
            let table_path_id: Vec<entity::TablePathId> = rows
                .iter()
                .map(|row| entity::TablePathId {
                    table_path: row.get(0),
                    table_id: row.get(1),
                    table_namespace: row.get(2),
                    domain: row.get(3),
                })
                .collect();
            entity::JniWrapper {
                table_path_id,
                ..Default::default()
            }
        }
        ResultType::TablePathIdWithOnlyPath => {
            let table_path_id: Vec<entity::TablePathId> = rows
                .iter()
                .map(|row| entity::TablePathId {
                    table_path: row.get(0),
                    ..Default::default()
                })
                .collect();
            entity::JniWrapper {
                table_path_id,
                ..Default::default()
            }
        }

        ResultType::Namespace => {
            let namespace: Vec<entity::Namespace> = rows
                .iter()
                .map(|row| entity::Namespace {
                    namespace: row.get(0),
                    properties: row.get::<_, serde_json::Value>(1).to_string(),
                    comment: row.get::<_, Option<String>>(2).unwrap_or(String::from("")),
                    domain: row.get(3),
                })
                .collect();
            entity::JniWrapper {
                namespace,
                ..Default::default()
            }
        }
        ResultType::TableInfo => {
            let table_info: Vec<entity::TableInfo> = rows
                .iter()
                .map(|row| entity::TableInfo {
                    table_id: row.get(0),
                    table_name: row.get(1),
                    table_path: row.get(2),
                    table_schema: row.get(3),
                    properties: row.get::<_, serde_json::Value>(4).to_string(),
                    partitions: row.get(5),
                    table_namespace: row.get(6),
                    domain: row.get(7),
                })
                .collect();
            entity::JniWrapper {
                table_info,
                ..Default::default()
            }
        }
        ResultType::PartitionInfo => {
            let partition_info: Vec<entity::PartitionInfo> = rows
                .iter()
                .map(|row| {
                    Ok(entity::PartitionInfo {
                        table_id: row.get(0),
                        partition_desc: row.get(1),
                        version: row.get::<_, i32>(2),
                        commit_op: entity::CommitOp::from_str_name(row.get(3)).ok_or(
                            LakeSoulMetaDataError::Internal("unknown commit_op".into()),
                        )? as i32,
                        snapshot: row_to_uuid_list(row),
                        timestamp: row.get::<_, i64>(5),
                        expression: row
                            .get::<_, Option<String>>(6)
                            .unwrap_or(String::from("")),
                        domain: row.get(7),
                    })
                })
                .collect::<Result<Vec<entity::PartitionInfo>>>()?;
            entity::JniWrapper {
                partition_info,
                ..Default::default()
            }
        }
        ResultType::PartitionInfoWithOnlyCommitOp => {
            let partition_info: Vec<entity::PartitionInfo> = rows
                .iter()
                .map(|row| {
                    Ok(entity::PartitionInfo {
                        commit_op: entity::CommitOp::from_str_name(row.get(0)).ok_or(
                            LakeSoulMetaDataError::Internal("unknown commit_op".into()),
                        )? as i32,
                        ..Default::default()
                    })
                })
                .collect::<Result<Vec<entity::PartitionInfo>>>()?;
            entity::JniWrapper {
                partition_info,
                ..Default::default()
            }
        }
        ResultType::DataCommitInfo => {
            let data_commit_info: Vec<entity::DataCommitInfo> = rows
                .iter()
                .map(|row| {
                    Ok(entity::DataCommitInfo {
                        table_id: row.get(0),
                        partition_desc: row.get(1),
                        commit_id: {
                            let (high, low) = row.get::<_, uuid::Uuid>(2).as_u64_pair();
                            Some(entity::Uuid { high, low })
                        },
                        file_ops: row
                            .get::<_, Vec<DataFileOp>>(3)
                            .iter()
                            .map(|data_file_op| data_file_op.as_proto_data_file_op())
                            .collect::<Result<Vec<entity::DataFileOp>>>()?,
                        commit_op: entity::CommitOp::from_str_name(row.get(4)).ok_or(
                            LakeSoulMetaDataError::Internal("unknown commit_op".into()),
                        )? as i32,
                        timestamp: row.get(5),
                        committed: row.get(6),
                        domain: row.get(7),
                    })
                })
                .collect::<Result<Vec<entity::DataCommitInfo>>>()?;
            entity::JniWrapper {
                data_commit_info,
                ..Default::default()
            }
        }
        ResultType::DiscardCompressedFileInfo => {
            let discard_compressed_file_info: Vec<entity::DiscardCompressedFileInfo> =
                rows.iter()
                    .map(|row| entity::DiscardCompressedFileInfo {
                        file_path: row.get(0),
                        table_path: row.get(1),
                        partition_desc: row.get(2),
                        timestamp: row.get(3),
                        t_date: row.get::<_, NaiveDate>(4).format("%Y-%m-%d").to_string(),
                    })
                    .collect();
            entity::JniWrapper {
                discard_compressed_file_info,
                ..Default::default()
            }
        }
    };
    Ok(wrapper.encode_to_vec())
}

/// Execute the insert for the coded Data Access Object.
#[instrument(level = "debug")]
pub async fn execute_insert(
    client: &mut PooledClient,
    insert_type: i32,
    wrapper: entity::JniWrapper,
) -> Result<i32> {
    if !(DAO_TYPE_INSERT_ONE_OFFSET..DAO_TYPE_QUERY_SCALAR_OFFSET).contains(&insert_type)
    {
        error!("Invalid insert_type_index: {:?}", insert_type);
        return Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput));
    }
    let insert_type = DaoType::try_from(insert_type)
        .map_err(|e| LakeSoulMetaDataError::Other(Box::new(e)))?;
    let (mut client, statement) = get_prepared_statement(client, &insert_type).await?;

    let result = match insert_type {
        DaoType::InsertNamespace if wrapper.namespace.len() == 1 => {
            let namespace = wrapper.namespace.first().unwrap();
            let properties: serde_json::Value =
                serde_json::from_str(&namespace.properties)?;
            client
                .execute(
                    &statement,
                    &[
                        &namespace.namespace,
                        &properties,
                        &namespace.comment,
                        &namespace.domain,
                    ],
                )
                .await
        }
        DaoType::InsertTableInfo if wrapper.table_info.len() == 1 => {
            let table_info = wrapper.table_info.first().unwrap();
            let properties: serde_json::Value =
                serde_json::from_str(&table_info.properties)?;
            client
                .execute(
                    &statement,
                    &[
                        &table_info.table_id,
                        &table_info.table_name,
                        &table_info.table_path,
                        &table_info.table_schema,
                        &properties,
                        &table_info.partitions,
                        &table_info.table_namespace,
                        &table_info.domain,
                    ],
                )
                .await
        }
        DaoType::InsertTableNameId if wrapper.table_name_id.len() == 1 => {
            let table_name_id = wrapper.table_name_id.first().unwrap();
            client
                .execute(
                    &statement,
                    &[
                        &table_name_id.table_id,
                        &table_name_id.table_name,
                        &table_name_id.table_namespace,
                        &table_name_id.domain,
                    ],
                )
                .await
        }
        DaoType::InsertTablePathId if wrapper.table_path_id.len() == 1 => {
            let table_path_id = wrapper.table_path_id.first().unwrap();
            client
                .execute(
                    &statement,
                    &[
                        &table_path_id.table_id,
                        &table_path_id.table_path,
                        &table_path_id.table_namespace,
                        &table_path_id.domain,
                    ],
                )
                .await
        }
        DaoType::InsertPartitionInfo if wrapper.partition_info.len() == 1 => {
            let partition_info = wrapper.partition_info.first().unwrap();
            let snapshot = partition_info
                .snapshot
                .iter()
                .map(|_uuid| uuid::Uuid::from_u64_pair(_uuid.high, _uuid.low))
                .collect::<Vec<uuid::Uuid>>();
            client
                .execute(
                    &statement,
                    &[
                        &partition_info.table_id,
                        &partition_info.partition_desc,
                        &partition_info.version,
                        &partition_info.commit_op().as_str_name(),
                        &snapshot,
                        &partition_info.expression,
                        &partition_info.domain,
                    ],
                )
                .await
        }
        DaoType::InsertDataCommitInfo if wrapper.data_commit_info.len() == 1 => {
            let data_commit_info = wrapper.data_commit_info.first().unwrap();
            let file_ops = data_commit_info
                .file_ops
                .iter()
                .map(DataFileOp::from_proto_data_file_op)
                .collect::<Result<Vec<DataFileOp>>>()?;
            let commit_id = data_commit_info
                .commit_id
                .as_ref()
                .ok_or(LakeSoulMetaDataError::Internal("commit_id missing".into()))?;
            let _uuid = uuid::Uuid::from_u64_pair(commit_id.high, commit_id.low);

            client
                .execute(
                    &statement,
                    &[
                        &data_commit_info.table_id,
                        &data_commit_info.partition_desc,
                        &_uuid,
                        &file_ops,
                        &data_commit_info.commit_op().as_str_name(),
                        &data_commit_info.timestamp,
                        &data_commit_info.committed,
                        &data_commit_info.domain,
                    ],
                )
                .await
        }
        DaoType::TransactionInsertPartitionInfo => {
            let mut partition_info_list = wrapper.partition_info.clone();
            let snapshot_container = partition_info_list.pop().unwrap();
            let result = {
                let transaction = client.transaction().await?;
                let transaction_insert_statement = match transaction
                    .prepare(
                        "insert into partition_info(
                        table_id,
                        partition_desc,
                        version,
                        commit_op,
                        snapshot,
                        expression,
                        domain
                    )
                    values($1::TEXT, $2::TEXT, $3::INT, $4::TEXT, $5::_UUID, $6::TEXT, $7::TEXT)",
                    )
                    .await
                {
                    Ok(statement) => statement,
                    Err(e) => return Err(LakeSoulMetaDataError::from(e)),
                };

                let update_statement = match transaction
                    .prepare("update data_commit_info set committed = 'true' where commit_id = $1::UUID")
                    .await
                {
                    Ok(statement) => statement,
                    Err(e) => return Err(LakeSoulMetaDataError::from(e)),
                };

                for partition_info in &partition_info_list {
                    let snapshot = partition_info
                        .snapshot
                        .iter()
                        .map(|_uuid| uuid::Uuid::from_u64_pair(_uuid.high, _uuid.low))
                        .collect::<Vec<uuid::Uuid>>();

                    let result = transaction
                        .execute(
                            &transaction_insert_statement,
                            &[
                                &partition_info.table_id,
                                &partition_info.partition_desc,
                                &partition_info.version,
                                &partition_info.commit_op().as_str_name(),
                                &snapshot,
                                &partition_info.expression,
                                &partition_info.domain,
                            ],
                        )
                        .await;

                    if let Some(e) = result.err() {
                        eprintln!("transaction insert error, err = {:?}", e);
                        return match transaction.rollback().await {
                            Ok(()) => Ok(0i32),
                            Err(e) => Err(LakeSoulMetaDataError::from(e)),
                        };
                    };
                }
                for _uuid in &snapshot_container.snapshot {
                    let uid = uuid::Uuid::from_u64_pair(_uuid.high, _uuid.low);
                    let result = transaction.execute(&update_statement, &[&uid]).await;
                    if let Some(e) = result.err() {
                        eprintln!("update committed error, err = {:?}", e);
                        return match transaction.rollback().await {
                            Ok(()) => Ok(0i32),
                            Err(e) => Err(LakeSoulMetaDataError::from(e)),
                        };
                    }
                }
                match transaction.commit().await {
                    Ok(()) => Ok(partition_info_list.len() as u64),
                    Err(e) => Err(e),
                }
            };
            match result {
                Ok(count) => Ok(count),
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::TransactionInsertDataCommitInfo => {
            let data_commit_info_list = wrapper.data_commit_info;
            let result = {
                let transaction = client.transaction().await?;
                let prepared = transaction
                    .prepare(
                        "insert into data_commit_info(
                        table_id,
                        partition_desc,
                        commit_id,
                        file_ops,
                        commit_op,
                        timestamp,
                        committed,
                        domain
                    )
                    values($1::TEXT, $2::TEXT, $3::UUID, $4::_data_file_op, $5::TEXT, $6::BIGINT, $7::BOOL, $8::TEXT)",
                    )
                    .await;
                let statement = match prepared {
                    Ok(statement) => statement,
                    Err(e) => return Err(LakeSoulMetaDataError::from(e)),
                };

                for data_commit_info in &data_commit_info_list {
                    let file_ops = data_commit_info
                        .file_ops
                        .iter()
                        .map(DataFileOp::from_proto_data_file_op)
                        .collect::<Result<Vec<DataFileOp>>>()?;
                    let commit_id = data_commit_info.commit_id.as_ref().ok_or(
                        LakeSoulMetaDataError::Internal("commit_id missing".to_string()),
                    )?;
                    let _uuid = uuid::Uuid::from_u64_pair(commit_id.high, commit_id.low);

                    let result = transaction
                        .execute(
                            &statement,
                            &[
                                &data_commit_info.table_id,
                                &data_commit_info.partition_desc,
                                &_uuid,
                                &file_ops,
                                &data_commit_info.commit_op().as_str_name(),
                                &data_commit_info.timestamp,
                                &data_commit_info.committed,
                                &data_commit_info.domain,
                            ],
                        )
                        .await;
                    if let Some(e) = result.err() {
                        eprintln!("transaction insert error, err = {:?}", e);
                        return match transaction.rollback().await {
                            Ok(()) => Ok(0i32),
                            Err(e) => Err(LakeSoulMetaDataError::from(e)),
                        };
                    };
                }
                match transaction.commit().await {
                    Ok(()) => Ok(data_commit_info_list.len() as u64),
                    Err(e) => Err(e),
                }
            };
            match result {
                Ok(count) => Ok(count),
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::InsertDiscardCompressedFileInfo
            if wrapper.discard_compressed_file_info.len() == 1 =>
        {
            let discard_compressed_file_info =
                wrapper.discard_compressed_file_info.first().unwrap();
            client
                .execute(
                    &statement,
                    &[
                        &discard_compressed_file_info.file_path,
                        &discard_compressed_file_info.table_path,
                        &discard_compressed_file_info.partition_desc,
                        &discard_compressed_file_info.timestamp,
                        &NaiveDate::parse_from_str(
                            &discard_compressed_file_info.t_date,
                            "%Y-%m-%d",
                        )
                        .unwrap(),
                    ],
                )
                .await
        }
        DaoType::TransactionInsertDiscardCompressedFile => {
            let discard_compressed_file_list = wrapper.discard_compressed_file_info;
            let result = {
                let transaction = client.transaction().await?;
                let insert_statement = match transaction
                    .prepare(
                        "insert into discard_compressed_file_info (
                            file_path,
                            table_path,
                            partition_desc,
                            timestamp,
                            t_date
                        ) values ($1::TEXT, $2::TEXT, $3::TEXT, $4::BIGINT, $5::DATE)",
                    )
                    .await
                {
                    Ok(statement) => statement,
                    Err(e) => return Err(LakeSoulMetaDataError::from(e)),
                };
                for discard_compressed_file_info in &discard_compressed_file_list {
                    let result = transaction
                        .execute(
                            &insert_statement,
                            &[
                                &discard_compressed_file_info.file_path,
                                &discard_compressed_file_info.table_path,
                                &discard_compressed_file_info.partition_desc,
                                &discard_compressed_file_info.timestamp,
                                &NaiveDate::parse_from_str(
                                    &discard_compressed_file_info.t_date,
                                    "%Y-%m-%d",
                                )
                                .unwrap(),
                            ],
                        )
                        .await;

                    if let Some(e) = result.err() {
                        eprintln!("transaction insert error, err = {:?}", e);
                        return match transaction.rollback().await {
                            Ok(()) => Ok(0i32),
                            Err(e) => Err(LakeSoulMetaDataError::from(e)),
                        };
                    };
                }
                match transaction.commit().await {
                    Ok(()) => Ok(discard_compressed_file_list.len() as u64),
                    Err(e) => Err(e),
                }
            };
            match result {
                Ok(count) => Ok(count),
                Err(e) => return Err(LakeSoulMetaDataError::from(e)),
            }
        }
        _ => {
            eprintln!("InvalidInput of type={:?}: {:?}", insert_type, wrapper);
            return Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput));
        }
    };
    match result {
        Ok(count) => Ok(count as i32),
        Err(e) => Err(LakeSoulMetaDataError::from(e)),
    }
}

/// Execute the update for the coded Data Access Object.
#[instrument(level = "debug")]
pub async fn execute_update(
    client: &mut PooledClient,
    update_type: i32,
    joined_string: String,
) -> Result<i32> {
    if update_type < DAO_TYPE_UPDATE_OFFSET {
        error!("Invalid update_type_index: {:?}", update_type);
        return Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput));
    }
    let update_type = DaoType::try_from(update_type)
        .map_err(|e| LakeSoulMetaDataError::Other(Box::new(e)))?;
    let (client, statement) = get_prepared_statement(client, &update_type).await?;

    let params = joined_string
        .split(PARAM_DELIM)
        .collect::<Vec<&str>>()
        .iter()
        .map(|str| str.to_string())
        .collect::<Vec<String>>();

    let result = match update_type {
        DaoType::DeleteNamespaceByNamespace
        | DaoType::DeletePartitionInfoByTableId
        | DaoType::DeleteDataCommitInfoByTableId
        | DaoType::DeleteTableNameIdByTableId
        | DaoType::DeleteTablePathIdByTableId
        | DaoType::DeleteTablePathIdByTablePath
        | DaoType::DeleteDiscardCompressedFileInfoByFilePath
        | DaoType::DeleteDiscardCompressedFileInfoByTablePath
            if params.len() == 1 =>
        {
            client.execute(&statement, &[&params[0]]).await
        }
        DaoType::DeleteDiscardCompressedFileByFilterCondition if params.len() == 3 => {
            client
                .execute(
                    &statement,
                    &[&params[0], &params[1], &i64::from_str(&params[2])?],
                )
                .await
        }
        DaoType::DeleteTableInfoByIdAndPath
        | DaoType::DeleteTableNameIdByTableNameAndNamespace
        | DaoType::DeletePartitionInfoByTableIdAndPartitionDesc
        | DaoType::DeleteDataCommitInfoByTableIdAndPartitionDesc
            if params.len() == 2 =>
        {
            client.execute(&statement, &[&params[0], &params[1]]).await
        }
        DaoType::UpdateTableInfoPropertiesById
        | DaoType::UpdateNamespacePropertiesByNamespace
            if params.len() == 2 =>
        {
            let properties: serde_json::Value = serde_json::from_str(&params[1])?;
            client.execute(&statement, &[&params[0], &properties]).await
        }
        DaoType::DeletePreviousVersionPartition if params.len() == 3 => {
            let ts = i64::from_str(&params[2])?;
            client
                .execute(&statement, &[&params[0], &params[1], &ts])
                .await
        }
        DaoType::DeleteOneDataCommitInfoByTableIdAndPartitionDescAndCommitId
            if params.len() == 3 =>
        {
            let commit_id: uuid::Uuid = uuid::Uuid::from_str(&params[2])?;
            client
                .execute(&statement, &[&params[0], &params[1], &commit_id])
                .await
        }
        DaoType::UpdateTableInfoById if params.len() == 4 => {
            let mut statement = "update table_info set ".to_owned();
            let mut idx = 2;
            let mut filter_params = Vec::<String>::with_capacity(3);
            if !params[1].is_empty() {
                statement += format!("table_name = ${}::TEXT ", idx).as_str();
                idx += 1;
                filter_params.push(params[1].clone());
            }
            if !params[2].is_empty() {
                if idx > 2 {
                    statement += ",";
                }
                statement += format!("table_path = ${}::TEXT ", idx).as_str();
                idx += 1;
                filter_params.push(params[2].clone());
            }
            if !params[3].is_empty() {
                if idx > 2 {
                    statement += ",";
                }
                statement += format!("table_schema = ${}::TEXT ", idx).as_str();
                idx += 1;
                filter_params.push(params[3].clone());
            }
            statement += " where table_id = $1::TEXT";
            match idx {
                3 => {
                    client
                        .execute(&statement, &[&params[0], &filter_params[0]])
                        .await
                }
                4 => {
                    client
                        .execute(
                            &statement,
                            &[&params[0], &filter_params[0], &filter_params[1]],
                        )
                        .await
                }
                5 => {
                    client
                        .execute(
                            &statement,
                            &[
                                &params[0],
                                &filter_params[0],
                                &filter_params[1],
                                &filter_params[2],
                            ],
                        )
                        .await
                }
                _ => todo!(),
            }
        }
        DaoType::DeleteDataCommitInfoByTableIdAndPartitionDescAndCommitIdList
            if params.len() == 3 =>
        {
            let concated_uuid = &params[2];
            if concated_uuid.len() % 32 != 0 {
                eprintln!(
                    "Invalid params of update_type={:?}, params={:?}",
                    update_type, params
                );
                return Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput));
            }

            let uuid_list = separate_uuid(concated_uuid)?;

            let uuid_str_list = "'".to_owned() + &uuid_list.join("','") + "'";

            let statement = format!(
                "delete from data_commit_info
                where table_id = $1::TEXT and partition_desc = $2::TEXT and commit_id in ({}) ",
                uuid_str_list
            );

            let statement = client.prepare(&statement).await?;
            client.execute(&statement, &[&params[0], &params[1]]).await
        }
        _ => {
            eprintln!("InvalidInput of type={:?}: {:?}", update_type, params);
            return Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput));
        }
    };
    match result {
        Ok(count) => Ok(count as i32),
        Err(e) => Err(LakeSoulMetaDataError::from(e)),
    }
}

/// Convert the timestamp from [`tokio_postgres::Row`] to the string.
fn ts_string(res: Result<Option<Row>, Error>) -> Result<Option<String>> {
    match res {
        Ok(Some(row)) => {
            let ts = row.get::<_, Option<i64>>(0);
            match ts {
                Some(ts) => Ok(Some(format!("{}", ts))),
                None => Ok(None),
            }
        }
        Err(e) => Err(LakeSoulMetaDataError::from(e)),
        Ok(None) => Ok(None),
    }
}

/// Execute the query scalar for the coded Data Access Object.
#[instrument(level = "debug")]
pub async fn execute_query_scalar(
    client: &mut PooledClient,
    query_type: i32,
    joined_string: String,
) -> Result<Option<String>, LakeSoulMetaDataError> {
    if !(DAO_TYPE_QUERY_SCALAR_OFFSET..DAO_TYPE_UPDATE_OFFSET).contains(&query_type) {
        error!("Invalid update_scalar_type_index: {:?}", query_type);
        return Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput));
    }
    let query_type = DaoType::try_from(query_type)
        .map_err(|e| LakeSoulMetaDataError::Other(Box::new(e)))?;
    let (client, statement) = get_prepared_statement(client, &query_type).await?;

    let params = get_params(joined_string);

    match query_type {
        DaoType::GetLatestTimestampFromPartitionInfoWithoutPartitionDesc
            if params.len() == 1 =>
        {
            let result = client.query_opt(&statement, &[&params[0]]).await;
            ts_string(result)
        }
        DaoType::GetLatestTimestampFromPartitionInfo if params.len() == 2 => {
            let result = client
                .query_opt(&statement, &[&params[0], &params[1]])
                .await;
            match result {
                Ok(Some(row)) => Ok(Some(format!("{}", row.get::<_, i64>(0)))),
                Ok(None) => Ok(None),
                Err(e) => Err(LakeSoulMetaDataError::from(e)),
            }
        }
        DaoType::GetLatestVersionUpToTimeFromPartitionInfo if params.len() == 3 => {
            let result = client
                .query_opt(
                    &statement,
                    &[&params[0], &params[1], &i64::from_str(&params[2])?],
                )
                .await;
            match result {
                Ok(Some(row)) => {
                    let ts = row.get::<_, Option<i32>>(0);
                    match ts {
                        Some(ts) => Ok(Some(format!("{}", ts))),
                        None => Ok(None),
                    }
                }
                Err(e) => Err(LakeSoulMetaDataError::from(e)),
                Ok(None) => Ok(None),
            }
        }
        DaoType::GetLatestVersionTimestampUpToTimeFromPartitionInfo
            if params.len() == 3 =>
        {
            let result = client
                .query_opt(
                    &statement,
                    &[&params[0], &params[1], &i64::from_str(&params[2])?],
                )
                .await;
            ts_string(result)
        }

        _ => {
            eprintln!("InvalidInput of type={:?}: {:?}", query_type, params);
            Err(LakeSoulMetaDataError::from(ErrorKind::InvalidInput))
        }
    }
}

pub async fn clean_meta_for_test(client: &PooledClient) -> Result<i32> {
    let result = client
        .batch_execute(
            "delete from namespace;
            delete from data_commit_info;
            delete from table_info;
            delete from table_path_id;
            delete from table_name_id;
            delete from partition_info;
            delete from discard_compressed_file_info",
            RW,
        )
        .await;
    match result {
        Ok(_) => Ok(0i32),
        Err(e) => Err(e),
    }
}

/// Create a pg connection, return pg client.
pub async fn create_connection(
    config: String,
    secondary_config: Option<String>,
) -> Result<PooledClient> {
    PooledClient::try_new(config, secondary_config).await
}

/// Convert the uuid list from [`tokio_postgres::Row`] to the [`entity::Uuid`] list.
fn row_to_uuid_list(row: &Row) -> Vec<entity::Uuid> {
    row.get::<_, Vec<uuid::Uuid>>(4)
        .iter()
        .map(|uuid| {
            let (high, low) = uuid.as_u64_pair();
            entity::Uuid { high, low }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use proto::proto::entity;

    #[test]
    fn test_entity() -> std::io::Result<()> {
        let namespace = entity::Namespace {
            namespace: "default".to_owned(),
            properties: "{}".to_owned(),
            comment: "".to_owned(),
            domain: "public".to_owned(),
        };
        println!("{:?}", namespace);
        println!("{:?}", entity::Namespace::default());

        let table_info = entity::TableInfo {
            table_id: uuid::Uuid::new_v4().to_string(),
            table_namespace: "default".to_owned(),
            table_name: "test_table_name".to_owned(),
            table_path: "test_table_path".to_owned(),
            table_schema: "StructType {}".to_owned(),
            properties: "{}".to_owned(),
            partitions: "".to_owned(),
            domain: "public".to_owned(),
        };
        println!("{:?}", table_info);
        println!("{:?}", table_info.encode_to_vec());
        println!("{:?}", table_info.encode_length_delimited_to_vec());
        println!("{:?}", table_info.encode_length_delimited_to_vec().len());
        println!("{:?}", entity::TableInfo::default());

        let meta_info = entity::MetaInfo {
            list_partition: vec![],
            table_info: None,
            read_partition_info: vec![],
        };
        println!("{:?}", meta_info);
        println!("{:?}", entity::MetaInfo::default());
        println!("{:?}", entity::TableNameId::default());
        println!("{:?}", entity::TablePathId::default());
        println!("{:?}", entity::PartitionInfo::default());
        println!("{:?}", entity::DataCommitInfo::default());

        let mut wrapper = entity::JniWrapper::default();
        println!("{:?}", wrapper);
        wrapper.namespace = vec![namespace];
        println!("{:?}", wrapper.namespace);

        Ok(())
    }
}
