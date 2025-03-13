# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

PARAM_DELIM = "__DELIM__"

DAO_TYPE_QUERY_ONE_OFFSET: int = 0
DAO_TYPE_QUERY_LIST_OFFSET = 100
DAO_TYPE_INSERT_ONE_OFFSET = 200
DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET = 300
DAO_TYPE_QUERY_SCALAR_OFFSET = 400
DAO_TYPE_UPDATE_OFFSET = 500


class DaoType:
    SelectNamespaceByNamespace = DAO_TYPE_QUERY_ONE_OFFSET,
    SelectTablePathIdByTablePath = DAO_TYPE_QUERY_ONE_OFFSET + 1,
    SelectTableInfoByTableId = DAO_TYPE_QUERY_ONE_OFFSET + 2,
    SelectTableNameIdByTableName = DAO_TYPE_QUERY_ONE_OFFSET + 3,
    SelectTableInfoByTableNameAndNameSpace = DAO_TYPE_QUERY_ONE_OFFSET + 4,
    SelectTableInfoByTablePath = DAO_TYPE_QUERY_ONE_OFFSET + 5,
    SelectTableInfoByIdAndTablePath = DAO_TYPE_QUERY_ONE_OFFSET + 6,

    SelectOnePartitionVersionByTableIdAndDesc = DAO_TYPE_QUERY_ONE_OFFSET + 7,
    SelectPartitionVersionByTableIdAndDescAndVersion = DAO_TYPE_QUERY_ONE_OFFSET + 8,

    SelectOneDataCommitInfoByTableIdAndPartitionDescAndCommitId = DAO_TYPE_QUERY_ONE_OFFSET + 9,

    # // ==== Query List ====

    ListNamespaces = DAO_TYPE_QUERY_LIST_OFFSET,
    ListTableNameByNamespace = DAO_TYPE_QUERY_LIST_OFFSET + 1,
    ListAllTablePath = DAO_TYPE_QUERY_LIST_OFFSET + 2,
    ListAllPathTablePathByNamespace = DAO_TYPE_QUERY_LIST_OFFSET + 3,

    # // Query Partition List
    ListPartitionByTableId = DAO_TYPE_QUERY_LIST_OFFSET + 4,
    ListPartitionDescByTableIdAndParList = DAO_TYPE_QUERY_LIST_OFFSET + 5,
    ListPartitionByTableIdAndDesc = DAO_TYPE_QUERY_LIST_OFFSET + 6,
    ListPartitionVersionByTableIdAndPartitionDescAndVersionRange = DAO_TYPE_QUERY_LIST_OFFSET + 7,
    ListPartitionVersionByTableIdAndPartitionDescAndTimestampRange = DAO_TYPE_QUERY_LIST_OFFSET + 8,
    ListCommitOpsBetweenVersions = DAO_TYPE_QUERY_LIST_OFFSET + 9,

    # // Query DataCommitInfo List
    ListDataCommitInfoByTableIdAndPartitionDescAndCommitList = DAO_TYPE_QUERY_LIST_OFFSET + 10,

    # // ==== Insert One ====
    InsertNamespace = DAO_TYPE_INSERT_ONE_OFFSET,
    InsertTablePathId = DAO_TYPE_INSERT_ONE_OFFSET + 1,
    InsertTableNameId = DAO_TYPE_INSERT_ONE_OFFSET + 2,
    InsertTableInfo = DAO_TYPE_INSERT_ONE_OFFSET + 3,
    InsertPartitionInfo = DAO_TYPE_INSERT_ONE_OFFSET + 4,
    InsertDataCommitInfo = DAO_TYPE_INSERT_ONE_OFFSET + 5,

    # // ==== Transaction Insert List ====
    TransactionInsertPartitionInfo = DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET,
    TransactionInsertDataCommitInfo = DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET + 1,
    TransactionInsertDiscardCompressedFile = DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET + 2,

    # // ==== Query SCALAR ====
    GetLatestTimestampFromPartitionInfo = DAO_TYPE_QUERY_SCALAR_OFFSET,
    GetLatestTimestampFromPartitionInfoWithoutPartitionDesc = DAO_TYPE_QUERY_SCALAR_OFFSET + 1,
    GetLatestVersionUpToTimeFromPartitionInfo = DAO_TYPE_QUERY_SCALAR_OFFSET + 2,
    GetLatestVersionTimestampUpToTimeFromPartitionInfo = DAO_TYPE_QUERY_SCALAR_OFFSET + 3,

    # // ==== Update ====
    # // Update Namespace
    DeleteNamespaceByNamespace = DAO_TYPE_UPDATE_OFFSET,
    UpdateNamespacePropertiesByNamespace = DAO_TYPE_UPDATE_OFFSET + 1,

    # // Update TableInfo
    DeleteTableInfoByIdAndPath = DAO_TYPE_UPDATE_OFFSET + 2,
    UpdateTableInfoPropertiesById = DAO_TYPE_UPDATE_OFFSET + 3,
    UpdateTableInfoById = DAO_TYPE_UPDATE_OFFSET + 4,

    # // Update TablePathId
    DeleteTablePathIdByTablePath = DAO_TYPE_UPDATE_OFFSET + 5,
    DeleteTablePathIdByTableId = DAO_TYPE_UPDATE_OFFSET + 6,
    # // Update TableNameId
    DeleteTableNameIdByTableNameAndNamespace = DAO_TYPE_UPDATE_OFFSET + 7,
    DeleteTableNameIdByTableId = DAO_TYPE_UPDATE_OFFSET + 8,
    # // Update PartitionInfo
    DeletePartitionInfoByTableIdAndPartitionDesc = DAO_TYPE_UPDATE_OFFSET + 9,
    DeletePartitionInfoByTableId = DAO_TYPE_UPDATE_OFFSET + 10,
    DeletePreviousVersionPartition = DAO_TYPE_UPDATE_OFFSET + 11,
    # // Update DataCommitInfo
    DeleteOneDataCommitInfoByTableIdAndPartitionDescAndCommitId = DAO_TYPE_UPDATE_OFFSET + 12,
    DeleteDataCommitInfoByTableIdAndPartitionDescAndCommitIdList = DAO_TYPE_UPDATE_OFFSET + 13,
    DeleteDataCommitInfoByTableIdAndPartitionDesc = DAO_TYPE_UPDATE_OFFSET + 14,
    DeleteDataCommitInfoByTableId = DAO_TYPE_UPDATE_OFFSET + 15,

    DeleteDiscardCompressedFileInfoByFilePath = DAO_TYPE_UPDATE_OFFSET + 16,
    DeleteDiscardCompressedFileByFilterCondition = DAO_TYPE_UPDATE_OFFSET + 17,
    DeleteDiscardCompressedFileInfoByTablePath = DAO_TYPE_UPDATE_OFFSET + 18,
