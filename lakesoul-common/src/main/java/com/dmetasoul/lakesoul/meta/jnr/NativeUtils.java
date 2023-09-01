// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.jnr;

public class NativeUtils {

    public static boolean NATIVE_METADATA_QUERY_ENABLED = true;

    public static boolean NATIVE_METADATA_UPDATE_ENABLED = true;

    public static int NATIVE_METADATA_MAX_RETRY_ATTEMPTS = 3;

    public static int DAO_TYPE_QUERY_ONE_OFFSET = 0;
    public static int DAO_TYPE_QUERY_LIST_OFFSET = 100;
    public static int DAO_TYPE_INSERT_ONE_OFFSET = 200;
    public static int DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET = 300;
    public static int DAO_TYPE_QUERY_SCALAR_OFFSET = 400;
    public static int DAO_TYPE_UPDATE_OFFSET = 500;

    public static final String PARAM_DELIM = "__DELIM__";

    public static final String PARTITION_DESC_DELIM = "_DELIM_";

    public enum CodedDaoType {
        // ==== Query One ====
        SelectNamespaceByNamespace(DAO_TYPE_QUERY_ONE_OFFSET, 1),
        SelectTablePathIdByTablePath(DAO_TYPE_QUERY_ONE_OFFSET + 1, 1),
        SelectTableInfoByTableId(DAO_TYPE_QUERY_ONE_OFFSET + 2, 1),
        SelectTableNameIdByTableName(DAO_TYPE_QUERY_ONE_OFFSET + 3, 2),
        SelectTableInfoByTableNameAndNameSpace(DAO_TYPE_QUERY_ONE_OFFSET + 4, 2),
        SelectTableInfoByTablePath(DAO_TYPE_QUERY_ONE_OFFSET + 5, 1),
        SelectTableInfoByIdAndTablePath(DAO_TYPE_QUERY_ONE_OFFSET + 6, 2),

        SelectOnePartitionVersionByTableIdAndDesc(DAO_TYPE_QUERY_ONE_OFFSET + 7, 2),
        SelectPartitionVersionByTableIdAndDescAndVersion(DAO_TYPE_QUERY_ONE_OFFSET + 8, 3),

        SelectOneDataCommitInfoByTableIdAndPartitionDescAndCommitId(DAO_TYPE_QUERY_ONE_OFFSET + 9, 3),
        SelectOneDataCommitInfoByTableId(DAO_TYPE_QUERY_ONE_OFFSET + 10, 3),

        // ==== Query List ====

        ListNamespaces(DAO_TYPE_QUERY_LIST_OFFSET),
        ListTableNameByNamespace(DAO_TYPE_QUERY_LIST_OFFSET + 1, 1),
        ListAllTablePath(DAO_TYPE_QUERY_LIST_OFFSET + 2),
        ListAllPathTablePathByNamespace(DAO_TYPE_QUERY_LIST_OFFSET + 3, 1),

        // Query Partition List
        ListPartitionByTableId(DAO_TYPE_QUERY_LIST_OFFSET + 4, 1),
        ListPartitionDescByTableIdAndParList(DAO_TYPE_QUERY_LIST_OFFSET + 5, 2),
        ListPartitionByTableIdAndDesc(DAO_TYPE_QUERY_LIST_OFFSET + 6, 2),
        ListPartitionVersionByTableIdAndPartitionDescAndVersionRange(DAO_TYPE_QUERY_LIST_OFFSET + 7, 4),
        ListPartitionVersionByTableIdAndPartitionDescAndTimestampRange(DAO_TYPE_QUERY_LIST_OFFSET + 8, 4),
        ListCommitOpsBetweenVersions(DAO_TYPE_QUERY_LIST_OFFSET + 9, 4),

        // Query DataCommitInfo List
        ListDataCommitInfoByTableIdAndPartitionDescAndCommitList(DAO_TYPE_QUERY_LIST_OFFSET + 10, 3),

        // ==== Insert One ====
        InsertNamespace(DAO_TYPE_INSERT_ONE_OFFSET),
        InsertTablePathId(DAO_TYPE_INSERT_ONE_OFFSET + 1),
        InsertTableNameId(DAO_TYPE_INSERT_ONE_OFFSET + 2),
        InsertTableInfo(DAO_TYPE_INSERT_ONE_OFFSET + 3),
        InsertPartitionInfo(DAO_TYPE_INSERT_ONE_OFFSET + 4),
        InsertDataCommitInfo(DAO_TYPE_INSERT_ONE_OFFSET + 5),

        // ==== Transaction Insert List ====
        TransactionInsertPartitionInfo(DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET),
        TransactionInsertDataCommitInfo(DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET + 1),

        // ==== Query SCALAR ====
        GetLatestTimestampFromPartitionInfo(DAO_TYPE_QUERY_SCALAR_OFFSET, 2),
        GetLatestTimestampFromPartitionInfoWithoutPartitionDesc(DAO_TYPE_QUERY_SCALAR_OFFSET + 1, 1),
        GetLatestVersionUpToTimeFromPartitionInfo(DAO_TYPE_QUERY_SCALAR_OFFSET + 2, 3),
        GetLatestVersionTimestampUpToTimeFromPartitionInfo(DAO_TYPE_QUERY_SCALAR_OFFSET + 3, 3),

        // ==== Update ====
        // Update Namespace
        DeleteNamespaceByNamespace(DAO_TYPE_UPDATE_OFFSET, 1),
        UpdateNamespacePropertiesByNamespace(DAO_TYPE_UPDATE_OFFSET + 1, 2),

        // Update TableInfo
        DeleteTableInfoByIdAndPath(DAO_TYPE_UPDATE_OFFSET + 2, 2),
        UpdateTableInfoPropertiesById(DAO_TYPE_UPDATE_OFFSET + 3, 2),
        UpdateTableInfoById(DAO_TYPE_UPDATE_OFFSET + 4, 4),

        // Update TablePathId
        DeleteTablePathIdByTablePath(DAO_TYPE_UPDATE_OFFSET + 5, 1),
        DeleteTablePathIdByTableId(DAO_TYPE_UPDATE_OFFSET + 6, 1),
        // Update TableNameId
        DeleteTableNameIdByTableNameAndNamespace(DAO_TYPE_UPDATE_OFFSET + 7, 2),
        DeleteTableNameIdByTableId(DAO_TYPE_UPDATE_OFFSET + 8, 1),
        // Update PartitionInfo
        DeletePartitionInfoByTableIdAndPartitionDesc(DAO_TYPE_UPDATE_OFFSET + 9, 2),
        DeletePartitionInfoByTableId(DAO_TYPE_UPDATE_OFFSET + 10, 1),
        DeletePreviousVersionPartition(DAO_TYPE_UPDATE_OFFSET + 11, 3),
        // Update DataCommitInfo
        DeleteOneDataCommitInfoByTableIdAndPartitionDescAndCommitId(DAO_TYPE_UPDATE_OFFSET + 12, 3),
        DeleteDataCommitInfoByTableIdAndPartitionDescAndCommitIdList(DAO_TYPE_UPDATE_OFFSET + 13, 3),
        DeleteDataCommitInfoByTableIdAndPartitionDesc(DAO_TYPE_UPDATE_OFFSET + 14, 2),
        DeleteDataCommitInfoByTableId(DAO_TYPE_UPDATE_OFFSET + 15, 1),
        ;

        private final int code;
        private final int paramsNum;

        CodedDaoType(int code) {
            this(code, 0);
        }

        CodedDaoType(int code, int paramsNum) {
            this.code = code;
            this.paramsNum = paramsNum;
        }

        public int getCode() {
            return code;
        }

        public int getParamsNum() {
            return paramsNum;
        }
    }

}
