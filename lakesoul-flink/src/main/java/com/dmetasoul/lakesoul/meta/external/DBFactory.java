// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.external;

import com.dmetasoul.lakesoul.meta.dao.*;

public class DBFactory {

    private static NamespaceDao namespaceDao;
    private static TableInfoDao tableInfoDao;
    private static TableNameIdDao TableNameIdDao;
    private static TablePathIdDao tablePathIdDao;
    private static DataCommitInfoDao dataCommitInfoDao;
    private static PartitionInfoDao partitionInfoDao;

    private DBFactory(){}

    public static NamespaceDao getNamespaceDao() {
        if (namespaceDao == null) {
            namespaceDao = new NamespaceDao();
        }
        return namespaceDao;
    }

    public static TableInfoDao getTableInfoDao() {
        if (tableInfoDao == null) {
            tableInfoDao = new TableInfoDao();
        }
        return tableInfoDao;
    }

    public static com.dmetasoul.lakesoul.meta.dao.TableNameIdDao getTableNameIdDao() {
        if (TableNameIdDao == null) {
            TableNameIdDao = new TableNameIdDao();
        }
        return TableNameIdDao;
    }

    public static TablePathIdDao getTablePathIdDao() {
        if (tablePathIdDao == null) {
            tablePathIdDao = new TablePathIdDao();
        }
        return tablePathIdDao;
    }

    public static DataCommitInfoDao getDataCommitInfoDao() {
        if (dataCommitInfoDao == null) {
            dataCommitInfoDao = new DataCommitInfoDao();
        }
        return dataCommitInfoDao;
    }

    public static PartitionInfoDao getPartitionInfoDao() {
        if (partitionInfoDao == null) {
            partitionInfoDao = new PartitionInfoDao();
        }
        return partitionInfoDao;
    }
}
