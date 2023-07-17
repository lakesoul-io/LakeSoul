// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import com.dmetasoul.lakesoul.meta.dao.DataCommitInfoDao;
import com.dmetasoul.lakesoul.meta.dao.NamespaceDao;
import com.dmetasoul.lakesoul.meta.dao.PartitionInfoDao;
import com.dmetasoul.lakesoul.meta.dao.TableInfoDao;
import com.dmetasoul.lakesoul.meta.dao.TablePathIdDao;
import com.dmetasoul.lakesoul.meta.dao.TableNameIdDao;

public class DBFactory {

    private static volatile NamespaceDao namespaceDao;
    private static volatile TableInfoDao tableInfoDao;
    private static volatile TableNameIdDao TableNameIdDao;
    private static volatile TablePathIdDao tablePathIdDao;
    private static volatile DataCommitInfoDao dataCommitInfoDao;
    private static volatile PartitionInfoDao partitionInfoDao;

    private DBFactory(){}

    public static NamespaceDao getNamespaceDao() {
        if (namespaceDao == null) {
            synchronized (NamespaceDao.class) {
                if (namespaceDao == null) {
                    namespaceDao = new NamespaceDao();
                }
            }
        }
        return namespaceDao;
    }

    public static TableInfoDao getTableInfoDao() {
        if (tableInfoDao == null) {
            synchronized (TableInfoDao.class) {
                if (tableInfoDao == null) {
                    tableInfoDao = new TableInfoDao();
                }
            }
        }
        return tableInfoDao;
    }

    public static TableNameIdDao getTableNameIdDao() {
        if (TableNameIdDao == null) {
            synchronized (TableNameIdDao.class) {
                if (TableNameIdDao == null) {
                    TableNameIdDao = new TableNameIdDao();
                }
            }
        }
        return TableNameIdDao;
    }

    public static TablePathIdDao getTablePathIdDao() {
        if (tablePathIdDao == null) {
            synchronized (TablePathIdDao.class) {
                if (tablePathIdDao == null) {
                    tablePathIdDao = new TablePathIdDao();
                }
            }
        }
        return tablePathIdDao;
    }

    public static DataCommitInfoDao getDataCommitInfoDao() {
        if (dataCommitInfoDao == null) {
            synchronized (DataCommitInfoDao.class) {
                if (dataCommitInfoDao == null) {
                    dataCommitInfoDao = new DataCommitInfoDao();
                }
            }
        }
        return dataCommitInfoDao;
    }

    public static PartitionInfoDao getPartitionInfoDao() {
        if (partitionInfoDao == null) {
            synchronized (PartitionInfoDao.class) {
                if (partitionInfoDao == null) {
                    partitionInfoDao = new PartitionInfoDao();
                }
            }
        }
        return partitionInfoDao;
    }
}
