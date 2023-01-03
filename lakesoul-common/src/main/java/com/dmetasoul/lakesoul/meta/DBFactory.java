/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.meta;

import com.dmetasoul.lakesoul.meta.dao.*;

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

    public static com.dmetasoul.lakesoul.meta.dao.TableNameIdDao getTableNameIdDao() {
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
