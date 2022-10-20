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

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.dao.*;
import com.dmetasoul.lakesoul.meta.entity.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class DBManager {

    private static final Logger LOG = LoggerFactory.getLogger(DBManager.class);

    private final NamespaceDao namespaceDao;
    private final TableInfoDao tableInfoDao;
    private final TableNameIdDao tableNameIdDao;
    private final TablePathIdDao tablePathIdDao;
    private final DataCommitInfoDao dataCommitInfoDao;
    private final PartitionInfoDao partitionInfoDao;

    public DBManager() {
        namespaceDao = DBFactory.getNamespaceDao();
        tableInfoDao = DBFactory.getTableInfoDao();
        tableNameIdDao = DBFactory.getTableNameIdDao();
        tablePathIdDao = DBFactory.getTablePathIdDao();
        dataCommitInfoDao = DBFactory.getDataCommitInfoDao();
        partitionInfoDao = DBFactory.getPartitionInfoDao();
    }

    public boolean isNamespaceExists(String table_namespace) {
        Namespace namespace = namespaceDao.findByNamespace(table_namespace);
        return namespace != null;
    }

    public boolean isTableExists(String tablePath) {
        TablePathId tablePathId = tablePathIdDao.findByTablePath(tablePath);
        if (tablePathId == null) {
            return false;
        }
        TableInfo tableInfo = tableInfoDao.selectByTableId(tablePathId.getTableId());
        return tableInfo != null;
    }

    public boolean isTableExistsByTableName(String tableName) {
        return isTableExistsByTableName(tableName, "default");
    }

    public boolean isTableExistsByTableName(String tableName, String tableNamespace) {
        TableNameId tableNameId = tableNameIdDao.findByTableName(tableName, tableNamespace);
        if (tableNameId == null) {
            return false;
        }
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableNameId.getTableId());
        return tableInfo != null;
    }

    public boolean isTableIdExists(String tablePath, String tableId) {
        TableInfo tableInfo = tableInfoDao.selectByIdAndTablePath(tableId, tablePath);
        return tableInfo != null;
    }

    public TableNameId shortTableName(String tableName, String tableNamespace) {
        return tableNameIdDao.findByTableName(tableName, tableNamespace);
    }

    public String getTablePathFromShortTableName(String tableName, String tableNamespace) {
        TableNameId tableNameId = tableNameIdDao.findByTableName(tableName, tableNamespace);
        if (tableNameId.getTableId() == null) return null;

        TableInfo tableInfo = tableInfoDao.selectByTableId(tableNameId.getTableId());
        return tableInfo.getTablePath();
    }

    public TableInfo getTableInfoByName(String tableName) {
        return getTableInfoByNameAndNamespace(tableName, "default");
    }

    public TableInfo getTableInfoByNameAndNamespace(String tableName, String namespace) {
        return tableInfoDao.selectByTableNameAndNameSpace(tableName, namespace);
    }

    public void createNewTable(String tableId, String namespace, String tableName, String tablePath, String tableSchema,
                               JSONObject properties, String partitions) {

        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId(tableId);
        tableInfo.setTableNamespace(namespace);
        tableInfo.setTableName(tableName);
        tableInfo.setTablePath(tablePath);
        tableInfo.setTableSchema(tableSchema);
        tableInfo.setPartitions(partitions);
        tableInfo.setProperties(properties);

        if (StringUtils.isNotBlank(tableName)) {
            if (!tableNameIdDao.insert(new TableNameId(tableName, tableId, namespace))) {
                throw new IllegalStateException("this table name already exists!");
            }
        }
        if (StringUtils.isNotBlank(tablePath)) {
            if (!tablePathIdDao.insert(new TablePathId(tablePath, tableId, namespace))) {
                tableNameIdDao.deleteByTableId(tableId);
                throw new IllegalStateException("this table path already exists!");
            }
        }
        if (!tableInfoDao.insert(tableInfo)) {
            tableNameIdDao.deleteByTableId(tableId);
            tablePathIdDao.deleteByTableId(tableId);
            throw new IllegalStateException("this table info already exists!");
        }
    }

    public List<String> listTables() {
        return tablePathIdDao.listAllPath();
    }

    public List<String> listTablePathsByNamespace(String table_namespace) {
        return tablePathIdDao.listAllPathByNamespace(table_namespace);
    }

    public TableInfo getTableInfoByPath(String tablePath) {
        return tableInfoDao.selectByTablePath(tablePath);
    }

    public PartitionInfo getSinglePartitionInfo(String tableId, String partitionDesc) {
        return partitionInfoDao.selectLatestPartitionInfo(tableId, partitionDesc);
    }

    //for partition snapshot with some version
    public PartitionInfo getSinglePartitionInfo(String tableId, String partitionDesc,int version) {
        return partitionInfoDao.findByKey(tableId, partitionDesc, version);
    }

    public List<PartitionInfo> getAllPartitionInfo(String tableId) {
        return partitionInfoDao.getPartitionDescByTableId(tableId);
    }

    public List<PartitionInfo> getOnePartitionVersions(String tableId,String partitionDesc) {
        return partitionInfoDao.getPartitionVersions(tableId,partitionDesc);
    }

    public void updateTableSchema(String tableId, String tableSchema) {
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        tableInfo.setTableSchema(tableSchema);
        tableInfoDao.updateByTableId(tableId, "", "", tableSchema);
    }

    public void deleteTableInfo(String tablePath, String tableId, String tableNamespace) {
        tablePathIdDao.delete(tablePath);
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        String tableName = tableInfo.getTableName();
        if (StringUtils.isNotBlank(tableName)) {
            tableNameIdDao.delete(tableName, tableNamespace);
        }
        tableInfoDao.deleteByIdAndPath(tableId, tablePath);
    }

    public void deletePartitionInfoByTableId(String tableId) {
        partitionInfoDao.deleteByTableId(tableId);
    }

    public void deletePartitionInfoByTableAndPartition(String tableId, String partitionDesc) {
        partitionInfoDao.deleteByTableIdAndPartitionDesc(tableId, partitionDesc);
        dataCommitInfoDao.deleteByTableIdAndPartitionDesc(tableId, partitionDesc);
    }

    public void logicDeletePartitionInfoByTableId(String tableId) {
        List<PartitionInfo> curPartitionInfoList = partitionInfoDao.getPartitionDescByTableId(tableId);
        for (PartitionInfo p : curPartitionInfoList) {
            int version = p.getVersion();
            p.setVersion(version + 1);
            p.setSnapshot(Collections.emptyList());
            p.setCommitOp("DeleteCommit");
            p.setExpression("");
        }
        partitionInfoDao.transactionInsert(curPartitionInfoList, Collections.emptyList());
    }

    public void logicDeletePartitionInfoByRangeId(String tableId, String partitionDesc) {
        PartitionInfo partitionInfo = getSinglePartitionInfo(tableId, partitionDesc);
        int version = partitionInfo.getVersion();
        partitionInfo.setVersion(version + 1);
        partitionInfo.setSnapshot(Collections.emptyList());
        partitionInfo.setCommitOp("DeleteCommit");
        partitionInfo.setExpression("");
        partitionInfoDao.insert(partitionInfo);
    }

    public void deleteDataCommitInfo(String tableId, String partitionDesc, UUID commitId) {
        if (StringUtils.isNotBlank(commitId.toString())) {
            dataCommitInfoDao.deleteByPrimaryKey(tableId, partitionDesc, commitId);
        } else {
            deleteDataCommitInfo(tableId, partitionDesc);
        }
    }

    public void deleteDataCommitInfo(String tableId, String partitionDesc) {
        if (StringUtils.isNotBlank(partitionDesc)) {
            dataCommitInfoDao.deleteByTableIdAndPartitionDesc(tableId, partitionDesc);
        } else {
            deleteDataCommitInfo(tableId);
        }
    }

    public void deleteDataCommitInfo(String tableId) {
        dataCommitInfoDao.deleteByTableId(tableId);
    }

    public void deleteShortTableName(String tableName, String tablePath, String tableNamespace) {
        tableNameIdDao.delete(tableName, tableNamespace);
    }

    public void addShortTableName(String tableName, String tablePath) {
        TableInfo tableInfo = getTableInfoByPath(tablePath);

        TableNameId tableNameId = new TableNameId();
        tableNameId.setTableId(tableInfo.getTableId());
        tableNameId.setTableName(tableName);
        tableNameIdDao.insert(tableNameId);
    }

    public void updateTableProperties(String tableId, JSONObject properties) {
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        tableInfo.setProperties(properties);
        tableInfoDao.updatePropertiesById(tableId, properties);
    }

    public void updateTableShortName(String tablePath, String tableId, String tableName, String tableNamespace) {

        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        if (tableInfo.getTableName() != null && !Objects.equals(tableInfo.getTableName(), "")) {
            if (!tableInfo.getTableName().equals(tableName)) {
                throw new IllegalStateException("Table name already exists " + tableInfo.getTableName() + " for table id "
                        + tableId);
            }
            return;
        }
        tableInfo.setTableName(tableName);
        tableInfo.setTablePath(tablePath);
        tableInfo.setTableNamespace(tableNamespace);
        tableInfoDao.updateByTableId(tableId, tableName, tablePath, "");

        TableNameId tableNameId = new TableNameId();
        tableNameId.setTableName(tableName);
        tableNameId.setTableId(tableId);
        tableNameId.setTableNamespace(tableNamespace);
        tableNameIdDao.insert(tableNameId);
    }

    public boolean batchCommitDataCommitInfo(List<DataCommitInfo> listData) {
        return dataCommitInfoDao.batchInsert(listData);
    }

    public boolean commitData(MetaInfo metaInfo, boolean changeSchema, String commitOp) {
        List<PartitionInfo> listPartitionInfo = metaInfo.getListPartition();
        TableInfo tableInfo = metaInfo.getTableInfo();
        String tableId = tableInfo.getTableId();

        if (tableInfo.getTableName() != null && !"".equals(tableInfo.getTableName())) {
            updateTableShortName(tableInfo.getTablePath(), tableInfo.getTableId(), tableInfo.getTableName(), tableInfo.getTableNamespace());
        }
       updateTableProperties(tableId, tableInfo.getProperties());

        List<PartitionInfo> newPartitionList = new ArrayList<>();
        Map<String, PartitionInfo> rawMap = new HashMap<>();
        Map<String, PartitionInfo> newMap = new HashMap<>();
        List<String> partitionDescList = new ArrayList<>();
        List<UUID> snapshotList = new ArrayList<>();

        for (PartitionInfo partitionInfo : listPartitionInfo) {
            String partitionDesc = partitionInfo.getPartitionDesc();
            rawMap.put(partitionDesc, partitionInfo);
            partitionDescList.add(partitionDesc);
            snapshotList.addAll(partitionInfo.getSnapshot());
        }

        Map<String, PartitionInfo> curMap = new HashMap<>();
        List<PartitionInfo> curPartitionInfoList = partitionInfoDao.findByTableIdAndParList(tableId, partitionDescList);
        for (PartitionInfo curPartition : curPartitionInfoList) {
            String partitionDesc = curPartition.getPartitionDesc();
            curMap.put(partitionDesc, curPartition);
        }

        if (commitOp.equals("AppendCommit")|| commitOp.equals("MergeCommit")) {
            for (PartitionInfo partitionInfo : listPartitionInfo) {
                String partitionDesc = partitionInfo.getPartitionDesc();
                PartitionInfo curPartitionInfo = curMap.get(partitionDesc);
                if (curPartitionInfo == null) {
                    curPartitionInfo = new PartitionInfo();
                    curPartitionInfo.setTableId(tableId);
                    curPartitionInfo.setPartitionDesc(partitionDesc);
                    curPartitionInfo.setVersion(-1);
                    curPartitionInfo.setSnapshot(new ArrayList<>());
                }
                List<UUID> curSnapshot = curPartitionInfo.getSnapshot();
                int curVersion = curPartitionInfo.getVersion();
                int newVersion = curVersion + 1;

                curSnapshot.addAll(partitionInfo.getSnapshot());
                curPartitionInfo.setVersion(newVersion);
                curPartitionInfo.setSnapshot(curSnapshot);
                curPartitionInfo.setCommitOp(commitOp);
                curPartitionInfo.setExpression(partitionInfo.getExpression());
                newMap.put(partitionDesc, curPartitionInfo);
                newPartitionList.add(curPartitionInfo);
            }
        } else if (commitOp.equals("CompactionCommit")|| commitOp.equals("UpdateCommit")) {
            for (PartitionInfo partitionInfo : listPartitionInfo) {
                String partitionDesc = partitionInfo.getPartitionDesc();
                PartitionInfo curPartitionInfo = curMap.get(partitionDesc);
                if (curPartitionInfo == null) {
                    curPartitionInfo = new PartitionInfo();
                    curPartitionInfo.setTableId(tableId);
                    curPartitionInfo.setPartitionDesc(partitionDesc);
                    curPartitionInfo.setVersion(-1);
                }
                int curVersion = curPartitionInfo.getVersion();
                int newVersion = curVersion + 1;

                curPartitionInfo.setVersion(newVersion);
                curPartitionInfo.setSnapshot(partitionInfo.getSnapshot());
                curPartitionInfo.setCommitOp(commitOp);
                curPartitionInfo.setExpression(partitionInfo.getExpression());

                newMap.put(partitionDesc, curPartitionInfo);
                newPartitionList.add(curPartitionInfo);
            }
        } else {
            throw new IllegalStateException("this operation is Illegal of the table:" + tableInfo.getTablePath());
        }

        boolean notConflict = partitionInfoDao.transactionInsert(newPartitionList, snapshotList);
        if (!notConflict) {
            switch(commitOp){
                case "AppendCommit":
                    notConflict = appendConflict(tableId, partitionDescList, rawMap, newMap, snapshotList, 0);
                    break;
                case "CompactionCommit":
                    notConflict = compactionConflict(tableId, partitionDescList, rawMap, newMap, snapshotList, 0);
                    break;
                case "UpdateCommit":
                    notConflict = updateConflict(tableId, partitionDescList, rawMap, newMap, snapshotList, 0);
                    break;
                case "MergeCommit":
                    notConflict = mergeConflict(tableId, partitionDescList, rawMap, newMap, snapshotList, 0);
            }
        }

        return notConflict;
    }

    public boolean appendConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                  Map<String, PartitionInfo> newMap, List<UUID> snapshotsList, int time) {
        List<PartitionInfo> curPartitionList = partitionInfoDao.findByTableIdAndParList(tableId, partitionDescList);
        List<PartitionInfo> newPartitionList = new ArrayList<>();
        for (PartitionInfo curPartitionInfo : curPartitionList) {

            String partitionDesc = curPartitionInfo.getPartitionDesc();
            int current = curPartitionInfo.getVersion();
            int lastVersion = newMap.get(partitionDesc).getVersion();

            if (current + 1 == lastVersion) {
                newPartitionList.add(newMap.get(partitionDesc));
            } else {
                List<UUID> curSnapshot = curPartitionInfo.getSnapshot();
                int curVersion = curPartitionInfo.getVersion();
                String curCommitOp = curPartitionInfo.getCommitOp();

                int newVersion = curVersion + 1;

                PartitionInfo partitionInfo = rawMap.get(partitionDesc);
                if (curCommitOp.equals("CompactionCommit") || curCommitOp.equals("AppendCommit")) {
                    curSnapshot.addAll(partitionInfo.getSnapshot());
                    curPartitionInfo.setVersion(newVersion);
                    curPartitionInfo.setSnapshot(curSnapshot);
                    curPartitionInfo.setCommitOp(partitionInfo.getCommitOp());
                    curPartitionInfo.setExpression(partitionInfo.getExpression());
                    newPartitionList.add(curPartitionInfo);
                    newMap.put(partitionDesc, curPartitionInfo);
                } else {
                    // other operate conflict, so fail
                    throw new IllegalStateException("this tableId:" + tableId + " exists conflicting manipulation currently!");
                }
            }
        }
        boolean conflictFlag = partitionInfoDao.transactionInsert(newPartitionList, snapshotsList);
        while (!conflictFlag && time < DBConfig.MAX_COMMIT_ATTEMPTS) {
            conflictFlag = appendConflict(tableId, partitionDescList, rawMap, newMap, snapshotsList,time+1);
        }

        return conflictFlag;
    }

    public boolean compactionConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                      Map<String, PartitionInfo> newMap, List<UUID> snapshotsList, int time) {
        List<PartitionInfo> curPartitionList = partitionInfoDao.findByTableIdAndParList(tableId, partitionDescList);
        List<PartitionInfo> newPartitionList = new ArrayList<>();
        for (PartitionInfo curPartitionInfo : curPartitionList) {

            String partitionDesc = curPartitionInfo.getPartitionDesc();
            int current = curPartitionInfo.getVersion();
            int lastVersion = newMap.get(partitionDesc).getVersion();

            if (current + 1 == lastVersion) {
                newPartitionList.add(newMap.get(partitionDesc));
            } else {
                List<UUID> curSnapshot = curPartitionInfo.getSnapshot();
                int curVersion = curPartitionInfo.getVersion();
                String curCommitOp = curPartitionInfo.getCommitOp();

                if (curCommitOp.equals("AppendCommit") || curCommitOp.equals("MergeCommit")) {
                    int newVersion = curVersion + 1;
                    PartitionInfo newPartitionInfo = new PartitionInfo();
                    newPartitionInfo.setTableId(tableId);
                    newPartitionInfo.setPartitionDesc(partitionDesc);
                    newPartitionInfo.setExpression(rawMap.get(partitionDesc).getExpression());
                    List<UUID> snapshot = new ArrayList<>();

                    PartitionInfo lastVersionPartitionInfo = partitionInfoDao.findByKey(tableId, partitionDesc,lastVersion-1);
                    List<UUID> lastSnapshot = lastVersionPartitionInfo.getSnapshot();

                    PartitionInfo partitionInfo = rawMap.get(partitionDesc);

                    newPartitionInfo.setVersion(newVersion);
                    curSnapshot.removeAll(lastSnapshot);
                    snapshot.addAll(partitionInfo.getSnapshot());
                    snapshot.addAll(curSnapshot);
                    newPartitionInfo.setSnapshot(snapshot);
                    newPartitionInfo.setCommitOp(partitionInfo.getCommitOp());
                    newPartitionList.add(newPartitionInfo);
                    newMap.put(partitionDesc, newPartitionInfo);
                } else if (curCommitOp.equals("CompactionCommit")){
                    partitionDescList.remove(partitionDesc);
                } else {
                    // other operate conflict, so fail
                    throw new IllegalStateException("this tableId:" + tableId + " exists conflicting manipulation currently!");
                }
            }
        }

        boolean conflictFlag = partitionInfoDao.transactionInsert(newPartitionList, snapshotsList);
        while (!conflictFlag && time < DBConfig.MAX_COMMIT_ATTEMPTS) {
            conflictFlag = appendConflict(tableId, partitionDescList, rawMap, newMap, snapshotsList, time+1);
        }

        return conflictFlag;
    }

    public boolean updateConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                  Map<String, PartitionInfo> newMap, List<UUID> snapshotsList, int time) {
        List<PartitionInfo> curPartitionList = partitionInfoDao.findByTableIdAndParList(tableId, partitionDescList);
        List<PartitionInfo> newPartitionList = new ArrayList<>();
        for (PartitionInfo curPartitionInfo : curPartitionList) {

            String partitionDesc = curPartitionInfo.getPartitionDesc();
            int current = curPartitionInfo.getVersion();
            int lastVersion = newMap.get(partitionDesc).getVersion();

            if (current + 1 == lastVersion) {
                newPartitionList.add(newMap.get(partitionDesc));
            } else {
                int curVersion = curPartitionInfo.getVersion();
                String curCommitOp = curPartitionInfo.getCommitOp();

                int newVersion = curVersion + 1;

                PartitionInfo partitionInfo = rawMap.get(partitionDesc);
                if (curCommitOp.equals("CompactionCommit")) {
                    curPartitionInfo.setVersion(newVersion);
                    curPartitionInfo.setSnapshot(partitionInfo.getSnapshot());
                    curPartitionInfo.setCommitOp(partitionInfo.getCommitOp());
                    curPartitionInfo.setExpression(partitionInfo.getExpression());
                    newPartitionList.add(curPartitionInfo);
                    newMap.put(partitionDesc, curPartitionInfo);
                } else {
                    // other operate conflict, so fail
                    throw new IllegalStateException("this tableId:" + tableId + " exists conflicting manipulation currently!");
                }
            }
        }
        boolean conflictFlag = partitionInfoDao.transactionInsert(newPartitionList, snapshotsList);
        while (!conflictFlag && time < DBConfig.MAX_COMMIT_ATTEMPTS) {
            conflictFlag = updateConflict(tableId, partitionDescList, rawMap, newMap, snapshotsList, time+1);
        }
        return conflictFlag;
    }

    public boolean mergeConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                 Map<String, PartitionInfo> newMap, List<UUID> snapshotsList, int time) {
        List<PartitionInfo> curPartitionList = partitionInfoDao.findByTableIdAndParList(tableId, partitionDescList);
        List<PartitionInfo> newPartitionList = new ArrayList<>();
        for (PartitionInfo curPartitionInfo : curPartitionList) {

            String partitionDesc = curPartitionInfo.getPartitionDesc();
            int current = curPartitionInfo.getVersion();
            int lastVersion = newMap.get(partitionDesc).getVersion();

            if (current + 1 == lastVersion) {
                newPartitionList.add(newMap.get(partitionDesc));
            } else {
                List<UUID> curSnapshot = curPartitionInfo.getSnapshot();
                int curVersion = curPartitionInfo.getVersion();
                String curCommitOp = curPartitionInfo.getCommitOp();

                int newVersion = curVersion + 1;

                PartitionInfo partitionInfo = rawMap.get(partitionDesc);
                if (curCommitOp.equals("CompactionCommit")) {
                    curSnapshot.addAll(partitionInfo.getSnapshot());
                    curPartitionInfo.setVersion(newVersion);
                    curPartitionInfo.setSnapshot(curSnapshot);
                    curPartitionInfo.setCommitOp(partitionInfo.getCommitOp());
                    curPartitionInfo.setExpression(partitionInfo.getExpression());
                    newPartitionList.add(curPartitionInfo);
                    newMap.put(partitionDesc, curPartitionInfo);
                } else {
                    // other operate conflict, so fail
                    throw new IllegalStateException("this tableId:" + tableId + " exists conflicting manipulation currently!");
                }
            }
        }
        boolean conflictFlag = partitionInfoDao.transactionInsert(newPartitionList, snapshotsList);
        while (!conflictFlag && time < DBConfig.MAX_COMMIT_ATTEMPTS) {
            conflictFlag = mergeConflict(tableId, partitionDescList, rawMap, newMap, snapshotsList, time+1);
        }

        return conflictFlag;
    }

    public List<DataCommitInfo> getTableSinglePartitionDataInfo(PartitionInfo partitionInfo) {
        String tableId = partitionInfo.getTableId();
        String partitionDesc = partitionInfo.getPartitionDesc();
        List<UUID> snapshotList = partitionInfo.getSnapshot();

        return dataCommitInfoDao.selectByTableIdPartitionDescCommitList(tableId, partitionDesc, snapshotList);
    }

    public List<DataCommitInfo> getPartitionSnapshot(String tableId, String partitionDesc, int version) {
        PartitionInfo partitionInfo = partitionInfoDao.findByKey(tableId, partitionDesc, version);
        List<UUID> commitList = partitionInfo.getSnapshot();
        return dataCommitInfoDao.selectByTableIdPartitionDescCommitList(tableId, partitionDesc, commitList);
    }

    public boolean rollbackPartitionByVersion(String tableId, String partitionDesc, int version) {
        PartitionInfo partitionInfo = partitionInfoDao.findByKey(tableId, partitionDesc, version);
        if(partitionInfo.getTableId() == null){
            return false;
        }
        PartitionInfo curPartitionInfo = partitionInfoDao.selectLatestPartitionInfo(tableId, partitionDesc);
        partitionInfo.setVersion(curPartitionInfo.getVersion() + 1);
        return partitionInfoDao.insert(partitionInfo);
    }

    public void commitDataCommitInfo(DataCommitInfo dataCommitInfo) throws IOException {
        String tableId = dataCommitInfo.getTableId();
        String partitionDesc = dataCommitInfo.getPartitionDesc();
        UUID commitId = dataCommitInfo.getCommitId();
        String commitOp = dataCommitInfo.getCommitOp();
        DataCommitInfo metaCommitInfo = dataCommitInfoDao.selectByPrimaryKey(tableId, partitionDesc, commitId);
        if (metaCommitInfo != null && metaCommitInfo.isCommitted()) {
            LOG.info("DataCommitInfo with tableId={}, commitId={} committed already",
                     tableId, commitId.toString());
            return;
        } else if (metaCommitInfo == null) {
            dataCommitInfoDao.insert(dataCommitInfo);
        }
        MetaInfo metaInfo = new MetaInfo();
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);

        List<UUID> snapshot = new ArrayList<>();
        snapshot.add(commitId);

        List<PartitionInfo> partitionInfoList = new ArrayList<>();
        PartitionInfo p = new PartitionInfo();
        p.setTableId(tableId);
        p.setPartitionDesc(partitionDesc);
        p.setCommitOp(commitOp);
        p.setSnapshot(snapshot);
        partitionInfoList.add(p);

        metaInfo.setTableInfo(tableInfo);
        metaInfo.setListPartition(partitionInfoList);

        commitData(metaInfo, false, commitOp);
    }

    //==============
    //namespace
    //==============
    public List<String> listNamespaces() {
        return namespaceDao.listNamespaces();
    }

    public void createNewNamespace(String name,
                                   JSONObject properties,
                                   String comment) {
        Namespace namespace = new Namespace();
        namespace.setNamespace(name);
        namespace.setProperties(properties);
        namespace.setComment(comment);

        boolean insertNamespaceFlag = namespaceDao.insert(namespace);
        if (!insertNamespaceFlag) {
            throw new IllegalStateException(String.format("namespace %s already exists!", name));
        }

    }

    public Namespace getNamespaceByNamespace(String namespace) {
        Namespace namespaceEntity = namespaceDao.findByNamespace(namespace);
        return namespaceEntity;
    }

    public void updateNamespaceProperties(String namespace, JSONObject properties) {
        Namespace namespaceEntity = namespaceDao.findByNamespace(namespace);
        namespaceEntity.setProperties(properties);
        namespaceDao.updatePropertiesByNamespace(namespace, properties);
    }

    public void deleteNamespace(String namespace) {
        namespaceDao.deleteByNamespace(namespace);
    }

    // just for test
    public void cleanMeta() {

        namespaceDao.clean();
        namespaceDao.insert(new Namespace("default"));
        dataCommitInfoDao.clean();
        tableInfoDao.clean();
        tablePathIdDao.clean();
        tableNameIdDao.clean();
        partitionInfoDao.clean();
    }

}
