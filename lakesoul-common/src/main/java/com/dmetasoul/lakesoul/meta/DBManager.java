// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.dao.*;
import com.dmetasoul.lakesoul.meta.entity.*;
import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;
import com.dmetasoul.lakesoul.meta.rbac.AuthZContext;
import com.dmetasoul.lakesoul.meta.rbac.AuthZEnforcer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_RANGE_PARTITION_SPLITTER;

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
        tableNamespace = tableNamespace == null ? "default" : tableNamespace;
        return tableNameIdDao.findByTableName(tableName, tableNamespace);
    }

    public String getTablePathFromShortTableName(String tableName, String tableNamespace) {
        TableNameId tableNameId = tableNameIdDao.findByTableName(tableName, tableNamespace);
        if (tableNameId == null) return null;

        TableInfo tableInfo = tableInfoDao.selectByTableId(tableNameId.getTableId());
        return tableInfo.getTablePath();
    }

    public TableInfo getTableInfoByName(String tableName) {
        return getTableInfoByNameAndNamespace(tableName, "default");
    }

    public TableInfo getTableInfoByTableId(String tableId) {
        return tableInfoDao.selectByTableId(tableId);
    }

    public TableInfo getTableInfoByNameAndNamespace(String tableName, String namespace) {
        return tableInfoDao.selectByTableNameAndNameSpace(tableName, namespace);
    }

    public void createNewTable(String tableId, String namespace, String tableName, String tablePath, String tableSchema,
                               JSONObject properties, String partitions) {

        TableInfo.Builder tableInfo = TableInfo.newBuilder();
        tableInfo.setTableId(tableId);
        tableInfo.setTableNamespace(namespace);
        tableInfo.setTableName(tableName);
        tableInfo.setTablePath(tablePath);
        tableInfo.setTableSchema(tableSchema);
        tableInfo.setPartitions(partitions);
        properties.put(DBConfig.TableInfoProperty.LAST_TABLE_SCHEMA_CHANGE_TIME, String.valueOf(System.currentTimeMillis()));
        tableInfo.setProperties(properties.toJSONString());

        String domain = getNameSpaceDomain(namespace);

        if (StringUtils.isNotBlank(tableName)) {
            tableNameIdDao.insert(TableNameIdDao.newTableNameId(tableName, tableId, namespace, domain));
        }
        if (StringUtils.isNotBlank(tablePath)) {
            boolean ex = false;
            try {
                tablePathIdDao.insert(TablePathIdDao.newTablePathId(tablePath, tableId, namespace, domain));
            } catch (Exception e) {
                ex = true;
                throw e;
            } finally {
                if (ex) {
                    tableNameIdDao.deleteByTableId(tableId);
                }
            }
        }
        boolean ex = false;
        try {
            tableInfo.setDomain(domain);
            tableInfoDao.insert(tableInfo.build());
        } catch (Exception e) {
            ex = true;
            throw e;
        } finally {
            if (ex) {
                tableNameIdDao.deleteByTableId(tableId);
                tablePathIdDao.deleteByTableId(tableId);
            }
        }
    }

    public List<String> listTables() {
        return tablePathIdDao.listAllPath();
    }

    public List<String> listTableNamesByNamespace(String tableNamespace) {
        return tableNameIdDao.listAllNameByNamespace(tableNamespace);
    }

    public List<String> listTablePathsByNamespace(String tableNamespace) {
        return tablePathIdDao.listAllPathByNamespace(tableNamespace);
    }

    public List<TableInfo> getTableInfosByNamespace(String tableNamespace){
        return tableInfoDao.selectByNamespace(tableNamespace);
    }

    public TableInfo getTableInfoByPath(String tablePath) {
        return tableInfoDao.selectByTablePath(tablePath);
    }

    public PartitionInfo getSinglePartitionInfo(String tableId, String partitionDesc) {
        return partitionInfoDao.selectLatestPartitionInfo(tableId, partitionDesc);
    }

    //for partition snapshot with some version
    public PartitionInfo getSinglePartitionInfo(String tableId, String partitionDesc, int version) {
        return partitionInfoDao.findByKey(tableId, partitionDesc, version);
    }

    public List<PartitionInfo> getAllPartitionInfo(String tableId) {
        return partitionInfoDao.getPartitionDescByTableId(tableId);
    }

    public List<PartitionInfo> getOnePartitionVersions(String tableId, String partitionDesc) {
        return partitionInfoDao.getPartitionVersions(tableId, partitionDesc);
    }

    public long getLastedTimestamp(String tableId, String partitionDesc) {
        return partitionInfoDao.getLatestTimestamp(tableId, partitionDesc);
    }

    public int getLastedVersionUptoTime(String tableId, String partitionDesc, long utcMills) {
        return partitionInfoDao.getLastedVersionUptoTime(tableId, partitionDesc, utcMills);
    }

    public long getLastedVersionTimestampUptoTime(String tableId, String partitionDesc, long utcMills) {
        return partitionInfoDao.getLastedVersionTimestampUptoTime(tableId, partitionDesc, utcMills);
    }


    public List<String> getDeleteFilePath(String tableId, String partitionDesc, long utcMills) {
        List<DataFileOp> fileOps = new ArrayList<>();
        List<String> deleteFilePathList = new ArrayList<>();
        if (StringUtils.isNotBlank(partitionDesc)) {
            deleteSinglePartitionMetaInfo(tableId, partitionDesc, utcMills, fileOps, deleteFilePathList);
        } else {
            List<String> allPartitionDesc = getTableAllPartitionDesc(tableId);
            allPartitionDesc.forEach(partition -> deleteSinglePartitionMetaInfo(tableId, partition, utcMills, fileOps,
                    deleteFilePathList));
        }
        return deleteFilePathList;
    }

    public void deleteSinglePartitionMetaInfo(String tableId, String partitionDesc, long utcMills,
                                              List<DataFileOp> fileOps, List<String> deleteFilePathList) {
        List<PartitionInfo> filterPartitionInfo = getFilterPartitionInfo(tableId, partitionDesc, utcMills);
        List<Uuid> snapshotList = new ArrayList<>();
        filterPartitionInfo.forEach(p -> snapshotList.addAll(p.getSnapshotList()));
        List<DataCommitInfo> filterDataCommitInfo =
                dataCommitInfoDao.selectByTableIdPartitionDescCommitList(tableId, partitionDesc, snapshotList);
        filterDataCommitInfo.forEach(dataCommitInfo -> fileOps.addAll(dataCommitInfo.getFileOpsList()));
        fileOps.forEach(fileOp -> deleteFilePathList.add(fileOp.getPath()));
        partitionInfoDao.deletePreviousVersionPartition(tableId, partitionDesc, utcMills);
        dataCommitInfoDao.deleteByTableIdPartitionDescCommitList(tableId, partitionDesc, snapshotList);
    }

    public List<PartitionInfo> getFilterPartitionInfo(String tableId, String partitionDesc, long utcMills) {
        long minValueToUtcMills = Long.MAX_VALUE;
        List<PartitionInfo> singlePartitionAllVersionList = getOnePartitionVersions(tableId, partitionDesc);
        Map<Long, PartitionInfo> timestampToPartition = new HashMap<>();
        List<PartitionInfo> filterPartition = new ArrayList<>();
        for (PartitionInfo p : singlePartitionAllVersionList) {
            long curTimestamp = p.getTimestamp();
            timestampToPartition.put(curTimestamp, p);
            if (curTimestamp > utcMills) {
                minValueToUtcMills = Math.min(minValueToUtcMills, curTimestamp);
            } else {
                filterPartition.add(p);
            }
        }
        PartitionInfo rearVersionPartition = timestampToPartition.get(minValueToUtcMills);
        if (rearVersionPartition == null) {
            return singlePartitionAllVersionList;
        } else if (rearVersionPartition.getCommitOp().equals(CommitOp.CompactionCommit) ||
                rearVersionPartition.getCommitOp().equals(CommitOp.UpdateCommit) || filterPartition.size() == 0) {
            return filterPartition;
        } else {
            throw new IllegalStateException(
                    "this operation is Illegal: later versions of snapshot depend on previous version snapshot");
        }
    }

    public void updateTableSchema(String tableId, String tableSchema) {
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        JSONObject propertiesJson = JSON.parseObject(tableInfo.getProperties());
        propertiesJson.put(DBConfig.TableInfoProperty.LAST_TABLE_SCHEMA_CHANGE_TIME, String.valueOf(System.currentTimeMillis()));
        tableInfoDao.updateByTableId(tableId, "", "", tableSchema);
        tableInfoDao.updatePropertiesById(tableId, propertiesJson.toJSONString());
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


    public void logicallyDropColumn(String tableId, List<String> droppedColumn) {
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        JSONObject propertiesJson = JSON.parseObject(tableInfo.getProperties());
        String droppedColumnProperty = (String) propertiesJson.get(DBConfig.TableInfoProperty.DROPPED_COLUMN);
        droppedColumnProperty = droppedColumnProperty == null ? "" : droppedColumnProperty;
        HashSet<String> set = new HashSet<>(Arrays.asList(droppedColumnProperty.split(DBConfig.TableInfoProperty.DROPPED_COLUMN_SPLITTER)));
        set.addAll(droppedColumn);
        propertiesJson.put(DBConfig.TableInfoProperty.DROPPED_COLUMN, String.join(DBConfig.TableInfoProperty.DROPPED_COLUMN_SPLITTER, droppedColumn));
        updateTableProperties(tableId, propertiesJson.toJSONString());
    }

    public void removeLogicallyDropColumn(String tableId) {
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        JSONObject propertiesJson = JSON.parseObject(tableInfo.getProperties());
        propertiesJson.remove(DBConfig.TableInfoProperty.DROPPED_COLUMN);
        tableInfoDao.updatePropertiesById(tableId, propertiesJson.toJSONString());
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
        List<PartitionInfo> deletingPartitionInfoList = curPartitionInfoList.stream().map(p -> {
            int version = p.getVersion();
            return p.toBuilder()
                    .setVersion(version + 1)
                    .clearSnapshot()
                    .setCommitOp(CommitOp.DeleteCommit)
                    .setExpression("")
                    .build();
        }).collect(Collectors.toList());
        if (!partitionInfoDao.transactionInsert(deletingPartitionInfoList, Collections.emptyList())) {
            throw new RuntimeException("Transactional insert partition info failed");
        }
    }

    public void logicDeletePartitionInfoByRangeId(String tableId, String partitionDesc) {
        PartitionInfo.Builder partitionInfo = getSinglePartitionInfo(tableId, partitionDesc).toBuilder();
        int version = partitionInfo.getVersion();
        partitionInfo
                .setVersion(version + 1)
                .clearSnapshot()
                .setCommitOp(CommitOp.DeleteCommit)
                .setExpression("");
        partitionInfoDao.insert(partitionInfo.build());
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

    public void updateTableProperties(String tableId, String properties) {
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        JSONObject newProperties = JSONObject.parseObject(properties);
        if (tableInfo != null) {
            JSONObject originProperties = JSON.parseObject(tableInfo.getProperties());
            if (tableInfo.getProperties() != null && originProperties.containsKey("domain")) {
                // do not modify domain in properties for this table
                newProperties.put("domain", originProperties.get("domain"));
            }
        }
        tableInfoDao.updatePropertiesById(tableId, newProperties.toJSONString());
    }

    public void updateTableShortName(String tablePath, String tableId, String tableName, String tableNamespace) {

        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        if (tableInfo.getTableName() != null && !Objects.equals(tableInfo.getTableName(), "")) {
            if (!tableInfo.getTableName().equals(tableName)) {
                throw new IllegalStateException(
                        "Table name already exists " + tableInfo.getTableName() + " for table id " + tableId);
            }
            return;
        }
        tableInfoDao.updateByTableId(tableId, tableName, tablePath, "");

        tableNameIdDao.insert(TableNameIdDao.newTableNameId(tableName, tableId, tableNamespace, tableInfo.getDomain()));
    }

    public boolean batchCommitDataCommitInfo(List<DataCommitInfo> listData) {
        List<DataCommitInfo> mappedListData = listData.stream().map(item -> {
            return item.toBuilder()
                    .setDomain(getTableDomain(item.getTableId()))
                    .build();
        }).collect(Collectors.toList());
        return dataCommitInfoDao.batchInsert(mappedListData);
    }

    public boolean commitData(MetaInfo metaInfo, boolean changeSchema, CommitOp commitOp) {
        List<PartitionInfo> listPartitionInfo = metaInfo.getListPartitionList();
        TableInfo tableInfo = metaInfo.getTableInfo();
        List<PartitionInfo> readPartitionInfo = metaInfo.getReadPartitionInfoList();
        String tableId = tableInfo.getTableId();

        if (!"".equals(tableInfo.getTableName())) {
            updateTableShortName(tableInfo.getTablePath(), tableInfo.getTableId(), tableInfo.getTableName(),
                    tableInfo.getTableNamespace());
        }
        updateTableProperties(tableId, tableInfo.getProperties());

        List<PartitionInfo> newPartitionList = new ArrayList<>();
        Map<String, PartitionInfo> rawMap = new HashMap<>();
        Map<String, PartitionInfo> newMap = new HashMap<>();
        Map<String, PartitionInfo> readPartitionMap = new HashMap<>();
        List<String> partitionDescList = new ArrayList<>();
        List<String> snapshotList = new ArrayList<>();

        for (PartitionInfo partitionInfo : listPartitionInfo) {
            String partitionDesc = partitionInfo.getPartitionDesc();
            rawMap.put(partitionDesc, partitionInfo);
            partitionDescList.add(partitionDesc);
            snapshotList.addAll(partitionInfo.getSnapshotList().stream().map(uuid -> DBUtil.toJavaUUID(uuid).toString()).collect(Collectors.toList()));
        }

        Map<String, PartitionInfo> curMap = getCurPartitionMap(tableId, partitionDescList);

        if (commitOp.equals(CommitOp.AppendCommit) || commitOp.equals(CommitOp.MergeCommit)) {
            for (PartitionInfo partitionInfo : listPartitionInfo) {
                String partitionDesc = partitionInfo.getPartitionDesc();
                PartitionInfo.Builder curPartitionInfo = getOrCreateCurPartitionInfo(curMap, partitionDesc, tableId).toBuilder();
                int curVersion = curPartitionInfo.getVersion();
                int newVersion = curVersion + 1;

                curPartitionInfo
                        .setVersion(newVersion)
                        .addAllSnapshot(partitionInfo.getSnapshotList())
                        .setCommitOp(commitOp)
                        .setExpression(partitionInfo.getExpression());
                newMap.put(partitionDesc, curPartitionInfo.build());
                newPartitionList.add(curPartitionInfo.build());
            }
        } else if (commitOp.equals(CommitOp.CompactionCommit) || commitOp.equals(CommitOp.UpdateCommit)) {
            if (readPartitionInfo != null) {
                for (PartitionInfo p : readPartitionInfo) {
                    readPartitionMap.put(p.getPartitionDesc(), p);
                }
            }
            for (PartitionInfo partitionInfo : listPartitionInfo) {
                String partitionDesc = partitionInfo.getPartitionDesc();
                PartitionInfo.Builder curPartitionInfo = getOrCreateCurPartitionInfo(curMap, partitionDesc, tableId).toBuilder();
                int curVersion = curPartitionInfo.getVersion();

                PartitionInfo readPartition = readPartitionMap.get(partitionDesc);
                int readPartitionVersion = 0;
                if (readPartition != null) {
                    readPartitionVersion = readPartition.getVersion();
                }

                int newVersion = curVersion + 1;

                if (readPartitionVersion == curVersion) {
                    curPartitionInfo.clearSnapshot().addAllSnapshot(partitionInfo.getSnapshotList());
                } else {
                    Set<CommitOp> middleCommitOps = partitionInfoDao.getCommitOpsBetweenVersions(tableId, partitionDesc,
                            readPartitionVersion + 1, curVersion);
                    if (commitOp.equals(CommitOp.UpdateCommit)) {
                        if (middleCommitOps.contains(CommitOp.UpdateCommit) ||
                                (middleCommitOps.size() > 1 && middleCommitOps.contains(CommitOp.CompactionCommit))) {
                            throw new IllegalStateException(
                                    "current operation conflicts with other data writing tasks, table path: " +
                                            tableInfo.getTablePath());
                        } else if (middleCommitOps.size() == 1 && middleCommitOps.contains(CommitOp.CompactionCommit)) {
                            List<PartitionInfo> midPartitions =
                                    getIncrementalPartitions(tableId, partitionDesc, readPartitionVersion + 1,
                                            curVersion);
                            for (PartitionInfo p : midPartitions) {
                                if (p.getCommitOp().equals(CommitOp.CompactionCommit) && p.getSnapshotCount() > 1) {
                                    throw new IllegalStateException(
                                            "current operation conflicts with other data writing tasks, table path: " +
                                                    tableInfo.getTablePath());
                                }
                            }
                            curPartitionInfo.clearSnapshot().addAllSnapshot(partitionInfo.getSnapshotList());
                        } else {
                            curPartitionInfo = updateSubmitPartitionSnapshot(partitionInfo, curPartitionInfo, readPartition);
                        }
                    } else {
                        if (middleCommitOps.contains(CommitOp.UpdateCommit) || middleCommitOps.contains(CommitOp.CompactionCommit)) {
                            partitionDescList.remove(partitionDesc);
                            snapshotList.removeAll(partitionInfo.getSnapshotList());
                            continue;
                        }
                        curPartitionInfo = updateSubmitPartitionSnapshot(partitionInfo, curPartitionInfo, readPartition);
                    }
                }

                curPartitionInfo.setVersion(newVersion);
                curPartitionInfo.setCommitOp(commitOp);
                curPartitionInfo.setExpression(partitionInfo.getExpression());

                newMap.put(partitionDesc, curPartitionInfo.build());
                newPartitionList.add(curPartitionInfo.build());
            }
        } else {
            throw new IllegalStateException("this operation is Illegal of the table:" + tableInfo.getTablePath());
        }

        boolean notConflict = partitionInfoDao.transactionInsert(newPartitionList, snapshotList);
        if (!notConflict) {
            switch (commitOp) {
                case AppendCommit:
                    notConflict = appendConflict(tableId, partitionDescList, rawMap, newMap, snapshotList, 0);
                    break;
                case CompactionCommit:
                    notConflict =
                            compactionConflict(tableId, partitionDescList, rawMap, readPartitionMap, snapshotList, 0);
                    break;
                case UpdateCommit:
                    notConflict = updateConflict(tableId, partitionDescList, rawMap, readPartitionMap, snapshotList, 0);
                    break;
                case MergeCommit:
                    notConflict = mergeConflict(tableId, partitionDescList, rawMap, newMap, snapshotList, 0);
            }
        }

        return notConflict;
    }

    public boolean appendConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                  Map<String, PartitionInfo> newMap, List<String> snapshotList, int retryTimes) {
        List<PartitionInfo> newPartitionList = new ArrayList<>();
        Map<String, PartitionInfo> curMap = getCurPartitionMap(tableId, partitionDescList);

        for (String partitionDesc : partitionDescList) {
            PartitionInfo.Builder curPartitionInfo = getOrCreateCurPartitionInfo(curMap, partitionDesc, tableId).toBuilder();
            int curVersion = curPartitionInfo.getVersion();

            int lastVersion = newMap.get(partitionDesc).getVersion();
            if (curVersion + 1 == lastVersion) {
                newPartitionList.add(newMap.get(partitionDesc));
            } else {
                CommitOp curCommitOp = curPartitionInfo.getCommitOp();

                int newVersion = curVersion + 1;
                PartitionInfo partitionInfo = rawMap.get(partitionDesc);
                if (curCommitOp.equals(CommitOp.CompactionCommit) || curCommitOp.equals(CommitOp.AppendCommit) ||
                        curCommitOp.equals(CommitOp.UpdateCommit)) {
                    curPartitionInfo
                            .setVersion(newVersion)
                            .addAllSnapshot(partitionInfo.getSnapshotList())
                            .setCommitOp(partitionInfo.getCommitOp())
                            .setExpression(partitionInfo.getExpression());
                    newPartitionList.add(curPartitionInfo.build());
                    newMap.put(partitionDesc, curPartitionInfo.build());
                } else {
                    // other operate conflict, so fail
                    throw new IllegalStateException(
                            "this tableId:" + tableId + " exists conflicting manipulation currently!");
                }
            }
        }

        boolean success = partitionInfoDao.transactionInsert(newPartitionList, snapshotList);
        if (!success && retryTimes < DBConfig.MAX_COMMIT_ATTEMPTS) {
            return appendConflict(tableId, partitionDescList, rawMap, newMap, snapshotList, retryTimes + 1);
        }
        return success;
    }

    public boolean compactionConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                      Map<String, PartitionInfo> readPartitionMap, List<String> snapshotList, int retryTime) {
        List<PartitionInfo> newPartitionList = new ArrayList<>();
        Map<String, PartitionInfo> curMap = getCurPartitionMap(tableId, partitionDescList);

        for (int i = 0; i < partitionDescList.size(); i++) {
            String partitionDesc = partitionDescList.get(i);
            PartitionInfo rawPartitionInfo = rawMap.get(partitionDesc);
            PartitionInfo.Builder curPartitionInfo = getOrCreateCurPartitionInfo(curMap, partitionDesc, tableId).toBuilder();
            int curVersion = curPartitionInfo.getVersion();

            PartitionInfo readPartition = readPartitionMap.get(partitionDesc);
            int readPartitionVersion = 0;
            if (readPartition != null) {
                readPartitionVersion = readPartition.getVersion();
            }

            int newVersion = curVersion + 1;
            if (readPartitionVersion == curVersion) {
                curPartitionInfo.clearSnapshot().addAllSnapshot(rawPartitionInfo.getSnapshotList());
            } else {
                Set<CommitOp> middleCommitOps =
                        partitionInfoDao.getCommitOpsBetweenVersions(tableId, partitionDesc, readPartitionVersion + 1,
                                curVersion);
                if (middleCommitOps.contains(CommitOp.UpdateCommit) || middleCommitOps.contains(CommitOp.CompactionCommit)) {
                    partitionDescList.remove(i);
                    snapshotList.removeAll(rawPartitionInfo.getSnapshotList());
                    i = i - 1;
                    continue;
                }
                curPartitionInfo = updateSubmitPartitionSnapshot(rawPartitionInfo, curPartitionInfo, readPartition);
            }

            curPartitionInfo.setVersion(newVersion);
            curPartitionInfo.setCommitOp(rawPartitionInfo.getCommitOp());
            curPartitionInfo.setExpression(rawPartitionInfo.getExpression());

            newPartitionList.add(curPartitionInfo.build());
        }

        boolean success = partitionInfoDao.transactionInsert(newPartitionList, snapshotList);
        if (!success && retryTime < DBConfig.MAX_COMMIT_ATTEMPTS) {
            return compactionConflict(tableId, partitionDescList, rawMap, readPartitionMap, snapshotList, retryTime + 1);
        }

        return success;
    }

    public boolean updateConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                  Map<String, PartitionInfo> readPartitionMap, List<String> snapshotList, int retryTime) {
        List<PartitionInfo> newPartitionList = new ArrayList<>();
        Map<String, PartitionInfo> curMap = getCurPartitionMap(tableId, partitionDescList);

        for (String partitionDesc : partitionDescList) {
            PartitionInfo rawPartitionInfo = rawMap.get(partitionDesc);
            PartitionInfo.Builder curPartitionInfo = getOrCreateCurPartitionInfo(curMap, partitionDesc, tableId).toBuilder();
            int curVersion = curPartitionInfo.getVersion();

            PartitionInfo readPartition = readPartitionMap.get(partitionDesc);
            int readPartitionVersion = 0;
            if (readPartition != null) {
                readPartitionVersion = readPartition.getVersion();
            }

            if (readPartitionVersion == curVersion) {
                curPartitionInfo.clearSnapshot().addAllSnapshot(rawPartitionInfo.getSnapshotList());
            } else {
                Set<CommitOp> middleCommitOps =
                        partitionInfoDao.getCommitOpsBetweenVersions(tableId, partitionDesc, readPartitionVersion + 1,
                                curVersion);
                if (middleCommitOps.contains(CommitOp.UpdateCommit) ||
                        (middleCommitOps.size() > 1 && middleCommitOps.contains(CommitOp.CompactionCommit))) {
                    throw new IllegalStateException(
                            "current operation conflicts with other write data tasks, table id is: " + tableId);
                } else if (middleCommitOps.size() == 1 && middleCommitOps.contains(CommitOp.CompactionCommit)) {
                    List<PartitionInfo> midPartitions =
                            getIncrementalPartitions(tableId, partitionDesc, readPartitionVersion + 1, curVersion);
                    for (PartitionInfo p : midPartitions) {
                        if (p.getCommitOp().equals(CommitOp.CompactionCommit) && p.getSnapshotCount() > 1) {
                            throw new IllegalStateException(
                                    "current operation conflicts with other data writing tasks, table id: " + tableId);
                        }
                    }
                    curPartitionInfo.clearSnapshot().addAllSnapshot(rawPartitionInfo.getSnapshotList());
                } else {
                    curPartitionInfo = updateSubmitPartitionSnapshot(rawPartitionInfo, curPartitionInfo, readPartition);
                }
            }

            int newVersion = curVersion + 1;
            curPartitionInfo.setVersion(newVersion);
            curPartitionInfo.setCommitOp(rawPartitionInfo.getCommitOp());
            curPartitionInfo.setExpression(rawPartitionInfo.getExpression());

            newPartitionList.add(curPartitionInfo.build());
        }

        boolean success = partitionInfoDao.transactionInsert(newPartitionList, snapshotList);
        if (!success && retryTime < DBConfig.MAX_COMMIT_ATTEMPTS) {
            return updateConflict(tableId, partitionDescList, rawMap, readPartitionMap, snapshotList, retryTime + 1);
        }
        return success;
    }

    public boolean mergeConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                 Map<String, PartitionInfo> newMap, List<String> snapshotList, int retryTime) {
        List<PartitionInfo> newPartitionList = new ArrayList<>();
        Map<String, PartitionInfo> curMap = getCurPartitionMap(tableId, partitionDescList);

        for (String partitionDesc : partitionDescList) {
            PartitionInfo.Builder curPartitionInfo = getOrCreateCurPartitionInfo(curMap, partitionDesc, tableId).toBuilder();
            int curVersion = curPartitionInfo.getVersion();

            int lastVersion = newMap.get(partitionDesc).getVersion();

            if (curVersion + 1 == lastVersion) {
                newPartitionList.add(newMap.get(partitionDesc));
            } else {
                CommitOp curCommitOp = curPartitionInfo.getCommitOp();

                PartitionInfo partitionInfo = rawMap.get(partitionDesc);
                int newVersion = curVersion + 1;
                if (curCommitOp.equals(CommitOp.CompactionCommit) || curCommitOp.equals(CommitOp.UpdateCommit) ||
                        curCommitOp.equals(CommitOp.MergeCommit)) {
                    curPartitionInfo
                            .setVersion(newVersion)
                            .addAllSnapshot(partitionInfo.getSnapshotList())
                            .setCommitOp(partitionInfo.getCommitOp())
                            .setExpression(partitionInfo.getExpression());
                    newPartitionList.add(curPartitionInfo.build());
                    newMap.put(partitionDesc, curPartitionInfo.build());
                } else {
                    // other operate conflict, so fail
                    throw new IllegalStateException(
                            "this tableId:" + tableId + " exists conflicting manipulation currently!");
                }
            }
        }

        boolean success = partitionInfoDao.transactionInsert(newPartitionList, snapshotList);
        if (!success && retryTime < DBConfig.MAX_COMMIT_ATTEMPTS) {
            return mergeConflict(tableId, partitionDescList, rawMap, newMap, snapshotList, retryTime + 1);
        }

        return success;
    }

    private PartitionInfo.Builder updateSubmitPartitionSnapshot(PartitionInfo rawPartitionInfo, PartitionInfo.Builder curPartitionInfo,
                                                                PartitionInfo readPartition) {
        List<Uuid> snapshot = new ArrayList<>(rawPartitionInfo.getSnapshotList());
        List<Uuid> curSnapshot = new ArrayList<>(curPartitionInfo.getSnapshotList());
        if (readPartition != null) {
            curSnapshot.removeAll(readPartition.getSnapshotList());
        }
        snapshot.addAll(curSnapshot);
        return curPartitionInfo
                .clearSnapshot()
                .addAllSnapshot(snapshot);
    }

    private PartitionInfo getOrCreateCurPartitionInfo(Map<String, PartitionInfo> curMap, String partitionDesc,
                                                      String tableId) {
        PartitionInfo curPartitionInfo = curMap.get(partitionDesc);
        if (curPartitionInfo == null) {
            curPartitionInfo = PartitionInfo.newBuilder()
                    .setTableId(tableId)
                    .setPartitionDesc(partitionDesc)
                    .setVersion(-1)
                    .setDomain(getTableDomain(tableId))
                    .build();
        } else {
            curPartitionInfo = curPartitionInfo.toBuilder()
                    .setDomain(getTableDomain(tableId))
                    .build();
        }
        return curPartitionInfo;
    }

    private Map<String, PartitionInfo> getCurPartitionMap(String tableId, List<String> partitionDescList) {
        List<PartitionInfo> curPartitionList = partitionInfoDao.findByTableIdAndParList(tableId, partitionDescList);
        Map<String, PartitionInfo> curMap = new HashMap<>();
        for (PartitionInfo curPartition : curPartitionList) {
            String partitionDesc = curPartition.getPartitionDesc();
            curMap.put(partitionDesc, curPartition);
        }
        return curMap;
    }

    public List<DataCommitInfo> getTableSinglePartitionDataInfo(PartitionInfo partitionInfo) {
        String tableId = partitionInfo.getTableId();
        String partitionDesc = partitionInfo.getPartitionDesc();
        List<Uuid> snapshotList = partitionInfo.getSnapshotList();

        return dataCommitInfoDao.selectByTableIdPartitionDescCommitList(tableId, partitionDesc, snapshotList);
    }

    public List<DataCommitInfo> getPartitionSnapshot(String tableId, String partitionDesc, int version) {
        PartitionInfo partitionInfo = partitionInfoDao.findByKey(tableId, partitionDesc, version);
        List<Uuid> commitList = partitionInfo.getSnapshotList();
        return dataCommitInfoDao.selectByTableIdPartitionDescCommitList(tableId, partitionDesc, commitList);
    }

    public List<PartitionInfo> getIncrementalPartitions(String tableId, String partitionDesc, int startVersion,
                                                        int endVersion) {
        return partitionInfoDao.getPartitionsFromVersion(tableId, partitionDesc, startVersion, endVersion);
    }

    public List<PartitionInfo> getOnePartition(String tableId, String partitionDesc) {
        return partitionInfoDao.getOnePartition(tableId, partitionDesc);
    }

    public List<PartitionInfo> getIncrementalPartitionsFromTimestamp(String tableId, String partitionDesc,
                                                                     long startTimestamp, long endTimestamp) {
        return partitionInfoDao.getPartitionsFromTimestamp(tableId, partitionDesc, startTimestamp, endTimestamp);
    }

    public DataCommitInfo selectByTableId(String tableId) {
        return dataCommitInfoDao.selectByTableId(tableId);
    }

    public List<DataCommitInfo> getDataCommitInfosFromUUIDs(String tableId, String partitionDesc,
                                                            List<Uuid> dataCommitUUIDs) {
        return dataCommitInfoDao.selectByTableIdPartitionDescCommitList(tableId, partitionDesc, dataCommitUUIDs);
    }

    public void rollbackPartitionByVersion(String tableId, String partitionDesc, int version) {
        PartitionInfo partitionInfo = partitionInfoDao.findByKey(tableId, partitionDesc, version);
        if (partitionInfo == null) {
            return;
        }
        PartitionInfo curPartitionInfo = partitionInfoDao.selectLatestPartitionInfo(tableId, partitionDesc);
        partitionInfoDao.insert(
                partitionInfo.toBuilder()
                        .setVersion(curPartitionInfo.getVersion() + 1)
                        .build());
    }

    private String getTableDomain(String tableId) {
        if (!AuthZEnforcer.authZEnabled()) {
            return "public";
        }
        TableInfo tableInfo = this.getTableInfoByTableId(tableId);
        if (tableInfo == null) {
            throw new IllegalStateException("target tableinfo does not exists");
        }
        return getNameSpaceDomain(tableInfo.getTableNamespace());
    }

    private String getNameSpaceDomain(String namespace) {
        if (!AuthZEnforcer.authZEnabled()) {
            return "public";
        }
        Namespace namespaceInfo = getNamespaceByNamespace(namespace);
        if (namespaceInfo == null) {
            throw new IllegalStateException("target namespace does not exists");
        }
        return namespaceInfo.getDomain();
    }

    public void commitDataCommitInfo(DataCommitInfo dataCommitInfo) {
        String tableId = dataCommitInfo.getTableId();
        String partitionDesc = dataCommitInfo.getPartitionDesc().replaceAll("/", LAKESOUL_RANGE_PARTITION_SPLITTER);
        Uuid commitId = dataCommitInfo.getCommitId();
        CommitOp commitOp = dataCommitInfo.getCommitOp();
        DataCommitInfo metaCommitInfo = dataCommitInfoDao.selectByPrimaryKey(tableId, partitionDesc, DBUtil.toJavaUUID(commitId).toString());
        if (metaCommitInfo != null && metaCommitInfo.getCommitted()) {
            LOG.info("DataCommitInfo with tableId={}, commitId={} committed already", tableId, commitId);
            return;
        } else if (metaCommitInfo == null) {
            dataCommitInfo = dataCommitInfo.toBuilder()
                    .setDomain(getTableDomain(tableId))
                    .build();
            dataCommitInfoDao.insert(dataCommitInfo);
        }
        MetaInfo.Builder metaInfo = MetaInfo.newBuilder();
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);

        List<Uuid> snapshot = new ArrayList<>();
        snapshot.add(commitId);

        List<PartitionInfo> partitionInfoList = new ArrayList<>();
        PartitionInfo.Builder p = PartitionInfo.newBuilder()
                .setTableId(tableId)
                .setPartitionDesc(partitionDesc)
                .setCommitOp(commitOp)
                .setDomain(getTableDomain(tableId))
                .addAllSnapshot(snapshot);
        partitionInfoList.add(p.build());

        metaInfo.setTableInfo(tableInfo);
        metaInfo.addAllListPartition(partitionInfoList);

        commitData(metaInfo.build(), false, commitOp);
    }

    //==============
    //namespace
    //==============
    public List<String> listNamespaces() {
        return namespaceDao.listNamespaces();
    }

    public void createNewNamespace(String name, String properties, String comment) {
        Namespace.Builder namespace = Namespace.newBuilder()
                .setNamespace(name)
                .setProperties(properties)
                .setComment(comment == null ? "" : comment);

        namespace.setDomain(AuthZEnforcer.authZEnabled()
                ? AuthZContext.getInstance().getDomain()
                : "public");
        namespaceDao.insert(namespace.build());

    }

    public Namespace getNamespaceByNamespace(String namespace) {
        return namespaceDao.findByNamespace(namespace);
    }

    public void updateNamespaceProperties(String namespace, String properties) {
        Namespace namespaceEntity = namespaceDao.findByNamespace(namespace);
        JSONObject originProperties = JSON.parseObject(namespaceEntity.getProperties());
        JSONObject newProperties = JSONObject.parseObject(properties);

        if (originProperties.containsKey("domain")) {
            // do not modify domain in properties for this table
            newProperties.put("domain", originProperties.get("domain"));
        }
        namespaceDao.updatePropertiesByNamespace(namespace, newProperties.toJSONString());
    }

    public List<String> getTableAllPartitionDesc(String tableId) {
        return partitionInfoDao.getAllPartitionDescByTableId(tableId);
    }

    public void deleteNamespace(String namespace) {
        namespaceDao.deleteByNamespace(namespace);
    }

    // just for test
    public void cleanMeta() {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            NativeMetadataJavaClient.cleanMeta();
            if (!AuthZEnforcer.authZEnabled()) {
                namespaceDao.insert(NamespaceDao.DEFAULT_NAMESPACE);
            }
            return;
        }
        namespaceDao.clean();
        if (!AuthZEnforcer.authZEnabled()) {
            namespaceDao.insert(NamespaceDao.DEFAULT_NAMESPACE);
        }
        dataCommitInfoDao.clean();
        tableInfoDao.clean();
        tablePathIdDao.clean();
        tableNameIdDao.clean();
        partitionInfoDao.clean();
    }
}
