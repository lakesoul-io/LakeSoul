/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.meta;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.dao.*;
import com.dmetasoul.lakesoul.meta.entity.*;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class DBManager {
    private TableInfoDao tableInfoDao;
    private TableNameIdDao tableNameIdDao;
    private TablePathIdDao tablePathIdDao;
    private DataCommitInfoDao dataCommitInfoDao;
    private PartitionInfoDao partitionInfoDao;

    public DBManager() {
        tableInfoDao = DBFactory.getTableInfoDao();
        tableNameIdDao = DBFactory.getTableNameIdDao();
        tablePathIdDao = DBFactory.getTablePathIdDao();
        dataCommitInfoDao = DBFactory.getDataCommitInfoDao();
        partitionInfoDao = DBFactory.getPartitionInfoDao();
    }

    //MetaVersion
    //tableInfo 存在，则tablePathId 也存在，两者同时存在
    public boolean isTableExists(String tablePath) {
        TablePathId tablePathId = tablePathIdDao.findByTablePath(tablePath);
        if (tablePathId == null) {
            return false;
        }
        TableInfo tableInfo = tableInfoDao.selectByTableId(tablePathId.getTableId());
        if (tableInfo == null) {
            return false;
        }
        return true;
    }

    // todo tablePath 和之前的tableName等价
    public boolean isTableIdExists(String tablePath, String tableId) {
        TableInfo tableInfo = tableInfoDao.selectByIdAndTablePath(tableId, tablePath);
        if (tableInfo != null) {
            return true;
        }
        return false;
    }

    //shortName = table_name,原返回一个元组，这里看下返回什么？
    public boolean isShortTableNameExists(String tableName) {
        TableNameId tableNameId = tableNameIdDao.findByTableName(tableName);
        if (tableNameId != null) {
            return true;
        }
        return false;
    }

    public TableNameId shortTableName(String tableName) {
        return tableNameIdDao.findByTableName(tableName);
    }

    //tableName == tablePath,shortTableName == tableName
    //返回tablePath
    public String getTableNameFromShortTableName(String tableName) {
        TableNameId tableNameId = tableNameIdDao.findByTableName(tableName);

        TableInfo tableInfo = tableInfoDao.selectByTableId(tableNameId.getTableId());
        return tableInfo.getTablePath();
    }

    public boolean isPartitionExists(String tableId, String partitionDesc) {
        PartitionInfo p = getSinglePartitionInfo(tableId, partitionDesc);
        if (p != null) {
            return true;
        }
        return false;
    }

    public void createNewTable(String tableId, String tableName, String tablePath, String tableSchema, JSONObject properties, String partitions) {
        // todo 之前这里有table_schema长度检测 table_schema.length > MetaUtils.MAX_SIZE_PER_VALUE
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId(tableId);
        tableInfo.setTableName(tableName);
        tableInfo.setTablePath(tablePath);
        tableInfo.setTableSchema(tableSchema);
        // todo list转换
        tableInfo.setPartitions(partitions);
        tableInfo.setProperties(properties);

        // todo 是否考虑事物机制
        boolean insertNameFlag = true;
        if (StringUtils.isNotBlank(tableName)) {
            insertNameFlag = tableNameIdDao.insert(new TableNameId(tableName, tableId));
        }
        boolean insertPathFlag = true;
        if (StringUtils.isNotBlank(tablePath)) {
            insertPathFlag = tablePathIdDao.insert(new TablePathId(tablePath, tableId));
        }
        if (insertNameFlag && insertPathFlag) {
            tableInfoDao.insert(tableInfo);
        } else {
            tableNameIdDao.delete(tableName);
            tablePathIdDao.delete(tablePath);
        }
        // todo 之前无返回值，false直接 throw 报错
    }

    /**
     * todo
     * 新的应该不需要的了
     * partition信息在写入数据时会有，
     * table_info新也已经写入
     */
    public void addPartition(){

    }

    //原返回table_name即路径，则现返回tablePath
    public List<String> listTables() {
        List<String> rsList = tablePathIdDao.listAllPath();
        return rsList;
    }

    /**
     * 之前参数table_name 对应现在table_path
     */
    public TableInfo getTableInfo(String tablePath) {
        TableInfo tableInfo = tableInfoDao.selectByTablePath(tablePath);
        return tableInfo;
    }

    public PartitionInfo getSinglePartitionInfo(String tableId, String partitionDesc) {
        PartitionInfo p = partitionInfoDao.selectLatestPartitionInfo(tableId, partitionDesc);
        return p;
    }

    // todo 目前表没有partitionId
    //  之前返回值类型为元组（boolean， String）
    public void getPartitionId(){

    }

    public List<PartitionInfo> getAllPartitionInfo(String tableId) {
        return partitionInfoDao.getPartitionDescByTableId(tableId);
    }

    // todo 目前不需要update partition
    // 还有一个同名不同参数方法
    public void updatePartitionInfo(){

    }

    public void updateTableSchema(String tableId, String tableSchema) {
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        tableInfo.setTableSchema(tableSchema);
        tableInfoDao.updateByTableId(tableId, "", "", tableSchema);
    }

    /**
     * 原参：table_name，table_id
     */
    public void deleteTableInfo(String tablePath, String tableId) {
        tablePathIdDao.delete(tablePath);
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        String tableName = tableInfo.getTableName();
        if (StringUtils.isNotBlank(tableName)) {
            tableNameIdDao.delete(tableName);
        }
        tableInfoDao.deleteByIdAndPath(tableId, tablePath);
    }

    public void deletePartitionInfoByTableId(String tableId) {
        partitionInfoDao.deleteByTableId(tableId);
    }

    public void deletePartitionInfoByRangeId(String tableId, String partitionDesc) {
        partitionInfoDao.deleteByTableIdAndPartitionDesc(tableId, partitionDesc);
    }

    public void deleteShortTableName(String tableName, String tablePath) {
        tableNameIdDao.delete(tableName);
    }

    // todo createNewTable 有了
    public void addShortTableName(String tableName, String tablePath) {
        TableInfo tableInfo = getTableInfo(tablePath);

        TableNameId tableNameId = new TableNameId();
        tableNameId.setTableId(tableInfo.getTableId());
        tableNameId.setTableName(tableName);
        tableNameIdDao.insert(tableNameId);
    }

    public void updateTableShortName(String tablePath, String tableId, String tableName) {

        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
        tableInfo.setTableName(tableName);
        tableInfo.setTablePath(tablePath);
        tableInfoDao.updateByTableId(tableId, tableName, tablePath, "");

        TableNameId tableNameId = new TableNameId();
        tableNameId.setTableName(tableName);
        tableNameId.setTableId(tableId);
        tableNameIdDao.insert(tableNameId);

        TablePathId tablePathId = new TablePathId();
        tablePathId.setTablePath(tablePath);
        tablePathId.setTableId(tableId);
        tablePathIdDao.insert(tablePathId);
    }



    public boolean batchCommitDataCommitInfo(List<DataCommitInfo> listData) {
        return dataCommitInfoDao.batchInsert(listData);
    }

    public boolean commitData(MetaInfo metaInfo, boolean changeSchema, String commitOp) {
        List<PartitionInfo> listPartitionInfo = metaInfo.getListPartition();
        TableInfo tableInfo = metaInfo.getTableInfo();
        String tableId = tableInfo.getTableId();

        List<PartitionInfo> newPartitionList = new ArrayList<>();
        Map<String, PartitionInfo> rawMap = new HashMap<>();
        Map<String, PartitionInfo> newMap = new HashMap<>();
        List<String> partitionDescList = new ArrayList<>();

//       优化之前的部分
//        for (PartitionInfo partitionInfo : listPartitionInfo) {
//            PartitionInfo newPartition = getCurPartitionInfo(partitionInfo, commitOp);
//            newPartitionList.add(newPartition);
//            String partitionDesc = partitionInfo.getPartitionDesc();
//            rawMap.put(partitionDesc, partitionInfo);
//            newMap.put(partitionDesc, newPartition);
//            partitionDescList.add(partitionDesc);
//        }

        for (PartitionInfo partitionInfo : listPartitionInfo) {
            String partitionDesc = partitionInfo.getPartitionDesc();
            String dataCommitOp = partitionInfo.getCommitOp();
            if (!dataCommitOp.equals(commitOp)) {
                //todo 报错
                return false;
            }
            rawMap.put(partitionDesc, partitionInfo);
            partitionDescList.add(partitionDesc);
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
            //todo 报错
            return false;
        }

        boolean notConflict = partitionInfoDao.transactionInsert(newPartitionList);
        if (!notConflict) {
            switch(commitOp){
                case "AppendCommit":
                    notConflict = appendConflict(tableId, partitionDescList, rawMap, newMap,0);
                    break;
                case "CompactionCommit":
                    notConflict = compactionConflict(tableId, partitionDescList, rawMap, newMap,0);
                    break;
                case "UpdateCommit":
                    notConflict = updateConflict(tableId, partitionDescList, rawMap, newMap, 0);
                    break;
                case "MergeCommit":
                    notConflict = mergeConflict(tableId, partitionDescList, rawMap, newMap, 0);
            }
        }

//        if (notConflict && changeSchema) {
//            updateTableSchema(tableId, tableInfo.getTableSchema());
//        }
        return notConflict;
    }

/* 优化之前的代码
    public PartitionInfo getCurPartitionInfo(PartitionInfo partitionInfo, String commitOp) {

        String tableId = partitionInfo.getTableId();
        String partitionDesc = partitionInfo.getPartitionDesc();
        List<UUID> snapshot = partitionInfo.getSnapshot();

        PartitionInfo curPartitionInfo = partitionInfoDao.selectLatestPartitionInfo(tableId, partitionDesc);
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

        switch(commitOp){
            case "append":
//                curSnapshot.addAll(snapshot);
//                curPartitionInfo.setVersion(newVersion);
//                curPartitionInfo.setSnapshot(curSnapshot);
//                curPartitionInfo.setCommitOp(commitOp);
//                curPartitionInfo.setExpression(partitionInfo.getExpression());
//                break;
            case "merge":
                curSnapshot.addAll(snapshot);
                curPartitionInfo.setVersion(newVersion);
                curPartitionInfo.setSnapshot(curSnapshot);
                curPartitionInfo.setCommitOp(commitOp);
                curPartitionInfo.setExpression(partitionInfo.getExpression());
                break;
            case "compaction":
//                curPartitionInfo.setVersion(newVersion);
//                curPartitionInfo.setSnapshot(partitionInfo.getSnapshot());
//                curPartitionInfo.setCommitOp(commitOp);
//                curPartitionInfo.setExpression(partitionInfo.getExpression());
//                break;
            case "update":
                curPartitionInfo.setVersion(newVersion);
                curPartitionInfo.setSnapshot(partitionInfo.getSnapshot());
                curPartitionInfo.setCommitOp(commitOp);
                curPartitionInfo.setExpression(partitionInfo.getExpression());

        }
        return curPartitionInfo;
    }
*/
    public boolean appendConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                  Map<String, PartitionInfo> newMap, int time) {
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
                    // 其余情况均冲入，本次数据写入失败
                    return false;
                }
            }
        }
        boolean conflictFlag = partitionInfoDao.transactionInsert(newPartitionList);
        while (!conflictFlag && time < DBConfig.MAX_COMMIT_ATTEMPTS) {
            conflictFlag = appendConflict(tableId, partitionDescList, rawMap, newMap, time+1);
        }

        return conflictFlag;
    }

    public boolean compactionConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                      Map<String, PartitionInfo> newMap, int time) {
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
                } else {
                    // 其余情况均或略本次操作
                    partitionDescList.remove(partitionDesc);
                }
            }
        }

        boolean conflictFlag = partitionInfoDao.transactionInsert(newPartitionList);
        while (!conflictFlag && time < DBConfig.MAX_COMMIT_ATTEMPTS) {
            conflictFlag = appendConflict(tableId, partitionDescList, rawMap, newMap, time+1);
        }

        return conflictFlag;
    }

    public boolean updateConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                  Map<String, PartitionInfo> newMap, int time) {
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
                    // 其余情况均冲入，本次数据写入失败
                    return false;
                }
            }
        }
        boolean conflictFlag = partitionInfoDao.transactionInsert(newPartitionList);
        while (!conflictFlag && time < DBConfig.MAX_COMMIT_ATTEMPTS) {
            conflictFlag = updateConflict(tableId, partitionDescList, rawMap, newMap, time+1);
        }
        return conflictFlag;
    }

    public boolean mergeConflict(String tableId, List<String> partitionDescList, Map<String, PartitionInfo> rawMap,
                                 Map<String, PartitionInfo> newMap, int time) {
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
                    // 其余情况均冲入，本次数据写入失败
                    return false;
                }
            }
        }
        boolean conflictFlag = partitionInfoDao.transactionInsert(newPartitionList);
        while (!conflictFlag && time < DBConfig.MAX_COMMIT_ATTEMPTS) {
            conflictFlag = mergeConflict(tableId, partitionDescList, rawMap, newMap, time+1);
        }

        return conflictFlag;
    }

    public List<DataCommitInfo> getTableSinglePartitionDataInfo(PartitionInfo partitionInfo) {
        String tableId = partitionInfo.getTableId();
        String partitionDesc = partitionInfo.getPartitionDesc();
        List<UUID> snapshotList = partitionInfo.getSnapshot();

        return dataCommitInfoDao.selectByTableIdPartitionDescCommitList(tableId, partitionDesc, snapshotList);
    }

}
