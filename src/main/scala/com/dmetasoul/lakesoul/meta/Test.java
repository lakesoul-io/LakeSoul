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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.dao.*;
import com.dmetasoul.lakesoul.meta.entity.*;

import java.sql.*;
import java.util.*;


public class Test {

    static TableInfoDao tableInfoDao = new TableInfoDao();
    static TablePathIdDao tablePathIdDao = new TablePathIdDao();
    static TableNameIdDao tableNameIdDao = new TableNameIdDao();
    static DataCommitInfoDao dataCommitInfoDao = new DataCommitInfoDao();
    static PartitionInfoDao partitionInfoDao = new PartitionInfoDao();

    public static void main(String[] args)  {
        DBUtil.init();
      //  DBUtil.cleanAllTable();
      // testCommitData();
       //testPartitionInfo();
      // testDataCommitInfo();;
       //testSelect();
        //testSelect();

    }

    public static void testSelect() {
        List<UUID> list = new ArrayList<>();
        list.add(UUID.fromString("3d25e59b-9c83-480e-8b10-fa092576bd3a"));
        list.add(UUID.fromString("ad25e59b-9c83-480e-8b10-fa092576bd3a"));
        List<DataCommitInfo> commitInfoList = dataCommitInfoDao.selectByTableIdPartitionDescCommitList("id_1", "1-1-3", list);
        System.out.println(commitInfoList.size());
    }

    public static void testAsList() {
        String[] aa = new String[]{"aa","bb"};
        List<String> cc = new ArrayList<>();
        Collections.addAll(cc, aa);
        cc.add("dd");
        System.out.println(cc.size());
    }

    public static void testArray() {
        Connection conn = null;
        String sql = "select * from partition_info where table_id = 'id_1' and partition_desc = '1-1-3'";
        Statement stat = null;
        try {
            conn = DBConnector.getConn();
            stat = conn.createStatement();
            ResultSet resultSet = stat.executeQuery(sql);

            while (resultSet.next()) {
                System.out.println(resultSet.getString("table_id"));
                System.out.println(resultSet.getString("partition_desc"));
                System.out.println(resultSet.getInt("version"));
                System.out.println(resultSet.getArray("snapshot"));
                Array snapshot = resultSet.getArray("snapshot");
                UUID[] uuids = (UUID[]) snapshot.getArray();
                List<UUID> uuidList = Arrays.asList(uuids);
                System.out.println(uuidList.size());
                System.out.println(uuidList.get(0));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void testCommitData() {
        MetaInfo metaInfo = new MetaInfo();
        TableInfo tableInfo = new TableInfo();
        PartitionInfo partitionInfo = new PartitionInfo();

        String commitOp = "compact";

        tableInfo.setTableId("id_11");
        tableInfo.setTablePath("path_11");
        tableInfo.setTableName("name_11");
        tableInfo.setTableSchema("schema_11");

        JSONObject jsonObject = JSON.parseObject("{\"k_11\":\"v_11\"}");
        System.out.println(jsonObject);
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(jsonObject);
        System.out.println(jsonArray);

        tableInfo.setPartitions(jsonArray.toJSONString());
        tableInfo.setProperties(jsonObject);
        metaInfo.setTableInfo(tableInfo);

        partitionInfo.setTableId("id_11");
        partitionInfo.setPartitionDesc("1-1-1");
        partitionInfo.setCommitOp(commitOp);

        UUID uuid = UUID.randomUUID();
        System.out.println(uuid);
        List<UUID> uuidList = new ArrayList<>();
        uuidList.add(uuid);

        partitionInfo.setSnapshot(uuidList);
        partitionInfo.setExpression("test_1");

        List<PartitionInfo> partitionInfoList = new ArrayList<>();
        partitionInfoList.add(partitionInfo);

        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2.setTableId("id_11");
        partitionInfo2.setPartitionDesc("1-1-2");
        partitionInfo2.setCommitOp(commitOp);

        UUID uuid_2 = UUID.randomUUID();
        System.out.println(uuid_2);
        List<UUID> uuidList_2 = new ArrayList<>();
        uuidList_2.add(uuid_2);

        partitionInfo2.setSnapshot(uuidList_2);
        partitionInfo2.setExpression("2_test");
        partitionInfoList.add(partitionInfo2);

        metaInfo.setListPartition(partitionInfoList);

        boolean result = new DBManager().commitData(metaInfo, false, commitOp);
        System.out.println(result);

    }

    public static void testInsert() {
        Connection conn = null;
        Statement pstmt = null;
        String sql = "insert into aaa values ('qq1', 'qq1')";
        try {
            conn = DBConnector.getConn();
            pstmt = conn.createStatement();
            boolean execute = pstmt.execute(sql);
            System.out.println(execute);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    //ysql不知batch操作
    public static void testBatch() {
        Connection conn = null;
        Statement pstmt = null;
        String sql1 = "insert into aaa(col_1, col_2) values ('zz1','zz1')";
        String sql2 = "insert into aaa(col_1, col_2) values ('zz2','zz2')";
        String sql3 = "insert into aaa(col_1, col_2) values ('zz3','zz3')";
        try {
            conn = DBConnector.getConn();
            pstmt = conn.createStatement();
            pstmt.addBatch(sql1);
            pstmt.addBatch(sql2);
            pstmt.addBatch(sql3);
            pstmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public static void testTransaction() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        Statement stmt = null;
        String sql1 = "update aaa set col_2 = ? where col_1 = ?";
        String sql2 = "insert into aaa (col_1, col_2) values (?, ?)";
        try {
            conn = DBConnector.getConn();
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql2);
            stmt = conn.createStatement();
            pstmt.setString(1, "ccc");
            pstmt.setString(2, "ccc");
            pstmt.execute();
            stmt.executeUpdate("update aaa set col_2 = 'ddd' where col_1 = 'aaa'");
            stmt.executeUpdate("update aaa set col_2 = 'ddd' where col_1 = 'aaa'");

            pstmt.setString(1, "aaa");
            pstmt.setString(2, "ddd");
            pstmt.execute();
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public static void testPartitionInfo() {
        String tableId = "id_10";
        String partitionDesc = "1-1-1";
        int version = 2;
        String commitOp = "append";
        UUID u1 = UUID.randomUUID();
        UUID u2 = UUID.randomUUID();
        List<UUID> uuidList = new ArrayList<UUID>() {
            {
                add(u1);
                add(u2);
            }
        };
        System.out.println(u1 + "---" + u2);
        String expression = "1243";
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setTableId(tableId);
        partitionInfo.setPartitionDesc(partitionDesc);
        partitionInfo.setVersion(version);
        partitionInfo.setCommitOp(commitOp);
        partitionInfo.setSnapshot(uuidList);
        partitionInfo.setExpression(expression);

//        partitionInfoDao.insert(partitionInfo);
//        partitionInfoDao.deleteByTableId("id_2");
//        partitionInfoDao.deleteByTableIdAndPartitionDesc(tableId, partitionDesc);
//
//        String tableId1 = "id_1";
//        List<String> patList = new ArrayList<>() {
//            {
//                add("1-1-1");
//                add("1-1-2");
//            }
//        };
//        List<PartitionInfo> list = partitionInfoDao.findByTableIdAndParList(tableId1, patList);
//        System.out.println(list.size());
//
//        List<String> parList = partitionInfoDao.getPartitionDescByTableId("id_1");
//        System.out.println(parList.size());
//
        PartitionInfo partitionInfo1 = partitionInfoDao.selectLatestPartitionInfo("id_1", "1-1-1");
        System.out.println(partitionInfo1.getSnapshot());
        List<String> ll = new ArrayList<>();
        ll.add("aa");
        ll.add("bb");
        System.out.println(ll);

    }

    public static void testDataCommitInfo() {
        DataCommitInfo dataCommitInfo = new DataCommitInfo();
        String tableId = "id_2";
        String partitionDesc = "1-1-1";
        dataCommitInfo.setTableId(tableId);
        dataCommitInfo.setPartitionDesc(partitionDesc);

        UUID uuid = UUID.randomUUID();
        System.out.println(uuid);
        dataCommitInfo.setCommitId(uuid);

        List<DataFileOp> fList = new ArrayList<>();
        DataFileOp dataFileOp = new DataFileOp();
        dataFileOp.setPath("path_10");
        dataFileOp.setFileOp("add");
        dataFileOp.setSize(100l);
        dataFileOp.setFileExistCols("a,b");
        DataFileOp dataFileOp1 = new DataFileOp();
        dataFileOp1.setPath("path_11");
        dataFileOp1.setFileOp("add");
        dataFileOp1.setSize(100l);
        dataFileOp1.setFileExistCols("a,b");
        fList.add(dataFileOp);
        fList.add(dataFileOp1);
        dataCommitInfo.setFileOps(fList);

        dataCommitInfo.setCommitOp("append");
        dataCommitInfo.setTimestamp(1234567891123l);

        dataCommitInfoDao.insert(dataCommitInfo);

        DataCommitInfo dataCommitInfo1 = dataCommitInfoDao.selectByPrimaryKey(tableId, partitionDesc, uuid);
        System.out.println(DBUtil.changeDataFileOpListToString(dataCommitInfo1.getFileOps()));

        dataCommitInfoDao.deleteByPrimaryKey(tableId, partitionDesc, uuid);

    }

    public static void testTableNameId() {
        TableNameId tableNameId = new TableNameId();
        tableNameId.setTableName("name_3");
        tableNameId.setTableId("id_3");
        tableNameIdDao.insert(tableNameId);

        String tableName = "name_3";
        TableNameId tableNameId1 = tableNameIdDao.findByTableName(tableName);

        System.out.println(tableNameId.getTableId());

        tableNameIdDao.delete("name_3");

        tableNameIdDao.updateTableId("name_1", "id_1");
    }

    public static void testTablePathId() {
        TablePathId tablePathId = new TablePathId();
        tablePathId.setTablePath("path_4");
        tablePathId.setTableId("id_4");
        tablePathIdDao.insert(tablePathId);

        String tablePath = "path_1";
        TablePathId tablePathId1 = tablePathIdDao.findByTablePath(tablePath);

        List<TablePathId> list = tablePathIdDao.listAll();
        System.out.println(tablePathId.getTableId());

        tablePathIdDao.delete("path_4");

        tablePathIdDao.updateTableId("path_1", "id_1");

    }

    public static void testUpdateTableInfo() {
        String tableId = "id_1";
        String tablePath = "path_1";
        String tableName = "name_1";
        String tableSchema = "schema_1";
        int i = tableInfoDao.updateByTableId(tableId, tableName, tablePath, tableSchema);
        System.out.println(i);
    }

    public static void testInsetTableInfo() {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableId("id_3");
        tableInfo.setTablePath("path_3");
        tableInfo.setTableName("name_3");
        tableInfo.setTableSchema("schema_3");
        String properties = "{\"k_1\":\"v_1\"}";
        String properties1 = "{\"k_2\":\"v_2\"}";
        JSONArray jsonObject = new JSONArray();
        jsonObject.add( DBUtil.stringToJSON(properties));
        jsonObject.add( DBUtil.stringToJSON(properties1));
        tableInfo.setProperties( DBUtil.stringToJSON(properties));
        tableInfo.setPartitions(jsonObject.toString());
        tableInfoDao.insert(tableInfo);
    }

    public static void selectTableInfo() {
        String tableId = "id_1";
        String tablePath = "path_1";
        String tableName = "name_2";
        TableInfo tableInfo = tableInfoDao.selectByTableId(tableId);
//        TableInfo tableInfo1 = tableInfoDao.selectByIdAndTablePath("", tablePath);
        System.out.println(tableInfo.getProperties());
//        System.out.println(tableInfo1.getTableSchema());

        //delete
        String deleteId = "id_2";
        tableInfoDao.deleteByTableId(deleteId);

    }

    public static void testJSONArray() {
        String s = "{\"k_1\":\"v_1\"}";
        String s1 = "{\"k_2\":\"v_2\"}";
        JSONObject jsonObject = DBUtil.stringToJSON(s);
//        JSONObject jsonObject = JSONObject.parseObject(s);
        JSONObject jsonObject1 = DBUtil.stringToJSON(s1);
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(jsonObject);
        jsonArray.add(jsonObject1);
        System.out.println(jsonArray);
        String s2 = jsonArray.toJSONString();
        System.out.println(s2);
        String s3 = jsonArray.toString();
        System.out.println(s3);

    }

    public static void tableInfo() {
        String tableId = "id_1";
        TableInfo tableinfo = tableInfoDao.selectByTableId(tableId);
        System.out.println(tableinfo.getProperties());
        System.out.println(tableinfo.getPartitions());
    }

    public static void testType() {
        Connection conn = null;
        String sql = "select * from data_commit_info";
        Statement stat = null;
        try {
            conn = DBConnector.getConn();
            stat = conn.createStatement();
            ResultSet resultSet = stat.executeQuery(sql);

            while (resultSet.next()) {
                System.out.println(resultSet.getString("table_id"));
                System.out.println(resultSet.getString("partition_desc"));
                System.out.println(resultSet.getString("commit_id"));
                System.out.println(resultSet.getString("file_ops"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void testClean() {
        DBUtil.cleanAllTable();
    }
}
