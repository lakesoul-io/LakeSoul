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
import com.dmetasoul.lakesoul.meta.entity.DataFileOp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DBUtil {
    private static final Logger logger = LogManager.getLogger( DBUtil.class);

    public static void init() {
        String tableInfo = "create table if not exists table_info (" +
                "table_id text," +
                "table_name text," +
                "table_path text," +
                "table_schema text," +
                "properties json," +
                "partitions text," +
                "primary key(table_id)" +
                ")";
        String tableNameId = "create table if not exists table_name_id (" +
                "table_name text," +
                "table_id text," +
                "primary key(table_name)" +
                ")";
        String tablePathId = "create table if not exists table_path_id (" +
                "table_path text," +
                "table_id text," +
                "primary key(table_path)" +
                ")";
        String dataFileOp = "create type data_file_op as (" +
                "path text," +
                "file_op text," +
                "size bigint," +
                "file_exist_cols text" +
                ")";
        String dataCommitInfo = "create table if not exists data_commit_info (" +
                "table_id text," +
                "partition_desc text," +
                "commit_id UUID," +
                "file_ops data_file_op[]," +
                "commit_op text," +
                "timestamp bigint," +
                "primary key(table_id, partition_desc, commit_id)" +
                ")";
        String partitionInfo = "create table if not exists partition_info (" +
                "table_id text," +
                "partition_desc text," +
                "version int," +
                "commit_op text," +
                "snapshot UUID[]," +
                "expression text," +
                "primary key(table_id, partition_desc, version)" +
                ")";
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DBConnector.getConn();
            stmt = conn.createStatement();
            stmt.execute(tableInfo);
            stmt.execute(tableNameId);
            stmt.execute(tablePathId);
            stmt.execute(dataFileOp);
            stmt.execute(dataCommitInfo);
            stmt.execute(partitionInfo);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn();
        }
    }

    public static void cleanAllTable() {
        String tableInfo = "truncate table table_info";
        String tableNameId = "truncate table table_name_id";
        String tablePathId = "truncate table table_path_id";
        String dataCommitInfo = "truncate table data_commit_info";
        String partitionInfo = "truncate table partition_info";
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DBConnector.getConn();
            stmt = conn.createStatement();
            stmt.addBatch(tableInfo);
            stmt.addBatch(tableNameId);
            stmt.addBatch(tablePathId);
            stmt.addBatch(dataCommitInfo);
            stmt.addBatch(partitionInfo);
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn();
        }
    }

    public static JSONObject stringToJSON(String s) {
        return JSONObject.parseObject(s);
    }

    public static String jsonToString(JSONObject o) {
        return JSON.toJSONString(o);
    }

    public static JSONArray stringToJSONArray(String s) {
        return JSONArray.parseArray(s);
    }

    public static String changeDataFileOpListToString(List<DataFileOp> dataFileOpList) {
        if (dataFileOpList.size() < 1) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (DataFileOp dataFileOp : dataFileOpList) {
            String path = dataFileOp.getPath();
            String fileOp = dataFileOp.getFileOp();
            long size = dataFileOp.getSize();
            String fileExistCols = dataFileOp.getFileExistCols();
            sb.append(String.format("\"(%s,%s,%s,\\\"%s\\\")\",", path, fileOp, size, fileExistCols));
        }
        sb = new StringBuilder(sb.substring(0, sb.length()-1));
        sb.append("}");
        return sb.toString();
    }

    public static List<DataFileOp> changeStringToDataFileOpList(String s) {
        List<DataFileOp> rsList = new ArrayList<>();
        if (!s.startsWith("{") || !s.endsWith("}")) {
            // todo 这里应该报错
            return rsList;
        }
        String[] fileOpTmp = s.substring(1, s.length()-1).split("\",\"");
        for (int i=0;i<fileOpTmp.length;i++) {
            String tmpElem = fileOpTmp[i].replace("\"","").replace("\\","");
            if (!tmpElem.startsWith("(") || !tmpElem.endsWith(")")) {
                // todo 报错
                continue;
            }
            tmpElem = tmpElem.substring(1, tmpElem.length()-1);
            DataFileOp dataFileOp = new DataFileOp();
            dataFileOp.setPath(tmpElem.substring(0, tmpElem.indexOf(",")));
            tmpElem = tmpElem.substring(tmpElem.indexOf(",") + 1);
            String fileOp = tmpElem.substring(0, tmpElem.indexOf(","));
            if (fileOp.equals("del")) {
                continue;
            }
            dataFileOp.setFileOp(fileOp);
            tmpElem = tmpElem.substring(tmpElem.indexOf(",") + 1);
            dataFileOp.setSize(Long.parseLong(tmpElem.substring(0, tmpElem.indexOf(","))));
            tmpElem = tmpElem.substring(tmpElem.indexOf(",") + 1);
            dataFileOp.setFileExistCols(tmpElem);
            rsList.add(dataFileOp);
        }
        return rsList;
    }

    public static String changeUUIDListToString(List<UUID> uuidList) {
        StringBuilder sb = new StringBuilder();
        if (uuidList.size() == 0) {
            return sb.toString();
        }
        for (UUID uuid : uuidList) {
            sb.append(String.format("'%s',", uuid.toString()));
        }
        sb = new StringBuilder(sb.substring(0, sb.length()-1));
        return sb.toString();
    }

    public static String changeUUIDListToOrderString(List<UUID> uuidList) {
        StringBuilder sb = new StringBuilder();
        if (uuidList.size() == 0) {
            return sb.toString();
        }
        for (UUID uuid : uuidList) {
            sb.append(String.format("%s,", uuid.toString()));
        }
        sb = new StringBuilder(sb.substring(0, sb.length()-1));
        return sb.toString();
    }

    public static List<UUID> changeStringToUUIDList(String s) {
        List<UUID> uuidList = new ArrayList<>();
        if (!s.startsWith("{") || !s.endsWith("}")) {
            // todo
            return uuidList;
        }
        s = s.substring(1, s.length() - 1);
        String[] uuids = s.split(",");
        for (String uuid : uuids) {
            uuidList.add(UUID.fromString(uuid));
        }
        return uuidList;
    }

    public static String changePartitionDescListToString(List<String> partitionDescList) {
        StringBuilder sb = new StringBuilder();
        if (partitionDescList.size() < 1) {
            return sb.append("''").toString();
        }
        for (String s : partitionDescList) {
            sb.append(String.format("'%s',", s));
        }
        return sb.substring(0, sb.length()-1);
    }

}
