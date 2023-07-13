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

package com.dmetasoul.lakesoul.meta.dao;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TableInfoDao {

    public TableInfo selectByTableId(String tableId) {

        String sql = String.format("select * from table_info where table_id = '%s'", tableId);
        return getTableInfo(sql);
    }

    public TableInfo selectByTableNameAndNameSpace(String tableName, String namespace) {
        String sql = String.format("select * from table_info where table_name = '%s'" +
                " and table_namespace='%s'", tableName, namespace);
        return getTableInfo(sql);
    }

    public TableInfo selectByTablePath(String tablePath) {
        String sql = String.format("select * from table_info where table_path = '%s'", tablePath);
        return getTableInfo(sql);
    }

    public TableInfo selectByIdAndTablePath(String tableId, String tablePath) {
        String sql = String.format("select * from table_info where table_id = '%s' and table_path = '%s' ", tableId,
                tablePath);
        return getTableInfo(sql);
    }

    public TableInfo selectByIdAndTableName(String tableId, String tableName) {
        String sql = String.format("select * from table_info where table_id = '%s' and table_name = '%s' ", tableId,
                tableName);
        return getTableInfo(sql);
    }

    private TableInfo getTableInfo(String sql) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        TableInfo tableInfo = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                tableInfo = tableInfoFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return tableInfo;
    }

    public void insert(TableInfo tableInfo) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(
                    "insert into table_info(table_id, table_name, table_path, table_schema, properties, partitions, table_namespace, domain) " +
                            "values (?, ?, ?, ?, ?, ?, ?, ?)");
            pstmt.setString(1, tableInfo.getTableId());
            pstmt.setString(2, tableInfo.getTableName());
            pstmt.setString(3, tableInfo.getTablePath());
            pstmt.setString(4, tableInfo.getTableSchema());
            pstmt.setString(5, tableInfo.getProperties());
            pstmt.setString(6, tableInfo.getPartitions());
            pstmt.setString(7, tableInfo.getTableNamespace());
            pstmt.setString(8, tableInfo.getDomain());
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public void deleteByTableId(String tableId) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from table_info where table_id = '%s' ", tableId);
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public void deleteByIdAndPath(String tableId, String tablePath) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql =
                String.format("delete from table_info where table_id = '%s' and table_path = '%s'", tableId, tablePath);
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public int updatePropertiesById(String tableId, String properties) {
        int result = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;
        StringBuilder sb = new StringBuilder();
        sb.append("update table_info set ");
        sb.append(String.format("properties = '%s'", properties));
        sb.append(String.format(" where table_id = '%s'", tableId));
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sb.toString());
            result = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return result;
    }

    public int updateByTableId(String tableId, String tableName, String tablePath, String tableSchema) {
        int result = 0;
        if (StringUtils.isBlank(tableName) && StringUtils.isBlank(tablePath) && StringUtils.isBlank(tableSchema)) {
            return result;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        StringBuilder sb = new StringBuilder();
        sb.append("update table_info set ");
        if (StringUtils.isNotBlank(tableName)) {
            sb.append(String.format("table_name = '%s', ", tableName));
        }
        if (StringUtils.isNotBlank(tablePath)) {
            sb.append(String.format("table_path = '%s', ", tablePath));
        }
        if (StringUtils.isNotBlank(tableSchema)) {
            sb.append(String.format("table_schema = '%s', ", tableSchema));
        }
        sb = new StringBuilder(sb.substring(0, sb.length() - 2));
        sb.append(String.format(" where table_id = '%s'", tableId));
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sb.toString());
            result = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return result;
    }

    public void clean() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "delete from table_info;";
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public static TableInfo tableInfoFromResultSet(ResultSet rs) throws SQLException {
        return TableInfo.newBuilder()
                .setTableId(rs.getString("table_id"))
                .setTableName(rs.getString("table_name"))
                .setTablePath(rs.getString("table_path"))
                .setTableSchema(rs.getString("table_schema"))
                .setProperties(rs.getString("properties"))
                .setPartitions(rs.getString("partitions"))
                .setTableNamespace(rs.getString("table_namespace"))
                .setDomain(rs.getString("domain"))
                .build();
    }
}
