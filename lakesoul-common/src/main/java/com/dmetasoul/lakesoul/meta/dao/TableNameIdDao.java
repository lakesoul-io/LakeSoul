// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.dao;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.entity.TableNameId;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TableNameIdDao {

    public TableNameId findByTableName(String tableName, String tableNamespace) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select * from table_name_id where table_name = '%s' and table_namespace = '%s'",
                tableName, tableNamespace);
        TableNameId tableNameId = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                tableNameId = tableNameIdFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return tableNameId;
    }

    public List<String> listAllNameByNamespace(String table_namespace) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql =
                String.format("select table_name from table_name_id where table_namespace = '%s'", table_namespace);
        List<String> list = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                list.add(tableName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public void insert(TableNameId tableNameId) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(
                    "insert into table_name_id (table_name, table_id, table_namespace, domain) values (?, ?, ?, ?)");
            pstmt.setString(1, tableNameId.getTableName());
            pstmt.setString(2, tableNameId.getTableId());
            pstmt.setString(3, tableNameId.getTableNamespace());
            pstmt.setString(4, tableNameId.getDomain());
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public void delete(String tableName, String tableNamespace) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql =
                String.format("delete from table_name_id where table_name = '%s' and table_namespace = '%s'", tableName,
                        tableNamespace);
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

    public void deleteByTableId(String tableId) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from table_name_id where table_id = '%s' ", tableId);
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

    public int updateTableId(String tableName, String table_id, String tableNamespace) {
        int result = 0;
        if (StringUtils.isBlank(table_id)) {
            return result;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format(
                "update table_name_id set table_id = '%s' where table_name = '%s' and table_namespace = '%s'", table_id,
                tableName, tableNamespace);
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
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
        String sql = "delete from table_name_id;";
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

    public static TableNameId tableNameIdFromResultSet(ResultSet rs) throws SQLException {
        return TableNameId.newBuilder()
                .setTableName(rs.getString("table_name"))
                .setTableId(rs.getString("table_id"))
                .setTableNamespace(rs.getString("table_namespace"))
                .setDomain(rs.getString("domain"))
                .build();
    }

    public static TableNameId newTableNameId(String tableName, String tableId, String namespace, String domain) {
        return TableNameId
                .newBuilder()
                .setTableName(tableName)
                .setTableId(tableId)
                .setTableNamespace(namespace)
                .setDomain(domain)
                .build();
    }
}
