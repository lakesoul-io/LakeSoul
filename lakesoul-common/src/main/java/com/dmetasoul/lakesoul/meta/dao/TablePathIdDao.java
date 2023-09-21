// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.dao;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.dmetasoul.lakesoul.meta.entity.TablePathId;
import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TablePathIdDao {

    public TablePathId findByTablePath(String tablePath) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectTablePathIdByTablePath,
                    Collections.singletonList(tablePath));
            if (jniWrapper == null) return null;
            List<TablePathId> tablePathIdList = jniWrapper.getTablePathIdList();
            return tablePathIdList.isEmpty() ? null : tablePathIdList.get(0);
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select * from table_path_id where table_path = '%s'", tablePath);
        TablePathId tablePathId = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                tablePathId = tablePathIdFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return tablePathId;
    }

    public List<TablePathId> listAll() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select * from table_path_id";
        List<TablePathId> list = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                list.add(tablePathIdFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public List<TablePathId> listAllByNamespace(String tableNamespace) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select * from table_path_id where table_namespace = '%s'", tableNamespace);
        List<TablePathId> list = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                list.add(tablePathIdFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public List<String> listAllPath() {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListAllTablePath,
                    Collections.emptyList());
            return jniWrapper.getTablePathIdList().stream().map(TablePathId::getTablePath).collect(Collectors.toList());
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select table_path from table_path_id";
        List<String> list = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tablePath = rs.getString("table_path");
                list.add(tablePath);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public List<String> listAllPathByNamespace(String tableNamespace) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListAllPathTablePathByNamespace,
                    Collections.singletonList(tableNamespace));
            return jniWrapper.getTablePathIdList().stream().map(TablePathId::getTablePath).collect(Collectors.toList());
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select table_path from table_path_id where table_namespace = '%s'", tableNamespace);
        List<String> list = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tablePath = rs.getString("table_path");
                list.add(tablePath);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public void insert(TablePathId tablePathId) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.insert(
                    NativeUtils.CodedDaoType.InsertTablePathId,
                    JniWrapper.newBuilder().addTablePathId(tablePathId).build());
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into table_path_id (table_path, table_id, table_namespace, domain) values (?, ?, ?, ?)");
            pstmt.setString(1, tablePathId.getTablePath());
            pstmt.setString(2, tablePathId.getTableId());
            pstmt.setString(3, tablePathId.getTableNamespace());
            pstmt.setString(4, tablePathId.getDomain());
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public void delete(String tablePath) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeleteTablePathIdByTablePath,
                    Collections.singletonList(tablePath));
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from table_path_id where table_path = '%s' ", tablePath);
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
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeleteTablePathIdByTableId,
                    Collections.singletonList(tableId));
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from table_path_id where table_id = '%s' ", tableId);
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

    public int updateTableId(String tablePath, String table_id) {
        int result = 0;
        if (StringUtils.isBlank(table_id)) {
            return result;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("update table_path_id set table_id = '%s' where table_path = '%s' ", table_id, tablePath);
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
        String sql = "delete from table_path_id;";
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

    public static TablePathId tablePathIdFromResultSet(ResultSet rs) throws SQLException {
        return TablePathId.newBuilder()
                .setTablePath(rs.getString("table_path"))
                .setTableId(rs.getString("table_id"))
                .setTableNamespace(rs.getString("table_namespace"))
                .setDomain(rs.getString("domain"))
                .build();
    }

    public static TablePathId newTablePathId(String tablePath, String tableId, String namespace, String domain) {
        return TablePathId
                .newBuilder()
                .setTablePath(tablePath)
                .setTableId(tableId)
                .setTableNamespace(namespace)
                .setDomain(domain)
                .build();
    }
}
