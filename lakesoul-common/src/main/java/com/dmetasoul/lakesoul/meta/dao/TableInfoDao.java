// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.dao;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TableInfoDao {

    public TableInfo selectByTableId(String tableId) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectTableInfoByTableId,
                    Collections.singletonList(tableId));
            if (jniWrapper == null) return null;
            List<TableInfo> tableInfoList = jniWrapper.getTableInfoList();
            return tableInfoList.isEmpty() ? null : tableInfoList.get(0);
        }
        String sql = String.format("select * from table_info where table_id = '%s'", tableId);
        return getTableInfo(sql);
    }

    public List<TableInfo> selectByNamespace(String namespace) {
        String sql = String.format("select * from table_info where table_namespace='%s'", namespace);
        return getTableInfos(sql);
    }

    public TableInfo selectByTableNameAndNameSpace(String tableName, String namespace) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectTableInfoByTableNameAndNameSpace,
                    Arrays.asList(tableName, namespace));
            if (jniWrapper == null) return null;
            List<TableInfo> tableInfoList = jniWrapper.getTableInfoList();
            return tableInfoList.isEmpty() ? null : tableInfoList.get(0);
        }
        String sql = String.format("select * from table_info where table_name = '%s'" +
                " and table_namespace='%s'", tableName, namespace);
        return getTableInfo(sql);
    }

    public TableInfo selectByTablePath(String tablePath) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectTableInfoByTablePath,
                    Collections.singletonList(tablePath));
            if (jniWrapper == null) return null;
            List<TableInfo> tableInfoList = jniWrapper.getTableInfoList();
            return tableInfoList.isEmpty() ? null : tableInfoList.get(0);
        }
        String sql = String.format("select * from table_info where table_path = '%s'", tablePath);
        return getTableInfo(sql);
    }

    public TableInfo selectByIdAndTablePath(String tableId, String tablePath) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectTableInfoByIdAndTablePath,
                    Arrays.asList(tableId, tablePath));
            if (jniWrapper == null) return null;
            List<TableInfo> tableInfoList = jniWrapper.getTableInfoList();
            return tableInfoList.isEmpty() ? null : tableInfoList.get(0);
        }
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

    private List<TableInfo> getTableInfos(String sql) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        ArrayList<TableInfo> tableinfos = new ArrayList<>(100);
        TableInfo tableInfo = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                tableInfo = tableInfoFromResultSet(rs);
                tableinfos.add(tableInfo);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return tableinfos;
    }

    public void insert(TableInfo tableInfo) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.insert(
                    NativeUtils.CodedDaoType.InsertTableInfo,
                    JniWrapper.newBuilder().addTableInfo(tableInfo).build());
            return;
        }
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

//    public void deleteByTableId(String tableId) {
//        Connection conn = null;
//        PreparedStatement pstmt = null;
//        String sql = String.format("delete from table_info where table_id = '%s' ", tableId);
//        try {
//            conn = DBConnector.getConn();
//            pstmt = conn.prepareStatement(sql);
//            pstmt.execute();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        } finally {
//            DBConnector.closeConn(pstmt, conn);
//        }
//    }

    public void deleteByIdAndPath(String tableId, String tablePath) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {

            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeleteTableInfoByIdAndPath,
                    Arrays.asList(tableId, tablePath));
            return;
        }
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
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            return NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.UpdateTableInfoPropertiesById,
                    Arrays.asList(tableId, properties));
        }
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
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            return NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.UpdateTableInfoById,
                    Arrays.asList(tableId, tableName, tablePath, tableSchema));
        }
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

    public static boolean isArrowKindSchema(String schema) {
        return schema.charAt(schema.indexOf('"') + 1) == 'f';
    }

    public static boolean isSparkKindSchema(String schema) {
        return schema.charAt(schema.indexOf('"') + 1) == 't';
    }
}
