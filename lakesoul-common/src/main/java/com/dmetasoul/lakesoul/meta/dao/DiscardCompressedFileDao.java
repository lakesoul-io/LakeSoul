// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.dao;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.entity.DiscardCompressedFileInfo;
import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DiscardCompressedFileDao {

    public DiscardCompressedFileInfo findByFilePath(String filePath) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectDiscardCompressedFileInfoByFilePath,
                    Collections.singletonList(filePath));
            if (jniWrapper == null) return null;
            List<DiscardCompressedFileInfo> discardCompressedFileInfoList = jniWrapper.getDiscardCompressedFileInfoList();
            return discardCompressedFileInfoList.isEmpty() ? null : discardCompressedFileInfoList.get(0);
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select file_path, table_path, partition_desc, timestamp, t_date "
                + "from discard_compressed_file_info where file_path = '%s'", filePath);
        DiscardCompressedFileInfo discardCompressedFileInfo = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                discardCompressedFileInfo = discardCompressedFileInfoFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return discardCompressedFileInfo;
    }

    public List<DiscardCompressedFileInfo> listAllDiscardCompressedFile() {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListAllDiscardCompressedFileInfo,
                    Collections.emptyList());
            if (jniWrapper == null) return null;
            return jniWrapper.getDiscardCompressedFileInfoList();
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select file_path, table_path, partition_desc, timestamp, t_date from discard_compressed_file_info";
        List<DiscardCompressedFileInfo> list = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                list.add(discardCompressedFileInfoFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public List<DiscardCompressedFileInfo> getDiscardCompressedFileBeforeTimestamp(long timestamp) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListDiscardCompressedFileInfoBeforeTimestamp,
                    Collections.singletonList(Long.toString(timestamp)));
            if (jniWrapper == null) return null;
            return jniWrapper.getDiscardCompressedFileInfoList();
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select file_path, table_path, partition_desc, timestamp, t_date "
                + "from discard_compressed_file_info where timestamp < %s", timestamp);
        List<DiscardCompressedFileInfo> list = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                list.add(discardCompressedFileInfoFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public List<DiscardCompressedFileInfo> getDiscardCompressedFileByFilterCondition(String tablePath, String partition, long timestamp) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListDiscardCompressedFileByFilterCondition,
                    Arrays.asList(tablePath, partition, Long.toString(timestamp)));
            if (jniWrapper == null) return null;
            return jniWrapper.getDiscardCompressedFileInfoList();
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select file_path, table_path, partition_desc, timestamp, t_date "
                + "from discard_compressed_file_info where table_path = '%s' and partition_desc = '%s' and timestamp < %s",
                timestamp, partition, timestamp);
        List<DiscardCompressedFileInfo> list = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                list.add(discardCompressedFileInfoFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public void insert(DiscardCompressedFileInfo discardCompressedFileInfo) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.insert(
                    NativeUtils.CodedDaoType.InsertDiscardCompressedFileInfo,
                    JniWrapper.newBuilder().addDiscardCompressedFileInfo(discardCompressedFileInfo).build());
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into discard_compressed_file_info (file_path, table_path, partition_desc, timestamp, t_date) values (?, ?, ?, ?, ?)");
            dataCommitInsert(pstmt, discardCompressedFileInfo);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public boolean batchInsert(List<DiscardCompressedFileInfo> discardCompressedFileInfoList) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.insert(
                    NativeUtils.CodedDaoType.TransactionInsertDiscardCompressedFile,
                    JniWrapper.newBuilder().addAllDiscardCompressedFileInfo(discardCompressedFileInfoList).build());
            return count > 0;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        boolean result = true;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into discard_compressed_file_info (file_path, table_path, partition_desc, timestamp, t_date) values (?, ?, ?, ?, ?)");
            conn.setAutoCommit(false);
            for (DiscardCompressedFileInfo discardCompressedFileInfo : discardCompressedFileInfoList) {
                dataCommitInsert(pstmt, discardCompressedFileInfo);
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return result;
    }

    private void dataCommitInsert(PreparedStatement pstmt, DiscardCompressedFileInfo discardCompressedFileInfo)
            throws SQLException {
        pstmt.setString(1, discardCompressedFileInfo.getFilePath());
        pstmt.setString(2, discardCompressedFileInfo.getTablePath());
        pstmt.setString(3, discardCompressedFileInfo.getPartitionDesc());
        pstmt.setLong(4, discardCompressedFileInfo.getTimestamp());
        pstmt.setString(5, discardCompressedFileInfo.getTDate());
        pstmt.execute();
    }

    public void deleteByFilePath(String filePath) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeleteDiscardCompressedFileInfoByFilePath,
                    Collections.singletonList(filePath));
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from discard_compressed_file_info where file_path = '%s' ", filePath);
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

    public void deleteByTablePath(String tablePath) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeleteDiscardCompressedFileInfoByTablePath,
                    Collections.singletonList(tablePath));
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from discard_compressed_file_info where table_path = '%s' ", tablePath);
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

    public void deleteDiscardCompressedFileByFilterCondition(String tablePath, String partition, long timestamp) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeleteDiscardCompressedFileByFilterCondition,
                    Arrays.asList(tablePath, partition, Long.toString(timestamp)));
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from discard_compressed_file_info where table_path = '%s' "
                + "and partition_desc = '%s' and timestamp < %s", tablePath, partition, timestamp);
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

    public void clean() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "delete from discard_compressed_file_info;";
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

    public static DiscardCompressedFileInfo discardCompressedFileInfoFromResultSet(ResultSet rs) throws SQLException {
        return DiscardCompressedFileInfo.newBuilder()
                .setFilePath(rs.getString("file_path"))
                .setTablePath(rs.getString("table_path"))
                .setPartitionDesc(rs.getString("partition_desc"))
                .setTimestamp(rs.getLong("timestamp"))
                .setTDate(rs.getString("t_date"))
                .build();
    }
}
