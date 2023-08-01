// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.dao;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.*;
import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class DataCommitInfoDao {

    public void insert(DataCommitInfo dataCommitInfo) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.insert(
                    NativeUtils.CodedDaoType.InsertDataCommitInfo,
                    JniWrapper.newBuilder().addDataCommitInfo(dataCommitInfo).build());
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(
                    "insert into data_commit_info (table_id, partition_desc, commit_id, file_ops, commit_op, " +
                            "timestamp, committed, domain)" +
                            " values (?, ?, ?, ?, ?, ?, ?, ?)");
            dataCommitInsert(pstmt, dataCommitInfo);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public void deleteByPrimaryKey(String tableId, String partitionDesc, UUID commitId) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeleteOneDataCommitInfoByTableIdAndPartitionDescAndCommitId,
                    Arrays.asList(tableId, partitionDesc, commitId.toString()));
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "delete from data_commit_info where table_id = ? and partition_desc = ? and commit_id = ? ";
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableId);
            pstmt.setString(2, partitionDesc);
            pstmt.setString(3, commitId.toString());
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public void deleteByTableIdPartitionDescCommitList(String tableId, String partitionDesc, List<Uuid> commitIdList) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeleteDataCommitInfoByTableIdAndPartitionDescAndCommitIdList,
                    Arrays.asList(tableId, partitionDesc,
                            commitIdList.stream().map(DBUtil::protoUuidToJniString).collect(Collectors.joining(""))
                    ));
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        if (commitIdList.size() < 1) {
            return;
        }

        String sql = String.format("delete from data_commit_info where table_id = ? and partition_desc = ? and " +
                "commit_id in (%s)", String.join(",", Collections.nCopies(commitIdList.size(), "?")));
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableId);
            pstmt.setString(2, partitionDesc);
            int index = 3;
            for (Uuid uuid : commitIdList) {
                pstmt.setString(index++, DBUtil.toJavaUUID(uuid).toString());
            }
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public void deleteByTableIdAndPartitionDesc(String tableId, String partitionDesc) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeleteDataCommitInfoByTableIdAndPartitionDesc,
                    Arrays.asList(tableId, partitionDesc));
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "delete from data_commit_info where table_id = ? and partition_desc = ?";
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableId);
            pstmt.setString(2, partitionDesc);
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
                    NativeUtils.CodedDaoType.DeleteDataCommitInfoByTableId,
                    Collections.singletonList(tableId));
            return;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "delete from data_commit_info where table_id = ?";
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableId);
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public DataCommitInfo selectByPrimaryKey(String tableId, String partitionDesc, String commitId) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectOneDataCommitInfoByTableIdAndPartitionDescAndCommitId,
                    Arrays.asList(tableId, partitionDesc, commitId));
            if (jniWrapper == null) return null;
            List<DataCommitInfo> dataCommitInfoList = jniWrapper.getDataCommitInfoList();
            return dataCommitInfoList.isEmpty() ? null : dataCommitInfoList.get(0);
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select * from data_commit_info where table_id = ? and partition_desc = ? and " +
                "commit_id = ?";
        DataCommitInfo dataCommitInfo = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableId);
            pstmt.setString(2, partitionDesc);
            pstmt.setString(3, commitId);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                dataCommitInfo = dataCommitInfoFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return dataCommitInfo;
    }

    public DataCommitInfo selectByTableId(String tableId) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectOneDataCommitInfoByTableId,
                    Collections.singletonList(tableId));
            if (jniWrapper == null) return null;
            List<DataCommitInfo> dataCommitInfoList = jniWrapper.getDataCommitInfoList();
            return dataCommitInfoList.isEmpty() ? null : dataCommitInfoList.get(0);
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql =
                String.format("select * from data_commit_info where table_id = '%s' order by timestamp DESC LIMIT 1",
                        tableId);
        DataCommitInfo dataCommitInfo = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                dataCommitInfo = dataCommitInfoFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return dataCommitInfo;
    }

    public List<DataCommitInfo> selectByTableIdPartitionDescCommitList(String tableId, String partitionDesc,
                                                                       List<Uuid> commitIdList) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            if (commitIdList.isEmpty()) {
                return Collections.emptyList();
            }
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListDataCommitInfoByTableIdAndPartitionDescAndCommitList,
                    Arrays.asList(tableId, partitionDesc,
                            commitIdList.stream().map(DBUtil::protoUuidToJniString).collect(Collectors.joining(""))
                    ));
            if (jniWrapper == null) return null;
            return jniWrapper.getDataCommitInfoList();
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<DataCommitInfo> commitInfoList = new ArrayList<>();
        if (commitIdList.size() < 1) {
            return commitInfoList;
        }
        String uuidListOrderString = commitIdList.stream().map(uuid -> DBUtil.toJavaUUID(uuid).toString()).collect(Collectors.joining(","));
        String sql = String.format("select * from data_commit_info where table_id = ? and partition_desc = ? and " +
                "commit_id in (%s) order by position(commit_id::text in ?) ", String.join(",", Collections.nCopies(commitIdList.size(), "?")));

        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableId);
            pstmt.setString(2, partitionDesc);
            int index = 3;
            for (Uuid uuid : commitIdList) {
                pstmt.setString(index++, DBUtil.toJavaUUID(uuid).toString());
            }
            pstmt.setString(index, uuidListOrderString);

            rs = pstmt.executeQuery();
            while (rs.next()) {
                DataCommitInfo dataCommitInfo = dataCommitInfoFromResultSet(rs);
                commitInfoList.add(dataCommitInfo);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return commitInfoList;
    }

    public static DataCommitInfo dataCommitInfoFromResultSet(ResultSet rs) throws SQLException {
        UUID uuid = UUID.fromString(rs.getString("commit_id"));
        Uuid commitId = DBUtil.toProtoUuid(uuid);
        return DataCommitInfo.newBuilder()
                .setTableId(rs.getString("table_id"))
                .setPartitionDesc(rs.getString("partition_desc"))
                .setCommitId(commitId)
                .addAllFileOps(DBUtil.changeStringToDataFileOpList(rs.getString("file_ops")))
                .setCommitOp(CommitOp.valueOf(rs.getString("commit_op")))
                .setTimestamp(rs.getLong("timestamp"))
                .setCommitted(rs.getBoolean("committed"))
                .setDomain(rs.getString("domain"))
                .build();
    }

    public boolean batchInsert(List<DataCommitInfo> listData) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            if (listData.isEmpty()) return true;
            Integer count = NativeMetadataJavaClient.insert(
                    NativeUtils.CodedDaoType.TransactionInsertDataCommitInfo,
                    JniWrapper.newBuilder().addAllDataCommitInfo(listData).build());
            return count > 0;
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        boolean result = true;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(
                    "insert into data_commit_info (table_id, partition_desc, commit_id, file_ops, commit_op, " +
                            "timestamp, committed, domain)" +
                            " values (?, ?, ?, ?, ?, ?, ?, ?)");
            conn.setAutoCommit(false);
            for (DataCommitInfo dataCommitInfo : listData) {
                dataCommitInsert(pstmt, dataCommitInfo);
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

    private void dataCommitInsert(PreparedStatement pstmt, DataCommitInfo dataCommitInfo) throws SQLException {
        pstmt.setString(1, dataCommitInfo.getTableId());
        pstmt.setString(2, dataCommitInfo.getPartitionDesc());
        pstmt.setString(3, DBUtil.toJavaUUID(dataCommitInfo.getCommitId()).toString());
        pstmt.setString(4, DBUtil.changeDataFileOpListToString(dataCommitInfo.getFileOpsList()));
        pstmt.setString(5, dataCommitInfo.getCommitOp().toString());
        pstmt.setLong(6, dataCommitInfo.getTimestamp());
        pstmt.setBoolean(7, dataCommitInfo.getCommitted());
        pstmt.setString(8, dataCommitInfo.getDomain());
        pstmt.execute();
    }

    public void clean() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "delete from data_commit_info;";
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
}
