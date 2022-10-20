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

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.DataCommitInfo;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DataCommitInfoDao {

    public void insert(DataCommitInfo dataCommitInfo) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into data_commit_info (table_id, partition_desc, commit_id, file_ops, commit_op, timestamp, committed)" +
                    " values (?, ?, ?, ?, ?, ?, ?)");
            dataCommitInsert(pstmt, dataCommitInfo);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public void deleteByPrimaryKey(String tableId, String partitionDesc, UUID commitId) {
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
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public void deleteByTableIdAndPartitionDesc(String tableId, String partitionDesc) {
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
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public void deleteByTableId(String tableId) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "delete from data_commit_info where table_id = ?";
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableId);
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public DataCommitInfo selectByPrimaryKey(String tableId, String partitionDesc, UUID commitId) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select * from data_commit_info where table_id = '%s' and partition_desc = '%s' and " +
                "commit_id = '%s'", tableId, partitionDesc, commitId);
        DataCommitInfo dataCommitInfo = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                dataCommitInfo = new DataCommitInfo();
                dataCommitInfo.setTableId(rs.getString("table_id"));
                dataCommitInfo.setPartitionDesc(rs.getString("partition_desc"));
                dataCommitInfo.setCommitId(UUID.fromString(rs.getString("commit_id")));
                dataCommitInfo.setFileOps(DBUtil.changeStringToDataFileOpList(rs.getString("file_ops")));
                dataCommitInfo.setCommitOp(rs.getString("commit_op"));
                dataCommitInfo.setTimestamp(rs.getLong("timestamp"));
                dataCommitInfo.setCommitted(rs.getBoolean("committed"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return dataCommitInfo;
    }

    public List<DataCommitInfo> selectByTableIdPartitionDescCommitList(String tableId, String partitionDesc, List<UUID> commitIdList) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<DataCommitInfo> commitInfoList = new ArrayList<>();
        if (commitIdList.size() < 1) {
            return commitInfoList;
        }
        String uuidListString = DBUtil.changeUUIDListToString(commitIdList);
        String uuidListOrderString = DBUtil.changeUUIDListToOrderString(commitIdList);
        String sql = String.format("select * from data_commit_info where table_id = '%s' and partition_desc = '%s' and " +
                "commit_id in (%s) order by position(commit_id::text in '%s') ", tableId, partitionDesc, uuidListString, uuidListOrderString);
//        String sql = String.format("select * from data_commit_info where table_id = '%s' and partition_desc = '%s' and " +
//                "commit_id in (%s) order by array_positions(array['%s'],commit_id::text) ", tableId, partitionDesc, uuidListString, uuidListOrderString);

        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                DataCommitInfo dataCommitInfo = new DataCommitInfo();
                dataCommitInfo.setTableId(rs.getString("table_id"));
                dataCommitInfo.setPartitionDesc(rs.getString("partition_desc"));
                dataCommitInfo.setCommitId(UUID.fromString(rs.getString("commit_id")));
                dataCommitInfo.setFileOps(DBUtil.changeStringToDataFileOpList(rs.getString("file_ops")));
                dataCommitInfo.setCommitOp(rs.getString("commit_op"));
                dataCommitInfo.setTimestamp(rs.getLong("timestamp"));
                dataCommitInfo.setCommitted(rs.getBoolean("committed"));
                commitInfoList.add(dataCommitInfo);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return commitInfoList;
    }

    public boolean batchInsert(List<DataCommitInfo> listData) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        boolean result = true;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into data_commit_info (table_id, partition_desc, commit_id, file_ops, commit_op, timestamp, committed)" +
                    " values (?, ?, ?, ?, ?, ?, ?)");
            conn.setAutoCommit(false);
            for (DataCommitInfo dataCommitInfo : listData) {
                dataCommitInsert(pstmt, dataCommitInfo);
            }
            conn.commit();
        } catch (SQLException e) {
            result = false;
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return result;
    }

    private void dataCommitInsert(PreparedStatement pstmt, DataCommitInfo dataCommitInfo) throws SQLException {
        pstmt.setString(1, dataCommitInfo.getTableId());
        pstmt.setString(2, dataCommitInfo.getPartitionDesc());
        pstmt.setString(3, dataCommitInfo.getCommitId().toString());
        pstmt.setString(4, DBUtil.changeDataFileOpListToString(dataCommitInfo.getFileOps()));
        pstmt.setString(5, dataCommitInfo.getCommitOp());
        pstmt.setLong(6, dataCommitInfo.getTimestamp());
        pstmt.setBoolean(7, dataCommitInfo.isCommitted());
        pstmt.execute();
    }

    public void clean() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "delete from data_commit_info;";
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }
}
