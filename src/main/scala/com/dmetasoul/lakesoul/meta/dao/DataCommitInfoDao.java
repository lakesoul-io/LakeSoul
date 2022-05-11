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
            pstmt = conn.prepareStatement("insert into data_commit_info (table_id, partition_desc, commit_id, file_ops, commit_op)" +
                    " values (?, ?, ?, ?, ?)");
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
        String sql = String.format("delete from data_commit_info where table_id = '%s' and partition_desc = '%s' and commit_id = '%s' ",
                tableId, partitionDesc, commitId);
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
            dataCommitInfo = new DataCommitInfo();
            while (rs.next()) {
                dataCommitInfo.setTableId(rs.getString("table_id"));
                dataCommitInfo.setPartitionDesc(rs.getString("partition_desc"));
                dataCommitInfo.setCommitId(UUID.fromString(rs.getString("commit_id")));
                dataCommitInfo.setFileOps(DBUtil.changeStringToDataFileOpList(rs.getString("file_ops")));
                dataCommitInfo.setCommitOp(rs.getString("commit_op"));
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
        String uuidListString = DBUtil.changeUUIDListToString(commitIdList);
        String sql = String.format("select * from data_commit_info where table_id = '%s' and partition_desc = '%s' and " +
                "commit_id in (%s)", tableId, partitionDesc, uuidListString);

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
            pstmt = conn.prepareStatement("insert into data_commit_info (table_id, partition_desc, commit_id, file_ops, commit_op)" +
                    " values (?, ?, ?, ?, ?)");
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
        pstmt.execute();
    }
}
