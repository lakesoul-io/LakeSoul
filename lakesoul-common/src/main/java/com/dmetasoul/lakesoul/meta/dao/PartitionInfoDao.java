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
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;

import java.sql.*;
import java.util.*;

public class PartitionInfoDao {

    public boolean insert(PartitionInfo partitionInfo) {
        boolean flag = true;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into partition_info (table_id, partition_desc, version, " +
                    "commit_op, snapshot, expression) values (?, ?, ?, ? ,?, ?)");
            insertSinglePartitionInfo(conn, pstmt, partitionInfo);
        } catch (SQLException e) {
            flag = false;
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return flag;
    }

    public boolean transactionInsert(List<PartitionInfo> partitionInfoList, List<UUID> snapshotList) {
        boolean flag = true;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into partition_info (table_id, partition_desc, version, " +
                    "commit_op, snapshot, expression) values (?, ?, ?, ? ,?, ?)");
            conn.setAutoCommit(false);
            for (PartitionInfo partitionInfo : partitionInfoList) {
                insertSinglePartitionInfo(conn, pstmt, partitionInfo);
            }
            pstmt = conn.prepareStatement("update data_commit_info set committed = 'true' where commit_id = ?");
            for (UUID uuid : snapshotList) {
                pstmt.setString(1, uuid.toString());
                pstmt.execute();
            }
            conn.commit();
        } catch (SQLException e) {
            flag = false;
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return flag;
    }

    private void insertSinglePartitionInfo(Connection conn, PreparedStatement pstmt, PartitionInfo partitionInfo) throws SQLException {
        Array array = conn.createArrayOf("UUID", partitionInfo.getSnapshot().toArray());
        pstmt.setString(1, partitionInfo.getTableId());
        pstmt.setString(2, partitionInfo.getPartitionDesc());
        pstmt.setInt(3, partitionInfo.getVersion());
        pstmt.setString(4, partitionInfo.getCommitOp());
        pstmt.setArray(5, array);
        pstmt.setString(6, partitionInfo.getExpression());
        pstmt.execute();
    }

    public void deleteByTableIdAndPartitionDesc(String tableId, String partitionDesc) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from partition_info where table_id = '%s' and partition_desc = '%s'", tableId, partitionDesc);
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

    public void deleteByTableId(String tableId) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "delete from partition_info where table_id = ? ";
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

    public List<PartitionInfo> findByTableIdAndParList(String tableId, List<String> partitionDescList) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String partitionString = DBUtil.changePartitionDescListToString(partitionDescList);
        String sql = String.format(
                "select m.table_id, t.partition_desc, m.version, m.commit_op, m.snapshot, m.expression from (" +
                "select table_id,partition_desc,max(version) from partition_info " +
                "where table_id = '%s' and partition_desc in (%s) " +
                "group by table_id,partition_desc) t " +
                "left join partition_info m on t.table_id = m.table_id and t.partition_desc = m.partition_desc and t.max = m.version", tableId, partitionString);
        List<PartitionInfo> rsList = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setTableId(rs.getString("table_id"));
                partitionInfo.setPartitionDesc(rs.getString("partition_desc"));
                partitionInfo.setVersion(rs.getInt("version"));
                partitionInfo.setCommitOp(rs.getString("commit_op"));
                Array snapshotArray = rs.getArray("snapshot");
                List<UUID> uuidList = new ArrayList<>();
                Collections.addAll(uuidList, (UUID[]) snapshotArray.getArray());
                partitionInfo.setSnapshot(uuidList);
                partitionInfo.setExpression(rs.getString("expression"));
                rsList.add(partitionInfo);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return rsList;
    }

    public PartitionInfo selectLatestPartitionInfo(String tableId, String partitionDesc) {
        String sql = String.format(
                "select m.table_id, t.partition_desc, m.version, m.commit_op, m.snapshot, m.expression from (" +
                        "select table_id,partition_desc,max(version) from partition_info " +
                        "where table_id = '%s' and partition_desc = '%s' " +
                        "group by table_id,partition_desc) t " +
                        "left join partition_info m on t.table_id = m.table_id and t.partition_desc = m.partition_desc and t.max = m.version", tableId, partitionDesc);
        return getPartitionInfo(sql);
    }

    public List<PartitionInfo> getPartitionVersions(String tableId,String partitionDesc) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<PartitionInfo> rsList = new ArrayList<>();
        String sql = String.format("select * from partition_info where table_id = '%s' and partition_desc = '%s'",
                tableId, partitionDesc);
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setTableId(rs.getString("table_id"));
                partitionInfo.setPartitionDesc(rs.getString("partition_desc"));
                partitionInfo.setVersion(rs.getInt("version"));
                rsList.add(partitionInfo);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return rsList;
    }

    public List<PartitionInfo> getPartitionDescByTableId(String tableId) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<PartitionInfo> rsList = new ArrayList<>();
        String sql = String.format(
                "select m.table_id, t.partition_desc, m.version, m.commit_op, m.snapshot, m.expression from (" +
                        "select table_id,partition_desc,max(version) from partition_info " +
                        "where table_id = '%s' " +
                        "group by table_id,partition_desc) t " +
                        "left join partition_info m on t.table_id = m.table_id and t.partition_desc = m.partition_desc and t.max = m.version", tableId);
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setTableId(rs.getString("table_id"));
                partitionInfo.setPartitionDesc(rs.getString("partition_desc"));
                partitionInfo.setVersion(rs.getInt("version"));
                partitionInfo.setCommitOp(rs.getString("commit_op"));
                Array snapshotArray = rs.getArray("snapshot");
                List<UUID> uuidList = new ArrayList<>();
                Collections.addAll(uuidList, (UUID[]) snapshotArray.getArray());
                partitionInfo.setSnapshot(uuidList);
                partitionInfo.setExpression(rs.getString("expression"));
                rsList.add(partitionInfo);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return rsList;
    }

    public PartitionInfo findByKey(String tableId, String partitionDesc, int version) {

        String sql = String.format("select * from partition_info where table_id = '%s' and partition_desc = '%s' and version = %d",
                tableId, partitionDesc, version);
        return getPartitionInfo(sql);
    }

    private PartitionInfo getPartitionInfo(String sql) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        PartitionInfo partitionInfo = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                partitionInfo = new PartitionInfo();
                partitionInfo.setTableId(rs.getString("table_id"));
                partitionInfo.setPartitionDesc(rs.getString("partition_desc"));
                partitionInfo.setVersion(rs.getInt("version"));
                partitionInfo.setCommitOp(rs.getString("commit_op"));
                Array snapshotArray = rs.getArray("snapshot");
                List<UUID> uuidList = new ArrayList<>();
                Collections.addAll(uuidList, (UUID[]) snapshotArray.getArray());
                partitionInfo.setSnapshot(uuidList);
                partitionInfo.setExpression(rs.getString("expression"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return partitionInfo;
    }

    public void clean() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "delete from partition_info;";
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
