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
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;

import java.sql.*;
import java.util.*;

public class PartitionInfoDao {

    public void insert(PartitionInfo partitionInfo) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into partition_info (table_id, partition_desc, version, " +
                    "commit_op, snapshot, expression, domain) values (?, ?, ?, ? ,?, ?, ?)");
            insertSinglePartitionInfo(conn, pstmt, partitionInfo);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public boolean transactionInsert(List<PartitionInfo> partitionInfoList, List<UUID> snapshotList) {
        boolean flag = true;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into partition_info (table_id, partition_desc, version, " +
                    "commit_op, snapshot, expression, domain) values (?, ?, ?, ? ,?, ?, ?)");
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
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            if (e.getMessage().contains("duplicate key value violates unique constraint")) {
                // only when primary key conflicts could we ignore the exception
                e.printStackTrace();
            } else {
                // throw exception in all other cases
                throw new RuntimeException(e);
            }
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return flag;
    }

    private void insertSinglePartitionInfo(Connection conn, PreparedStatement pstmt, PartitionInfo partitionInfo)
            throws SQLException {
        Array array = conn.createArrayOf("UUID", partitionInfo.getSnapshot().toArray());
        pstmt.setString(1, partitionInfo.getTableId());
        pstmt.setString(2, partitionInfo.getPartitionDesc());
        pstmt.setInt(3, partitionInfo.getVersion());
        pstmt.setString(4, partitionInfo.getCommitOp());
        pstmt.setArray(5, array);
        pstmt.setString(6, partitionInfo.getExpression());
        pstmt.setString(7, partitionInfo.getDomain());
        pstmt.execute();
    }

    public void deleteByTableIdAndPartitionDesc(String tableId, String partitionDesc) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql =
                String.format("delete from partition_info where table_id = '%s' and partition_desc = '%s'", tableId,
                        partitionDesc);
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
        String sql = "delete from partition_info where table_id = ? ";
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

    public void deletePreviousVersionPartition(String tableId, String partitionDesc, long utcMills) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "delete from partition_info where table_id = ? and partition_desc = ? and timestamp <= ?";
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableId);
            pstmt.setString(2, partitionDesc);
            pstmt.setLong(3, utcMills);
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public List<PartitionInfo> findByTableIdAndParList(String tableId, List<String> partitionDescList) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String descPlaceholders = "?";
        if (!partitionDescList.isEmpty()) {
            descPlaceholders = String.join(",", Collections.nCopies(partitionDescList.size(), "?"));
        }
        String sql = String.format(
                "select m.table_id, t.partition_desc, m.version, m.commit_op, m.snapshot, m.expression, m.domain from (" +
                        "select table_id,partition_desc,max(version) from partition_info " +
                        "where table_id = ? and partition_desc in (%s) " +
                        "group by table_id,partition_desc) t " +
                        "left join partition_info m on t.table_id = m.table_id and t.partition_desc = m.partition_desc and t.max = m.version",
                descPlaceholders);
        List<PartitionInfo> rsList = new ArrayList<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableId);
            int index = 2;
            if (partitionDescList.isEmpty()) {
                pstmt.setString(index, "''");
            } else {
                for (String partition : partitionDescList) {
                    pstmt.setString(index++, partition);
                }
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setTableId(rs.getString("table_id"));
                partitionInfo.setPartitionDesc(rs.getString("partition_desc"));
                partitionInfo.setVersion(rs.getInt("version"));
                partitionInfo.setCommitOp(rs.getString("commit_op"));
                partitionInfo.setDomain(rs.getString("domain"));
                Array snapshotArray = rs.getArray("snapshot");
                List<UUID> uuidList = new ArrayList<>();
                Collections.addAll(uuidList, (UUID[]) snapshotArray.getArray());
                partitionInfo.setSnapshot(uuidList);
                partitionInfo.setExpression(rs.getString("expression"));
                rsList.add(partitionInfo);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return rsList;
    }

    public PartitionInfo selectLatestPartitionInfo(String tableId, String partitionDesc) {
        String sql = String.format(
                "select m.table_id, t.partition_desc, m.version, m.commit_op, m.snapshot, m.expression, m.domain from (" +
                        "select table_id,partition_desc,max(version) from partition_info " +
                        "where table_id = '%s' and partition_desc = '%s' " + "group by table_id,partition_desc) t " +
                        "left join partition_info m on t.table_id = m.table_id and t.partition_desc = m" +
                        ".partition_desc and t.max = m.version",
                tableId, partitionDesc);
        return getPartitionInfo(sql);
    }

    public long getLastedTimestamp(String tableId, String partitionDesc) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql;
        if (null == partitionDesc || "".equals(partitionDesc)) {
            sql = String.format("select max(timestamp) as timestamp from partition_info where table_id = '%s'",
                    tableId);
        } else {
            sql = String.format(
                    "select max(timestamp) as timestamp from partition_info where table_id = '%s' and partition_desc " +
                            "= '%s'",
                    tableId, partitionDesc);
        }
        long timestamp = -1;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                timestamp = rs.getLong("timestamp");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return timestamp;
    }

    public int getLastedVersionUptoTime(String tableId, String partitionDesc, long utcMills) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format(
                "select count(*) as total,max(version) as version from partition_info where table_id = '%s' and " +
                        "partition_desc = '%s' and timestamp <= %d",
                tableId, partitionDesc, utcMills);
        int version = -1;
        int total;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                total = rs.getInt("total");
                if (total == 0) {
                    break;
                } else {
                    version = rs.getInt("version");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return version;
    }

    public long getLastedVersionTimestampUptoTime(String tableId, String partitionDesc, long utcMills) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format(
                "select count(*) as total,max(timestamp) as timestamp from partition_info where table_id = '%s' and " +
                        "partition_desc = '%s' and timestamp < %d",
                tableId, partitionDesc, utcMills);
        long timestamp = 0L;
        int total;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                total = rs.getInt("total");
                if (total == 0) {
                    break;
                } else {
                    timestamp = rs.getLong("timestamp");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return timestamp;
    }

    public List<PartitionInfo> getPartitionVersions(String tableId, String partitionDesc) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<PartitionInfo> rsList = new ArrayList<>();
        String sql =
                String.format("select * from partition_info where table_id = '%s' and partition_desc = '%s'", tableId,
                        partitionDesc);
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
                partitionInfo.setTimestamp(rs.getLong("timestamp"));
                partitionInfo.setDomain(rs.getString("domain"));
                Array snapshotArray = rs.getArray("snapshot");
                List<UUID> uuidList = new ArrayList<>();
                Collections.addAll(uuidList, (UUID[]) snapshotArray.getArray());
                partitionInfo.setSnapshot(uuidList);
                partitionInfo.setExpression(rs.getString("expression"));
                rsList.add(partitionInfo);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
                "select m.table_id, t.partition_desc, m.version, m.commit_op, m.snapshot, m.expression, m.domain from (" +
                        "select table_id,partition_desc,max(version) from partition_info " + "where table_id = '%s' " +
                        "group by table_id,partition_desc) t " +
                        "left join partition_info m on t.table_id = m.table_id and t.partition_desc = m" +
                        ".partition_desc and t.max = m.version",
                tableId);
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
                partitionInfo.setDomain(rs.getString("domain"));
                Array snapshotArray = rs.getArray("snapshot");
                List<UUID> uuidList = new ArrayList<>();
                Collections.addAll(uuidList, (UUID[]) snapshotArray.getArray());
                partitionInfo.setSnapshot(uuidList);
                partitionInfo.setExpression(rs.getString("expression"));
                rsList.add(partitionInfo);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return rsList;
    }

    public PartitionInfo findByKey(String tableId, String partitionDesc, int version) {

        String sql = String.format(
                "select * from partition_info where table_id = '%s' and partition_desc = '%s' and version = %d",
                tableId, partitionDesc, version);
        return getPartitionInfo(sql);
    }

    public List<PartitionInfo> getPartitionsFromVersion(String tableId, String partitionDesc, int startVersion,
                                                        int endVersion) {
        String sql = String.format(
                "select * from partition_info where table_id = '%s' and partition_desc = '%s' and version >= %d and " +
                        "version <= %d",
                tableId, partitionDesc, startVersion, endVersion);
        return getPartitionInfos(sql);
    }

    public List<PartitionInfo> getOnePartition(String tableId, String partitionDesc) {
        String sql =
                String.format("select * from partition_info where table_id = '%s' and partition_desc = '%s' limit 1",
                        tableId, partitionDesc);
        return getPartitionInfos(sql);
    }

    public List<PartitionInfo> getPartitionsFromTimestamp(String tableId, String partitionDesc, long startTimestamp,
                                                          long endTimestamp) {
        String sql = String.format(
                "select * from partition_info where table_id = '%s' and partition_desc = '%s' and timestamp >= %d and" +
                        " timestamp < %d",
                tableId, partitionDesc, startTimestamp, endTimestamp);
        return getPartitionInfos(sql);
    }

    public List<String> getAllPartitionDescByTableId(String tableId) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<String> rsList = new ArrayList<>();
        String sql = "select different(partition_desc) from partition_info";
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                rsList.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return rsList;
    }

    public Set<String> getCommitOpsBetweenVersions(String tableId, String partitionDesc, int firstVersion,
                                                   int secondVersion) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Set<String> commitOps = new HashSet<>();
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("select distinct(commit_op) from partition_info where table_id = ? and " +
                    "partition_desc = ? and version between ? and ?");
            pstmt.setString(1, tableId);
            pstmt.setString(2, partitionDesc);
            pstmt.setInt(3, firstVersion);
            pstmt.setInt(4, secondVersion);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                commitOps.add(rs.getString("commit_op"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return commitOps;
    }

    private List<PartitionInfo> getPartitionInfos(String sql) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        PartitionInfo partitionInfo;
        List<PartitionInfo> partitions = new ArrayList<>();
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
                partitionInfo.setTimestamp(rs.getLong("timestamp"));
                partitionInfo.setDomain(rs.getString("domain"));
                Array snapshotArray = rs.getArray("snapshot");
                List<UUID> uuidList = new ArrayList<>();
                Collections.addAll(uuidList, (UUID[]) snapshotArray.getArray());
                partitionInfo.setSnapshot(uuidList);
                partitionInfo.setExpression(rs.getString("expression"));
                partitions.add(partitionInfo);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return partitions;
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
                partitionInfo.setDomain(rs.getString("domain"));
                Array snapshotArray = rs.getArray("snapshot");
                List<UUID> uuidList = new ArrayList<>();
                Collections.addAll(uuidList, (UUID[]) snapshotArray.getArray());
                partitionInfo.setSnapshot(uuidList);
                partitionInfo.setExpression(rs.getString("expression"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return partitionInfo;
    }

    public void clean() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "delete from partition_info;";
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
