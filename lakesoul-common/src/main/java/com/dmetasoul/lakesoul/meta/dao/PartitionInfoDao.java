// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.dao;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.*;
import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class PartitionInfoDao {

    public void insert(PartitionInfo partitionInfo) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.insert(
                    NativeUtils.CodedDaoType.InsertPartitionInfo,
                    JniWrapper.newBuilder().addPartitionInfo(partitionInfo).build());
            return;
        }
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

    public boolean transactionInsert(List<PartitionInfo> partitionInfoList, List<String> snapshotList) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            if (partitionInfoList.isEmpty()) return true;
            Integer count = NativeMetadataJavaClient.insert(
                    NativeUtils.CodedDaoType.TransactionInsertPartitionInfo,
                    JniWrapper.newBuilder().addAllPartitionInfo(partitionInfoList).build());
            return count > 0;
        }
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
            for (String uuid : snapshotList) {
                pstmt.setString(1, uuid);
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
        Array array = conn.createArrayOf("UUID", partitionInfo.getSnapshotList().stream().map(DBUtil::toJavaUUID).toArray());
        pstmt.setString(1, partitionInfo.getTableId());
        pstmt.setString(2, partitionInfo.getPartitionDesc());
        pstmt.setInt(3, partitionInfo.getVersion());
        pstmt.setString(4, partitionInfo.getCommitOp().name());
        pstmt.setArray(5, array);
        pstmt.setString(6, partitionInfo.getExpression());
        pstmt.setString(7, partitionInfo.getDomain());
        pstmt.execute();
    }

    public void deleteByTableIdAndPartitionDesc(String tableId, String partitionDesc) {
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeletePartitionInfoByTableIdAndPartitionDesc,
                    Arrays.asList(tableId, partitionDesc));
            return;
        }
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
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(NativeUtils.CodedDaoType.DeletePartitionInfoByTableId, Collections.singletonList(tableId));
            return;
        }
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
        if (NativeUtils.NATIVE_METADATA_UPDATE_ENABLED) {
            Integer count = NativeMetadataJavaClient.update(
                    NativeUtils.CodedDaoType.DeletePreviousVersionPartition,
                    Arrays.asList(tableId, partitionDesc, Long.toString(utcMills)));
            return;
        }
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
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            if (partitionDescList.isEmpty()) return Collections.emptyList();
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListPartitionDescByTableIdAndParList,
                    Arrays.asList(tableId,
                            String.join(NativeUtils.PARTITION_DESC_DELIM, partitionDescList)));
            if (jniWrapper == null) return null;
            return jniWrapper.getPartitionInfoList();
        }
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
                rsList.add(partitionInfoFromResultSetWithoutTimestamp(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return rsList;
    }

    public PartitionInfo selectLatestPartitionInfo(String tableId, String partitionDesc) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectOnePartitionVersionByTableIdAndDesc,
                    Arrays.asList(tableId, partitionDesc));
            if (jniWrapper == null) return null;
            List<PartitionInfo> partitionInfoList = jniWrapper.getPartitionInfoList();
            return partitionInfoList.isEmpty() ? null : partitionInfoList.get(0);
        }
        String sql = String.format(
                "select m.table_id, t.partition_desc, m.version, m.commit_op, m.snapshot, m.expression, m.domain from (" +
                        "select table_id,partition_desc,max(version) from partition_info " +
                        "where table_id = '%s' and partition_desc = '%s' " + "group by table_id,partition_desc) t " +
                        "left join partition_info m on t.table_id = m.table_id and t.partition_desc = m" +
                        ".partition_desc and t.max = m.version",
                tableId, partitionDesc);
        return getPartitionInfo(sql);
    }

    public long getLatestTimestamp(String tableId, String partitionDesc) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            if (null == partitionDesc || "".equals(partitionDesc)) {
                List<String> result = NativeMetadataJavaClient.queryScalar(
                        NativeUtils.CodedDaoType.GetLatestTimestampFromPartitionInfoWithoutPartitionDesc,
                        Collections.singletonList(tableId));
                if (result == null || result.isEmpty()) return -1;
                return Long.parseLong(result.get(0));
            } else {
                List<String> result = NativeMetadataJavaClient.queryScalar(
                        NativeUtils.CodedDaoType.GetLatestTimestampFromPartitionInfo,
                        Arrays.asList(tableId, partitionDesc));
                if (result == null || result.isEmpty()) return -1;
                return Long.parseLong(result.get(0));
            }
        }
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
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            List<String> result = NativeMetadataJavaClient.queryScalar(
                    NativeUtils.CodedDaoType.GetLatestVersionUpToTimeFromPartitionInfo,
                    Arrays.asList(tableId, partitionDesc, Long.toString(utcMills)));
            if (result == null || result.isEmpty()) return -1;
            return Integer.parseInt(result.get(0));
        }
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
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            List<String> result = NativeMetadataJavaClient.queryScalar(
                    NativeUtils.CodedDaoType.GetLatestVersionTimestampUpToTimeFromPartitionInfo,
                    Arrays.asList(tableId, partitionDesc, Long.toString(utcMills)));
            if (result == null || result.isEmpty()) return 0;
            return Long.parseLong(result.get(0));
        }
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
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListPartitionByTableIdAndDesc,
                    Arrays.asList(tableId, partitionDesc));
            if (jniWrapper == null) return null;
            return jniWrapper.getPartitionInfoList();
        }
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
                rsList.add(partitionInfoFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return rsList;
    }

    public List<PartitionInfo> getPartitionDescByTableId(String tableId) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListPartitionByTableId,
                    Collections.singletonList(tableId));
            if (jniWrapper == null) return null;
            return jniWrapper.getPartitionInfoList();
        }
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
                rsList.add(partitionInfoFromResultSetWithoutTimestamp(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(rs, pstmt, conn);
        }
        return rsList;
    }

    public PartitionInfo findByKey(String tableId, String partitionDesc, int version) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectPartitionVersionByTableIdAndDescAndVersion,
                    Arrays.asList(tableId, partitionDesc, Integer.toString(version)));
            if (jniWrapper == null) return null;
            List<PartitionInfo> partitionInfoList = jniWrapper.getPartitionInfoList();
            return partitionInfoList.isEmpty() ? null : partitionInfoList.get(0);
        }
        String sql = String.format(
                "select * from partition_info where table_id = '%s' and partition_desc = '%s' and version = %d",
                tableId, partitionDesc, version);
        return getPartitionInfo(sql);
    }

    public List<PartitionInfo> getPartitionsFromVersion(String tableId, String partitionDesc, int startVersion,
                                                        int endVersion) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListPartitionVersionByTableIdAndPartitionDescAndVersionRange,
                    Arrays.asList(tableId, partitionDesc, Integer.toString(startVersion), Integer.toString(endVersion)));
            if (jniWrapper == null) return null;
            return jniWrapper.getPartitionInfoList();
        }
        String sql = String.format(
                "select * from partition_info where table_id = '%s' and partition_desc = '%s' and version >= %d and " +
                        "version <= %d",
                tableId, partitionDesc, startVersion, endVersion);
        return getPartitionInfos(sql);
    }

    public List<PartitionInfo> getOnePartition(String tableId, String partitionDesc) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.SelectOnePartitionVersionByTableIdAndDesc,
                    Arrays.asList(tableId, partitionDesc));
            if (jniWrapper == null) return null;
            return jniWrapper.getPartitionInfoList();
        }
        String sql =
                String.format("select * from partition_info where table_id = '%s' and partition_desc = '%s' limit 1",
                        tableId, partitionDesc);
        return getPartitionInfos(sql);
    }

    public List<PartitionInfo> getPartitionsFromTimestamp(String tableId, String partitionDesc, long startTimestamp,
                                                          long endTimestamp) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListPartitionVersionByTableIdAndPartitionDescAndTimestampRange,
                    Arrays.asList(tableId, partitionDesc, Long.toString(startTimestamp), Long.toString(endTimestamp)));
            if (jniWrapper == null) return null;
            return jniWrapper.getPartitionInfoList();
        }
        String sql = String.format(
                "select * from partition_info where table_id = '%s' and partition_desc = '%s' and timestamp >= %d and" +
                        " timestamp < %d",
                tableId, partitionDesc, startTimestamp, endTimestamp);
        return getPartitionInfos(sql);
    }

    public List<String> getAllPartitionDescByTableId(String tableId) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListPartitionByTableId,
                    Collections.singletonList(tableId));
            if (jniWrapper == null) return null;
            return jniWrapper.getPartitionInfoList().stream().map(PartitionInfo::getPartitionDesc).collect(Collectors.toList());
        }
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

    public Set<CommitOp> getCommitOpsBetweenVersions(String tableId, String partitionDesc, int firstVersion,
                                                     int secondVersion) {
        if (NativeUtils.NATIVE_METADATA_QUERY_ENABLED) {
            JniWrapper jniWrapper = NativeMetadataJavaClient.query(
                    NativeUtils.CodedDaoType.ListCommitOpsBetweenVersions,
                    Arrays.asList(tableId, partitionDesc, Integer.toString(firstVersion), Long.toString(secondVersion)));
            if (jniWrapper == null) return null;
            return jniWrapper.getPartitionInfoList().stream().map(PartitionInfo::getCommitOp).collect(Collectors.toSet());
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Set<CommitOp> commitOps = new HashSet<>();
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
                commitOps.add(CommitOp.valueOf(rs.getString("commit_op")));
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
                partitions.add(partitionInfoFromResultSet(rs));
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
                partitionInfo = partitionInfoFromResultSetWithoutTimestamp(rs);
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

    public static PartitionInfo partitionInfoFromResultSet(ResultSet rs) throws SQLException {
        PartitionInfo.Builder partitionInfo = PartitionInfo.newBuilder()
                .setTableId(rs.getString("table_id"))
                .setPartitionDesc(rs.getString("partition_desc"))
                .setVersion(rs.getInt("version"))
                .setCommitOp(CommitOp.valueOf(rs.getString("commit_op")))
                .setDomain(rs.getString("domain"))
                .setTimestamp(rs.getLong("timestamp"));
        Array snapshotArray = rs.getArray("snapshot");
        partitionInfo.addAllSnapshot(Arrays.stream((UUID[]) snapshotArray.getArray()).map(DBUtil::toProtoUuid).collect(Collectors.toList()));
        String expr = rs.getString("expression");
        partitionInfo.setExpression(expr == null ? "" : expr);
        return partitionInfo.build();
    }

    public static PartitionInfo partitionInfoFromResultSetWithoutTimestamp(ResultSet rs) throws SQLException {
        PartitionInfo.Builder partitionInfo = PartitionInfo.newBuilder()
                .setTableId(rs.getString("table_id"))
                .setPartitionDesc(rs.getString("partition_desc"))
                .setVersion(rs.getInt("version"))
                .setCommitOp(CommitOp.valueOf(rs.getString("commit_op")));
        Array snapshotArray = rs.getArray("snapshot");
        partitionInfo.addAllSnapshot(Arrays.stream((UUID[]) snapshotArray.getArray()).map(DBUtil::toProtoUuid).collect(Collectors.toList()));
        String expr = rs.getString("expression");
        partitionInfo.setExpression(expr == null ? "" : expr);
        return partitionInfo.build();
    }
}
