package org.apache.flink.lakesoul.entry.clean;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.util.*;

public class CleanUtils {

    private static final Logger logger = LoggerFactory.getLogger(CleanUtils.class);

    public void cleanPartitionInfo(String table_id, String partition_desc, int version, Connection connection) throws SQLException {
        String sql = "DELETE FROM partition_info where table_id= '" + table_id +
                "' and partition_desc ='" + partition_desc + "' and version = '" + version + "'";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.executeUpdate();
            logger.info(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("删除partition_info数据异常");
        }
    }

    /**
     * 使用 Flink FileSystem API 删除文件列表
     * 支持 hdfs://, s3://, s3a://, file:// 和本地路径
     */
    public void deleteFile(List<String> filePathList) throws SQLException {
        for (String filePath : filePathList) {
            deleteFile(filePath);
        }
    }

    /**
     * 使用 Flink FileSystem API 删除单个文件
     * 支持 hdfs://, s3://, s3a://, file:// 和本地路径
     */
    public void deleteFile(String filePath) throws SQLException {
        try {
            // 将路径转换为 URI
            URI fileUri;
            if (filePath.startsWith("hdfs://") || filePath.startsWith("s3://") ||
                    filePath.startsWith("s3a://") || filePath.startsWith("file://")) {
                fileUri = URI.create(filePath);
            } else {
                // 本地路径，转换为 file:// URI
                fileUri = new java.io.File(filePath).toURI();
            }

            Path path = new Path(fileUri);
            FileSystem fs = FileSystem.get(fileUri);

            try {
                if (fs.exists(path)) {
                    // false 表示不递归删除（只删除文件，不删除目录）
                    boolean deleted = fs.delete(path, false);
                    if (deleted) {
                        logger.info("文件已删除: {}", filePath);
                        // 尝试删除空的父目录
                        deleteEmptyParentDirectories(fs, path.getParent());
                    } else {
                        logger.warn("文件删除失败: {}", filePath);
                    }
                } else {
                    logger.info("文件不存在: {}", filePath);
                }
            } finally {
                // Flink FileSystem 不需要显式关闭，但为了安全起见，如果实现了 Closeable 则关闭
                if (fs instanceof java.io.Closeable) {
                    try {
                        ((java.io.Closeable) fs).close();
                    } catch (IOException e) {
                        logger.warn("关闭文件系统失败", e);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("删除文件失败: {}", filePath, e);
            throw new SQLException("删除文件失败: " + filePath, e);
        } catch (Exception e) {
            logger.error("删除文件时发生异常: {}", filePath, e);
            throw new SQLException("删除文件时发生异常: " + filePath, e);
        }
    }

    public boolean getCompactVersion(String tableId, String partitionDesc, long version, Connection connection) throws SQLException {

        String snapshotSql = "SELECT snapshot FROM partition_info " +
                "WHERE table_id = ? AND partition_desc = ? AND version = ?";

        List<UUID> snapshotCommitIds = new ArrayList<>();

        try (PreparedStatement ps = connection.prepareStatement(snapshotSql)) {
            ps.setString(1, tableId);
            ps.setString(2, partitionDesc);
            ps.setLong(3, version);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Array snapshotArray = rs.getArray("snapshot");
                    if (snapshotArray != null) {
                        UUID[] snapshot = (UUID[]) snapshotArray.getArray();
                        snapshotCommitIds.addAll(Arrays.asList(snapshot));
                    }
                }
            }
        }

        String fileSql = "SELECT unnest(file_ops) AS op FROM data_commit_info WHERE commit_id = ANY(?)";

        try (PreparedStatement ps = connection.prepareStatement(fileSql)) {
            Array uuidArray = connection.createArrayOf("uuid", snapshotCommitIds.toArray());
            ps.setArray(1, uuidArray);

            try (ResultSet rs = ps.executeQuery()) {
                boolean allCompact = true; // 假设全部都包含 "compact_"
                while (rs.next()){
                    Object op = rs.getObject("op");
                    String path = op.toString();
                    logger.info("当前压缩的文件目录：" + path);
                    logger.info("oldCompaction: " + path.contains("compact_"));
                    if (!path.contains("compact_")) {
                        allCompact = false;
                        break;
                    }
                }
                return allCompact;
            }
        }
    }

    public void deleteFileAndDataCommitInfo(List<String> snapshot, String tableId, String partitionDesc, Connection connection, Boolean oldCompaction) {
        snapshot.forEach(commitId -> {
                    if (oldCompaction) {
                        String sql = "SELECT \n" +
                                "    dci.table_id, \n" +
                                "    dci.partition_desc, \n" +
                                "    dci.commit_id, \n" +
                                "    file_op.path \n" +
                                "FROM \n" +
                                "    data_commit_info dci, \n" +
                                "    unnest(dci.file_ops) AS file_op \n" +
                                "WHERE \n" +
                                "    dci.table_id = '" + tableId + "' \n" +
                                "    AND dci.partition_desc = '" + partitionDesc + "' \n" +
                                "    AND dci.commit_id = '" + commitId + "'";
                        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                            // 执行删除操作
                            ResultSet pathSet = preparedStatement.executeQuery();
                            logger.info(sql);
                            List<String> oldCompactionFileList = new ArrayList<>();
                            while (pathSet.next()) {
                                String path = pathSet.getString("path");
                                oldCompactionFileList.add(path);
                            }
                            if (!oldCompactionFileList.isEmpty()){
                                deleteFile(oldCompactionFileList);
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();

                        }
                    }
                    String deleteDataCommitInfoSql = "DELETE FROM data_commit_info \n" +
                            "WHERE table_id = '" + tableId + "' \n" +
                            "AND commit_id = '" + commitId + "' \n" +
                            "AND partition_desc = '" + partitionDesc + "'";
                    try (PreparedStatement preparedStatement = connection.prepareStatement(deleteDataCommitInfoSql)) {
                        logger.info(deleteDataCommitInfoSql);
                        preparedStatement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    /**
     * 使用 Flink FileSystem API 删除空的父目录
     */
    private void deleteEmptyParentDirectories(FileSystem fs, Path directory) {
        if (directory == null) {
            return;
        }
        try {
            // 检查目录是否存在
            if (!fs.exists(directory)) {
                return;
            }

            // 获取目录状态
            org.apache.flink.core.fs.FileStatus[] statuses = fs.listStatus(directory);
            // 如果目录为空，删除它并递归删除父目录
            if (statuses == null || statuses.length == 0) {
                boolean deleted = fs.delete(directory, false);
                if (deleted) {
                    logger.debug("删除空目录: {}", directory);
                    // 递归删除父目录
                    Path parent = directory.getParent();
                    if (parent != null && !parent.equals(directory)) {
                        deleteEmptyParentDirectories(fs, parent);
                    }
                }
            }
        } catch (IOException e) {
            // 删除空目录失败不影响主流程，只记录日志
            logger.debug("删除空目录失败: {}", directory, e);
        }
    }

    public List<String> getTableIdByTableName(String tableNames, Connection connection) throws SQLException {
        if (tableNames == null) {
            return null;
        }
        List<String> tableList = new ArrayList<>();
        for (String table : tableNames.split(",")) {
            String dbName = table.split("\\.")[0];
            String tableName = table.split("\\.")[1];
            String sql = "select table_id from table_name_id where table_name = ? and table_namespace = ?";
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1,tableName);
                preparedStatement.setString(2, dbName);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()){
                        String tableId = resultSet.getString("table_id");
                        tableList.add(tableId);
                    }
                }
            }
        }
        // 注意：不再关闭 connection，由调用者管理连接生命周期
        return tableList;
    }

    /**
     * 获取分区的最后更新时间（最大timestamp）
     */
    public Long getPartitionLastUpdateTime(String tableId, String partitionDesc, Connection connection) throws SQLException {
        String sql = "SELECT MAX(timestamp) as max_timestamp FROM partition_info " +
                "WHERE table_id = ? AND partition_desc = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableId);
            ps.setString(2, partitionDesc);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Long maxTimestamp = rs.getLong("max_timestamp");
                    if (rs.wasNull()) {
                        return null;
                    }
                    return maxTimestamp;
                }
            }
        }
        return null;
    }

    /**
     * 获取分区所有版本的snapshot列表
     */
    public List<String> getAllPartitionSnapshots(String tableId, String partitionDesc, Connection connection) throws SQLException {
        List<String> allSnapshots = new ArrayList<>();
        String sql = "SELECT snapshot FROM partition_info " +
                "WHERE table_id = ? AND partition_desc = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableId);
            ps.setString(2, partitionDesc);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Array snapshotArray = rs.getArray("snapshot");
                    if (snapshotArray != null) {
                        UUID[] snapshot = (UUID[]) snapshotArray.getArray();
                        for (UUID uuid : snapshot) {
                            allSnapshots.add(uuid.toString());
                        }
                    }
                }
            }
        }
        return allSnapshots;
    }

    /**
     * 删除整个分区的所有数据
     * 包括：数据文件、data_commit_info记录、partition_info记录
     */
    public void deleteEntirePartition(String tableId, String partitionDesc, Connection connection) throws SQLException {
        logger.info("开始删除分区所有数据: tableId={}, partitionDesc={}", tableId, partitionDesc);

        // 1. 获取该分区所有版本的snapshot
        List<String> allSnapshots = getAllPartitionSnapshots(tableId, partitionDesc, connection);
        logger.info("分区 {} 共有 {} 个snapshot需要处理", partitionDesc, allSnapshots.size());

        // 2. 删除所有相关的数据文件和data_commit_info记录
        Set<String> processedCommitIds = new HashSet<>();
        List<String> allFilePaths = new ArrayList<>();

        for (String commitId : allSnapshots) {
            if (processedCommitIds.contains(commitId)) {
                continue;
            }
            processedCommitIds.add(commitId);

            // 获取该commit_id的所有文件路径
            String fileSql = "SELECT \n" +
                    "    dci.table_id, \n" +
                    "    dci.partition_desc, \n" +
                    "    dci.commit_id, \n" +
                    "    file_op.path \n" +
                    "FROM \n" +
                    "    data_commit_info dci, \n" +
                    "    unnest(dci.file_ops) AS file_op \n" +
                    "WHERE \n" +
                    "    dci.table_id = ? \n" +
                    "    AND dci.partition_desc = ? \n" +
                    "    AND dci.commit_id = ?::uuid";

            try (PreparedStatement ps = connection.prepareStatement(fileSql)) {
                ps.setString(1, tableId);
                ps.setString(2, partitionDesc);
                ps.setString(3, commitId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String path = rs.getString("path");
                        if (path != null) {
                            allFilePaths.add(path);
                        }
                    }
                }
            }

            // 删除data_commit_info记录
            String deleteDataCommitInfoSql = "DELETE FROM data_commit_info \n" +
                    "WHERE table_id = ? \n" +
                    "AND commit_id = ?::uuid \n" +
                    "AND partition_desc = ?";
            try (PreparedStatement ps = connection.prepareStatement(deleteDataCommitInfoSql)) {
                ps.setString(1, tableId);
                ps.setString(2, commitId);
                ps.setString(3, partitionDesc);
                int deleted = ps.executeUpdate();
                logger.info("删除data_commit_info记录: commitId={}, 删除{}条", commitId, deleted);
            }
        }

        // 3. 删除所有数据文件
        if (!allFilePaths.isEmpty()) {
            logger.info("开始删除分区 {} 的 {} 个文件", partitionDesc, allFilePaths.size());
            deleteFile(allFilePaths);
        }

        // 4. 删除partition_info中该分区的所有记录
        String deletePartitionInfoSql = "DELETE FROM partition_info " +
                "WHERE table_id = ? AND partition_desc = ?";
        try (PreparedStatement ps = connection.prepareStatement(deletePartitionInfoSql)) {
            ps.setString(1, tableId);
            ps.setString(2, partitionDesc);
            int deleted = ps.executeUpdate();
            logger.info("删除partition_info记录: tableId={}, partitionDesc={}, 删除{}条",
                    tableId, partitionDesc, deleted);
        }

        logger.info("完成删除分区所有数据: tableId={}, partitionDesc={}", tableId, partitionDesc);
    }

    /**
     * 获取所有需要检查的分区列表（table_id, partition_desc, max_timestamp）
     */
    public List<PartitionExpiredInfo> getAllPartitionsForExpiredCheck(List<String> targetTableIds, Connection connection) throws SQLException {
        List<PartitionExpiredInfo> partitions = new ArrayList<>();

        String sql;
        PreparedStatement ps;

        if (targetTableIds != null && !targetTableIds.isEmpty()) {
            // 如果指定了目标表，只检查这些表
            String placeholders = String.join(",", Collections.nCopies(targetTableIds.size(), "?"));
            sql = "SELECT table_id, partition_desc, MAX(timestamp) as max_timestamp " +
                    "FROM partition_info " +
                    "WHERE table_id IN (" + placeholders + ") " +
                    "GROUP BY table_id, partition_desc";
            ps = connection.prepareStatement(sql);
            for (int i = 0; i < targetTableIds.size(); i++) {
                ps.setString(i + 1, targetTableIds.get(i));
            }
        } else {
            // 检查所有表
            sql = "SELECT table_id, partition_desc, MAX(timestamp) as max_timestamp " +
                    "FROM partition_info " +
                    "GROUP BY table_id, partition_desc";
            ps = connection.prepareStatement(sql);
        }

        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String tableId = rs.getString("table_id");
                String partitionDesc = rs.getString("partition_desc");
                long maxTimestamp = rs.getLong("max_timestamp");
                partitions.add(new PartitionExpiredInfo(tableId, partitionDesc, maxTimestamp));
            }
        }

        return partitions;
    }

    /**
     * 分区过期信息类
     */
    public static class PartitionExpiredInfo {
        public final String tableId;
        public final String partitionDesc;
        public final long maxTimestamp;

        public PartitionExpiredInfo(String tableId, String partitionDesc, long maxTimestamp) {
            this.tableId = tableId;
            this.partitionDesc = partitionDesc;
            this.maxTimestamp = maxTimestamp;
        }

        @Override
        public String toString() {
            return "PartitionExpiredInfo{" +
                    "tableId='" + tableId + '\'' +
                    ", partitionDesc='" + partitionDesc + '\'' +
                    ", maxTimestamp=" + maxTimestamp +
                    '}';
        }
    }

}
