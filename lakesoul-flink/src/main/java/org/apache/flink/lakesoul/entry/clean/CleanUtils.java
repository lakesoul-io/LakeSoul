// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
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

    public void deleteFile(List<String> filePathList) throws SQLException {
        for (String filePath : filePathList) {
            deleteFile(filePath);
        }
    }

    public void deleteFile(String filePath) throws SQLException {
        try {
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
        return tableList;
    }

}
