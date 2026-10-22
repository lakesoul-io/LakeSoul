// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class CleanUtils {

    private static final Logger logger = LoggerFactory.getLogger(CleanUtils.class);

    public void cleanPartitionInfo(
            String table_id, String partition_desc, int version, Connection connection)
            throws SQLException {
        String sql =
                "DELETE FROM partition_info where table_id= '"
                        + table_id
                        + "' and partition_desc ='"
                        + partition_desc
                        + "' and version = '"
                        + version
                        + "' and pinned = false";
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
        Path path = new Path(filePath);
        try {
            FileSystem fs = path.getFileSystem();
            if (!fs.exists(path)) {
                logger.info("文件不存在: {}", filePath);
                deleteBlobref(fs, filePath);
                return;
            }
            if (!fs.delete(path, false)) {
                logger.warn("文件删除失败: {}", filePath);
                return;
            }
            logger.info("文件已删除: {}", filePath);
            deleteBlobref(fs, filePath);
        } catch (IOException e) {
            logger.error("删除文件失败: {}", filePath, e);
            throw new SQLException("删除文件失败: " + filePath, e);
        }
    }

    /** Remove the `<data_file>.blobref` sidecar; failures are surfaced so callers can retry. */
    private void deleteBlobref(FileSystem fs, String filePath) throws SQLException {
        Path sidecar = new Path(filePath + ".blobref");
        try {
            if (fs.exists(sidecar) && !fs.delete(sidecar, false)) {
                logger.warn("blobref 删除失败: {}", sidecar);
                throw new SQLException("删除 blobref 失败: " + sidecar);
            }
            logger.info("blobref 已删除: {}", sidecar);
        } catch (IOException e) {
            logger.error("删除 blobref 失败: {}", sidecar, e);
            throw new SQLException("删除 blobref 失败: " + sidecar, e);
        }
    }

    public boolean getCompactVersion(
            String tableId, String partitionDesc, long version, Connection connection)
            throws SQLException {

        String snapshotSql =
                "SELECT snapshot FROM partition_info "
                        + "WHERE table_id = ? AND partition_desc = ? AND version = ?";

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

        String fileSql =
                "SELECT unnest(file_ops) AS op FROM data_commit_info WHERE commit_id = ANY(?)";

        try (PreparedStatement ps = connection.prepareStatement(fileSql)) {
            Array uuidArray = connection.createArrayOf("uuid", snapshotCommitIds.toArray());
            ps.setArray(1, uuidArray);

            try (ResultSet rs = ps.executeQuery()) {
                boolean allCompact = true; // 假设全部都包含 "compact_"
                while (rs.next()) {
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

    /**
     * Deletes files and data_commit_info rows of an old snapshot.
     *
     * <p>An UpdateCommit replaces the partition snapshot but re-adds unchanged files with their
     * original paths (see Spark {@code TransactionCommit.createDataCommitInfo}), and leveled
     * compaction reuses paths as well. A file is therefore only removed when the latest partition
     * version no longer references it.
     *
     * @param oldCompaction whether files may be deleted; false deletes metadata only
     */
    public void deleteFileAndDataCommitInfo(
            List<String> snapshot,
            String tableId,
            String partitionDesc,
            Connection connection,
            Boolean oldCompaction) {
        Set<String> livePaths = null;
        if (oldCompaction) {
            try {
                if (hasMissingLiveCommit(connection, tableId, partitionDesc)) {
                    logger.error(
                            "{}/{} latest snapshot has missing data_commit_info, skip file"
                                    + " deletion",
                            tableId,
                            partitionDesc);
                } else {
                    livePaths = getLatestSnapshotPaths(connection, tableId, partitionDesc);
                    if (livePaths.isEmpty()) {
                        logger.warn(
                                "no live file path resolved for {}/{}, skip file deletion",
                                tableId,
                                partitionDesc);
                        livePaths = null;
                    }
                }
            } catch (SQLException e) {
                logger.error(
                        "failed to resolve live files for {}/{}, skip file deletion",
                        tableId,
                        partitionDesc,
                        e);
            }
        }

        for (String commitId : snapshot) {
            if (livePaths != null) {
                List<String> commitPaths;
                try {
                    commitPaths = getCommitFilePaths(connection, tableId, partitionDesc, commitId);
                } catch (SQLException e) {
                    logger.error(
                            "failed to query files of commit {}, keep data_commit_info for retry",
                            commitId,
                            e);
                    continue;
                }
                List<String> deletable = new ArrayList<>();
                int protectedCount = 0;
                for (String path : commitPaths) {
                    if (livePaths.contains(path)) {
                        protectedCount++;
                    } else {
                        deletable.add(path);
                    }
                }
                if (protectedCount > 0) {
                    logger.warn(
                            "skip {} files of commit {} still referenced by the latest snapshot of"
                                    + " {}/{}",
                            protectedCount,
                            commitId,
                            tableId,
                            partitionDesc);
                }
                if (!deletable.isEmpty()) {
                    try {
                        deleteFile(deletable);
                    } catch (SQLException e) {
                        logger.error(
                                "failed to delete files of commit {}, keep data_commit_info for"
                                        + " retry",
                                commitId,
                                e);
                        continue;
                    }
                }
            }

            deleteDataCommitInfo(connection, tableId, partitionDesc, commitId);
        }
    }

    private void deleteDataCommitInfo(
            Connection connection, String tableId, String partitionDesc, String commitId) {
        String deleteDataCommitInfoSql =
                "DELETE FROM data_commit_info \n"
                        + "WHERE table_id = '"
                        + tableId
                        + "' \n"
                        + "AND commit_id = '"
                        + commitId
                        + "' \n"
                        + "AND partition_desc = '"
                        + partitionDesc
                        + "' \n"
                        + "AND pinned = false";
        try (PreparedStatement preparedStatement =
                connection.prepareStatement(deleteDataCommitInfoSql)) {
            logger.info(deleteDataCommitInfoSql);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * All add paths referenced by the latest partition version. The union of adds is a conservative
     * check: leaking a file is acceptable, deleting a referenced one is not.
     */
    private Set<String> getLatestSnapshotPaths(
            Connection connection, String tableId, String partitionDesc) throws SQLException {
        String sql =
                "SELECT DISTINCT file_op.path \n"
                        + "FROM partition_info p \n"
                        + "JOIN data_commit_info d \n"
                        + "  ON d.table_id = p.table_id \n"
                        + " AND d.partition_desc = p.partition_desc \n"
                        + " AND d.commit_id = ANY(p.snapshot) \n"
                        + "CROSS JOIN LATERAL unnest(d.file_ops) AS file_op \n"
                        + "WHERE p.table_id = ? \n"
                        + "  AND p.partition_desc = ? \n"
                        + "  AND p.version = (SELECT max(version) FROM partition_info \n"
                        + "                   WHERE table_id = ? AND partition_desc = ?) \n"
                        + "  AND file_op.file_op = 'add'";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableId);
            ps.setString(2, partitionDesc);
            ps.setString(3, tableId);
            ps.setString(4, partitionDesc);
            try (ResultSet rs = ps.executeQuery()) {
                Set<String> paths = new HashSet<>();
                while (rs.next()) {
                    paths.add(rs.getString("path"));
                }
                return paths;
            }
        }
    }

    /**
     * Whether a commit of the latest snapshot has no data_commit_info row. Without the full live
     * set file deletion is unsafe.
     */
    private boolean hasMissingLiveCommit(
            Connection connection, String tableId, String partitionDesc) throws SQLException {
        String sql =
                "SELECT count(*) \n"
                        + "FROM partition_info p, unnest(p.snapshot) c \n"
                        + "WHERE p.table_id = ? \n"
                        + "  AND p.partition_desc = ? \n"
                        + "  AND p.version = (SELECT max(version) FROM partition_info \n"
                        + "                   WHERE table_id = ? AND partition_desc = ?) \n"
                        + "  AND NOT EXISTS (SELECT 1 FROM data_commit_info d \n"
                        + "                  WHERE d.table_id = p.table_id \n"
                        + "                    AND d.partition_desc = p.partition_desc \n"
                        + "                    AND d.commit_id = c)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableId);
            ps.setString(2, partitionDesc);
            ps.setString(3, tableId);
            ps.setString(4, partitionDesc);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1) > 0;
            }
        }
    }

    private List<String> getCommitFilePaths(
            Connection connection, String tableId, String partitionDesc, String commitId)
            throws SQLException {
        String sql =
                "SELECT file_op.path \n"
                        + "FROM data_commit_info dci, unnest(dci.file_ops) AS file_op \n"
                        + "WHERE dci.table_id = ? \n"
                        + "  AND dci.partition_desc = ? \n"
                        + "  AND dci.commit_id = ? \n"
                        + "  AND dci.pinned = false";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableId);
            ps.setString(2, partitionDesc);
            ps.setObject(3, UUID.fromString(commitId));
            try (ResultSet rs = ps.executeQuery()) {
                List<String> paths = new ArrayList<>();
                while (rs.next()) {
                    paths.add(rs.getString("path"));
                }
                return paths;
            }
        }
    }

    /** 使用 Flink FileSystem API 删除空的父目录 */
    private void deleteEmptyParentDirs(FileSystem fs, Path dir) {
        try {
            while (dir != null && fs.exists(dir)) {
                FileStatus[] statuses = fs.listStatus(dir);
                if (statuses != null && statuses.length > 0) {
                    return;
                }

                if (!fs.delete(dir, false)) {
                    return;
                }

                logger.debug("删除空目录: {}", dir);
                dir = dir.getParent();
            }
        } catch (IOException e) {
            logger.debug("删除空目录失败: {}", dir, e);
        }
    }

    public List<String> getTableIdByTableName(String tableNames, Connection connection)
            throws SQLException {
        if (tableNames == null) {
            return null;
        }
        List<String> tableList = new ArrayList<>();
        for (String table : tableNames.split(",")) {
            String dbName = table.split("\\.")[0];
            String tableName = table.split("\\.")[1];
            String sql =
                    "select table_id from table_name_id where table_name = ? and table_namespace ="
                            + " ?";
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, dbName);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String tableId = resultSet.getString("table_id");
                        tableList.add(tableId);
                    }
                }
            }
        }
        return tableList;
    }
}
