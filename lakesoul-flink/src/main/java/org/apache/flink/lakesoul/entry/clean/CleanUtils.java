// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CleanUtils {

    private static final Logger logger = LoggerFactory.getLogger(CleanUtils.class);

    public void write(String record) {
        String filePath = "./record.txt";
        try (FileWriter writer = new FileWriter(filePath, true)) {
            writer.write(record + "\n"); // 将内容写入文件
            System.out.println("内容已成功写入文件: " + filePath);
        } catch (IOException e) {
            System.err.println("写入文件时发生错误: " + e.getMessage());
        }
    }

    //实现从pg里删除记录
    public void deleteDataCommitInfo(String table_id, String commit_id, String partition_desc) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/lakesoul_test", "lakesoul_test", "lakesoul_test");
        String sql = "DELETE FROM data_commit_info where table_id= '" + table_id +
                "' and commit_id= '" + commit_id +
                "' and partition_desc ='" + partition_desc + "'";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            // 执行删除操作
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            // 处理SQL异常
            e.printStackTrace();
            logger.info("删除data_commit_info数据异常");
        }
    }

    public boolean partitionExist(String tableId, String partitionDesc, Connection connection) {
        String sql = "SELECT 1 FROM partition_info WHERE table_id = ? AND partition_desc = ? LIMIT 1";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableId);
            ps.setString(2, partitionDesc);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }


    public void deletePartitionInfo(String table_id, String partition_desc, String commit_id) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/lakesoul_test", "lakesoul_test", "lakesoul_test");
        String sql = "DELETE FROM partition_info where table_id= '" + table_id +
                "' and partition_desc ='" + partition_desc + "' and '" + commit_id + "' = ANY(snapshot)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            int rowsDeleted = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("删除partition_info数据异常");
        }
    }

    public void cleanPartitionInfo(String table_id, String partition_desc, int version, Connection connection) throws SQLException {
        UUID id = UUID.randomUUID();
        logger.info("Clean-[{}]: begin", id);
        String sql = "DELETE FROM partition_info where table_id= '" + table_id +
                "' and partition_desc ='" + partition_desc + "' and version = '" + version + "'";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.executeUpdate();
            logger.info(sql);
            logger.info("Clean-[{}]: success",id);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("删除partition_info数据异常");
            logger.info("Clean-[{}]: fail",id);
        }
    }

    private static final String HDFS_URI_PREFIX = "hdfs:/";
    private static final String S3_URI_PREFIX = "s3:/";

    public void deleteFile(List<String> filePathList) throws SQLException {
        UUID id = UUID.randomUUID();
        logger.info("[Clean-{}]: begin",id);
        Configuration hdfsConfig = new Configuration();
        for (String filePath : filePathList) {
            try{
                if (filePath.startsWith(HDFS_URI_PREFIX) || filePath.startsWith(S3_URI_PREFIX)) {
                    deleteHdfsFile(filePath, hdfsConfig);
                } else if (filePath.startsWith("file:/")) {
                        URI uri = new URI(filePath);
                        String actualPath = new File(uri.getPath()).getAbsolutePath();
                        deleteLocalFile(actualPath);
                } else {
                    deleteLocalFile(filePath);
                }
            }
            catch (URISyntaxException e) {
                e.printStackTrace();
                logger.info("无法解析文件URI: {}", filePath);
                logger.info("Clean-[{}]: fail",id);
            }
            catch (IOException e) {
                e.printStackTrace();
                logger.info("Clean-[{}]: fail",id);
            }
        }
        logger.info("Clean-[{}]: success",id);
    }

    private void deleteHdfsFile(String filePath, Configuration hdfsConfig) throws IOException {
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), hdfsConfig);
            Path path = new Path(filePath);
            if (fs.exists(path)) {
                // false 表示不递归删除
                fs.delete(path, false);
                logger.info("=============================HDFS/s3 文件已删除: {}", filePath);
                deleteEmptyParentDirectories(fs, path.getParent());
                fs.close();
            } else {
                logger.info("=============================HDFS/s3 文件不存在: {}", filePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("=============================删除 HDFS/s3 文件失败: {}", filePath);
            throw new IOException(filePath + "fail to delete");
        }
    }

    private void deleteLocalFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            if (file.delete()) {
                logger.info("本地文件已删除：{}", filePath);
                deleteEmptyParentDirectories(file.getParentFile());
            } else {
                logger.info("本地文件删除失败: {}", filePath);
                throw new IOException(file + "fail to delete");
            }
        } else {
            logger.info("=============================本地文件不存在: {}", filePath);
            throw new IOException(file + "not found");
        }
    }


    public boolean getCompactVersion(String tableId, String partitionDesc, int version, Connection connection) throws SQLException {
        String snapshotSql = "SELECT snapshot FROM partition_info " +
                "WHERE table_id = ? AND partition_desc = ? AND version = ?";

        List<UUID> snapshotCommitIds = new ArrayList<>();

        try (PreparedStatement ps = connection.prepareStatement(snapshotSql)) {
            ps.setString(1, tableId);
            ps.setString(2, partitionDesc);
            ps.setInt(3, version);

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
                if (rs.next()){
                    Object op = rs.getObject("op");
                    String path = op.toString();
                    logger.info("当前压缩的文件目录：" + path);
                    logger.info("oldCompaction: " + path.contains("compact_"));
                    return path.contains("compact_");
                }
            }
        }
        return true;
    }


    public void deleteFileAndDataCommitInfo(List<String> snapshot, String tableId, String partitionDesc, Connection connection, Boolean oldCompaction) {
        snapshot.forEach(commitId -> {
                    if (oldCompaction) {
                        logger.info("清理旧版压缩数据");
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
                            // 处理SQL异常
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

    public void cleanDiscardFile(long expiredTime, Connection connection) throws SQLException {
        logger.info("expiredTime: " + expiredTime);
        logger.info("从discard_compressed_file_info表中清理过期数据");
        System.out.println("从discard_compressed_file_info表中清理过期数据");
        long currentTimeMillis = System.currentTimeMillis();
        String querySql = "SELECT file_path FROM discard_compressed_file_info WHERE timestamp < ?";
        String deleteSql = "DELETE FROM discard_compressed_file_info WHERE file_path = ?";
        try (
                PreparedStatement selectStmt = connection.prepareStatement(querySql);
                PreparedStatement deleteStmt = connection.prepareStatement(deleteSql)
        ) {
            selectStmt.setLong(1, currentTimeMillis - expiredTime);
            ResultSet resultSet = selectStmt.executeQuery();

            List<String> pathList = new ArrayList<>();

            while (resultSet.next()) {
                String filePath = resultSet.getString("file_path");
                deleteStmt.setString(1, filePath);
                deleteStmt.executeUpdate();
                pathList.add(filePath);
            }
            deleteFile(pathList);
        }

    }

    private void deleteEmptyParentDirectories(FileSystem fs, Path directory) throws IOException {
        if (directory == null) {
            return;
        }
        // 检查目录是否为空
        if (fs.listStatus(directory).length == 0) {
            fs.delete(directory, false); // 删除空目录
            deleteEmptyParentDirectories(fs, directory.getParent());
        }
    }

    private void deleteEmptyParentDirectories(File directory) {
        if (directory == null) {
            return; // 根目录不需要处理
        }
        // 检查目录是否为空
        if (Objects.requireNonNull(directory.list()).length == 0) {
            if (directory.delete()) {
                deleteEmptyParentDirectories(directory.getParentFile());
            }
        }
    }

    public String[] parseFileOpsString(String fileOPs) {
        String[] fileInfo = new String[2];
        // 正则表达式匹配文件路径和其他信息
        Pattern pattern = Pattern.compile("\\(([^,]+),([^,]+),([^,]+),(\"[^\"]*\"|[^,)]+)\\)");
        Matcher matcher = pattern.matcher(fileOPs);
        if (matcher.find()) {
            String filePath = matcher.group(1);
            fileInfo[0] = filePath; // 文件路径
            fileInfo[1] = matcher.group(4); // 其他信息（如字段列表）
        } else {
            logger.info("=============================未找到匹配的文件路径!");
        }
        return fileInfo;
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
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,tableName);
            preparedStatement.setString(2, dbName);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                String tableId = resultSet.getString("table_id");
                tableList.add(tableId);
            }

        }
        connection.close();
        return tableList;

    }

}

