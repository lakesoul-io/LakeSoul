// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class DiscardFileDeleteFunction extends ProcessFunction<String, String> {
    private final ConcurrentLinkedQueue<String> GLOBAL_PENDING_DELETES = new ConcurrentLinkedQueue<>();
    private ExecutorService asyncExecutor;
    private ScheduledExecutorService scheduler;
    private final int batchSize = 500;
    private final long flushIntervalMs = 5000;

    private final String pgUrl;
    private final String pgUserName;
    private final String pgPasswd;
    private transient DataSource dataSource;
    private static final Logger log = LoggerFactory.getLogger(DiscardFileDeleteFunction.class);

    public DiscardFileDeleteFunction(String pgUrl, String pgUserName, String pgPasswd) {
        this.pgUrl = pgUrl;
        this.pgUserName = pgUserName;
        this.pgPasswd = pgPasswd;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 HikariCP
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(pgUrl);
        config.setUsername(pgUserName);
        config.setPassword(pgPasswd);
        config.setDriverClassName("org.postgresql.Driver");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(10000);
        config.setIdleTimeout(60000);
        config.setMaxLifetime(300000);
        config.setAutoCommit(true);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        dataSource = new HikariDataSource(config);
        if (asyncExecutor == null) {
            asyncExecutor = Executors.newFixedThreadPool(1);
        }
        if (scheduler == null) {
            scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(this::flushBatchDeletes, 0, flushIntervalMs, TimeUnit.MILLISECONDS);
        }
    }
    @Override
    public void processElement(String filePath, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
        GLOBAL_PENDING_DELETES.add(filePath);
    }
    private void flushBatchDeletes() {
        if (GLOBAL_PENDING_DELETES.isEmpty()) return;
        List<String> batch = new ArrayList<>();
        while (!GLOBAL_PENDING_DELETES.isEmpty() && batch.size() < batchSize) {
            String path = GLOBAL_PENDING_DELETES.poll();
            if (path != null) batch.add(path);
        }

        if (batch.isEmpty()) return;
        asyncExecutor.submit(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "DELETE FROM discard_compressed_file_info WHERE file_path = ANY(?)")) {
                ps.setArray(1, conn.createArrayOf("text", batch.toArray()));
                int rowsDeleted = ps.executeUpdate();
                log.info("批量删除数据库记录 {} 条", rowsDeleted);
            } catch (SQLException e) {
                log.error("批量删除数据库失败", e);
            }
            CleanUtils cleanUtils = new CleanUtils();
            batch.forEach(path -> {
                try {
                    cleanUtils.deleteFile(path);
                } catch (Exception e) {
                    log.error("删除文件失败 [{}]", path, e);
                }
            });
        });

    }
}
