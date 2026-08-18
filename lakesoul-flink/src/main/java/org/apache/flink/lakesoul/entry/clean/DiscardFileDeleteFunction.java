// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

/**
 * Deletes discard-compressed file records and the corresponding files on disk.
 *
 * <p>Pending file paths are buffered in an in-memory {@link ConcurrentLinkedQueue} (fast path), and
 * periodically flushed by a {@link ScheduledExecutorService}. The queue is synced into Flink
 * operator state in {@link #snapshotState} so pending deletes are checkpointed and survive task
 * failure/restart. Only paths whose deletion succeeded are dropped; failed paths are re-queued and
 * retried on the next flush.
 */
public class DiscardFileDeleteFunction extends ProcessFunction<String, String>
        implements CheckpointedFunction {

    private static final Logger log = LoggerFactory.getLogger(DiscardFileDeleteFunction.class);

    private static final String STATE_NAME = "pending-deletes";
    private final int batchSize = 50;
    private final long flushIntervalMs = 10000;

    private final String pgUrl;
    private final String pgUserName;
    private final String pgPasswd;

    private transient DataSource dataSource;
    private transient ExecutorService asyncExecutor;
    private transient ScheduledExecutorService scheduler;

    /** In-memory buffer of file paths waiting to be deleted. Thread-safe, fast path. */
    private final ConcurrentLinkedQueue<String> pendingDeletes = new ConcurrentLinkedQueue<>();

    /** Checkpointed snapshot of {@link #pendingDeletes}, synced in {@link #snapshotState}. */
    private transient ListState<String> checkpointedState;

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

        asyncExecutor = Executors.newFixedThreadPool(8);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(
                this::flushBatchDeletes, 0, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void processElement(
            String filePath, ProcessFunction<String, String>.Context ctx, Collector<String> out)
            throws Exception {
        pendingDeletes.add(filePath);
    }

    private void flushBatchDeletes() {
        log.info("flush batch deletes, pending {}", pendingDeletes.size());
        if (pendingDeletes.isEmpty()) {
            return;
        }

        // Drain the queue into batches.
        List<List<String>> batches = new ArrayList<>();
        while (!pendingDeletes.isEmpty()) {
            List<String> batch = new ArrayList<>();
            while (!pendingDeletes.isEmpty() && batch.size() < batchSize) {
                String path = pendingDeletes.poll();
                if (path != null) {
                    batch.add(path);
                }
            }
            if (!batch.isEmpty()) {
                batches.add(batch);
            }
        }

        // Delete batches in parallel, collecting futures so we can wait for the results.
        List<Future<Boolean>> futures = new ArrayList<>();
        for (List<String> batch : batches) {
            futures.add(asyncExecutor.submit(() -> deleteBatch(batch)));
        }

        // Wait for all deletions to complete. Failed batches are re-queued so they are checkpointed
        // and retried on the next flush; successful batches stay dropped.
        for (int i = 0; i < batches.size(); i++) {
            boolean success = false;
            try {
                success = futures.get(i).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("删除批次被中断，重新入队待重试", e);
            } catch (ExecutionException e) {
                log.error("删除批次执行异常，重新入队待重试", e);
            }
            if (!success) {
                pendingDeletes.addAll(batches.get(i));
            }
        }
    }

    private boolean deleteBatch(List<String> batch) {
        log.info("start delete batch {}", batch.size());
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "DELETE FROM discard_compressed_file_info WHERE file_path ="
                                        + " ANY(?)")) {
            ps.setArray(1, conn.createArrayOf("text", batch.toArray()));
            int rowsDeleted = ps.executeUpdate();
            log.info("批量删除数据库记录 {} 条", rowsDeleted);
        } catch (SQLException e) {
            log.error("批量删除数据库失败", e);
            return false;
        }

        CleanUtils cleanUtils = new CleanUtils();
        boolean allSucceeded = true;
        for (String path : batch) {
            try {
                cleanUtils.deleteFile(path);
            } catch (Exception e) {
                log.error("删除文件失败 [{}]", path, e);
                allSucceeded = false;
            }
        }
        if (allSucceeded) {
            log.info("finish delete batch {}", batch.size());
        }
        return allSucceeded;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        checkpointedState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>(STATE_NAME, String.class));
        if (context.isRestored()) {
            for (String path : checkpointedState.get()) {
                pendingDeletes.add(path);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Sync the in-memory queue into operator state so pending deletes are checkpointed.
        checkpointedState.update(new ArrayList<>(pendingDeletes));
    }

    @Override
    public void close() throws Exception {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        if (asyncExecutor != null) {
            asyncExecutor.shutdownNow();
            asyncExecutor = null;
        }
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
            dataSource = null;
        }
    }
}
