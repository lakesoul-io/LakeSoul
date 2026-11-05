// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.DataSource;

/**
 * Deletes discard-compressed file records and the corresponding files on disk.
 *
 * <p>Pending file paths are tracked in Flink keyed state ({@link #pendingDeletes}) instead of an
 * in-memory queue, so they are checkpointed and survive task failure/restart without a background
 * thread touching operator state. A processing-time timer drives the periodic flush on the task
 * thread.
 *
 * <p>Flink only supports timers on keyed streams, so this function is a {@link
 * KeyedProcessFunction} and {@code NewCleanJob} applies it to a stream keyed by a constant key:
 * each subtask holds one pending list and one flush timer.
 */
public class DiscardFileDeleteFunction extends KeyedProcessFunction<String, String, String> {

    private static final Logger log = LoggerFactory.getLogger(DiscardFileDeleteFunction.class);

    private static final String PENDING_DELETES_STATE = "pending-deletes";
    private static final String FLUSH_SCHEDULED_STATE = "flush-scheduled";
    private final int batchSize = 50;
    private final long flushIntervalMs = 10000;

    private final String pgUrl;
    private final String pgUserName;
    private final String pgPasswd;

    private transient DataSource dataSource;
    private transient ExecutorService asyncExecutor;

    /** Checkpointed list of file paths that still need to be deleted. */
    private transient ListState<String> pendingDeletes;

    /** Whether a flush timer is already registered, so one flush is not scheduled per record. */
    private transient ValueState<Boolean> flushScheduled;

    public DiscardFileDeleteFunction(String pgUrl, String pgUserName, String pgPasswd) {
        this.pgUrl = pgUrl;
        this.pgUserName = pgUserName;
        this.pgPasswd = pgPasswd;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        pendingDeletes =
                getRuntimeContext()
                        .getListState(
                                new ListStateDescriptor<>(PENDING_DELETES_STATE, String.class));
        flushScheduled =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>(FLUSH_SCHEDULED_STATE, Boolean.class));

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
    }

    @Override
    public void processElement(
            String filePath,
            KeyedProcessFunction<String, String, String>.Context ctx,
            Collector<String> out)
            throws Exception {
        pendingDeletes.add(filePath);
        if (flushScheduled.value() == null) {
            scheduleFlush(ctx);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
            throws Exception {
        flushScheduled.clear();
        List<String> remaining = flushBatchDeletes();
        // Failed batches stay in state; retry on the next timer instead of waiting for new
        // records.
        if (!remaining.isEmpty()) {
            scheduleFlush(ctx);
        }
    }

    private void scheduleFlush(KeyedProcessFunction<String, String, String>.Context ctx)
            throws Exception {
        flushScheduled.update(true);
        long fireTime = ctx.timerService().currentProcessingTime() + flushIntervalMs;
        ctx.timerService().registerProcessingTimeTimer(fireTime);
    }

    /**
     * Deletes all pending paths and returns the paths that could not be deleted, so they can be
     * retried.
     */
    private List<String> flushBatchDeletes() throws Exception {
        // Snapshot the pending paths out of state before deleting, so the async deletes do not
        // race with subsequent checkpointing.
        List<String> pending = new ArrayList<>();
        for (String path : pendingDeletes.get()) {
            pending.add(path);
        }
        if (pending.isEmpty()) {
            return pending;
        }

        log.info("flush batch deletes, pending {}", pending.size());

        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < pending.size(); i += batchSize) {
            batches.add(
                    new ArrayList<>(pending.subList(i, Math.min(i + batchSize, pending.size()))));
        }

        // Delete batches in parallel, collecting futures so we can wait for the results.
        List<Future<Boolean>> futures = new ArrayList<>();
        for (List<String> batch : batches) {
            futures.add(asyncExecutor.submit(() -> deleteBatch(batch)));
        }

        // Wait for all deletions to complete. Only paths whose deletion succeeded are removed
        // from state; failed batches stay in state and are retried on the next flush.
        List<String> remaining = new ArrayList<>();
        for (int i = 0; i < batches.size(); i++) {
            boolean success = false;
            try {
                success = futures.get(i).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("删除批次被中断，保留待重试", e);
            } catch (ExecutionException e) {
                log.error("删除批次执行异常，保留待重试", e);
            }
            if (!success) {
                remaining.addAll(batches.get(i));
            }
        }
        pendingDeletes.update(remaining);
        return remaining;
    }

    private boolean deleteBatch(List<String> batch) {
        log.info("start delete batch {}", batch.size());
        List<String> deletable = batch;
        try (Connection conn = dataSource.getConnection()) {
            List<String> pinned = pinnedPaths(conn, batch);
            if (!pinned.isEmpty()) {
                deletable = new ArrayList<>(batch);
                deletable.removeAll(pinned);
                log.info("skip {} pinned discard files", pinned.size());
            }
            if (deletable.isEmpty()) {
                return true;
            }
            try (PreparedStatement ps =
                    conn.prepareStatement(
                            "DELETE FROM discard_compressed_file_info WHERE file_path ="
                                    + " ANY(?)")) {
                ps.setArray(1, conn.createArrayOf("text", deletable.toArray()));
                int rowsDeleted = ps.executeUpdate();
                log.info("批量删除数据库记录 {} 条", rowsDeleted);
            }
        } catch (SQLException e) {
            log.error("批量删除数据库失败", e);
            return false;
        }

        CleanUtils cleanUtils = new CleanUtils();
        boolean allSucceeded = true;
        for (String path : deletable) {
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

    private List<String> pinnedPaths(Connection conn, List<String> batch) throws SQLException {
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "SELECT p.path FROM unnest(?::text[]) AS p(path)"
                                + " WHERE EXISTS (SELECT 1 FROM data_commit_info dci,"
                                + " unnest(dci.file_ops) AS f WHERE dci.pinned = true"
                                + " AND f.path = p.path)")) {
            ps.setArray(1, conn.createArrayOf("text", batch.toArray()));
            List<String> paths = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    paths.add(rs.getString(1));
                }
            }
            return paths;
        }
    }

    @Override
    public void close() throws Exception {
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
