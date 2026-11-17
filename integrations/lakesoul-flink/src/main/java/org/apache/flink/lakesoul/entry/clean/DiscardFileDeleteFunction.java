// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.dmetasoul.lakesoul.meta.DataFileReadLeaseGuard;
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

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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
    private final long dataExpiredTime;

    private transient DataSource dataSource;
    private transient ExecutorService asyncExecutor;

    /** Checkpointed list of file paths that still need to be deleted. */
    private transient ListState<String> pendingDeletes;

    /** Whether a flush timer is already registered, so one flush is not scheduled per record. */
    private transient ValueState<Boolean> flushScheduled;

    public DiscardFileDeleteFunction(
            String pgUrl, String pgUserName, String pgPasswd, long dataExpiredTime) {
        this.pgUrl = pgUrl;
        this.pgUserName = pgUserName;
        this.pgPasswd = pgPasswd;
        this.dataExpiredTime = dataExpiredTime;
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
        boolean allSucceeded = true;
        CleanUtils cleanUtils = new CleanUtils();
        try (Connection conn = dataSource.getConnection()) {
            // Files referenced by a pinned (tagged read) commit or by a snapshot that is still
            // inside the retention window stay. The discard queue itself already waits for the
            // retention window; this check covers snapshots that re-added the path after it was
            // discarded (UpdateCommit and leveled compaction reuse paths).
            List<String> pinned =
                    protectedPaths(conn, batch, System.currentTimeMillis() - dataExpiredTime);
            List<String> candidates = batch;
            if (!pinned.isEmpty()) {
                candidates = new ArrayList<>(batch);
                candidates.removeAll(pinned);
                log.info("skip {} protected discard files", pinned.size());
            }
            if (candidates.isEmpty()) {
                return true;
            }
            DataFileReadLeaseGuard readers = readersForBatch(conn, candidates);
            try {
                // One protection probe and one fence commit for the whole batch.
                List<String> deletable =
                        readers == null
                                ? candidates
                                : readers.retireBatch(candidates, Collections.emptyList());
                java.util.Set<String> allowed = new java.util.HashSet<>();
                for (String path : deletable) {
                    allowed.add(DataFileReadLeaseGuard.normalizePath(path));
                }
                for (String path : candidates) {
                    if (!allowed.contains(DataFileReadLeaseGuard.normalizePath(path))) {
                        // Retain both the database row and checkpointed retry
                        // state until the reader/tag releases this file.
                        allSucceeded = false;
                        continue;
                    }
                    try {
                        cleanUtils.deleteFile(path);
                        try (PreparedStatement ps =
                                conn.prepareStatement(
                                        "delete from discard_compressed_file_info where file_path ="
                                                + " ?")) {
                            ps.setString(1, path);
                            ps.executeUpdate();
                        }
                        if (readers != null) {
                            readers.commit();
                        }
                    } catch (Exception error) {
                        log.error(
                                "discard file cleanup failed [{}], retain for retry", path, error);
                        allSucceeded = false;
                    }
                }
                return allSucceeded;
            } finally {
                if (readers != null) {
                    try {
                        readers.close();
                    } catch (SQLException error) {
                        log.error("failed to close discard cleanup guard", error);
                    }
                }
            }
        } catch (SQLException error) {
            log.error("failed to guard discard file cleanup", error);
            return false;
        }
    }

    static List<String> protectedPaths(Connection conn, List<String> batch, long retainedAfter)
            throws SQLException {
        if (batch == null || batch.isEmpty()) {
            return new ArrayList<>();
        }
        Array paths = conn.createArrayOf("text", batch.toArray());
        // A batch belongs to a handful of tables; probing only those tables uses
        // the (table_id, pinned) index instead of scanning every pinned commit.
        List<String> tableIds = singleOwnerTableIds(conn, paths);
        if (tableIds == null) {
            return protectedPathsGlobal(conn, paths, retainedAfter);
        }
        Array tableIdsArray = conn.createArrayOf("text", tableIds.toArray());
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "select distinct f.path from data_commit_info dci,"
                                + " unnest(dci.file_ops) as f"
                                + " where dci.pinned = true"
                                + " and dci.table_id = any(?::text[])"
                                + " and f.path = any(?::text[])"
                                + " union"
                                + " select distinct f.path from partition_info pi"
                                + " join data_commit_info dci"
                                + "   on dci.table_id = pi.table_id"
                                + "  and dci.partition_desc = pi.partition_desc"
                                + "  and dci.commit_id = any(pi.snapshot)"
                                + " cross join lateral unnest(dci.file_ops) as f"
                                + " where pi.table_id = any(?::text[])"
                                + "  and f.path = any(?::text[])"
                                + "  and f.file_op = 'add'"
                                + "  and (pi.timestamp >= ? or pi.pinned"
                                + "       or pi.version = (select max(p2.version)"
                                + "           from partition_info p2"
                                + "          where p2.table_id = pi.table_id"
                                + "            and p2.partition_desc = pi.partition_desc))")) {
            ps.setArray(1, tableIdsArray);
            ps.setArray(2, paths);
            ps.setArray(3, tableIdsArray);
            ps.setArray(4, paths);
            ps.setLong(5, retainedAfter);
            List<String> protectedPaths = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    protectedPaths.add(rs.getString(1));
                }
            }
            return protectedPaths;
        }
    }

    /**
     * Owning tables of the batch, when every batch table path belongs to exactly one table.
     *
     * <p>{@code null} means the fast path cannot prove it matches the global pinned probe: a table
     * path shared by several tables (or missing from {@code table_info}) could be pinned by another
     * table, so the caller falls back to scanning all pinned commits.
     */
    private static List<String> singleOwnerTableIds(Connection conn, Array paths)
            throws SQLException {
        int distinctPaths;
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "select count(distinct table_path),"
                                + " count(*) filter (where table_path is null)"
                                + " from discard_compressed_file_info"
                                + " where file_path = any(?::text[])")) {
            ps.setArray(1, paths);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                distinctPaths = rs.getInt(1);
                if (rs.getInt(2) > 0) {
                    return null;
                }
            }
        }
        if (distinctPaths == 0) {
            return null;
        }
        List<String> tableIds = new ArrayList<>();
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "select ti.table_path, count(distinct ti.table_id) as owners,"
                            + " min(ti.table_id) as table_id from table_info ti where ti.table_path"
                            + " = any(select distinct table_path   from"
                            + " discard_compressed_file_info where file_path = any(?::text[]))"
                            + " group by ti.table_path")) {
            ps.setArray(1, paths);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (rs.getInt("owners") != 1) {
                        return null;
                    }
                    tableIds.add(rs.getString("table_id"));
                }
            }
        }
        return tableIds.size() == distinctPaths ? tableIds : null;
    }

    /** Global protected-path probe used when the owning tables are not unambiguous. */
    private static List<String> protectedPathsGlobal(
            Connection conn, Array paths, long retainedAfter) throws SQLException {
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "SELECT p.path FROM unnest(?::text[]) AS p(path)"
                                + " WHERE EXISTS (SELECT 1 FROM data_commit_info dci,"
                                + " unnest(dci.file_ops) AS f WHERE dci.pinned = true"
                                + " AND f.path = p.path)"
                                + " OR EXISTS (SELECT 1 FROM partition_info pi"
                                + " JOIN data_commit_info dci"
                                + "   ON dci.table_id = pi.table_id"
                                + "  AND dci.partition_desc = pi.partition_desc"
                                + "  AND dci.commit_id = ANY(pi.snapshot)"
                                + " CROSS JOIN LATERAL unnest(dci.file_ops) AS f"
                                + " WHERE f.path = p.path AND f.file_op = 'add'"
                                + "   AND (pi.timestamp >= ? OR pi.pinned"
                                + "        OR pi.version = (SELECT max(p2.version)"
                                + "            FROM partition_info p2"
                                + "           WHERE p2.table_id = pi.table_id"
                                + "             AND p2.partition_desc = pi.partition_desc)))")) {
            ps.setArray(1, paths);
            ps.setLong(2, retainedAfter);
            List<String> protectedPaths = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    protectedPaths.add(rs.getString(1));
                }
            }
            return protectedPaths;
        }
    }

    /**
     * Retirement fences are only needed when one of the owning tables declares a vector index.
     * Otherwise {@code null} keeps the original deletion path with no metadata round trips.
     */
    private DataFileReadLeaseGuard readersForBatch(Connection conn, List<String> batch)
            throws SQLException {
        if (!needsRetirementFences(conn, batch)) {
            return null;
        }
        return DataFileReadLeaseGuard.acquireFenceOnlyOrNull(conn);
    }

    /** True when at least one table owning these discard files has a vector index. */
    public static boolean needsRetirementFences(Connection conn, List<String> batch)
            throws SQLException {
        if (batch == null || batch.isEmpty()) {
            return false;
        }
        String sql =
                // Avoid the jsonb `?` operator here: JDBC would parse it as a placeholder.
                "select exists (select 1 from table_info t"
                        + " where jsonb_typeof((t.properties::jsonb) -> 'vector_index_columns')"
                        + " = 'array'"
                        + " and exists (select 1 from discard_compressed_file_info d"
                        + "   where d.file_path = any(?::text[]) and d.table_path = t.table_path))";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setArray(1, conn.createArrayOf("text", batch.toArray(new String[0])));
            try (java.sql.ResultSet rows = ps.executeQuery()) {
                rows.next();
                return rows.getBoolean(1);
            }
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
