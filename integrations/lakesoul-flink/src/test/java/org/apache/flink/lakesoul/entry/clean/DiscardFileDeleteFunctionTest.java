// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.clean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * PostgreSQL-backed checks for the discard-file protection probe.
 *
 * <p>A discarded path that was re-added by a snapshot inside the retention window (UpdateCommit and
 * leveled compaction reuse paths) must survive; once that snapshot is outside the window only the
 * latest snapshot still protects its files.
 */
public class DiscardFileDeleteFunctionTest {

    private static final String JDBC_URL =
            System.getenv()
                    .getOrDefault(
                            "LAKESOUL_PG_URL",
                            "jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified");
    private static final String USER =
            System.getenv().getOrDefault("LAKESOUL_PG_USERNAME", "lakesoul_test");
    private static final String PASSWORD =
            System.getenv().getOrDefault("LAKESOUL_PG_PASSWORD", "lakesoul_test");
    private static final String PARTITION = "-5";

    private final String tableId = "discard_guard_" + UUID.randomUUID().toString().replace("-", "");
    private final String viewId = "discard_view_" + UUID.randomUUID().toString().replace("-", "");
    private final String oldCommit = UUID.randomUUID().toString();
    private final String recentCommit = UUID.randomUUID().toString();
    private final String latestCommit = UUID.randomUUID().toString();

    private Path root;
    private String oldPath;
    private String recentPath;
    private String latestPath;

    @Before
    public void insertMeta() throws Exception {
        Class.forName("org.postgresql.Driver");
        root = Files.createTempDirectory("discard-guard");
        oldPath = root.resolve("old.parquet").toString();
        recentPath = root.resolve("recent.parquet").toString();
        latestPath = root.resolve("latest.parquet").toString();

        long now = System.currentTimeMillis();
        try (Connection conn = open();
                Statement st = conn.createStatement()) {
            st.executeUpdate(
                    String.format(
                            "insert into table_info(table_id, table_namespace, table_name,"
                                    + " table_path, table_schema, properties, partitions, domain)"
                                    + " values"
                                    + " ('%s','default','%s','file://%s','[]','{}',';','public')",
                            tableId, tableId, root));
            insertCommit(st, oldCommit, oldPath, now - 2 * 86_400_000L);
            insertCommit(st, recentCommit, recentPath, now);
            insertCommit(st, latestCommit, latestPath, now);
            insertPartition(st, 0, oldCommit, now - 2 * 86_400_000L);
            // The recent snapshot re-adds a path that was already discarded.
            insertPartition(st, 1, recentCommit, now - 5 * 60_000L);
            insertPartition(st, 2, latestCommit, now);
            insertDiscard(st, oldPath, now - 2 * 86_400_000L);
            insertDiscard(st, recentPath, now - 2 * 86_400_000L);
            insertDiscard(st, latestPath, now - 2 * 86_400_000L);
        }
    }

    @After
    public void dropMeta() throws Exception {
        if (root == null) {
            return;
        }
        try (Connection conn = open();
                Statement st = conn.createStatement()) {
            st.executeUpdate(
                    "delete from discard_compressed_file_info where table_path = 'file://"
                            + root
                            + "'");
            st.executeUpdate("delete from ivm.cursors where view_id = '" + viewId + "'");
            st.executeUpdate("delete from ivm.epochs where view_id = '" + tableId + "'");
            st.executeUpdate("delete from data_commit_info where table_id = '" + tableId + "'");
            st.executeUpdate("delete from partition_info where table_id = '" + tableId + "'");
            st.executeUpdate("delete from table_info where table_id = '" + tableId + "'");
        }
    }

    @Test
    public void retainedSnapshotKeepsAReaddedDiscardPath() throws Exception {
        long retainedAfter = System.currentTimeMillis() - 86_400_000L;
        List<String> paths = Arrays.asList(oldPath, recentPath, latestPath);
        try (Connection conn = open()) {
            List<String> protectedPaths =
                    DiscardFileDeleteFunction.protectedPaths(conn, paths, retainedAfter);
            assertFalse(
                    "an expired snapshot no longer protects its file",
                    protectedPaths.contains(oldPath));
            assertTrue(
                    "a retained snapshot protects the re-added file",
                    protectedPaths.contains(recentPath));
            assertTrue(
                    "the latest snapshot always protects its files",
                    protectedPaths.contains(latestPath));
            assertEquals(2, protectedPaths.size());
        }
    }

    @Test
    public void expiredSnapshotDoesNotProtectAReaddedDiscardPath() throws Exception {
        // Mirrors the e2e clean job with --dataExpiredTime 0: only the latest snapshot and pinned
        // commits protect discard files.
        long retainedAfter = System.currentTimeMillis();
        List<String> paths = Arrays.asList(oldPath, recentPath, latestPath);
        try (Connection conn = open()) {
            List<String> protectedPaths =
                    DiscardFileDeleteFunction.protectedPaths(conn, paths, retainedAfter);
            assertFalse(protectedPaths.contains(oldPath));
            assertFalse(protectedPaths.contains(recentPath));
            assertTrue(
                    "the latest snapshot always protects its files",
                    protectedPaths.contains(latestPath));
            assertEquals(1, protectedPaths.size());
        }
    }

    @Test
    public void cursorRetainsDiscardPathsAboveTheWatermark() throws Exception {
        long retainedAfter = System.currentTimeMillis();
        try (Connection conn = open();
                Statement st = conn.createStatement()) {
            st.executeUpdate(
                    String.format(
                            "insert into ivm.cursors(view_id, source_table_id, partition_desc,"
                                    + " last_version, last_timestamp) values"
                                    + " ('%s','%s','%s',1,%d)",
                            viewId, tableId, PARTITION, retainedAfter - 5 * 60_000L));
        }
        List<String> paths = Arrays.asList(oldPath, recentPath, latestPath);
        try (Connection conn = open()) {
            List<String> protectedPaths =
                    DiscardFileDeleteFunction.protectedPaths(conn, paths, retainedAfter);
            assertFalse(
                    "the version below the cursor is reclaimable",
                    protectedPaths.contains(oldPath));
            assertTrue(
                    "the version at the cursor is still needed",
                    protectedPaths.contains(recentPath));
            assertTrue("the latest version is always needed", protectedPaths.contains(latestPath));
            assertEquals(2, protectedPaths.size());
        }
    }

    @Test
    public void pendingEpochRetainsItsBeforeImageDiscardPath() throws Exception {
        long retainedAfter = System.currentTimeMillis();
        try (Connection conn = open();
                Statement st = conn.createStatement()) {
            st.executeUpdate(
                    String.format(
                            "insert into ivm.epochs(view_id, generation, epoch, window_key, status,"
                                    + " to_versions, mv_versions_before, state_versions_before,"
                                    + " created_at) values"
                                    + " ('%s',1,1,'pending-discard','pending','[]'::jsonb,"
                                    + " '[{\"partition_desc\":\"%s\",\"version\":0}]'::jsonb,"
                                    + " '[]'::jsonb,0)",
                            tableId, PARTITION));
        }
        List<String> paths = Arrays.asList(oldPath, recentPath, latestPath);
        try (Connection conn = open()) {
            List<String> protectedPaths =
                    DiscardFileDeleteFunction.protectedPaths(conn, paths, retainedAfter);
            assertTrue(
                    "a pending epoch's before image protects its old file",
                    protectedPaths.contains(oldPath));
            assertFalse(protectedPaths.contains(recentPath));
            assertTrue("the latest version is always needed", protectedPaths.contains(latestPath));
            assertEquals(2, protectedPaths.size());
        }
    }

    private static Connection open() throws Exception {
        return DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
    }

    private void insertCommit(Statement st, String commit, String path, long timestamp)
            throws Exception {
        st.executeUpdate(
                String.format(
                        "insert into data_commit_info(table_id, partition_desc, commit_id,"
                                + " file_ops, commit_op, committed, timestamp, domain, pinned)"
                                + " values ('%s','%s','%s'::uuid,"
                                + " ARRAY[ROW('%s','add',1,'id')::data_file_op],"
                                + " 'AppendCommit',true,%d,'public',false)",
                        tableId, PARTITION, commit, path, timestamp));
    }

    private void insertPartition(Statement st, int version, String commit, long timestamp)
            throws Exception {
        st.executeUpdate(
                String.format(
                        "insert into partition_info(table_id, partition_desc, version,"
                                + " commit_op, snapshot, timestamp, domain, pinned)"
                                + " values ('%s','%s',%d,'AppendCommit',ARRAY['%s'::uuid],%d,"
                                + " 'public',false)",
                        tableId, PARTITION, version, commit, timestamp));
    }

    private void insertDiscard(Statement st, String path, long timestamp) throws Exception {
        st.executeUpdate(
                String.format(
                        "insert into discard_compressed_file_info(file_path, table_path,"
                                + " partition_desc, timestamp) values ('%s','file://%s','%s',%d)",
                        path, root, PARTITION, timestamp));
    }
}
