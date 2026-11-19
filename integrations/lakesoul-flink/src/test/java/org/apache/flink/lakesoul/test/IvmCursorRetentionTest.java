// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.flink.lakesoul.entry.clean.CleanUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.UUID;

/**
 * PostgreSQL-backed checks that the clean job treats the oldest IVM source cursor as a retention
 * watermark.
 *
 * <p>A version at or above the cursor may still be read by the view's next window. Its files and
 * commit rows must survive even when the clean-job timestamp window has already expired; only
 * versions below every cursor are reclaimable.
 */
public class IvmCursorRetentionTest {

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

    private final String tableId = "ivm_cursor_" + UUID.randomUUID().toString().replace("-", "");
    private final String viewId = "ivm_view_" + UUID.randomUUID().toString().replace("-", "");
    private final UUID commit1 = UUID.randomUUID();
    private final UUID commit2 = UUID.randomUUID();
    private final UUID commit3 = UUID.randomUUID();

    private Connection connection;
    private Path dir;
    private Path shared;
    private Path expired;

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue("PostgreSQL is not available, skipping", canConnect());
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        dir = Files.createTempDirectory("ivm-cursor");
        shared = dir.resolve("shared.parquet");
        expired = dir.resolve("expired.parquet");
        Files.write(shared, new byte[] {1});
        Files.write(expired, new byte[] {2});

        long now = System.currentTimeMillis();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    String.format(
                            "insert into table_info(table_id, table_namespace, table_name,"
                                    + " table_path, table_schema, properties, partitions, domain)"
                                    + " values"
                                    + " ('%s','default','%s','file://%s','[]','{}',';','public')",
                            tableId, tableId, dir));
            insertCommit(statement, commit1, now - 3_000, shared, expired);
            insertCommit(statement, commit2, now - 2_000, shared);
            insertCommit(statement, commit3, now - 1_000, shared);
            insertPartition(statement, 1, commit1, now - 3_000);
            insertPartition(statement, 2, commit2, now - 2_000);
            insertPartition(statement, 3, commit3, now - 1_000);
            // The view has consumed version 2; version 3 is the latest but is
            // still the next window's upper bound.
            statement.executeUpdate(
                    String.format(
                            "insert into ivm.cursors(view_id, source_table_id, partition_desc,"
                                    + " last_version, last_timestamp)"
                                    + " values ('%s','%s','%s',2,%d)",
                            viewId, tableId, PARTITION, now - 2_000));
        }
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("delete from ivm.cursors where view_id = '" + viewId + "'");
                statement.executeUpdate("delete from ivm.epochs where view_id = '" + tableId + "'");
                statement.executeUpdate(
                        "delete from data_commit_info where table_id = '" + tableId + "'");
                statement.executeUpdate(
                        "delete from partition_info where table_id = '" + tableId + "'");
                statement.executeUpdate(
                        "delete from table_info where table_id = '" + tableId + "'");
            }
            connection.close();
        }
    }

    @Test
    public void cleanupKeepsAFileReadByAVersionAboveTheCursor() throws Exception {
        CleanUtils cleanUtils = new CleanUtils();
        assertTrue(
                cleanUtils.deleteFileAndDataCommitInfo(
                        Collections.singletonList(commit1.toString()),
                        tableId,
                        PARTITION,
                        connection,
                        true,
                        Long.MAX_VALUE));

        assertTrue("the re-added shared file must survive", Files.exists(shared));
        assertFalse("the file only in the expired commit should be deleted", Files.exists(expired));
        assertEquals("the superseded commit row should be removed", 0, countCommit(commit1));
    }

    @Test
    public void cursorRetainsTheVersionAtOrAboveItsWatermark() throws Exception {
        CleanUtils cleanUtils = new CleanUtils();
        assertTrue(
                "version 3 is still ahead of the cursor",
                cleanUtils.isVersionRetainedByIvmCursor(connection, tableId, PARTITION, 3));
        assertFalse(
                "version 1 is below the cursor",
                cleanUtils.isVersionRetainedByIvmCursor(connection, tableId, PARTITION, 1));

        cleanUtils.cleanPartitionInfo(tableId, PARTITION, 3, connection);
        assertEquals("the cursor-retained version must stay", 1, countVersion(3));

        cleanUtils.cleanPartitionInfo(tableId, PARTITION, 1, connection);
        assertEquals("the version below the cursor may be removed", 0, countVersion(1));
    }

    @Test
    public void pendingEpochKeepsItsBeforeImage() throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    String.format(
                            "insert into ivm.epochs(view_id, generation, epoch, window_key, status,"
                                    + " to_versions, mv_versions_before, state_versions_before,"
                                    + " created_at) values"
                                    + " ('%s',1,1,'pending-test','pending','[]'::jsonb,"
                                    + " '[{\"partition_desc\":\"%s\",\"version\":1}]'::jsonb,"
                                    + " '[]'::jsonb,0)",
                            tableId, PARTITION));
        }

        CleanUtils cleanUtils = new CleanUtils();
        assertTrue(cleanUtils.isVersionRetainedByPendingEpoch(connection, tableId, PARTITION, 1));
        cleanUtils.cleanPartitionInfo(tableId, PARTITION, 1, connection);
        assertEquals("the pending epoch's before image must stay", 1, countVersion(1));
    }

    private void insertCommit(Statement statement, UUID commit, long timestamp, Path... paths)
            throws Exception {
        StringBuilder fileOps = new StringBuilder("ARRAY[");
        for (int i = 0; i < paths.length; i++) {
            if (i > 0) {
                fileOps.append(", ");
            }
            fileOps.append(
                    String.format("ROW('%s','add',1,'id')::data_file_op", paths[i].toString()));
        }
        fileOps.append(']');
        statement.executeUpdate(
                String.format(
                        "insert into data_commit_info(table_id, partition_desc, commit_id,"
                                + " file_ops, commit_op, committed, timestamp, domain, pinned)"
                                + " values ('%s','%s','%s'::uuid, %s, 'AppendCommit', true, %d,"
                                + " 'public', false)",
                        tableId, PARTITION, commit, fileOps, timestamp));
    }

    private void insertPartition(Statement statement, int version, UUID commit, long timestamp)
            throws Exception {
        statement.executeUpdate(
                String.format(
                        "insert into partition_info(table_id, partition_desc, version,"
                                + " commit_op, snapshot, timestamp, domain, pinned)"
                                + " values ('%s','%s',%d,'AppendCommit',ARRAY['%s'::uuid],%d,"
                                + " 'public',false)",
                        tableId, PARTITION, version, commit, timestamp));
    }

    private int countCommit(UUID commit) throws Exception {
        return count(
                "select count(*) from data_commit_info where table_id = '"
                        + tableId
                        + "' and commit_id = '"
                        + commit
                        + "'");
    }

    private int countVersion(int version) throws Exception {
        return count(
                "select count(*) from partition_info where table_id = '"
                        + tableId
                        + "' and partition_desc = '"
                        + PARTITION
                        + "' and version = "
                        + version);
    }

    private int count(String sql) throws Exception {
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(sql)) {
            rows.next();
            return rows.getInt(1);
        }
    }

    private static boolean canConnect() {
        try (Connection ignored = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }
}
