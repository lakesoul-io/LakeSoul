// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.UUID;

/**
 * Regression test for the compaction cleanup: an UpdateCommit re-adds unchanged files with their
 * original paths, so deleting every file of an older snapshot used to remove files that are still
 * referenced by the latest partition version.
 */
public class CleanUtilsLiveFileGuardTest {

    private static final String JDBC_URL =
            System.getenv()
                    .getOrDefault(
                            "LAKESOUL_PG_URL", "jdbc:postgresql://127.0.0.1:5432/lakesoul_test");
    private static final String USER =
            System.getenv().getOrDefault("LAKESOUL_PG_USERNAME", "lakesoul_test");
    private static final String PASSWORD =
            System.getenv().getOrDefault("LAKESOUL_PG_PASSWORD", "lakesoul_test");

    private Connection connection;
    private String tableId;
    private Path dir;
    private Path sharedFile;
    private Path expiredFile;
    private Path updateFile;
    private UUID compactionCommit;
    private UUID updateCommit;

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue("PostgreSQL is not available, skipping", canConnect());

        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);

        tableId = "clean_guard_" + UUID.randomUUID().toString().replace("-", "");
        dir = Files.createTempDirectory("clean-guard");
        sharedFile = Files.write(dir.resolve("shared.parquet"), new byte[] {1});
        expiredFile = Files.write(dir.resolve("expired.parquet"), new byte[] {2});
        updateFile = Files.write(dir.resolve("update.parquet"), new byte[] {3});

        compactionCommit = UUID.randomUUID();
        updateCommit = UUID.randomUUID();

        insertCommit(
                compactionCommit,
                "CompactionCommit",
                new String[][] {
                    {sharedFile.toUri().toString(), "add"},
                    {expiredFile.toUri().toString(), "add"}
                });
        insertCommit(
                updateCommit,
                "UpdateCommit",
                new String[][] {
                    // unchanged file is re-added with the same path by the update commit
                    {sharedFile.toUri().toString(), "add"},
                    {updateFile.toUri().toString(), "add"},
                    {expiredFile.toUri().toString(), "del"}
                });

        insertPartition(1, "CompactionCommit", compactionCommit);
        insertPartition(2, "UpdateCommit", updateCommit);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try (PreparedStatement ps =
                    connection.prepareStatement(
                            "delete from data_commit_info where table_id = ?")) {
                ps.setString(1, tableId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps =
                    connection.prepareStatement("delete from partition_info where table_id = ?")) {
                ps.setString(1, tableId);
                ps.executeUpdate();
            }
            connection.close();
        }
        if (dir != null && Files.exists(dir)) {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    Files.deleteIfExists(p);
                                } catch (Exception ignored) {
                                }
                            });
        }
    }

    @Test
    public void sharedFileOfOldCompactionSurvivesUpdateCommit() throws Exception {
        new CleanUtils()
                .deleteFileAndDataCommitInfo(
                        Collections.singletonList(compactionCommit.toString()),
                        tableId,
                        "-5",
                        connection,
                        true);

        assertTrue(
                "file re-added by the update commit must not be deleted", Files.exists(sharedFile));
        assertFalse(
                "file removed by the update commit should be deleted", Files.exists(expiredFile));
        assertTrue("file of the update commit must not be touched", Files.exists(updateFile));

        assertEquals(0, countCommits(compactionCommit));
        assertEquals(1, countCommits(updateCommit));
    }

    @Test
    public void livePathLookupFailureKeepsFilesAndMetadata() throws Exception {
        // A transient lookup failure (e.g. statement timeout) must abort the cleanup before
        // any data_commit_info row is removed: the file paths only live in those rows.
        Connection broken = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        broken.close();

        assertThrows(
                RuntimeException.class,
                () ->
                        new CleanUtils()
                                .deleteFileAndDataCommitInfo(
                                        Collections.singletonList(compactionCommit.toString()),
                                        tableId,
                                        "-5",
                                        broken,
                                        true));

        assertTrue("shared file must survive a failed lookup", Files.exists(sharedFile));
        assertTrue("expired file must survive a failed lookup", Files.exists(expiredFile));
        assertTrue("update file must survive a failed lookup", Files.exists(updateFile));
        assertEquals("superseded commit metadata must be kept", 1, countCommits(compactionCommit));
        assertEquals("update commit metadata must be kept", 1, countCommits(updateCommit));
    }

    private void insertCommit(UUID commitId, String commitOp, String[][] fileOps) throws Exception {
        StringBuilder array = new StringBuilder("ARRAY[");
        for (int i = 0; i < fileOps.length; i++) {
            if (i > 0) {
                array.append(", ");
            }
            array.append("ROW(?, ?, 1, 'id')::data_file_op");
        }
        array.append("]");
        String sql =
                "insert into data_commit_info(table_id, partition_desc, commit_id, file_ops,"
                        + " commit_op, committed, timestamp, domain)"
                        + " values (?, '-5', ?, "
                        + array
                        + ", ?, true, ?, 'public')";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int index = 1;
            ps.setString(index++, tableId);
            ps.setObject(index++, commitId);
            for (String[] op : fileOps) {
                ps.setString(index++, op[0]);
                ps.setString(index++, op[1]);
            }
            ps.setString(index++, commitOp);
            ps.setLong(index, System.currentTimeMillis());
            ps.executeUpdate();
        }
    }

    private void insertPartition(int version, String commitOp, UUID commitId) throws Exception {
        String sql =
                "insert into partition_info(table_id, partition_desc, version, commit_op,"
                        + " timestamp, snapshot, domain) values (?, '-5', ?, ?, ?, ?, 'public')";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableId);
            ps.setInt(2, version);
            ps.setString(3, commitOp);
            ps.setLong(4, System.currentTimeMillis());
            ps.setArray(5, connection.createArrayOf("uuid", new Object[] {commitId}));
            ps.executeUpdate();
        }
    }

    private int countCommits(UUID commitId) throws Exception {
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "select count(*) from data_commit_info where table_id = ? and commit_id"
                                + " = ?")) {
            ps.setString(1, tableId);
            ps.setObject(2, commitId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private static boolean canConnect() {
        try {
            Class.forName("org.postgresql.Driver");
            try (Connection ignored = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }
}
