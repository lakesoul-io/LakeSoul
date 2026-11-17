// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;

import org.apache.flink.lakesoul.entry.clean.CleanUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.UUID;

/**
 * PostgreSQL-backed checks that the clean job protects every retained snapshot, not only the latest
 * one.
 *
 * <p>A recent snapshot can reference an old commit or an old file path. Removing the old version
 * must not delete either while the recent snapshot is still inside the clean job's retention
 * window.
 */
public class CleanRetainedSnapshotTest {

    private static final String URL =
            System.getenv()
                    .getOrDefault(
                            "LAKESOUL_PG_URL",
                            "jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified");
    private static final String PARTITION = "-5";

    private final String tableId =
            "retained_cleanup_" + UUID.randomUUID().toString().replace("-", "");
    private final String oldCommit = UUID.randomUUID().toString();
    private final String recentCommit = UUID.randomUUID().toString();
    private final String latestCommit = UUID.randomUUID().toString();

    private Path root;
    private Path oldFile;
    private Path recentFile;
    private Path latestFile;

    @BeforeClass
    public static void configureMetadata() {
        NativeUtils.NATIVE_METADATA_QUERY_ENABLED = false;
        NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = false;
        System.setProperty(DBUtil.urlKey, URL);
        System.setProperty(
                DBUtil.usernameKey,
                System.getenv().getOrDefault("LAKESOUL_PG_USERNAME", "lakesoul_test"));
        System.setProperty(
                DBUtil.passwordKey,
                System.getenv().getOrDefault("LAKESOUL_PG_PASSWORD", "lakesoul_test"));
    }

    @Before
    public void insertMeta() throws Exception {
        root = Files.createTempDirectory("retained-snapshot");
        oldFile = root.resolve("old.parquet");
        recentFile = root.resolve("recent.parquet");
        latestFile = root.resolve("latest.parquet");
        Files.write(oldFile, new byte[] {1});
        Files.write(recentFile, new byte[] {2});
        Files.write(latestFile, new byte[] {3});

        long now = System.currentTimeMillis();
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement()) {
            st.executeUpdate(
                    String.format(
                            "insert into table_info(table_id, table_namespace, table_name,"
                                    + " table_path, table_schema, properties, partitions, domain)"
                                    + " values"
                                    + " ('%s','default','%s','file://%s','[]','{}',';','public')",
                            tableId, tableId, root));
            insertCommit(st, oldCommit, oldFile, now - 2 * 86_400_000L);
            insertCommit(st, recentCommit, recentFile, now);
            insertCommit(st, latestCommit, latestFile, now);
            insertPartition(st, 0, oldCommit, now - 2 * 86_400_000L);
            // The recent snapshot still references the old commit. Its version is inside the
            // one-day retention window even though the commit timestamp is old.
            insertPartition(st, 1, oldCommit, now - 5 * 60_000L);
            insertPartition(st, 2, latestCommit, now);
        }
    }

    private void insertCommit(Statement st, String commit, Path file, long timestamp)
            throws Exception {
        st.executeUpdate(
                String.format(
                        "insert into data_commit_info(table_id, partition_desc, commit_id,"
                                + " file_ops, commit_op, committed, timestamp, domain, pinned)"
                                + " values ('%s','%s','%s'::uuid,"
                                + " ARRAY[ROW('%s','add',1,'id')::data_file_op],"
                                + " 'AppendCommit',true,%d,'public',false)",
                        tableId, PARTITION, commit, file, timestamp));
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

    @After
    public void dropMeta() throws Exception {
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement()) {
            st.executeUpdate("delete from data_commit_info where table_id = '" + tableId + "'");
            st.executeUpdate("delete from partition_info where table_id = '" + tableId + "'");
            st.executeUpdate("delete from table_info where table_id = '" + tableId + "'");
        }
    }

    @Test
    public void recentSnapshotKeepsAnOldSharedCommitAndFile() throws Exception {
        long retainedAfter = System.currentTimeMillis() - 86_400_000L;
        CleanUtils cleanUtils = new CleanUtils();
        try (Connection conn = DBConnector.getConn()) {
            assertTrue(
                    cleanUtils.deleteFileAndDataCommitInfo(
                            Collections.singletonList(oldCommit),
                            tableId,
                            PARTITION,
                            conn,
                            true,
                            retainedAfter));
        }
        assertTrue("a retained snapshot still references the old file", Files.exists(oldFile));
        assertEquals(
                "a retained snapshot still references the old commit",
                1,
                count(
                        "select count(*) from data_commit_info where commit_id = '"
                                + oldCommit
                                + "'"));
    }

    @Test
    public void recentSnapshotKeepsAnOldFilePath() throws Exception {
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement()) {
            // The recent snapshot re-adds the old path under a new commit id; the latest
            // snapshot no longer references it.
            st.executeUpdate(
                    String.format(
                            "update partition_info set snapshot = ARRAY['%s'::uuid]"
                                    + " where table_id = '%s' and partition_desc = '%s'"
                                    + " and version = 1",
                            recentCommit, tableId, PARTITION));
            st.executeUpdate(
                    String.format(
                            "update data_commit_info set file_ops ="
                                    + " ARRAY[ROW('%s','add',1,'id')::data_file_op]"
                                    + " where commit_id = '%s'",
                            oldFile, recentCommit));
        }

        long retainedAfter = System.currentTimeMillis() - 86_400_000L;
        CleanUtils cleanUtils = new CleanUtils();
        try (Connection conn = DBConnector.getConn()) {
            assertTrue(
                    cleanUtils.deleteFileAndDataCommitInfo(
                            Collections.singletonList(oldCommit),
                            tableId,
                            PARTITION,
                            conn,
                            true,
                            retainedAfter));
        }
        assertTrue("a retained snapshot re-adds the old path", Files.exists(oldFile));
        assertEquals(
                "the old commit row is no longer referenced",
                0,
                count(
                        "select count(*) from data_commit_info where commit_id = '"
                                + oldCommit
                                + "'"));
    }

    private int count(String sql) throws Exception {
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
