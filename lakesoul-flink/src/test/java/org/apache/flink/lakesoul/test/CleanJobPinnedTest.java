// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dmetasoul.lakesoul.meta.DBConnector;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.lakesoul.entry.clean.NewCleanJob;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

/**
 * Runs the real clean job on a mini cluster against a logical-replication PostgreSQL and checks
 * that discard files pinned by a snapshot survive while unpinned ones are deleted.
 */
public class CleanJobPinnedTest extends AbstractTestBase {

    private static final String JDBC_URL =
            System.getenv()
                    .getOrDefault(
                            "LAKESOUL_PG_URL", "jdbc:postgresql://127.0.0.1:5432/lakesoul_test");

    private static String sourceHost() {
        String withoutScheme = JDBC_URL.replace("jdbc:postgresql://", "");
        String hostPort = withoutScheme.substring(0, withoutScheme.indexOf('/'));
        return hostPort.substring(0, hostPort.indexOf(':'));
    }

    private static String sourcePort() {
        String withoutScheme = JDBC_URL.replace("jdbc:postgresql://", "");
        String hostPort = withoutScheme.substring(0, withoutScheme.indexOf('/'));
        return hostPort.substring(hostPort.indexOf(':') + 1);
    }

    @BeforeClass
    public static void configureMetadata() throws Exception {
        // The PostgreSQL instance (with wal_level=logical and the LakeSoul schema) is provided by
        // the test environment, e.g. a CI service container; the test never starts one itself.
        String walLevel = query("show wal_level");
        assertTrue(
                "compaction-clean e2e needs wal_level=logical, got " + walLevel,
                walLevel.trim().equals("logical"));
        System.setProperty("lakesoul.pg.url", JDBC_URL);
        System.setProperty(
                "lakesoul.pg.username",
                System.getenv().getOrDefault("LAKESOUL_PG_USERNAME", "lakesoul_test"));
        System.setProperty(
                "lakesoul.pg.password",
                System.getenv().getOrDefault("LAKESOUL_PG_PASSWORD", "lakesoul_test"));
    }

    private static String query(String sql) throws Exception {
        Class.forName("org.postgresql.Driver");
        try (Connection conn =
                        java.sql.DriverManager.getConnection(
                                JDBC_URL,
                                System.getenv()
                                        .getOrDefault("LAKESOUL_PG_USERNAME", "lakesoul_test"),
                                System.getenv()
                                        .getOrDefault("LAKESOUL_PG_PASSWORD", "lakesoul_test"));
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    public void pinnedDiscardFileSurvives() throws Exception {
        String tableId = "clean_job_" + UUID.randomUUID().toString().replace("-", "");
        String partition = "-5";
        String pinnedCommit = UUID.randomUUID().toString();
        Path dir = Files.createTempDirectory("clean-job-pinned");
        Path pinnedFile = dir.resolve("pinned.parquet");
        Path plainFile = dir.resolve("plain.parquet");
        Files.write(pinnedFile, new byte[] {1});
        Files.write(plainFile, new byte[] {2});

        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement()) {
            long now = System.currentTimeMillis();
            st.executeUpdate(
                    String.format(
                            "insert into table_info(table_id, table_namespace, table_name,"
                                + " table_path, table_schema, properties, partitions, domain)"
                                + " values"
                                + " ('%s','default','%s','file://%s','[]','{}',';','public')",
                            tableId, tableId, dir));
            st.executeUpdate(
                    String.format(
                            "insert into data_commit_info(table_id, partition_desc, commit_id,"
                                    + " file_ops, commit_op, committed, timestamp, domain, pinned)"
                                    + " values ('%s','%s','%s'::uuid,"
                                    + " ARRAY[ROW('%s','add',1,'id')::data_file_op],"
                                    + " 'AppendCommit',true,%d,'public',true)",
                            tableId, partition, pinnedCommit, pinnedFile, now));
            st.executeUpdate(
                    String.format(
                            "insert into discard_compressed_file_info(file_path, table_path,"
                                + " partition_desc, timestamp) values ('%s','file://%s','%s',%d),"
                                + " ('%s','file://%s','%s',%d)",
                            pinnedFile,
                            dir,
                            partition,
                            now - 60_000,
                            plainFile,
                            dir,
                            partition,
                            now - 60_000));
        }

        ParameterTool parameter =
                ParameterTool.fromMap(
                        new java.util.HashMap<String, String>() {
                            {
                                put("source_db.host", sourceHost());
                                put("source_db.port", sourcePort());
                                put(
                                        "source_db.dbName",
                                        System.getenv()
                                                .getOrDefault("LAKESOUL_PG_DB", "lakesoul_test"));
                                put(
                                        "source_db.user",
                                        System.getenv()
                                                .getOrDefault(
                                                        "LAKESOUL_PG_USERNAME", "lakesoul_test"));
                                put(
                                        "source_db.password",
                                        System.getenv()
                                                .getOrDefault(
                                                        "LAKESOUL_PG_PASSWORD", "lakesoul_test"));
                                put("slotName", "clean_job_" + tableId);
                                put("plugName", "pgoutput");
                                put("schemaList", "public");
                                put("splitSize", "1");
                                put("url", JDBC_URL);
                                put("source.parallelism", "1");
                                put("dataExpiredTime", "2000");
                            }
                        });

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        NewCleanJob.buildPipeline(env, parameter);
        org.apache.flink.core.execution.JobClient job = env.executeAsync();
        try {
            long deadline = System.currentTimeMillis() + 90_000;
            boolean plainDeleted = false;
            while (System.currentTimeMillis() < deadline && !plainDeleted) {
                Thread.sleep(2000);
                plainDeleted = !Files.exists(plainFile);
                assertTrue("pinned file must survive the clean job", Files.exists(pinnedFile));
            }
            assertTrue("unpinned discard file was not deleted", plainDeleted);
            try (Connection conn = DBConnector.getConn();
                    Statement st = conn.createStatement();
                    ResultSet rs =
                            st.executeQuery(
                                    "select count(*) from discard_compressed_file_info"
                                            + " where file_path = '"
                                            + pinnedFile
                                            + "'")) {
                rs.next();
                assertTrue("pinned discard row must survive", rs.getInt(1) == 1);
            }
            try (Connection conn = DBConnector.getConn();
                    Statement st = conn.createStatement();
                    ResultSet rs =
                            st.executeQuery(
                                    "select count(*) from discard_compressed_file_info"
                                            + " where file_path = '"
                                            + plainFile
                                            + "'")) {
                rs.next();
                assertFalse("unpinned discard row must be removed", rs.getInt(1) == 1);
            }
        } finally {
            job.cancel();
            try (Connection conn = DBConnector.getConn();
                    Statement st = conn.createStatement()) {
                st.executeUpdate(
                        "delete from discard_compressed_file_info where table_path = 'file://"
                                + dir
                                + "'");
                st.executeUpdate("delete from data_commit_info where table_id = '" + tableId + "'");
                st.executeUpdate("delete from table_info where table_id = '" + tableId + "'");
            }
        }
    }
}
