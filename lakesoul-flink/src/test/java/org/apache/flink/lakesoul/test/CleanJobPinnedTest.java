// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dmetasoul.lakesoul.meta.DBConnector;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.lakesoul.entry.clean.NewCleanJob;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Runs the real clean job on a mini cluster against a logical-replication PostgreSQL and checks
 * that discard files pinned by a snapshot survive while unpinned ones are deleted.
 */
public class CleanJobPinnedTest extends AbstractTestBase {

    private static final String CONTAINER = "lakesoul-clean-job-pg";
    private static final String PG_PORT = "55432";
    private static final String JDBC_URL =
            "jdbc:postgresql://127.0.0.1:" + PG_PORT + "/lakesoul_test";

    private static String run(String... command) throws Exception {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        int status = process.waitFor();
        if (status != 0) {
            throw new IllegalStateException(
                    "command failed (" + status + "): " + String.join(" ", command) + "\n" + output);
        }
        return output;
    }

    private static String metaInitSql() {
        for (String candidate :
                new String[] {
                    "../script/meta_init.sql",
                    "script/meta_init.sql",
                    "../../script/meta_init.sql"
                }) {
            File file = new File(candidate);
            if (file.isFile()) {
                return file.getAbsolutePath();
            }
        }
        throw new IllegalStateException("meta_init.sql not found");
    }

    @BeforeClass
    public static void startPostgres() throws Exception {
        run("docker", "rm", "-f", CONTAINER);
        run(
                "docker",
                "run",
                "-d",
                "--name",
                CONTAINER,
                "-p",
                PG_PORT + ":5432",
                "-e",
                "POSTGRES_USER=lakesoul_test",
                "-e",
                "POSTGRES_PASSWORD=lakesoul_test",
                "-e",
                "POSTGRES_DB=lakesoul_test",
                "postgres:14",
                "-c",
                "wal_level=logical");
        boolean ready = false;
        for (int attempt = 0; attempt < 60 && !ready; attempt++) {
            Thread.sleep(1000);
            try {
                run(
                        "docker",
                        "exec",
                        CONTAINER,
                        "psql",
                        "-U",
                        "lakesoul_test",
                        "-d",
                        "lakesoul_test",
                        "-c",
                        "select 1");
                ready = true;
            } catch (Exception ignored) {
                // keep waiting
            }
        }
        assertTrue("postgres did not become ready", ready);
        Process psql =
                new ProcessBuilder(
                                "docker",
                                "exec",
                                "-i",
                                CONTAINER,
                                "psql",
                                "-U",
                                "lakesoul_test",
                                "-d",
                                "lakesoul_test",
                                "-v",
                                "ON_ERROR_STOP=1")
                        .redirectErrorStream(true)
                        .start();
        Files.copy(Path.of(metaInitSql()), psql.getOutputStream());
        psql.getOutputStream().close();
        String output = new String(psql.getInputStream().readAllBytes());
        assertTrue("schema init failed: " + output, psql.waitFor() == 0);
        System.setProperty("lakesoul.pg.url", JDBC_URL);
        System.setProperty("lakesoul.pg.username", "lakesoul_test");
        System.setProperty("lakesoul.pg.password", "lakesoul_test");
    }

    @AfterClass
    public static void stopPostgres() throws Exception {
        run("docker", "rm", "-f", CONTAINER);
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
                                    + " values ('%s','default','%s','file://%s','[]','{}',';','public')",
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
                                    + " partition_desc, timestamp)"
                                    + " values ('%s','file://%s','%s',%d), ('%s','file://%s','%s',%d)",
                            pinnedFile, dir, partition, now - 60_000,
                            plainFile, dir, partition, now - 60_000));
        }

        ParameterTool parameter =
                ParameterTool.fromMap(
                        new java.util.HashMap<String, String>() {
                            {
                                put("source_db.host", "127.0.0.1");
                                put("source_db.port", PG_PORT);
                                put("source_db.dbName", "lakesoul_test");
                                put("source_db.user", "lakesoul_test");
                                put("source_db.password", "lakesoul_test");
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
                st.executeUpdate(
                        "delete from data_commit_info where table_id = '" + tableId + "'");
                st.executeUpdate("delete from table_info where table_id = '" + tableId + "'");
            }
        }
    }
}
