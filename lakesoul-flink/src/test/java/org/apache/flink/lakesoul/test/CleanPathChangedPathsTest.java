// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.dmetasoul.lakesoul.meta.DataFileReadLeaseGuard;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.lakesoul.entry.clean.DiscardFileDeleteFunction;
import org.apache.flink.lakesoul.entry.clean.TtlBroadcastProcessFunction;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Regression tests for the changed cleanup paths: vector-index gating, batched retirement, the TTL
 * partition drop and the discard-file gate. Tables without a vector index must keep the original
 * filesystem-only behaviour and write no retirement fence.
 */
public class CleanPathChangedPathsTest {
    private static final String JDBC_URL =
            System.getenv().getOrDefault(
                    "LAKESOUL_PG_URL", "jdbc:postgresql://127.0.0.1:5432/lakesoul_test");
    private static final String USER =
            System.getenv().getOrDefault("LAKESOUL_PG_USERNAME", "lakesoul_test");
    private static final String PASSWORD =
            System.getenv().getOrDefault("LAKESOUL_PG_PASSWORD", "lakesoul_test");
    private static final String VECTOR_PROPERTIES =
            "{\"vector_index_columns\":[{\"column\":\"vec\"}]}";
    private static final String PLAIN_PROPERTIES = "{}";
    private static final String TABLE_SCHEMA =
            "{\"fields\":[{\"name\":\"c1\",\"type\":{\"name\":\"utf8\"},\"nullable\":false,\"children\":[],\"metadata\":[]},{\"name\":\"c2\",\"type\":{\"name\":\"int\",\"isSigned\":true,\"bitWidth\":32},\"nullable\":true,\"children\":[],\"metadata\":[]},{\"name\":\"c3\",\"type\":{\"name\":\"floatingpoint\",\"precision\":\"DOUBLE\"},\"nullable\":true,\"children\":[],\"metadata\":[]}],\"metadata\":[]}";

    private Connection connection;
    private final List<String> tableIds = new ArrayList<>();
    private final List<Path> dirs = new ArrayList<>();
    private String mainPartition = "-5";

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue("PostgreSQL is not available, skipping", canConnect());
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            for (String tableId : tableIds) {
                for (String sql : Arrays.asList(
                        "delete from index_read_lease where index_prefix = ?",
                        "delete from data_commit_info where table_id = ?",
                        "delete from partition_info where table_id = ?",
                        "delete from discard_compressed_file_info where table_path = ?",
                        "delete from table_name_id where table_id = ?",
                        "delete from table_path_id where table_id = ?",
                        "delete from table_info where table_id = ?")) {
                    try (PreparedStatement ps = connection.prepareStatement(sql)) {
                        ps.setString(1, tableId);
                        ps.executeUpdate();
                    }
                }
            }
            try (PreparedStatement ps =
                    connection.prepareStatement(
                            "delete from index_drop where index_prefix like '/tmp/clean-path/%'")) {
                ps.executeUpdate();
            }
            connection.close();
        }
        for (Path dir : dirs) {
            if (dir != null && Files.exists(dir)) {
                Files.walk(dir)
                        .sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> p.toFile().delete());
            }
        }
    }

    // ---------------------------------------------------------------- guard gate

    @Test
    public void guardIsSkippedForTablesWithoutVectorIndex() throws Exception {
        String plain = registerTable(PLAIN_PROPERTIES);
        String vector = registerTable(VECTOR_PROPERTIES);

        assertNull(
                "a table without vector_index_columns must not acquire the guard",
                DataFileReadLeaseGuard.acquireOrNull(connection, plain));
        try (DataFileReadLeaseGuard guard =
                DataFileReadLeaseGuard.acquireOrNull(connection, vector)) {
            assertNotNull("a vector table must acquire the guard", guard);
            assertEquals(false, guard.hasActiveReaders());
        }
    }

    @Test
    public void retireBatchDefersLeasedFilesAndFencesOnlyTheRest() throws Exception {
        String tableId = registerTable(VECTOR_PROPERTIES);
        Path dir = tempDir();
        List<String> files = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Path file = Files.write(dir.resolve("f-" + i + ".parquet"), new byte[] {1});
            files.add(file.toUri().toString());
        }
        lease(tableId, files.subList(0, 2));

        try (DataFileReadLeaseGuard guard = DataFileReadLeaseGuard.acquire(connection, tableId)) {
            List<String> allowed = guard.retireBatch(files, Collections.emptyList());
            assertEquals("only unprotected files may be retired", 3, allowed.size());
            for (String file : files.subList(0, 2)) {
                assertFalse(
                        "leased file must be deferred",
                        allowed.contains(DataFileReadLeaseGuard.normalizePath(file)));
            }
            assertEquals(
                "one fence per retired file",
                1,
                countFences(DataFileReadLeaseGuard.normalizePath(files.get(2))));
        }
        // Idempotent: running the same batch again neither fails nor duplicates fences.
        try (DataFileReadLeaseGuard guard = DataFileReadLeaseGuard.acquire(connection, tableId)) {
            assertEquals(3, guard.retireBatch(files, Collections.emptyList()).size());
        }
    }

    // ------------------------------------------------------------ TTL drop partition

    @Test
    public void ttlDropPartitionKeepsPlainTableOnTheFilesystemOnlyPath() throws Exception {
        String tableId = registerTable(PLAIN_PROPERTIES);
        Path dir = tempDir();
        List<String> files = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            files.add(Files.write(dir.resolve("p-" + i + ".parquet"), new byte[] {1}).toUri().toString());
        }
        insertCommitAndPartition(tableId, files);
        TtlBroadcastProcessFunction function = ttlFunction();

        assertTrue("plain table cleanup must succeed", function.dropPartition(tableId, mainPartition));
        for (String file : files) {
            assertFalse("file must be deleted: " + file, Files.exists(Path.of(java.net.URI.create(file))));
        }
        assertEquals(0, countPartitions(tableId));
        assertEquals("plain table cleanup must not fence", 0, countFencesForTableFiles(tableId));
        assertFalse(Files.exists(Path.of(dir.toUri())));
    }

    @Test
    public void ttlDropPartitionDefersVectorTableWhileTheLeaseIsLive() throws Exception {
        String tableId = registerTable(VECTOR_PROPERTIES);
        Path dir = tempDir();
        List<String> files = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            files.add(Files.write(dir.resolve("v-" + i + ".parquet"), new byte[] {1}).toUri().toString());
        }
        insertCommitAndPartition(tableId, files);
        lease(tableId, files.subList(0, 1));
        TtlBroadcastProcessFunction function = ttlFunction();

        assertFalse("a live lease must defer the partition drop", function.dropPartition(tableId, mainPartition));
        for (String file : files) {
            assertTrue("no file may be deleted while deferred: " + file,
                    Files.exists(Path.of(java.net.URI.create(file))));
        }
        assertEquals("partition metadata must stay", 1, countPartitions(tableId));
    }

    // --------------------------------------------------------------- discard gate

    @Test
    public void discardBatchOnlyNeedsFencesForVectorTables() throws Exception {
        String plain = registerTable(PLAIN_PROPERTIES);
        String vector = registerTable(VECTOR_PROPERTIES);
        Path dir = tempDir();
        String plainFile = Files.write(dir.resolve("d-plain.parquet"), new byte[] {1}).toUri().toString();
        String vectorFile = Files.write(dir.resolve("d-vector.parquet"), new byte[] {1}).toUri().toString();
        insertDiscard(plain, plainFile);
        insertDiscard(vector, vectorFile);

        assertFalse(
                "plain-table discard must not be fenced",
                DiscardFileDeleteFunction.needsRetirementFences(
                        connection, Collections.singletonList(plainFile)));
        assertTrue(
                "vector-table discard needs retirement fences",
                DiscardFileDeleteFunction.needsRetirementFences(
                        connection, Collections.singletonList(vectorFile)));
        assertFalse(
                "an unknown path must not force the guard",
                DiscardFileDeleteFunction.needsRetirementFences(
                        connection, Collections.singletonList("s3://bucket/missing.parquet")));
    }

    // ------------------------------------------------------------------ fixtures

    private TtlBroadcastProcessFunction ttlFunction() {
        return new TtlBroadcastProcessFunction(
                new MapStateDescriptor<>("ttl", Types.STRING, Types.INT), 1);
    }

    private String registerTable(String properties) throws Exception {
        String tableId = "clean_path_" + UUID.randomUUID().toString().replace("-", "");
        String tablePath = "/tmp/clean-path/" + tableId;
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "insert into table_info(table_id, table_namespace, table_name, table_path,"
                                + " table_schema, partitions, properties)"
                                + " values(?, 'default', ?, ?, ?, 'c2;c1', ?::json)")) {
            ps.setString(1, tableId);
            ps.setString(2, tableId);
            ps.setString(3, tablePath);
            ps.setString(4, TABLE_SCHEMA);
            ps.setString(5, properties);
            ps.executeUpdate();
        }
        tableIds.add(tableId);
        return tableId;
    }

    private Path tempDir() throws Exception {
        Path dir = Files.createTempDirectory("clean-path-");
        dirs.add(dir);
        return dir;
    }

    private void lease(String tableId, List<String> files) throws Exception {
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "insert into index_read_lease(lease_id,kind,index_prefix,shard_id,commit_id,"
                                + "generation,version,owner,data_files,expires_at)"
                                + " values(?,'snapshot',?,0,0,0,0,'clean-path-test',?,"
                                + " clock_timestamp() + interval '2 minutes')")) {
            ps.setObject(1, UUID.randomUUID());
            ps.setString(2, tableId);
            ps.setArray(3, connection.createArrayOf("text", files.toArray()));
            ps.executeUpdate();
        }
    }

    private void insertCommitAndPartition(String tableId, List<String> files) throws Exception {
        UUID commit = UUID.randomUUID();
        StringBuilder array = new StringBuilder("ARRAY[");
        for (int i = 0; i < files.size(); i++) {
            if (i > 0) {
                array.append(", ");
            }
            array.append("ROW(?, 'add', 1, 'id')::data_file_op");
        }
        array.append("]");
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "insert into data_commit_info(table_id, partition_desc, commit_id, file_ops,"
                                + " commit_op, committed, timestamp, domain)"
                                + " values(?, ?, ?, " + array + ", 'AppendCommit', true, ?, 'public')")) {
            int index = 1;
            ps.setString(index++, tableId);
            ps.setString(index++, mainPartition);
            ps.setObject(index++, commit);
            for (String file : files) {
                ps.setString(index++, file);
            }
            ps.setLong(index, System.currentTimeMillis());
            ps.executeUpdate();
        }
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "insert into partition_info(table_id, partition_desc, version, commit_op,"
                                + " timestamp, snapshot, domain)"
                                + " values(?, ?, 1, 'AppendCommit', ?, ?, 'public')")) {
            ps.setString(1, tableId);
            ps.setString(2, mainPartition);
            ps.setLong(3, System.currentTimeMillis());
            ps.setArray(4, connection.createArrayOf("uuid", new Object[] {commit}));
            ps.executeUpdate();
        }
    }

    private void insertDiscard(String tableId, String file) throws Exception {
        String tablePath = "/tmp/clean-path/" + tableId;
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "insert into discard_compressed_file_info(file_path, table_path,"
                                + " partition_desc) values(?, ?, ?)")) {
            ps.setString(1, file);
            ps.setString(2, tablePath);
            ps.setString(3, mainPartition);
            ps.executeUpdate();
        }
    }

    private int countFences(String path) throws Exception {
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "select count(*) from index_drop where kind = 'data_file'"
                                + " and index_prefix = ?")) {
            ps.setString(1, path);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private int countFencesForTableFiles(String tableId) throws Exception {
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "select count(*) from index_drop"
                                + " where index_prefix like '/tmp/clean-path/%'")) {
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private int countPartitions(String tableId) throws Exception {
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "select count(*) from partition_info where table_id = ?")) {
            ps.setString(1, tableId);
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
