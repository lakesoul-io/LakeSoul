// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** PostgreSQL-only integration tests; no JNI, Spark, or Flink runtime required. */
public class DataFileReadLeaseGuardTest {
    private final String tableId = "read_lease_cleaner_" + UUID.randomUUID();
    private final UUID leaseId = UUID.randomUUID();
    private Connection connection;
    private Path directory;
    private Path file;

    private Connection connect() throws Exception {
        return DriverManager.getConnection(
                System.getenv()
                        .getOrDefault(
                                "LAKESOUL_PG_URL",
                                "jdbc:postgresql://127.0.0.1:5432/lakesoul_test"),
                System.getenv().getOrDefault("LAKESOUL_PG_USERNAME", "lakesoul_test"),
                System.getenv().getOrDefault("LAKESOUL_PG_PASSWORD", "lakesoul_test"));
    }

    @Before
    public void setup() throws Exception {
        Class.forName("org.postgresql.Driver");
        connection = connect();
        directory = Files.createTempDirectory("read-lease-cleaner-");
        file = Files.write(directory.resolve("vectors.parquet"), new byte[] {1, 2, 3});
    }

    private void pin(String path) throws Exception {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "insert into"
                            + " index_read_lease(lease_id,kind,index_prefix,shard_id,commit_id,generation,version,owner,data_files,expires_at)"
                            + " values(?,'snapshot',?,0,0,0,0,'junit-cleaner',?,clock_timestamp() +"
                            + " interval '2 minutes')")) {
            statement.setObject(1, leaseId);
            statement.setString(2, tableId);
            statement.setArray(3, connection.createArrayOf("text", new String[] {path}));
            statement.executeUpdate();
        }
    }

    private boolean hasFence(String path, String kind) throws Exception {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "select 1 from index_drop where kind = ? and index_prefix = ?")) {
            statement.setString(1, kind);
            statement.setString(2, DataFileReadLeaseGuard.normalizePath(path));
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    @After
    public void cleanup() throws Exception {
        if (connection != null) {
            if (!connection.getAutoCommit()) {
                connection.rollback();
                connection.setAutoCommit(true);
            }
            try (PreparedStatement statement =
                    connection.prepareStatement(
                            "delete from index_read_lease where lease_id = ?")) {
                statement.setObject(1, leaseId);
                statement.executeUpdate();
            }
            try (PreparedStatement statement =
                    connection.prepareStatement(
                            "delete from data_commit_info where table_id = ?")) {
                statement.setString(1, tableId);
                statement.executeUpdate();
            }
            if (directory != null) {
                String root = DataFileReadLeaseGuard.normalizePath(directory.toString());
                try (PreparedStatement statement =
                        connection.prepareStatement(
                                "delete from index_drop where kind in"
                                        + " ('data_file','data_directory') and (index_prefix = ? or"
                                        + " left(index_prefix,length(?) + 1) = ? || '/')")) {
                    statement.setString(1, root);
                    statement.setString(2, root);
                    statement.setString(3, root);
                    statement.executeUpdate();
                }
            }
            connection.close();
        }
        if (directory != null) {
            assertTrue(
                    directory.toAbsolutePath().toString().startsWith("/tmp/read-lease-cleaner-"));
            try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(
                                path -> {
                                    try {
                                        Files.deleteIfExists(path);
                                    } catch (IOException error) {
                                        throw new RuntimeException(error);
                                    }
                                });
            }
        }
    }

    @Test
    public void liveSnapshotSkipsRetirementAndKeepsFile() throws Exception {
        pin(file.toUri().toString());
        int isolation = connection.getTransactionIsolation();
        try (DataFileReadLeaseGuard guard = DataFileReadLeaseGuard.acquire(connection, tableId)) {
            assertTrue(guard.hasActiveReaders());
            assertTrue(guard.isProtected(file.toString(), false));
            assertFalse(guard.retire(file.toString(), false));
        }
        assertTrue(connection.getAutoCommit());
        assertEquals(isolation, connection.getTransactionIsolation());
        assertTrue(Files.exists(file));
        assertFalse(hasFence(file.toString(), "data_file"));
    }

    @Test
    public void failedFilesystemDeletionKeepsCommittedFenceForRetry() throws Exception {
        try {
            try (DataFileReadLeaseGuard guard =
                    DataFileReadLeaseGuard.acquire(connection, tableId)) {
                assertTrue(guard.retire(file.toUri().toString(), false));
                throw new IOException("simulated object store delete failure");
            }
        } catch (IOException expected) {
            assertEquals("simulated object store delete failure", expected.getMessage());
        }
        assertTrue(Files.exists(file));
        assertTrue(hasFence(file.toString(), "data_file"));
        try (DataFileReadLeaseGuard guard = DataFileReadLeaseGuard.acquire(connection, tableId)) {
            assertTrue(guard.retire(file.toString(), false));
            Files.delete(file);
        }
        assertFalse(Files.exists(file));
        assertTrue(hasFence(file.toString(), "data_file"));
    }

    @Test
    public void pendingPhysicalDeletionCannotAdmitNewSnapshot() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        try (DataFileReadLeaseGuard guard = DataFileReadLeaseGuard.acquire(connection, tableId)) {
            Future<Boolean> admission =
                    executor.submit(
                            () -> {
                                try (Connection reader = connect()) {
                                    reader.setTransactionIsolation(
                                            Connection.TRANSACTION_READ_COMMITTED);
                                    reader.setAutoCommit(false);
                                    entered.countDown();
                                    try (PreparedStatement lock =
                                            reader.prepareStatement(
                                                    "select"
                                                        + " pg_advisory_xact_lock_shared(hashtextextended('lakesoul:data:retention',73409521))")) {
                                        lock.execute();
                                    }
                                    try (PreparedStatement check =
                                            reader.prepareStatement(
                                                    "select 1 from index_drop where kind ="
                                                        + " 'data_file' and index_prefix = ?")) {
                                        check.setString(1, file.toString());
                                        try (ResultSet rows = check.executeQuery()) {
                                            boolean allowed = !rows.next();
                                            reader.rollback();
                                            return allowed;
                                        }
                                    }
                                }
                            });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            try {
                admission.get(100, TimeUnit.MILLISECONDS);
                fail("snapshot acquisition must wait for the cleanup transaction");
            } catch (TimeoutException expected) {
                // The admission transaction is blocked on the exact Rust lock.
            }
            assertTrue(guard.retire(file.toString(), false));
            assertFalse(admission.get(2, TimeUnit.SECONDS));
            assertTrue("filesystem deletion is still pending", Files.exists(file));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void directoryProtectionUsesSegmentBoundaryAndUriNormalization() throws Exception {
        Path first = directory.resolve("compact_1");
        Path sibling = directory.resolve("compact_10");
        pin(sibling.resolve("child.parquet").toUri().toString());
        try (DataFileReadLeaseGuard guard = DataFileReadLeaseGuard.acquire(connection, tableId)) {
            assertFalse(guard.isProtected(first.toString(), true));
            assertTrue(guard.isProtected(sibling.toString(), true));
            assertTrue(guard.retire(first.toString(), true));
            assertFalse(guard.retire(sibling.toString(), true));
        }
        assertTrue(hasFence(first.toString(), "data_directory"));
        assertFalse(hasFence(sibling.toString(), "data_directory"));
    }

    @Test
    public void cleanupWaitsForHeartbeatBeforeCollectingExpiredTuple() throws Exception {
        pin(file.toUri().toString());
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "update index_read_lease set expires_at = clock_timestamp() - interval '1"
                                + " second' where lease_id = ?")) {
            statement.setObject(1, leaseId);
            statement.executeUpdate();
        }
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection heartbeat = connect()) {
            heartbeat.setAutoCommit(false);
            try (PreparedStatement statement =
                    heartbeat.prepareStatement(
                            "update index_read_lease set expires_at = clock_timestamp() + interval"
                                    + " '2 minutes' where lease_id = ?")) {
                statement.setObject(1, leaseId);
                statement.executeUpdate();
            }
            Future<Boolean> cleanup =
                    executor.submit(
                            () -> {
                                try (Connection cleaner = connect();
                                        DataFileReadLeaseGuard guard =
                                                DataFileReadLeaseGuard.acquire(cleaner, tableId)) {
                                    return guard.retire(file.toString(), false);
                                }
                            });
            try {
                cleanup.get(100, TimeUnit.MILLISECONDS);
                fail("cleanup must wait for the in-flight heartbeat");
            } catch (TimeoutException expected) {
                // The cleaner waits for the update and rechecks its expiry.
            }
            heartbeat.commit();
            assertFalse(cleanup.get(2, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
        assertTrue(Files.exists(file));
        assertFalse(hasFence(file.toString(), "data_file"));
    }

    @Test
    public void refusesFilesystemAndBucketRoots() throws Exception {
        try (DataFileReadLeaseGuard guard = DataFileReadLeaseGuard.acquire(connection, tableId)) {
            assertThrows(java.sql.SQLException.class, () -> guard.retire("/", true));
            assertThrows(java.sql.SQLException.class, () -> guard.retire("s3://bucket/", true));
        }
    }

    @Test
    public void snapshotTagPinsRemainProtected() throws Exception {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "insert into"
                            + " data_commit_info(table_id,partition_desc,commit_id,file_ops,commit_op,committed,timestamp,pinned)"
                            + " values(?,'-5',?,ARRAY[ROW(?,'add',1,'id')::data_file_op],'AppendCommit',true,0,true)")) {
            statement.setString(1, tableId);
            statement.setObject(2, UUID.randomUUID());
            statement.setString(3, file.toUri().toString());
            statement.executeUpdate();
        }
        try (DataFileReadLeaseGuard guard = DataFileReadLeaseGuard.acquire(connection, tableId)) {
            assertFalse(guard.retire(file.toString(), false));
        }
        assertTrue(Files.exists(file));
    }
}
