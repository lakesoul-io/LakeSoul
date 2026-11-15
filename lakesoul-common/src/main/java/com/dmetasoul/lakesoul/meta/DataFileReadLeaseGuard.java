// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Coordinates original-file cleanup with renewable native batch snapshots.
 *
 * <p>Admission takes the shared retention lock; retirement takes its exclusive
 * counterpart and the same table lock. A retirement fence is committed before
 * filesystem deletion, so admission remains blocked for those immutable paths
 * after the transaction lock is released. A failed delete keeps its fence for
 * retry. This connection must be dedicated to this cleanup scope and initially
 * in autocommit mode. Close always rolls back pending work and restores settings.
 */
public final class DataFileReadLeaseGuard implements AutoCloseable {
    private final Connection connection;
    private final String tableId;
    private final int previousIsolation;
    private final boolean leaseTableExists;
    private boolean locked;
    private boolean closed;

    public static DataFileReadLeaseGuard acquire(Connection connection, String tableId)
            throws SQLException {
        return new DataFileReadLeaseGuard(connection, tableId);
    }

    private DataFileReadLeaseGuard(Connection connection, String tableId) throws SQLException {
        if (!connection.getAutoCommit()) {
            throw new SQLException("file cleanup requires a dedicated autocommit connection");
        }
        this.connection = connection;
        this.tableId = tableId;
        this.previousIsolation = connection.getTransactionIsolation();
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        connection.setAutoCommit(false);
        boolean initialized = false;
        boolean leases = false;
        try {
            lock();
            try (PreparedStatement statement = connection.prepareStatement(
                    "select to_regclass('index_read_lease') is not null,"
                            + " to_regclass('index_drop') is not null");
                    ResultSet rows = statement.executeQuery()) {
                rows.next();
                leases = rows.getBoolean(1);
                if (!rows.getBoolean(2)) {
                    throw new SQLException("apply the LakeSoul index metadata migration before cleanup");
                }
            }
            if (leases) {
                expireRows();
            }
            initialized = true;
        } finally {
            if (!initialized) {
                try {
                    connection.rollback();
                } finally {
                    connection.setAutoCommit(true);
                    connection.setTransactionIsolation(previousIsolation);
                }
            }
        }
        this.leaseTableExists = leases;
    }

    private void lock() throws SQLException {
        if (locked) {
            return;
        }
        // Must match Rust index_read_lease.rs, including seed and lock order.
        try (PreparedStatement statement = connection.prepareStatement(
                "select pg_advisory_xact_lock(hashtextextended('lakesoul:data:retention', 73409521))")) {
            statement.execute();
        }
        if (tableId != null) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "select pg_advisory_xact_lock(hashtextextended(?, 73409521))")) {
                statement.setString(1, "lakesoul:data:" + tableId);
                statement.execute();
            }
        }
        locked = true;
        if (leaseTableExists) {
            expireRows();
        }
    }

    private void expireRows() throws SQLException {
        // DELETE waits for an in-flight heartbeat row lock and rechecks the
        // new expiry. A plain SELECT could read its old expired tuple while
        // renewal is still committing, allowing a live reader's file deletion.
        try (PreparedStatement statement = connection.prepareStatement(
                "delete from index_read_lease where expires_at <= clock_timestamp()")) {
            statement.executeUpdate();
        }
    }

    /** Conservative table-wide check used before removing partition metadata. */
    public boolean hasActiveReaders() throws SQLException {
        lock();
        if (!leaseTableExists || tableId == null) {
            return false;
        }
        String sql = "select exists (select 1 from index_read_lease l"
                + " where l.expires_at > clock_timestamp() and"
                + " ((l.kind = 'snapshot' and l.index_prefix = ?) or exists ("
                + " select 1 from data_commit_info d, unnest(d.file_ops) f,"
                + " unnest(l.data_files) p(path) where d.table_id = ? and"
                + " regexp_replace(regexp_replace(f.path, '^file:/+', '/'), '^s3a://', 's3://')"
                + " = regexp_replace(regexp_replace(p.path, '^file:/+', '/'), '^s3a://', 's3://'))))";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tableId);
            statement.setString(2, tableId);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getBoolean(1);
            }
        }
    }

    /** True for a leased path or a pinned snapshot/tag path, including descendants of a directory. */
    public boolean isProtected(String path, boolean directory) throws SQLException {
        lock();
        String canonical = normalizePath(path);
        String predicate = "(regexp_replace(regexp_replace(p.path, '^file:/+', '/'),"
                + " '^s3a://', 's3://') = ? or (? and left("
                + " regexp_replace(regexp_replace(p.path, '^file:/+', '/'), '^s3a://', 's3://'),"
                + " length(?) + 1) = ? || '/'))";
        if (leaseTableExists) {
            String sql = "select exists (select 1 from index_read_lease l,"
                    + " unnest(l.data_files) p(path) where l.expires_at > clock_timestamp() and "
                    + predicate + ")";
            if (matches(sql, canonical, directory)) {
                return true;
            }
        }
        String pinned = "select exists (select 1 from data_commit_info d, unnest(d.file_ops) p"
                + " where d.pinned = true and " + predicate + ")";
        return matches(pinned, canonical, directory);
    }

    private boolean matches(String sql, String path, boolean directory) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, path);
            statement.setBoolean(2, directory);
            statement.setString(3, path);
            statement.setString(4, path);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getBoolean(1);
            }
        }
    }

    /**
     * Atomically refuse protected paths or persist their retirement fence.
     * The successful return permits deleting that immutable file/directory.
     * Directory fences must only be used for immutable compaction directories,
     * never for reusable table or partition roots.
     */
    public boolean retire(String path, boolean immutableDirectory) throws SQLException {
        String canonical = normalizePath(path);
        if (canonical.isEmpty() || canonical.equals("/")
                || canonical.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://[^/]+$")) {
            throw new SQLException("refusing to retire a filesystem or object-store root");
        }
        if (isProtected(path, immutableDirectory)) {
            return false;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "insert into index_drop(kind,index_prefix,reason) values(?,?,?)"
                        + " on conflict(kind,index_prefix) do nothing")) {
            statement.setString(1, immutableDirectory ? "data_directory" : "data_file");
            statement.setString(2, normalizePath(path));
            statement.setString(3, "original data cleanup retirement fence");
            statement.executeUpdate();
        }
        // Fence first: a crash or filesystem failure can leak an immutable
        // object but can never admit a new snapshot of a deleted object.
        commit();
        return true;
    }

    /** Commit cleanup metadata only after the corresponding filesystem work succeeds. */
    public void commit() throws SQLException {
        connection.commit();
        locked = false;
    }

    public static String normalizePath(String path) {
        String normalized = path.replaceFirst("^file:/+", "/")
                .replaceFirst("^s3a://", "s3://");
        while (normalized.endsWith("/") && normalized.length() > 1) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            connection.rollback();
        } finally {
            try {
                connection.setAutoCommit(true);
            } finally {
                connection.setTransactionIsolation(previousIsolation);
            }
        }
    }
}
