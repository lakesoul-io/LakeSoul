// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Coordinates original-file cleanup with renewable native batch snapshots.
 *
 * <p>Only tables that declare a vector index column need this coordination:
 * {@link #acquireOrNull(Connection, String)} returns {@code null} for every other
 * table, so an ordinary lakehouse clean keeps its pre-existing code path with no
 * metadata round trips at all. The same happens when the index metadata
 * migration is absent, so a rolling upgrade can never fail cleanup.
 *
 * <p>Retirement is batched: one protection probe for the whole batch, one
 * multi-row fence insert, one commit. The fence is committed before the caller
 * deletes the filesystem objects, so a crash can leak an immutable object but
 * can never admit a new snapshot of a deleted object. A failed delete keeps its
 * fence for retry.
 *
 * <p>This connection must be dedicated to this cleanup scope and initially in
 * autocommit mode. Close always rolls back pending work and restores settings.
 */
public final class DataFileReadLeaseGuard implements AutoCloseable {
    private static final String FENCE_REASON = "original data cleanup retirement fence";
    private static final String CANONICAL =
            "regexp_replace(regexp_replace(%s, '^file:/+', '/'), '^s3a://', 's3://')";

    private final Connection connection;
    private final String tableId;
    private final int previousIsolation;
    private final boolean leaseTableExists;
    private boolean locked;
    private boolean closed;

    /**
     * A guard for a cleanup that must protect original files, or {@code null}
     * when this table needs no protection (no vector index column, unknown
     * table, or the index metadata migration is not applied yet).
     */
    public static DataFileReadLeaseGuard acquireOrNull(Connection connection, String tableId)
            throws SQLException {
        if (tableId == null || !tableHasVectorIndex(connection, tableId)) {
            return null;
        }
        return acquire(connection, tableId);
    }

    /**
     * A guard for a fence-only cleanup (for example discarded compaction files
     * that belong to no resolved table), or {@code null} when the migration is
     * absent.
     */
    public static DataFileReadLeaseGuard acquireFenceOnlyOrNull(Connection connection)
            throws SQLException {
        if (!fenceTableExists(connection)) {
            return null;
        }
        return new DataFileReadLeaseGuard(connection, null);
    }

    /**
     * Strict variant: requires the index metadata migration and does not gate on
     * the table's vector configuration.
     */
    public static DataFileReadLeaseGuard acquire(Connection connection, String tableId)
            throws SQLException {
        return new DataFileReadLeaseGuard(connection, tableId);
    }

    /**
     * Whether the table declares at least one non-empty {@code vector_index_columns}
     * entry. Uses the primary-key lookup of {@code table_info}; safe to call per
     * table/partition.
     */
    public static boolean tableHasVectorIndex(Connection connection, String tableId)
            throws SQLException {
        if (tableId == null) {
            return false;
        }
        String sql = "select coalesce(case"
                + " when jsonb_typeof((properties::jsonb) -> 'vector_index_columns') = 'array'"
                + " then jsonb_array_length((properties::jsonb) -> 'vector_index_columns')"
                + " else 0 end, 0) > 0"
                + " from table_info where table_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tableId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() && rows.getBoolean(1);
            }
        }
    }

    private static boolean fenceTableExists(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select to_regclass('index_drop') is not null");
                ResultSet rows = statement.executeQuery()) {
            rows.next();
            return rows.getBoolean(1);
        }
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
                + " " + String.format(CANONICAL, "f.path") + " = "
                + String.format(CANONICAL, "p.path") + ")))";
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
        List<String> protectedPaths = isProtectedBatch(
                Collections.singletonList(path), directory);
        return !protectedPaths.isEmpty();
    }

    /**
     * Subset of {@code paths} whose immutable objects must not be deleted yet:
     * referenced by a live read lease or by a pinned commit of this table.
     *
     * <p>Two cheap probes run first (live leases, pinned commits) so a clean of a
     * vector table without readers costs a few milliseconds regardless of batch
     * size. The detailed probe runs only when protection can exist.
     */
    public List<String> isProtectedBatch(List<String> paths, boolean directory)
            throws SQLException {
        List<String> result = new ArrayList<>();
        if (paths == null || paths.isEmpty()) {
            return result;
        }
        lock();
        if (!hasAnyLiveLease() && !hasAnyPinnedCommit()) {
            return result;
        }
        String canonicalPath = String.format(CANONICAL, "lp.path");
        String canonicalCommit = String.format(CANONICAL, "f.path");
        String descendant = directory
                ? " or left(%s, length(w.path) + 1) = w.path || '/'"
                : "";
        String sql = "select distinct w.path from unnest(?::text[]) as w(path) where"
                + " exists (select 1 from index_read_lease l"
                + "   cross join lateral unnest(l.data_files) as lp(path)"
                + "   where l.expires_at > clock_timestamp() and ("
                + canonicalPath + " = w.path"
                + String.format(descendant, canonicalPath) + "))"
                + " or exists (select 1 from data_commit_info d"
                + "   cross join lateral unnest(d.file_ops) as f"
                + "   where d.table_id = ? and d.pinned = true and ("
                + canonicalCommit + " = w.path"
                + String.format(descendant, canonicalCommit) + "))";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setArray(1, textArray(paths));
            statement.setString(2, tableId);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    result.add(rows.getString(1));
                }
            }
        }
        return result;
    }

    private boolean hasAnyLiveLease() throws SQLException {
        if (!leaseTableExists) {
            return false;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "select exists (select 1 from index_read_lease where expires_at > clock_timestamp())");
                ResultSet rows = statement.executeQuery()) {
            rows.next();
            return rows.getBoolean(1);
        }
    }

    private boolean hasAnyPinnedCommit() throws SQLException {
        if (tableId == null) {
            return false;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "select exists (select 1 from data_commit_info where table_id = ? and pinned = true)")) {
            statement.setString(1, tableId);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getBoolean(1);
            }
        }
    }

    /**
     * Atomically persist retirement fences for every unprotected path, commit
     * them once, and return the paths the caller may now delete. Protected paths
     * are left out. One round-trip amortises the whole batch.
     */
    public List<String> retireBatch(List<String> files, List<String> directories)
            throws SQLException {
        List<String> canonicalFiles = canonicalize(files);
        List<String> canonicalDirectories = canonicalize(directories);
        Set<String> protectedPaths = new LinkedHashSet<>(
                isProtectedBatch(canonicalFiles, false));
        protectedPaths.addAll(isProtectedBatch(canonicalDirectories, true));
        List<String> allowed = new ArrayList<>(canonicalFiles.size() + canonicalDirectories.size());
        for (String path : canonicalFiles) {
            if (!protectedPaths.contains(path)) {
                allowed.add(path);
            }
        }
        List<String> allowedDirectories = new ArrayList<>(canonicalDirectories.size());
        for (String path : canonicalDirectories) {
            if (!protectedPaths.contains(path)) {
                allowedDirectories.add(path);
            }
        }
        if (!allowed.isEmpty() || !allowedDirectories.isEmpty()) {
            insertFences(allowed, allowedDirectories);
        }
        // Fence first: a crash or filesystem failure can leak an immutable
        // object but can never admit a new snapshot of a deleted object.
        commit();
        List<String> result = new ArrayList<>(allowed.size() + allowedDirectories.size());
        result.addAll(allowed);
        result.addAll(allowedDirectories);
        return result;
    }

    private void insertFences(List<String> files, List<String> directories) throws SQLException {
        String sql = "insert into index_drop(kind,index_prefix,reason)"
                + " select k, p, '" + FENCE_REASON + "'"
                + " from unnest(?::text[], ?::text[]) as t(k, p)"
                + " on conflict(kind,index_prefix) do nothing";
        List<String> kinds = new ArrayList<>(files.size() + directories.size());
        List<String> paths = new ArrayList<>(files.size() + directories.size());
        for (String path : files) {
            kinds.add("data_file");
            paths.add(path);
        }
        for (String path : directories) {
            kinds.add("data_directory");
            paths.add(path);
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setArray(1, textArray(kinds));
            statement.setArray(2, textArray(paths));
            statement.executeUpdate();
        }
    }

    private List<String> canonicalize(List<String> paths) throws SQLException {
        List<String> canonical = new ArrayList<>(paths == null ? 0 : paths.size());
        if (paths == null) {
            return canonical;
        }
        for (String path : paths) {
            String normalized = normalizePath(path);
            if (normalized.isEmpty() || normalized.equals("/")
                    || normalized.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://[^/]+$")) {
                throw new SQLException("refusing to retire a filesystem or object-store root");
            }
            if (!canonical.contains(normalized)) {
                canonical.add(normalized);
            }
        }
        return canonical;
    }

    /** Single-path retirement kept for API compatibility; prefer {@link #retireBatch}. */
    public boolean retire(String path, boolean immutableDirectory) throws SQLException {
        List<String> allowed = immutableDirectory
                ? retireBatch(Collections.emptyList(), Collections.singletonList(path))
                : retireBatch(Collections.singletonList(path), Collections.emptyList());
        return !allowed.isEmpty();
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

    private Array textArray(List<String> values) throws SQLException {
        return connection.createArrayOf("text", values.toArray(new String[0]));
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
