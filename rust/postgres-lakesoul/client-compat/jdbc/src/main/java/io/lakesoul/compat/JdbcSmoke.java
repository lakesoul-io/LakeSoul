// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package io.lakesoul.compat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A deliberately small JDBC workflow test: connect, DDL, prepared batch insert, query, transaction
 * rollback, metadata discovery, fetch-size cursor, and UPSERT. It is not a replacement for the Rust
 * SQL contract suites; it proves that the PostgreSQL JDBC driver can execute the common import and
 * reporting paths.
 */
public final class JdbcSmoke {
    private static final int ROWS = 6;

    private JdbcSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                    "usage: JdbcSmoke <host> <port> <database> <user> <password> <table>");
        }
        String host = args[0];
        String port = args[1];
        String database = args[2];
        String user = args[3];
        String password = args[4];
        String table = args[5];

        String url =
                "jdbc:postgresql://"
                        + host
                        + ":"
                        + port
                        + "/"
                        + database
                        + "?sslmode=disable&ApplicationName=lakesoul-jdbc-compat";
        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            check(connection.isValid(2), "Connection.isValid");
            run(connection, table);
        }
        System.out.println("JDBC PASS table=" + table);
    }

    private static void run(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + table);
            statement.executeUpdate(
                    "CREATE TABLE "
                            + table
                            + " (id bigint primary key, name text, amount numeric(20,4))");
        }

        try (PreparedStatement insert =
                connection.prepareStatement(
                        "INSERT INTO " + table + " (id, name, amount) VALUES (?, ?, ?)")) {
            for (int i = 1; i <= ROWS; i++) {
                insert.setLong(1, i);
                insert.setString(2, "row-" + i);
                insert.setBigDecimal(3, new BigDecimal("12.3400"));
                insert.addBatch();
            }
            int[] counts = insert.executeBatch();
            check(counts.length == ROWS, "PreparedStatement.addBatch/executeBatch count");
        }

        try (PreparedStatement select =
                connection.prepareStatement(
                        "SELECT id, name, amount FROM " + table + " WHERE id = ?")) {
            select.setLong(1, 1);
            try (ResultSet result = select.executeQuery()) {
                check(result.next(), "prepared SELECT returned a row");
                check(result.getLong("id") == 1, "prepared SELECT id");
                check("row-1".equals(result.getString("name")), "prepared SELECT name");
                check(
                        new BigDecimal("12.3400").compareTo(result.getBigDecimal("amount")) == 0,
                        "prepared SELECT amount");
                check(!result.next(), "prepared SELECT returned one row");
            }
        }

        connection.setAutoCommit(false);
        try (PreparedStatement insert =
                connection.prepareStatement(
                        "INSERT INTO " + table + " (id, name, amount) VALUES (?, ?, ?)")) {
            insert.setLong(1, 100);
            insert.setString(2, "rolled-back");
            insert.setBigDecimal(3, BigDecimal.ONE);
            insert.executeUpdate();
        }
        connection.rollback();
        connection.setAutoCommit(true);
        check(countRows(connection, table) == ROWS, "transaction rollback count");

        DatabaseMetaData metadata = connection.getMetaData();
        check(hasTable(metadata, table), "DatabaseMetaData.getTables");
        check(hasColumn(metadata, table, "id"), "DatabaseMetaData.getColumns(id)");
        check(hasColumn(metadata, table, "name"), "DatabaseMetaData.getColumns(name)");
        check(hasPrimaryKey(metadata, table), "DatabaseMetaData.getPrimaryKeys");

        try (Statement statement = connection.createStatement()) {
            statement.setFetchSize(2);
            try (ResultSet result =
                    statement.executeQuery("SELECT id FROM " + table + " ORDER BY id")) {
                int seen = 0;
                while (result.next()) {
                    seen++;
                }
                check(seen == ROWS, "Statement.setFetchSize rows");
            }
        }

        try (PreparedStatement upsert =
                connection.prepareStatement(
                        "INSERT INTO "
                                + table
                                + " (id, name, amount) VALUES (?, ?, ?) "
                                + "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name")) {
            upsert.setLong(1, 1);
            upsert.setString(2, "updated");
            upsert.setBigDecimal(3, BigDecimal.ONE);
            upsert.executeUpdate();
        }
        check("updated".equals(nameOf(connection, table, 1)), "JDBC UPSERT");

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    private static int countRows(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("SELECT count(*) FROM " + table)) {
            check(result.next(), "count query returned a row");
            return result.getInt(1);
        }
    }

    private static String nameOf(Connection connection, String table, long id) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT name FROM " + table + " WHERE id = ?")) {
            statement.setLong(1, id);
            try (ResultSet result = statement.executeQuery()) {
                check(result.next(), "name lookup returned a row");
                return result.getString(1);
            }
        }
    }

    private static boolean hasTable(DatabaseMetaData metadata, String table) throws SQLException {
        try (ResultSet result = metadata.getTables(null, "public", table, new String[] {"TABLE"})) {
            return result.next();
        }
    }

    private static boolean hasColumn(DatabaseMetaData metadata, String table, String column)
            throws SQLException {
        try (ResultSet result = metadata.getColumns(null, "public", table, column)) {
            return result.next();
        }
    }

    private static boolean hasPrimaryKey(DatabaseMetaData metadata, String table)
            throws SQLException {
        try (ResultSet result = metadata.getPrimaryKeys(null, "public", table)) {
            return result.next();
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
