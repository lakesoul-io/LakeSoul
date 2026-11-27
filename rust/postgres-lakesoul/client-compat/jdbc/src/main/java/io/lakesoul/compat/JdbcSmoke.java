// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package io.lakesoul.compat;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
        if (args.length != 6 && args.length != 7) {
            throw new IllegalArgumentException(
                    "usage: JdbcSmoke <host> <port> <database> <user> <password> <table>"
                            + " [sslmode]");
        }
        String host = args[0];
        String port = args[1];
        String database = args[2];
        String user = args[3];
        String password = args[4];
        String table = args[5];
        String sslmode = args.length == 7 ? args[6] : "disable";

        String url =
                "jdbc:postgresql://"
                        + host
                        + ":"
                        + port
                        + "/"
                        + database
                        + "?sslmode="
                        + sslmode
                        + "&ApplicationName=lakesoul-jdbc-compat";
        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            check(connection.isValid(2), "Connection.isValid");
            run(connection, table);
        }
        System.out.println("JDBC PASS table=" + table);
    }

    private static void run(Connection connection, String table) throws SQLException, IOException {
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

        String jsonTable = table + "_jsonb";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + jsonTable);
            statement.executeUpdate(
                    "CREATE TABLE " + jsonTable + " (id bigint primary key, payload jsonb)");
        }
        try (PreparedStatement insert =
                connection.prepareStatement(
                        "INSERT INTO " + jsonTable + " (id, payload) VALUES (?, ?)")) {
            org.postgresql.util.PGobject payload = new org.postgresql.util.PGobject();
            payload.setType("jsonb");
            payload.setValue("{\"b\": 2, \"a\": 1}");
            insert.setLong(1, 1);
            insert.setObject(2, payload);
            insert.executeUpdate();
        }
        try (PreparedStatement select =
                connection.prepareStatement(
                        "SELECT payload, payload->>'a', payload ?? 'b' FROM "
                                + jsonTable
                                + " WHERE id = ?")) {
            select.setLong(1, 1);
            try (ResultSet result = select.executeQuery()) {
                check(result.next(), "jsonb SELECT returned a row");
                check("{\"a\":1,\"b\":2}".equals(result.getString(1)), "jsonb canonical text");
                check("1".equals(result.getString(2)), "jsonb ->> value");
                check(result.getBoolean(3), "jsonb ? value");
            }
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE " + jsonTable);
        }

        String deleteTable = table + "_delete";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + deleteTable);
            statement.executeUpdate(
                    "CREATE TABLE " + deleteTable + " (id bigint primary key, val bigint)");
            statement.executeUpdate(
                    "INSERT INTO " + deleteTable + " VALUES (1, 10), (2, 20), (3, 30)");
            check(
                    statement.executeUpdate("DELETE FROM " + deleteTable + " WHERE id = 2") == 1,
                    "DELETE count");
        }
        try (PreparedStatement select =
                connection.prepareStatement("SELECT id FROM " + deleteTable + " ORDER BY id")) {
            try (ResultSet result = select.executeQuery()) {
                check(result.next() && result.getLong(1) == 1, "DELETE surviving row 1");
                check(result.next() && result.getLong(1) == 3, "DELETE surviving row 3");
                check(!result.next(), "DELETE left two rows");
            }
        }
        try (PreparedStatement delete =
                connection.prepareStatement(
                        "DELETE FROM " + deleteTable + " WHERE id = ? RETURNING id, val")) {
            delete.setLong(1, 3);
            try (ResultSet result = delete.executeQuery()) {
                check(result.next(), "DELETE RETURNING returned a row");
                check(result.getLong(1) == 3 && result.getLong(2) == 30, "DELETE RETURNING values");
            }
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("TRUNCATE TABLE " + deleteTable);
            try (ResultSet result = statement.executeQuery("SELECT count(*) FROM " + deleteTable)) {
                check(result.next() && result.getLong(1) == 0, "TRUNCATE count");
            }
            statement.executeUpdate("DROP TABLE " + deleteTable);
        }

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

        CopyManager copy = connection.unwrap(PGConnection.class).getCopyAPI();
        long textRows =
                copy.copyIn(
                        "COPY " + table + " (id, name) FROM STDIN",
                        new ByteArrayInputStream(
                                "200\tcopy-text\n201\t\\N\n".getBytes(StandardCharsets.UTF_8)));
        check(textRows == 2, "CopyManager.copyIn text rows");
        check("copy-text".equals(nameOf(connection, table, 200)), "CopyManager text value");
        check(nameOfOrNull(connection, table, 201) == null, "CopyManager text NULL");

        long csvRows =
                copy.copyIn(
                        "COPY " + table + " (id, name) FROM STDIN WITH (FORMAT csv, HEADER true)",
                        new ByteArrayInputStream(
                                "id,name\n202,\"copied, csv\"\n".getBytes(StandardCharsets.UTF_8)));
        check(csvRows == 1, "CopyManager.copyIn csv rows");
        check("copied, csv".equals(nameOf(connection, table, 202)), "CopyManager csv value");

        long binaryRows =
                copy.copyIn(
                        "COPY " + table + " (id, name) FROM STDIN WITH (FORMAT binary)",
                        new ByteArrayInputStream(binaryCopyStream()));
        check(binaryRows == 2, "CopyManager.copyIn binary rows");
        check("copy-binary".equals(nameOf(connection, table, 300)), "CopyManager binary value");
        check(nameOfOrNull(connection, table, 301) == null, "CopyManager binary NULL");

        try {
            copy.copyIn(
                    "COPY " + table + " (id, name) FROM STDIN",
                    new ByteArrayInputStream("1\tduplicate\n".getBytes(StandardCharsets.UTF_8)));
            check(false, "a duplicate-key COPY must fail");
        } catch (SQLException expected) {
            check("23505".equals(expected.getSQLState()), "CopyManager duplicate SQLSTATE");
        }

        String outTable = table + "_out";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + outTable);
            statement.executeUpdate(
                    "CREATE TABLE " + outTable + " (id bigint primary key, name text)");
            statement.executeUpdate(
                    "INSERT INTO " + outTable + " (id, name) VALUES (1, 'alice'), (2, 'bob')");
        }
        StringWriter csv = new StringWriter();
        long outRows =
                copy.copyOut(
                        "COPY " + outTable + " (id, name) TO STDOUT WITH (FORMAT csv, HEADER true)",
                        csv);
        check(outRows == 2, "CopyManager.copyOut csv rows");
        String[] lines = csv.toString().split("\n");
        check(lines.length == 3 && "id,name".equals(lines[0]), "CopyManager csv header");
        java.util.Arrays.sort(lines, 1, lines.length);
        check("1,alice".equals(lines[1]), "CopyManager csv row 1");
        check("2,bob".equals(lines[2]), "CopyManager csv row 2");

        StringWriter text = new StringWriter();
        check(
                copy.copyOut("COPY " + outTable + " (id, name) TO STDOUT", text) == 2,
                "CopyManager.copyOut text rows");
        String[] textLines = text.toString().split("\n");
        java.util.Arrays.sort(textLines);
        check("1\talice".equals(textLines[0]), "CopyManager text row 1");
        check("2\tbob".equals(textLines[1]), "CopyManager text row 2");

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + outTable);
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

    /** A two-row PostgreSQL binary COPY stream with one NULL field. */
    private static byte[] binaryCopyStream() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(new byte[] {'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xff, '\r', '\n', 0});
        out.writeBytes(ByteBuffer.allocate(4).putInt(0).array());
        out.writeBytes(ByteBuffer.allocate(4).putInt(0).array());
        binaryCopyRow(out, 300L, "copy-binary");
        binaryCopyRow(out, 301L, null);
        out.writeBytes(ByteBuffer.allocate(2).putShort((short) -1).array());
        return out.toByteArray();
    }

    private static void binaryCopyRow(ByteArrayOutputStream out, long id, String name)
            throws IOException {
        out.writeBytes(ByteBuffer.allocate(2).putShort((short) 2).array());
        byte[] idBytes = ByteBuffer.allocate(8).putLong(id).array();
        out.writeBytes(ByteBuffer.allocate(4).putInt(idBytes.length).array());
        out.writeBytes(idBytes);
        if (name == null) {
            out.writeBytes(ByteBuffer.allocate(4).putInt(-1).array());
        } else {
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            out.writeBytes(ByteBuffer.allocate(4).putInt(nameBytes.length).array());
            out.writeBytes(nameBytes);
        }
    }

    private static String nameOfOrNull(Connection connection, String table, long id)
            throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT name FROM " + table + " WHERE id = ?")) {
            statement.setLong(1, id);
            try (ResultSet result = statement.executeQuery()) {
                check(result.next(), "name lookup returned a row");
                return result.getString(1);
            }
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
