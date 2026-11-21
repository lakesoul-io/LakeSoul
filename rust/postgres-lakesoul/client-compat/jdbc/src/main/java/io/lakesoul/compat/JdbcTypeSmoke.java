// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package io.lakesoul.compat;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.UUID;

/** JDBC coverage for the semantic types and the type metadata drivers rely on. */
public final class JdbcTypeSmoke {
    private JdbcTypeSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                    "usage: JdbcTypeSmoke <host> <port> <database> <user> <password> <table>");
        }
        String url =
                "jdbc:postgresql://"
                        + args[0]
                        + ":"
                        + args[1]
                        + "/"
                        + args[2]
                        + "?sslmode=disable&ApplicationName=lakesoul-jdbc-types-compat";
        String table = args[5] + "_types";
        try (Connection connection = DriverManager.getConnection(url, args[3], args[4])) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE IF EXISTS " + table);
                statement.executeUpdate(
                        "CREATE TABLE "
                                + table
                                + " (id bigint primary key, u uuid, ua uuid[], m money, "
                                + "n numeric(60,10), v varchar(4))");
            }

            UUID uuid = UUID.fromString("11111111-2222-3333-4444-555555555555");
            BigDecimal big = new BigDecimal("123456789012345678901234567890.1234567890");
            try (PreparedStatement insert =
                    connection.prepareStatement(
                            "INSERT INTO "
                                    + table
                                    + " (id, u, ua, m, n, v) VALUES (?, ?, ?, ?, ?, ?)")) {
                ParameterMetaData parameters = insert.getParameterMetaData();
                check("uuid".equals(parameters.getParameterTypeName(2)), "uuid parameter type");
                check("money".equals(parameters.getParameterTypeName(4)), "money parameter type");
                check(
                        "numeric".equals(parameters.getParameterTypeName(5)),
                        "numeric parameter type");
                insert.setLong(1, 1);
                insert.setObject(2, uuid);
                insert.setArray(
                        3, connection.createArrayOf("uuid", new UUID[] {uuid, UUID.randomUUID()}));
                insert.setBigDecimal(4, new BigDecimal("123.45"));
                insert.setBigDecimal(5, big);
                insert.setString(6, "abcd");
                check(insert.executeUpdate() == 1, "semantic types insert");
            }

            try (PreparedStatement select =
                    connection.prepareStatement("SELECT u, ua, m, n, v FROM " + table)) {
                ResultSetMetaData metadata = select.getMetaData();
                check("uuid".equals(metadata.getColumnTypeName(1)), "uuid column type");
                // JDBC reports the pg_type name of the array type, which is
                // PostgreSQL's internal `_uuid` spelling.
                check(
                        "_uuid".equals(metadata.getColumnTypeName(2)),
                        "uuid[] column type: " + metadata.getColumnTypeName(2));
                check("money".equals(metadata.getColumnTypeName(3)), "money column type");
                check("numeric".equals(metadata.getColumnTypeName(4)), "numeric column type");
                check(
                        "varchar".equals(metadata.getColumnTypeName(5)),
                        "varchar column type: " + metadata.getColumnTypeName(5));
                try (ResultSet result = select.executeQuery()) {
                    check(result.next(), "semantic types row");
                    check(uuid.equals(result.getObject(1, UUID.class)), "uuid value");
                    Array array = result.getArray(2);
                    check(
                            ((UUID[]) array.getArray())[0].equals(uuid),
                            "uuid[] first element");
                    check("$123.45".equals(result.getString(3)), "money value");
                    check(big.compareTo(result.getBigDecimal(4)) == 0, "large numeric value");
                    check("abcd".equals(result.getString(5)), "varchar value");
                    check(!result.next(), "only one semantic types row");
                }
            }
        }
        System.out.println("JDBC TYPES PASS table=" + table);
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
