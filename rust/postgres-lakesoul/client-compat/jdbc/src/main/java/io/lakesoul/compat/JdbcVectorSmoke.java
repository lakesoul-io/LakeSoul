// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package io.lakesoul.compat;

import com.pgvector.PGvector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;

/** JDBC coverage for the pgvector-compatible wire type. */
public final class JdbcVectorSmoke {
    private JdbcVectorSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6 && args.length != 7) {
            throw new IllegalArgumentException(
                    "usage: JdbcVectorSmoke <host> <port> <database> <user> <password> <table>"
                            + " [sslmode]");
        }
        String sslmode = args.length == 7 ? args[6] : "disable";
        String url =
                "jdbc:postgresql://"
                        + args[0]
                        + ":"
                        + args[1]
                        + "/"
                        + args[2]
                        + "?sslmode="
                        + sslmode
                        + "&ApplicationName=lakesoul-jdbc-pgvector-compat";
        String vectorTable =
                System.getenv().getOrDefault("CLIENT_COMPAT_VECTOR_TABLE", "client_compat_vector");
        try (Connection connection = DriverManager.getConnection(url, args[3], args[4])) {
            // The real pgvector JDBC adapter resolves the type name through
            // pg_type/to_regtype, which LakeSoul exposes without ./extension.
            PGvector.addVectorType(connection);
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET ivfflat.probes = 4");
            }

            try (PreparedStatement select =
                    connection.prepareStatement(
                            "SELECT id, vec FROM " + vectorTable + " ORDER BY id")) {
                try (ResultSet result = select.executeQuery()) {
                    check(result.next(), "first vector row");
                    check(result.getLong("id") == 1, "first vector id");
                    PGvector value = (PGvector) result.getObject("vec");
                    check(
                            Arrays.equals(value.toArray(), new float[] {1.0f, 2.0f, 3.0f}),
                            "first vector value");
                    ResultSetMetaData metadata = result.getMetaData();
                    check(
                            "vector".equalsIgnoreCase(metadata.getColumnTypeName(2)),
                            "vector column type name: " + metadata.getColumnTypeName(2));
                    check(result.next(), "second vector row");
                    check(
                            Arrays.equals(
                                    ((PGvector) result.getObject("vec")).toArray(),
                                    new float[] {4.0f, 5.0f, 6.0f}),
                            "second vector value");
                    check(!result.next(), "only two vector rows");
                }
            }

            try (PreparedStatement nearest =
                    connection.prepareStatement(
                            "SELECT id FROM " + vectorTable + " ORDER BY vec <-> ? LIMIT 2")) {
                nearest.setObject(1, new PGvector(new float[] {1.0f, 2.0f, 3.0f}));
                try (ResultSet result = nearest.executeQuery()) {
                    check(result.next(), "nearest row");
                    check(result.getLong(1) == 1, "nearest id");
                    check(result.next(), "second nearest row");
                    check(result.getLong(1) == 2, "second nearest id");
                    check(!result.next(), "only two nearest rows");
                }
            }
        }
        System.out.println("JDBC PGvector PASS table=" + vectorTable);
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
