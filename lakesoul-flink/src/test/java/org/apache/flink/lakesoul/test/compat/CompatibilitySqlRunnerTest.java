// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.compat;

import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.lakesoul.tool.JobOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class CompatibilitySqlRunnerTest extends AbstractTestBase {

    @Test
    public void runCompatibilitySql() throws Exception {
        String sqlFile = System.getProperty("lakesoul.compat.sqlFile", "");
        Assume.assumeTrue("lakesoul.compat.sqlFile is required", !sqlFile.isEmpty());

        TableEnvironment env = LakeSoulTestUtils.createTableEnvInBatchMode();
        configureObjectStore(env.getConfig().getConfiguration());
        env.registerCatalog("lakesoul", new LakeSoulCatalog());
        env.useCatalog("lakesoul");
        env.useDatabase("default");

        List<String> statements = splitStatements(Files.readString(Path.of(sqlFile), StandardCharsets.UTF_8));
        String outputFile = System.getProperty("lakesoul.compat.output", "");
        for (int i = 0; i < statements.size(); i++) {
            String statement = statements.get(i);
            TableResult result = env.executeSql(statement);
            boolean collectResult = !outputFile.isEmpty() && i == statements.size() - 1
                    && statement.trim().toLowerCase().startsWith("select");
            if (collectResult) {
                writeResult(result, Path.of(outputFile));
            } else {
                if (result.getJobClient().isPresent()) {
                    result.await();
                }
            }
        }
    }

    private static void configureObjectStore(Configuration configuration) {
        setIfPresent(configuration, JobOptions.S3_BUCKET.key(), "lakesoul.compat.s3.bucket");
        setIfPresent(configuration, JobOptions.S3_ENDPOINT.key(), "lakesoul.compat.s3.endpoint");
        setIfPresent(configuration, JobOptions.S3_ACCESS_KEY.key(), "lakesoul.compat.s3.accessKey");
        setIfPresent(configuration, JobOptions.S3_SECRET_KEY.key(), "lakesoul.compat.s3.secretKey");
        setIfPresent(configuration, JobOptions.S3_PATH_STYLE_ACCESS.key(), "lakesoul.compat.s3.pathStyleAccess");
    }

    private static void setIfPresent(Configuration configuration, String key, String property) {
        String value = System.getProperty(property, "");
        if (!value.isEmpty()) {
            configuration.setString(key, value);
        }
    }

    private static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                inString = !inString;
            }
            if (c == ';' && !inString) {
                addStatement(statements, current);
            } else {
                current.append(c);
            }
        }
        addStatement(statements, current);
        return statements;
    }

    private static void addStatement(List<String> statements, StringBuilder current) {
        String statement = current.toString().trim();
        current.setLength(0);
        if (!statement.isEmpty() && !statement.startsWith("--")) {
            statements.add(statement);
        }
    }

    private static void writeResult(TableResult result, Path outputFile) throws Exception {
        Files.createDirectories(outputFile.getParent());
        List<String> columns = result.getResolvedSchema().getColumnNames();
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
        }
        Files.writeString(outputFile, toJson(columns, rows), StandardCharsets.UTF_8);
    }

    private static String toJson(List<String> columns, List<Row> rows) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"columns\":[");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            appendJsonString(builder, columns.get(i));
        }
        builder.append("],\"rows\":[");
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            if (rowIndex > 0) {
                builder.append(',');
            }
            Row row = rows.get(rowIndex);
            StringJoiner joiner = new StringJoiner(",", "[", "]");
            for (int fieldIndex = 0; fieldIndex < row.getArity(); fieldIndex++) {
                joiner.add(jsonValue(row.getField(fieldIndex)));
            }
            builder.append(joiner);
        }
        builder.append("]}");
        return builder.toString();
    }

    private static String jsonValue(Object value) throws IOException {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        StringBuilder builder = new StringBuilder();
        appendJsonString(builder, value.toString());
        return builder.toString();
    }

    private static void appendJsonString(StringBuilder builder, String value) throws IOException {
        builder.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\b':
                    builder.append("\\b");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        builder.append(String.format("\\u%04x", (int) c));
                    } else {
                        builder.append(c);
                    }
            }
        }
        builder.append('"');
    }
}
