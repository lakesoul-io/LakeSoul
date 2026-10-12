// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
// This file is modified from
// https://github.com/apache/flink-kubernetes-operator/blob/main/examples/flink-sql-runner-example/src/main/java/org/apache/flink/examples/SqlRunner.java

package org.apache.flink.lakesoul.entry.sql.flink;

import com.dmetasoul.lakesoul.util.SensitiveConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.*;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.List;

public class ExecuteSql {

    private static final Logger LOG = LoggerFactory.getLogger(ExecuteSql.class);

    private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
    private static final String LINE_DELIMITER = "\n";

    private static final String COMMENT_PATTERN = "(--.*)|(((\\/\\*)+?[\\w\\W]+?(\\*\\/)+))";

    public static void executeSqlFileContent(
            String script, StreamTableEnvironment tableEnv, StreamExecutionEnvironment env)
            throws Exception {
        List<String> statements = parseStatements(script);
        Parser parser = ((TableEnvironmentInternal) tableEnv).getParser();

        tableEnv.executeSql("create catalog lakesoul with('type'='lakesoul')");
        tableEnv.executeSql("use catalog lakesoul");

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        Boolean hasModifiedOp = false;
        int statementNumber = 0;
        for (String statement : statements) {
            Operation operation = parseStatement(parser, statement, ++statementNumber);
            System.out.println("Executing SQL statement #" + statementNumber);
            if (operation instanceof SetOperation) {
                SetOperation setOperation = (SetOperation) operation;
                if (setOperation.getKey().isPresent() && setOperation.getValue().isPresent()) {
                    printConfig(setOperation.getKey().get(), setOperation.getValue().get());
                    tableEnv.getConfig()
                            .getConfiguration()
                            .setString(setOperation.getKey().get(), setOperation.getValue().get());
                } else if (setOperation.getKey().isPresent()) {
                    String value =
                            tableEnv.getConfig()
                                    .getConfiguration()
                                    .getString(setOperation.getKey().get(), "");
                    printConfig(setOperation.getKey().get(), value);
                } else {
                    System.out.println(
                            MessageFormatter.format(
                                            "All configs: {}",
                                            SensitiveConfig.redact(
                                                    tableEnv.getConfig()
                                                            .getConfiguration()
                                                            .toMap()))
                                    .getMessage());
                }
            } else if (operation instanceof CreateTableASOperation) {
                String message = "CTAS statement #" + statementNumber + " is not supported";
                System.out.println(message);
                throw new RuntimeException(message);
            } else if (operation instanceof BeginStatementSetOperation
                    || operation instanceof EndStatementSetOperation) {
                continue;
            } else if (operation instanceof ModifyOperation) {
                // add insertion to statement set
                hasModifiedOp = true;
                statementSet.addInsertSql(statement);
            } else if ((operation instanceof QueryOperation)
                    || (operation instanceof AddJarOperation)) {
                LOG.warn(
                        "SQL statement #{} ({}) is ignored",
                        statementNumber,
                        operation.getClass().getSimpleName());
            } else if (operation instanceof CreateCatalogOperation) {
                CreateCatalogOperation createCatalogOperation = (CreateCatalogOperation) operation;
                if (createCatalogOperation.getCatalogName().equals("lakesoul")) {
                    continue;
                } else {
                    tableEnv.executeSql(statement);
                }
            } else {
                // SHOW results may contain credentials from table options. Execute without
                // copying those diagnostic results into the job logs.
                tableEnv.executeSql(statement);
            }
        }
        if (hasModifiedOp) {
            statementSet.attachAsDataStream();
            Configuration conf = (Configuration) env.getConfiguration();

            // try get k8s cluster name
            String k8sClusterID = conf.getString("kubernetes.cluster-id", "");
            env.execute(k8sClusterID.isEmpty() ? null : k8sClusterID);
        } else {
            System.out.println("There's no INSERT INTO statement, the program will terminate");
        }
    }

    static void printConfig(String key, String value) {
        System.out.println(
                MessageFormatter.format("Config {}={}", key, SensitiveConfig.redact(key, value))
                        .getMessage());
    }

    static Operation parseStatement(Parser parser, String statement, int statementNumber) {
        try {
            return parser.parse(statement).get(0);
        } catch (Exception e) {
            // Preserve structured diagnostics, but never attach a cause that can echo SQL.
            throw new IllegalArgumentException(
                    "Failed to parse SQL statement #"
                            + statementNumber
                            + ": "
                            + SqlParserDiagnostics.format(e));
        }
    }

    public static List<String> parseStatements(String script) {
        String formatted = formatSqlFile(script).replaceAll(COMMENT_PATTERN, "");

        List<String> statements = new ArrayList<String>();

        StringBuilder current = null;
        for (String line : formatted.split("\n")) {
            String trimmed = line.trim();
            if (StringUtils.isBlank(trimmed)) {
                continue;
            }
            if (current == null) {
                current = new StringBuilder();
            }
            if (trimmed.startsWith("BEGIN STATEMENT SET") || trimmed.equals("END;")) {
                // we do not directly execute user's statement set
                // instead extract all insert statements out
                continue;
            }
            current.append(trimmed);
            current.append("\n");
            if (trimmed.endsWith(STATEMENT_DELIMITER)) {
                statements.add(current.toString());
                current = null;
            }
        }
        return statements;
    }

    public static String formatSqlFile(String content) {
        String trimmed = content.trim();
        StringBuilder formatted = new StringBuilder();
        formatted.append(trimmed);
        if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
            formatted.append(STATEMENT_DELIMITER);
        }
        formatted.append(LINE_DELIMITER);
        return formatted.toString();
    }
}
