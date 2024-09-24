// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
// This file is modified from https://github.com/apache/flink-kubernetes-operator/blob/main/examples/flink-sql-runner-example/src/main/java/org/apache/flink/examples/SqlRunner.java

package org.apache.flink.lakesoul.entry.sql.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.SetOperation;
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

    public static void executeSqlFileContent(String script, StreamTableEnvironment tableEnv,
                                             StreamExecutionEnvironment env)
            throws Exception {
        List<String> statements = parseStatements(script);
        Parser parser = ((TableEnvironmentInternal) tableEnv).getParser();

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        Boolean hasModifiedOp = false;
        for (String statement : statements) {
            Operation operation;
            try {
                operation = parser.parse(statement).get(0);
            } catch (Exception e) {
                System.out.println("Parse statement " + statement + " failed: ");
                System.out.println(ExceptionUtils.getRootCauseMessage(e));
                throw e;
            }
            if (operation instanceof SetOperation) {
                SetOperation setOperation = (SetOperation) operation;
                if (setOperation.getKey().isPresent() && setOperation.getValue().isPresent()) {
                    System.out.println(MessageFormatter.format("\n======Setting config: {}={}",
                            setOperation.getKey().get(),
                            setOperation.getValue().get()).getMessage());
                    tableEnv.getConfig().getConfiguration()
                            .setString(setOperation.getKey().get(), setOperation.getValue().get());
                } else if (setOperation.getKey().isPresent()) {
                    String value = tableEnv.getConfig().getConfiguration().getString(setOperation.getKey().get(), "");
                    System.out.println(MessageFormatter.format("Config {}={}",
                            setOperation.getKey().get(), value).getMessage());
                } else {
                    System.out.println(MessageFormatter.format("All configs: {}",
                            tableEnv.getConfig().getConfiguration()).getMessage());
                }
            } else if (operation instanceof CreateTableASOperation) {
                String message = String.format("CTAS statement is not supported: %s", statement);
                System.out.println(message);
                throw new RuntimeException(message);
            } else if (operation instanceof ModifyOperation) {
                System.out.println(MessageFormatter.format("\n======Executing insertion:\n{}", statement).getMessage());
                // add insertion to statement set
                hasModifiedOp = true;
                statementSet.addInsertSql(statement);
            } else if ((operation instanceof QueryOperation) || (operation instanceof AddJarOperation)) {
                LOG.warn("SQL Statement {} is ignored", statement);
            } else {
                // for all show/alter/create catalog/use but not select statements
                // execute and print results
                System.out.println(MessageFormatter.format("\n======Executing:\n{}", statement).getMessage());
                tableEnv.executeSql(statement).print();
            }
        }
        if (hasModifiedOp) {
            statementSet.attachAsDataStream();
            Configuration conf = (Configuration) env.getConfiguration();

            // try get k8s cluster name
            String k8sClusterID = conf.getString("kubernetes.cluster-id", "");
            env.execute(k8sClusterID.isEmpty() ? null : k8sClusterID + "-job");
        } else {
            System.out.println("There's no INSERT INTO statement, the program will terminate");
        }
    }

    public static List<String> parseStatements(String script) {
        String formatted =
                formatSqlFile(script)
                        .replaceAll(COMMENT_PATTERN, "");

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
