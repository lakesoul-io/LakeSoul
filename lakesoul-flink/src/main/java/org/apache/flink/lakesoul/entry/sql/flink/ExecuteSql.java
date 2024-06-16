// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
// This file is modified from https://github.com/apache/flink-kubernetes-operator/blob/main/examples/flink-sql-runner-example/src/main/java/org/apache/flink/examples/SqlRunner.java

package org.apache.flink.lakesoul.entry.sql.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.SetOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;

public class ExecuteSql {

    private static final Logger LOG = LoggerFactory.getLogger(ExecuteSql.class);

    private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
    private static final String LINE_DELIMITER = "\n";

    private static final String COMMENT_PATTERN = "(--.*)|(((\\/\\*)+?[\\w\\W]+?(\\*\\/)+))";

    public static void executeSqlFileContent(String script, TableEnvironment tableEnv)
            throws ExecutionException, InterruptedException {
        List<String> statements = parseStatements(script);
        Parser parser = ((TableEnvironmentInternal) tableEnv).getParser();
        for (String statement : statements) {
            Operation operation = parser.parse(statement).get(0);
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
            } else if (operation instanceof ModifyOperation || operation instanceof BeginStatementSetOperation) {
                System.out.println(MessageFormatter.format("\n======Executing insertion:\n{}", statement).getMessage());
                // execute insertion and do not wait for results for stream mode
                if (tableEnv.getConfig().get(RUNTIME_MODE) == RuntimeExecutionMode.BATCH) {
                    tableEnv.executeSql(statement).await();
                } else {
                    tableEnv.executeSql(statement);
                }
            } else {
                // for all show/select/alter/create catalog/use, etc. statements
                // execute and print results
                System.out.println(MessageFormatter.format("\n======Executing:\n{}", statement).getMessage());
                tableEnv.executeSql(statement).print();
            }
        }
    }

    public static List<String> parseStatements(String script) {
        String formatted =
                formatSqlFile(script)
                        .replaceAll(COMMENT_PATTERN, "");

        List<String> statements = new ArrayList<String>();

        StringBuilder current = null;
        boolean statementSet = false;
        for (String line : formatted.split("\n")) {
            String trimmed = line.trim();
            if (StringUtils.isBlank(trimmed)) {
                continue;
            }
            if (current == null) {
                current = new StringBuilder();
            }
            if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
                statementSet = true;
            }
            current.append(trimmed);
            current.append("\n");
            if (trimmed.endsWith(STATEMENT_DELIMITER)) {
                if (!statementSet || trimmed.equals("END;")) {
                    statements.add(current.toString());
                    current = null;
                    statementSet = false;
                }
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
