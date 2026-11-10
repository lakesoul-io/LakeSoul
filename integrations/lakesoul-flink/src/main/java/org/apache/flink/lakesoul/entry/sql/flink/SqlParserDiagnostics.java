// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.flink;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Extracts parser diagnostics without copying SQL text, encountered tokens or raw messages. */
final class SqlParserDiagnostics {
    private static final String PARSE_EXCEPTION = "org.apache.calcite.sql.parser.SqlParseException";
    private static final String GRAMMAR_CONSTANTS =
            "org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants";
    private static final int MAX_CAUSES = 16;
    private static final int MAX_EXPECTED_TOKENS = 12;

    private SqlParserDiagnostics() {}

    static String format(Throwable error) {
        StringBuilder diagnostic = new StringBuilder();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable parseError = null;
        Class<?> parseType = null;
        Throwable current = error;
        while (current != null && seen.size() < MAX_CAUSES && seen.add(current)) {
            if (diagnostic.length() > 0) {
                diagnostic.append(" -> ");
            }
            String name = current.getClass().getSimpleName();
            diagnostic.append(name.isEmpty() ? current.getClass().getName() : name);
            Class<?> candidate = parseExceptionType(current.getClass());
            if (parseError == null && candidate != null) {
                parseError = current;
                parseType = candidate;
            }
            current = current.getCause();
        }
        if (current != null) {
            diagnostic.append(" -> ...");
        }
        if (parseError == null) {
            return diagnostic + "; parser message omitted to avoid exposing SQL text";
        }
        try {
            appendStructuredDiagnostics(diagnostic, parseError, parseType);
        } catch (ReflectiveOperationException | RuntimeException | LinkageError ignored) {
            // Optional planner metadata must not replace the original failure with a new error.
            diagnostic.append("; some structured parser diagnostics unavailable");
        }
        return diagnostic.toString();
    }

    private static Class<?> parseExceptionType(Class<?> type) {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            if (PARSE_EXCEPTION.equals(current.getName())) {
                return current;
            }
        }
        return null;
    }

    private static void appendStructuredDiagnostics(
            StringBuilder diagnostic, Throwable error, Class<?> type)
            throws ReflectiveOperationException {
        // Calcite can live in Flink's isolated planner loader. Use its public metadata methods,
        // without linking application classes to planner implementation types.
        Method getPosition = type.getMethod("getPos");
        Object position = getPosition.invoke(error);
        if (position != null) {
            Class<?> positionType = getPosition.getReturnType();
            int line = (Integer) positionType.getMethod("getLineNum").invoke(position);
            int column = (Integer) positionType.getMethod("getColumnNum").invoke(position);
            if (line > 0 && column > 0) {
                // Coordinates are statement-relative, not original script-file coordinates.
                diagnostic
                        .append(" at statement line ")
                        .append(line)
                        .append(", column ")
                        .append(column);
            }
        }
        int[][] sequences = (int[][]) type.getMethod("getExpectedTokenSequences").invoke(error);
        String[] images = (String[]) type.getMethod("getTokenImages").invoke(error);
        if (sequences == null || images == null) {
            return;
        }
        Class<?> constants = Class.forName(GRAMMAR_CONSTANTS, false, type.getClassLoader());
        Set<String> grammarTokens =
                new HashSet<>(Arrays.asList((String[]) constants.getField("tokenImage").get(null)));
        Set<String> expected = new TreeSet<>();
        for (int[] sequence : sequences) {
            if (sequence == null || sequence.length == 0) {
                continue;
            }
            // Only grammar-defined images are safe; never use actual encountered-token text.
            int index = sequence[0];
            if (index >= 0 && index < images.length && grammarTokens.contains(images[index])) {
                expected.add(images[index]);
            }
        }
        if (!expected.isEmpty()) {
            diagnostic
                    .append("; expected next token: ")
                    .append(
                            expected.stream()
                                    .limit(MAX_EXPECTED_TOKENS)
                                    .collect(Collectors.joining(", ")));
            if (expected.size() > MAX_EXPECTED_TOKENS) {
                diagnostic.append(", ...");
            }
        }
    }
}
