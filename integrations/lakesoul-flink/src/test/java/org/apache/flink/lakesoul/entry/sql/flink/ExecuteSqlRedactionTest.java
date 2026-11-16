// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.flink;

import static org.junit.Assert.*;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class ExecuteSqlRedactionTest {
    @Test
    public void setDiagnosticsRedactBothCredentialAliasesAndTokens() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try (PrintStream captured = new PrintStream(output, true, "UTF-8")) {
            System.setOut(captured);
            ExecuteSql.printConfig("s3.access-key", "access-sentinel");
            ExecuteSql.printConfig("fs.s3a.secret.key", "secret-sentinel");
            ExecuteSql.printConfig("AWS_SESSION_TOKEN", "token-sentinel");
            ExecuteSql.printConfig("s3.endpoint", "http://localhost:9000");
        } finally {
            System.setOut(original);
        }

        String diagnostic = output.toString("UTF-8");
        assertFalse(diagnostic.contains("access-sentinel"));
        assertFalse(diagnostic.contains("secret-sentinel"));
        assertFalse(diagnostic.contains("token-sentinel"));
        assertTrue(diagnostic.contains("s3.access-key=[REDACTED]"));
        assertTrue(diagnostic.contains("fs.s3a.secret.key=[REDACTED]"));
        assertTrue(diagnostic.contains("http://localhost:9000"));
    }

    @Test
    public void parserFailuresDoNotExposeStatementsOrRawCauses() throws Exception {
        String statement = "SET 's3.secret-key' = 'secret-sentinel'";
        AtomicReference<String> parsed = new AtomicReference<>();
        Parser parser =
                (Parser)
                        Proxy.newProxyInstance(
                                Parser.class.getClassLoader(),
                                new Class<?>[] {Parser.class},
                                (proxy, method, arguments) -> {
                                    parsed.set((String) arguments[0]);
                                    throw new IllegalArgumentException(
                                            "Invalid SQL: " + arguments[0]);
                                });

        try {
            ExecuteSql.parseStatement(parser, statement, 7);
            fail("parsing should fail");
        } catch (IllegalArgumentException error) {
            assertTrue(error.getMessage().startsWith("Failed to parse SQL statement #7:"));
            assertTrue(error.getMessage().contains("IllegalArgumentException"));
            assertNull(error.getCause());
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (PrintStream captured = new PrintStream(output, true, "UTF-8")) {
                error.printStackTrace(captured);
            }
            assertFalse(output.toString("UTF-8").contains("secret-sentinel"));
        }
        assertEquals(statement, parsed.get());
    }

    @Test
    public void realSyntaxFailureKeepsPositionAndExpectedGrammarWithoutTheEncounteredSecret()
            throws Exception {
        String statement = "SELECT 1\nFROM 'secret-sentinel';";
        SqlParserException original = realParserFailure(statement);
        // Confirm that this is an actual disclosure in the original Flink diagnostic.
        assertTrue(original.getMessage().contains("secret-sentinel"));

        IllegalArgumentException sanitized = failure(parserThatThrows(original), statement);
        String diagnostic = sanitized.getMessage();
        assertTrue(diagnostic.contains("SqlParserException -> SqlParseException"));
        assertTrue(diagnostic.contains("at statement line 2, column 6"));
        assertTrue(diagnostic.contains("expected next token:"));
        assertTrue(diagnostic.contains("\"(\""));
        assertNull(sanitized.getCause());
        assertFalse(stackTrace(sanitized).contains("secret-sentinel"));
    }

    @Test
    public void unterminatedLiteralFailureKeepsPositionWithoutCopyingInput() throws Exception {
        String statement = "SELECT 'secret-sentinel";
        SqlParserException original = realParserFailure(statement);
        IllegalArgumentException sanitized = failure(parserThatThrows(original), statement);

        assertTrue(sanitized.getMessage().contains("SqlParserException"));
        assertTrue(sanitized.getMessage().contains("at statement line 1, column"));
        assertNull(sanitized.getCause());
        assertFalse(stackTrace(sanitized).contains("secret-sentinel"));
    }

    @Test
    public void expectedTokensAreRestrictedToGrammarAndRawNestedDiagnosticsAreNotAttached()
            throws Exception {
        RuntimeException rawCause = new IllegalStateException("secret-sentinel");
        rawCause.addSuppressed(new IllegalArgumentException("token-sentinel"));
        SqlParseException parseError =
                new SqlParseException(
                        "Invalid SQL with access-sentinel",
                        new SqlParserPos(3, 5),
                        new int[][] {{0}, {1}, {2}, {3}, {-1}, {20}, {}, null},
                        new String[] {
                            "\"=\"",
                            "\"SELECT\"",
                            "\"secret-sentinel\"",
                            "\"ACCESS_SECRET_SENTINEL\""
                        },
                        rawCause);
        SqlParserException original = new SqlParserException("token-sentinel", parseError);
        IllegalArgumentException sanitized = failure(parserThatThrows(original), "SELECT 1");

        assertTrue(sanitized.getMessage().contains("IllegalStateException"));
        assertTrue(sanitized.getMessage().contains("at statement line 3, column 5"));
        assertTrue(sanitized.getMessage().contains("expected next token: \"=\", \"SELECT\""));
        assertNull(sanitized.getCause());
        assertEquals(0, sanitized.getSuppressed().length);
        String trace = stackTrace(sanitized);
        for (String secret :
                new String[] {
                    "access-sentinel", "secret-sentinel", "token-sentinel", "ACCESS_SECRET_SENTINEL"
                }) {
            assertFalse(trace.contains(secret));
        }
    }

    @Test(timeout = 1000)
    public void cyclicCausesKeepErrorTypesWithoutLoopingOrCopyingRawMessages() throws Exception {
        RuntimeException outer = new IllegalArgumentException("secret-sentinel");
        RuntimeException inner = new IllegalStateException("token-sentinel");
        outer.initCause(inner);
        inner.initCause(outer);
        IllegalArgumentException sanitized = failure(parserThatThrows(outer), "SELECT 1");

        assertTrue(
                sanitized
                        .getMessage()
                        .contains("IllegalArgumentException -> IllegalStateException"));
        assertTrue(sanitized.getMessage().contains("parser message omitted"));
        assertFalse(stackTrace(sanitized).contains("secret-sentinel"));
        assertFalse(stackTrace(sanitized).contains("token-sentinel"));
    }

    @Test
    public void largeExpectedTokenListsAreBoundedAndMissingMetadataIsHandled() {
        String[] images = Arrays.copyOf(FlinkSqlParserImplConstants.tokenImage, 40);
        int[][] sequences = new int[images.length][];
        for (int i = 0; i < images.length; i++) {
            sequences[i] = new int[] {i};
        }
        SqlParseException original =
                new SqlParseException(
                        "secret-sentinel", new SqlParserPos(1, 1), sequences, images, null);
        String diagnostic = SqlParserDiagnostics.format(original);
        assertTrue(diagnostic.endsWith(", ..."));
        assertEquals(
                13,
                diagnostic
                        .substring(diagnostic.indexOf("expected next token:"))
                        .split(", ")
                        .length);
        assertFalse(diagnostic.contains("secret-sentinel"));

        SqlParseException missingMetadata =
                new SqlParseException("secret-sentinel", null, null, null, null);
        assertEquals("SqlParseException", SqlParserDiagnostics.format(missingMetadata));

        RuntimeException deep = new RuntimeException("secret-sentinel");
        for (int i = 0; i < 20; i++) {
            deep = new RuntimeException("secret-sentinel", deep);
        }
        String chain = SqlParserDiagnostics.format(deep);
        assertEquals(17, chain.substring(0, chain.indexOf(';')).split(" -> ").length);
        assertFalse(chain.contains("secret-sentinel"));
    }

    @Test
    public void successfulParsingReturnsTheOriginalOperationAndAuthenticationValue() {
        SetOperation expected = new SetOperation("s3.secret-key", "secret-sentinel");
        AtomicReference<String> parsed = new AtomicReference<>();
        Parser parser =
                (Parser)
                        Proxy.newProxyInstance(
                                Parser.class.getClassLoader(),
                                new Class<?>[] {Parser.class},
                                (proxy, method, arguments) -> {
                                    parsed.set((String) arguments[0]);
                                    return Collections.singletonList(expected);
                                });
        String statement = "SET 's3.secret-key' = 'secret-sentinel';";
        assertSame(expected, ExecuteSql.parseStatement(parser, statement, 1));
        assertEquals(statement, parsed.get());
        assertEquals("secret-sentinel", expected.getValue().get());
    }

    @Test
    public void isolatedPlannerTypesAndUnavailableGrammarMetadataAreHandled() throws Exception {
        URL plannerJar =
                SqlParseException.class.getProtectionDomain().getCodeSource().getLocation();
        for (boolean hideGrammar : new boolean[] {false, true}) {
            try (URLClassLoader loader =
                    new URLClassLoader(new URL[] {plannerJar}, getClass().getClassLoader()) {
                        @Override
                        protected Class<?> loadClass(String name, boolean resolve)
                                throws ClassNotFoundException {
                            if (hideGrammar
                                    && name.equals(
                                            "org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants")) {
                                throw new ClassNotFoundException("secret-sentinel");
                            }
                            if (name.startsWith("org.apache.calcite.")
                                    || name.startsWith("org.apache.flink.sql.parser.")) {
                                synchronized (getClassLoadingLock(name)) {
                                    Class<?> type = findLoadedClass(name);
                                    if (type == null) {
                                        type = findClass(name);
                                    }
                                    if (resolve) {
                                        resolveClass(type);
                                    }
                                    return type;
                                }
                            }
                            return super.loadClass(name, resolve);
                        }
                    }) {
                Class<?> positionType =
                        loader.loadClass("org.apache.calcite.sql.parser.SqlParserPos");
                Object position =
                        positionType.getConstructor(int.class, int.class).newInstance(4, 7);
                Throwable isolated =
                        (Throwable)
                                loader.loadClass("org.apache.calcite.sql.parser.SqlParseException")
                                        .getConstructor(
                                                String.class,
                                                positionType,
                                                int[][].class,
                                                String[].class,
                                                Throwable.class)
                                        .newInstance(
                                                "secret-sentinel",
                                                position,
                                                new int[][] {{0}},
                                                new String[] {"\"=\""},
                                                null);
                assertFalse(isolated instanceof SqlParseException);
                SqlParserException original = new SqlParserException("secret-sentinel", isolated);
                IllegalArgumentException sanitized =
                        failure(parserThatThrows(original), "SELECT 1");
                assertTrue(sanitized.getMessage().contains("at statement line 4, column 7"));
                if (hideGrammar) {
                    assertTrue(
                            sanitized
                                    .getMessage()
                                    .contains("structured parser diagnostics unavailable"));
                } else {
                    assertTrue(sanitized.getMessage().contains("expected next token: \"=\""));
                }
                assertFalse(stackTrace(sanitized).contains("secret-sentinel"));
            }
        }
    }

    private static SqlParserException realParserFailure(String statement) {
        CalciteParser parser =
                new CalciteParser(SqlParser.config().withParserFactory(FlinkSqlParserImpl.FACTORY));
        try {
            parser.parseSqlList(statement);
            throw new AssertionError("parsing should fail");
        } catch (SqlParserException error) {
            return error;
        }
    }

    private static Parser parserThatThrows(RuntimeException error) {
        return (Parser)
                Proxy.newProxyInstance(
                        Parser.class.getClassLoader(),
                        new Class<?>[] {Parser.class},
                        (proxy, method, arguments) -> {
                            throw error;
                        });
    }

    private static IllegalArgumentException failure(Parser parser, String statement) {
        try {
            ExecuteSql.parseStatement(parser, statement, 7);
            throw new AssertionError("parsing should fail");
        } catch (IllegalArgumentException error) {
            return error;
        }
    }

    private static String stackTrace(Throwable error) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (PrintStream captured = new PrintStream(output, true, "UTF-8")) {
            error.printStackTrace(captured);
        }
        return output.toString("UTF-8");
    }

    @Test
    public void statementParsingKeepsTheOriginalAuthenticationValue() {
        String statement = "SET 's3.secret-key' = 'secret-sentinel';";
        assertEquals(statement, ExecuteSql.parseStatements(statement).get(0).trim());
    }
}
