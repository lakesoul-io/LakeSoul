/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.test.connector.sink;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.*;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.apache.flink.lakesoul.LakeSoulOptions.LAKESOUL_TABLE_PATH;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.HASH_BUCKET_NUM;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStageId;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStreamNodeId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class LakeSoulTableSinkCase {

    private static LakeSoulCatalog lakeSoulCatalog;

    @BeforeClass
    public static void createCatalog() throws IOException {
        lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);
        lakeSoulCatalog.open();
    }

    @AfterClass
    public static void closeCatalog() {
        if (lakeSoulCatalog != null) {
            lakeSoulCatalog.close();
        }
    }

    @Test
    public void testLakeSoulTableSinkWithParallelismInBatch() {
        final TableEnvironment tEnv = LakeSoulTestUtils.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        testLakeSoulTableSinkWithParallelismBase(
                tEnv, "== Abstract Syntax Tree ==\n" +
                        "LogicalSink(table=[lakesoul.db1.test_table], fields=[EXPR$0, EXPR$1])\n" +
                        "+- LogicalProject(EXPR$0=[1], EXPR$1=[1])\n" +
                        "   +- LogicalValues(tuples=[[{ 0 }]])\n" +
                        "\n" +
                        "== Optimized Physical Plan ==\n" +
                        "Sink(table=[lakesoul.db1.test_table], fields=[EXPR$0, EXPR$1])\n" +
                        "+- Calc(select=[1 AS EXPR$0, 1 AS EXPR$1])\n" +
                        "   +- Values(tuples=[[{ 0 }]], values=[ZERO])\n" +
                        "\n" +
                        "== Optimized Execution Plan ==\n" +
                        "Sink(table=[lakesoul.db1.test_table], fields=[EXPR$0, EXPR$1])\n" +
                        "+- Calc(select=[1 AS EXPR$0, 1 AS EXPR$1])\n" +
                        "   +- Values(tuples=[[{ 0 }]], values=[ZERO])\n" +
                        "\n" +
                        "== Physical Execution Plan ==\n" +
                        "{\n" +
                        "  \"nodes\" : [ {\n" +
                        "    \"id\" : ,\n" +
                        "    \"type\" : \"Source: Values(tuples=[[{ 0 }]], values=[ZERO])\",\n" +
                        "    \"pact\" : \"Data Source\",\n" +
                        "    \"contents\" : \"Source: Values(tuples=[[{ 0 }]], values=[ZERO])\",\n" +
                        "    \"parallelism\" : 1\n" +
                        "  }, {\n" +
                        "    \"id\" : ,\n" +
                        "    \"type\" : \"Calc(select=[1 AS EXPR$0, 1 AS EXPR$1])\",\n" +
                        "    \"pact\" : \"Operator\",\n" +
                        "    \"contents\" : \"Calc(select=[1 AS EXPR$0, 1 AS EXPR$1])\",\n" +
                        "    \"parallelism\" : 1,\n" +
                        "    \"predecessors\" : [ {\n" +
                        "      \"id\" : ,\n" +
                        "      \"ship_strategy\" : \"FORWARD\",\n" +
                        "      \"side\" : \"second\"\n" +
                        "    } ]\n" +
                        "  }, {\n" +
                        "    \"id\" : ,\n" +
                        "    \"type\" : \"Sink Unnamed Writer\",\n" +
                        "    \"pact\" : \"Operator\",\n" +
                        "    \"contents\" : \"Sink Unnamed Writer\",\n" +
                        "    \"parallelism\" : 3,\n" +
                        "    \"predecessors\" : [ {\n" +
                        "      \"id\" : ,\n" +
                        "      \"ship_strategy\" : \"CUSTOM\",\n" +
                        "      \"side\" : \"second\"\n" +
                        "    } ]\n" +
                        "  }, {\n" +
                        "    \"id\" : ,\n" +
                        "    \"type\" : \"Sink Unnamed Committer\",\n" +
                        "    \"pact\" : \"Operator\",\n" +
                        "    \"contents\" : \"Sink Unnamed Committer\",\n" +
                        "    \"parallelism\" : 1,\n" +
                        "    \"predecessors\" : [ {\n" +
                        "      \"id\" : ,\n" +
                        "      \"ship_strategy\" : \"REBALANCE\",\n" +
                        "      \"side\" : \"second\"\n" +
                        "    } ]\n" +
                        "  } ]\n" +
                        "}");
    }

    @Test
    public void testLakeSoulTableSinkWithParallelismInStreaming() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final TableEnvironment tEnv =
                LakeSoulTestUtils.createTableEnvInStreamingMode(env, SqlDialect.DEFAULT);
        testLakeSoulTableSinkWithParallelismBase(
                tEnv, "== Abstract Syntax Tree ==\n" +
                        "LogicalSink(table=[lakesoul.db1.test_table], fields=[EXPR$0, EXPR$1])\n" +
                        "+- LogicalProject(EXPR$0=[1], EXPR$1=[1])\n" +
                        "   +- LogicalValues(tuples=[[{ 0 }]])\n" +
                        "\n" +
                        "== Optimized Physical Plan ==\n" +
                        "Sink(table=[lakesoul.db1.test_table], fields=[EXPR$0, EXPR$1])\n" +
                        "+- Calc(select=[1 AS EXPR$0, 1 AS EXPR$1])\n" +
                        "   +- Values(type=[RecordType(INTEGER ZERO)], tuples=[[{ 0 }]])\n" +
                        "\n" +
                        "== Optimized Execution Plan ==\n" +
                        "Sink(table=[lakesoul.db1.test_table], fields=[EXPR$0, EXPR$1])\n" +
                        "+- Calc(select=[1 AS EXPR$0, 1 AS EXPR$1])\n" +
                        "   +- Values(tuples=[[{ 0 }]])\n" +
                        "\n" +
                        "== Physical Execution Plan ==\n" +
                        "{\n" +
                        "  \"nodes\" : [ {\n" +
                        "    \"id\" : ,\n" +
                        "    \"type\" : \"Source: Values(tuples=[[{ 0 }]])\",\n" +
                        "    \"pact\" : \"Data Source\",\n" +
                        "    \"contents\" : \"Source: Values(tuples=[[{ 0 }]])\",\n" +
                        "    \"parallelism\" : 1\n" +
                        "  }, {\n" +
                        "    \"id\" : ,\n" +
                        "    \"type\" : \"Calc(select=[1 AS EXPR$0, 1 AS EXPR$1])\",\n" +
                        "    \"pact\" : \"Operator\",\n" +
                        "    \"contents\" : \"Calc(select=[1 AS EXPR$0, 1 AS EXPR$1])\",\n" +
                        "    \"parallelism\" : 1,\n" +
                        "    \"predecessors\" : [ {\n" +
                        "      \"id\" : ,\n" +
                        "      \"ship_strategy\" : \"FORWARD\",\n" +
                        "      \"side\" : \"second\"\n" +
                        "    } ]\n" +
                        "  }, {\n" +
                        "    \"id\" : ,\n" +
                        "    \"type\" : \"Sink Unnamed\",\n" +
                        "    \"pact\" : \"Operator\",\n" +
                        "    \"contents\" : \"Sink Unnamed\",\n" +
                        "    \"parallelism\" : 3,\n" +
                        "    \"predecessors\" : [ {\n" +
                        "      \"id\" : ,\n" +
                        "      \"ship_strategy\" : \"CUSTOM\",\n" +
                        "      \"side\" : \"second\"\n" +
                        "    } ]\n" +
                        "  } ]\n" +
                        "}");
    }

    private void testLakeSoulTableSinkWithParallelismBase(
            final TableEnvironment tEnv, String expected) {
        tEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        tEnv.useCatalog(lakeSoulCatalog.getName());
        tEnv.executeSql("create database db1");
        tEnv.useDatabase("db1");
        tEnv.executeSql("DROP TABLE IF EXISTS test_table");

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE test_table ("
                                + " id int,"
                                + " real_col int"
                                + ") WITH ("
                                + "'"
                                + HASH_BUCKET_NUM.key()
                                + "'= '3',"
                                + "'"
                                + LAKESOUL_TABLE_PATH.key()
                                + "'='/tmp/test_table',"
                                + "'"
                                + FactoryUtil.FORMAT.key()
                                + "'='lakesoul'"
                                + ")"));
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        final String actual =
                tEnv.explainSql(
                        "insert into test_table select 1, 1", ExplainDetail.JSON_EXECUTION_PLAN);
        System.out.println(replaceStreamNodeId(replaceStageId(actual)));

        assertEquals(
                replaceStreamNodeId(replaceStageId(actual)),
                expected
                );

        tEnv.executeSql("drop database db1 cascade");
    }

    @Test
    public void testBatchAppend() throws Exception {
        TableEnvironment tEnv = LakeSoulTestUtils.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        tEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        tEnv.useCatalog(lakeSoulCatalog.getName());
        tEnv.executeSql("create database if not exists db1");
        tEnv.useDatabase("db1");
        try {
            String ddl = String.format("create table append_table (i int, j int) with ('%s'='/tmp/append_table', '%s'='lakesoul')", LAKESOUL_TABLE_PATH.key(), FactoryUtil.FORMAT.key());
            tEnv.executeSql(ddl);
            tEnv.executeSql("insert into append_table select 1, 1").await();
            tEnv.executeSql("insert into append_table select 2, 2").await();
            List<Row> rows =
                    CollectionUtil.iteratorToList(
                            tEnv.executeSql("select * from append_table").collect());
            rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
            Assert.assertEquals(Arrays.asList(Row.of(1, 1), Row.of(2, 2)), rows);
        } finally {
            tEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test
    public void testDefaultSerPartStreamingWrite() throws Exception {
        testStreamingWrite(true, false,  this::checkDirExists);
    }

//    @Test
//    public void testPartStreamingWrite() throws Exception {
//        testStreamingWrite(true, false, "parquet", this::checkSuccessFiles);
//        // disable vector orc writer test for hive 2.x due to dependency conflict
//        if (!lakeSoulCatalog.getHiveVersion().startsWith("2.")) {
//            testStreamingWrite(true, false, "orc", this::checkSuccessFiles);
//        }
//    }

//    @Test
//    public void testNonPartStreamingWrite() throws Exception {
//        testStreamingWrite(false, false, "parquet", (p) -> {});
//        // disable vector orc writer test for hive 2.x due to dependency conflict
//        if (!lakeSoulCatalog.getHiveVersion().startsWith("2.")) {
//            testStreamingWrite(false, false, "orc", (p) -> {});
//        }
//    }

//    @Test
//    public void testPartStreamingMrWrite() throws Exception {
//        testStreamingWrite(true, true, "parquet", this::checkSuccessFiles);
//        // doesn't support writer 2.0 orc table
//        if (!lakeSoulCatalog.getHiveVersion().startsWith("2.0")) {
//            testStreamingWrite(true, true, "orc", this::checkSuccessFiles);
//        }
//    }

//    @Test
//    public void testNonPartStreamingMrWrite() throws Exception {
//        testStreamingWrite(false, true, "parquet", (p) -> {});
//        // doesn't support writer 2.0 orc table
//        if (!lakeSoulCatalog.getHiveVersion().startsWith("2.0")) {
//            testStreamingWrite(false, true, "orc", (p) -> {});
//        }
//    }

    @Test
    public void testStreamingAppend() throws Exception {
        testStreamingWrite(
                false,
                false,
                (p) -> {
                    Configuration config = new Configuration();
                    config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
                    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(config);
                    env.setParallelism(1);

                    env.enableCheckpointing(1000);
                    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);

                    CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
//                    if (parameter.get(JOB_CHECKPOINT_MODE.key(), JOB_CHECKPOINT_MODE.defaultValue()).equals("AT_LEAST_ONCE")) {
//                        checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
//                    }
                    env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
                    env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
                    StreamTableEnvironment streamEnv = LakeSoulTestUtils.createTableEnvInStreamingMode(env);
                    streamEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
                    streamEnv.useCatalog(lakeSoulCatalog.getName());

                    TableEnvironment bEnv = LakeSoulTestUtils.createTableEnvWithLakeSoulCatalog(lakeSoulCatalog);
                    try {
//                        bEnv.executeSql(
                        streamEnv.executeSql(
                                        "insert into db1.sink_table select 6,'a','b','2020-05-03','12'")
                                .await();
                    } catch (Exception e) {
                        Assert.fail("Failed to execute sql: " + e.getMessage());
                    }

                    assertBatch(
                            "db1.sink_table",
                            Arrays.asList(
                                    "+I[1, a, b, 2020-05-03, 7]",
                                    "+I[1, a, b, 2020-05-03, 7]",
                                    "+I[2, p, q, 2020-05-03, 8]",
                                    "+I[2, p, q, 2020-05-03, 8]",
                                    "+I[3, x, y, 2020-05-03, 9]",
                                    "+I[3, x, y, 2020-05-03, 9]",
                                    "+I[4, x, y, 2020-05-03, 10]",
                                    "+I[4, x, y, 2020-05-03, 10]",
                                    "+I[5, x, y, 2020-05-03, 11]",
                                    "+I[5, x, y, 2020-05-03, 11]",
                                    "+I[6, a, b, 2020-05-03, 12]"));
                });
    }


    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> strings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Assert.assertTrue(iter.hasNext());
            strings.add(iter.next().toString());
        }
        strings.sort(String::compareTo);
        return strings;
    }

    private void checkDirExists(String path) {
        File basePath = new File(path, "d=2020-05-03");
        Assert.assertEquals(5, basePath.list().length);
        Assert.assertTrue(new File(basePath, "e=7").exists());
        Assert.assertTrue(new File(basePath, "e=8").exists());
        Assert.assertTrue(new File(basePath, "e=9").exists());
        Assert.assertTrue(new File(basePath, "e=10").exists());
        Assert.assertTrue(new File(basePath, "e=11").exists());
    }

    private void testStreamingWrite(
            boolean part, boolean useMr, Consumer<String> pathConsumer)
            throws Exception {
        StreamExecutionEnvironment env = LakeSoulTestUtils.createStreamExecutionEnvironment();
        StreamTableEnvironment tEnv = LakeSoulTestUtils.createTableEnvInStreamingMode(env);
        tEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        tEnv.useCatalog(lakeSoulCatalog.getName());
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//        if (useMr) {
//            tEnv.getConfig()
//                    .getConfiguration()
//                    .set(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER, true);
//        } else {
//            tEnv.getConfig()
//                    .getConfiguration()
//                    .set(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER, false);
//        }

        try {
            tEnv.executeSql("create database if not exists db1");
            tEnv.useDatabase("db1");

            // prepare source
            List<Row> data =
                    Arrays.asList(
                            Row.of(1, "a", "b", "2020-05-03", "7"),
                            Row.of(2, "p", "q", "2020-05-03", "8"),
                            Row.of(3, "x", "y", "2020-05-03", "9"),
                            Row.of(4, "x", "y", "2020-05-03", "10"),
                            Row.of(5, "x", "y", "2020-05-03", "11"));
            DataStream<Row> stream =
                    env.addSource(
                            new FiniteTestSource<>(data),
                            new RowTypeInfo(
                                    Types.INT,
                                    Types.STRING,
                                    Types.STRING,
                                    Types.STRING,
                                    Types.STRING));
            tEnv.createTemporaryView("my_table", stream, $("a"), $("b"), $("c"), $("d"), $("e"));

            // DDL
            tEnv.executeSql("DROP TABLE IF EXISTS sink_table");
            tEnv.executeSql(
                    "create table if not exists sink_table (a int,b string,c string"
                            + ",d string,e string"
                            + ") "
                            + (part ? "partitioned by (d ,e ) " : "")

                            + " WITH ("
                            + "'"
                            + PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key()
                            + "'='$d $e:00:00',"
                            + "'"
                            + SINK_PARTITION_COMMIT_DELAY.key()
                            + "'='1h',"
                            + "'"
                            + SINK_PARTITION_COMMIT_POLICY_KIND.key()
                            + "'='metastore,success-file',"
                            + "'"
                            + SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME.key()
                            + "'='_MY_SUCCESS',"
                            + "'"
                            + LAKESOUL_TABLE_PATH.key()
                            + "'='/tmp/sink_table',"
                            + "'"
                            + FactoryUtil.FORMAT.key()
                            + "'='lakesoul'"
                            + ")");

            // hive dialect only works with hive tables at the moment, switch to default dialect
            tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//            tEnv.sqlQuery("select * from my_table").executeInsert("sink_table").await();
            tEnv.executeSql("insert into db1.sink_table values " +
                    "(1,'a','b','2020-05-03','7')," +
                    "(1,'a','b','2020-05-03','7')," +
                    "(2,'p','q','2020-05-03','8')," +
                    "(2,'p','q','2020-05-03','8')," +
                    "(3,'x','y','2020-05-03','9')," +
                    "(3,'x','y','2020-05-03','9')," +
                    "(4,'x','y','2020-05-03','10')," +
                    "(4,'x','y','2020-05-03','10')," +
                    "(5,'x','y','2020-05-03','11')," +
                    "(5,'x','y','2020-05-03','11')").await();

            assertBatch(
                    "db1.sink_table",
                    Arrays.asList(
                            "+I[1, a, b, 2020-05-03, 7]",
                            "+I[1, a, b, 2020-05-03, 7]",
                            "+I[2, p, q, 2020-05-03, 8]",
                            "+I[2, p, q, 2020-05-03, 8]",
                            "+I[3, x, y, 2020-05-03, 9]",
                            "+I[3, x, y, 2020-05-03, 9]",
                            "+I[4, x, y, 2020-05-03, 10]",
                            "+I[4, x, y, 2020-05-03, 10]",
                            "+I[5, x, y, 2020-05-03, 11]",
                            "+I[5, x, y, 2020-05-03, 11]"));

            pathConsumer.accept(
                    URI.create(
                                    lakeSoulCatalog
                                            .getTable(ObjectPath.fromString("db1.sink_table"))
                                            .getOptions()
                                            .get("path"))
                            .getPath());
        } finally {
            tEnv.executeSql("drop database db1 cascade");
        }
    }

    private void assertBatch(String table, List<String> expected) {
        // using batch table env to query.
        List<String> results = new ArrayList<>();
        TableEnvironment batchTEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
        batchTEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchTEnv.useCatalog(lakeSoulCatalog.getName());
        batchTEnv
                .executeSql("select * from " + table)
                .collect()
                .forEachRemaining(r -> results.add(r.toString()));
        results.sort(String::compareTo);
        expected.sort(String::compareTo);
        Assert.assertEquals(expected, results);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // using batch table env to query.
        String table = "db1.sink_table";
        List<String> results = new ArrayList<>();
        TableEnvironment batchTEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
        LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);

        batchTEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchTEnv.useCatalog(lakeSoulCatalog.getName());
        batchTEnv.executeSql("create database if not exists db1");
        batchTEnv.useDatabase("db1");

        boolean part = false;
        String ddl =
                "create table if not exists db1.sink_table (a int,b string,c string"
                        + (part ? "" : ",d string,e string")
                        + ") "
                        + (part ? "partitioned by (d string,e string) " : "")

                        + " WITH ("
                        + "'"
                        + PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key()
                        + "'='$d $e:00:00',"
                        + "'"
                        + SINK_PARTITION_COMMIT_DELAY.key()
                        + "'='1h',"
                        + "'"
                        + SINK_PARTITION_COMMIT_POLICY_KIND.key()
                        + "'='metastore,success-file',"
                        + "'"
                        + SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME.key()
                        + "'='_MY_SUCCESS',"
                        + "'"
                        + LAKESOUL_TABLE_PATH.key()
                        + "'='/tmp/sink_table',"
                        + "'"
                        + FactoryUtil.FORMAT.key()
                        + "'='lakesoul'"
                        + ")";
        batchTEnv.executeSql(ddl);



        StreamExecutionEnvironment env = LakeSoulTestUtils.createStreamExecutionEnvironment();
        StreamTableEnvironment tEnv = LakeSoulTestUtils.createTableEnvInStreamingMode(env);
        tEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        tEnv.useCatalog(lakeSoulCatalog.getName());
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        tEnv.executeSql(ddl);

        tEnv.executeSql("insert into db1.sink_table select 6,'a','b','2020-05-03','12'").await();
//        batchTEnv.executeSql("insert into db1.sink_table select 6,'a','b','2020-05-03','12'").await();

        batchTEnv
                .executeSql("select * from " + table)
                .collect()
                .forEachRemaining(r -> results.add(r.toString()));
        results.sort(String::compareTo);
        System.out.println(results);
//        expected.sort(String::compareTo);
//        Assert.assertEquals(expected, results);
    }
}
