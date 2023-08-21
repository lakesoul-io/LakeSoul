// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.fail;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.MockLakeSoulCatalog;
import org.apache.flink.lakesoul.test.MockTableSource;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.lakesoul.test.MockTableSource.MockSplitEnumerator.indexBound;
import static org.assertj.core.api.Assertions.assertThat;

public class LakeSoulSinkFailTest extends AbstractTestBase {

    private static final LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);
    public static Map<String, Tuple3<ResolvedSchema, String, MockTableSource.StopBehavior>> parameters;
    static String dropSourceSql = "drop table if exists test_source";
    static String createSourceSqlFormat = "create table if not exists test_source %s " +
            "with ('connector'='lakesoul', 'path'='/', 'hashBucketNum'='2', " + "'discoveryinterval'='1000'" + ")";
    static String dropSinkSql = "drop table if exists test_sink";
    static String createSinkSqlFormat = "create table if not exists test_sink %s %s" +
            "with ('connector'='lakesoul', 'path'='%s', 'hashBucketNum'='%d')";
    private static ArrayList<Integer> indexArr;
    private static StreamExecutionEnvironment streamExecEnv;
    private static StreamTableEnvironment streamTableEnv;

    private static TableEnvironment batchEnv;

    private static MockLakeSoulCatalog.TestLakeSoulCatalog testLakeSoulCatalog;
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() {
        streamExecEnv = LakeSoulTestUtils.createStreamExecutionEnvironment(2, 4000L, 4000L);
        streamTableEnv = LakeSoulTestUtils.createTableEnvInStreamingMode(streamExecEnv);
        testLakeSoulCatalog = new MockLakeSoulCatalog.TestLakeSoulCatalog();
        LakeSoulTestUtils.registerLakeSoulCatalog(streamTableEnv, testLakeSoulCatalog);
        batchEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
        LakeSoulTestUtils.registerLakeSoulCatalog(batchEnv, lakeSoulCatalog);

        indexArr = new ArrayList<>();
        for (int i = 0; i < indexBound; i++) {
            indexArr.add(i);
        }
        parameters = new HashMap<>();
        parameters.put("testLakeSoulSinkFailOnCheckpointing", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash", DataTypes.INT()), Column.physical("range",
                                        DataTypes.STRING()),
                                Column.physical("value", DataTypes.DOUBLE())), Collections.emptyList(),
                        UniqueConstraint.primaryKey("primary key", Collections.singletonList("hash"))),
                "PARTITIONED BY (`range`)", MockTableSource.StopBehavior.FAIL_ON_CHECKPOINTING));

        parameters.put("testLakeSoulSinkStopPostgresOnCheckpointing", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash", DataTypes.INT()), Column.physical("range",
                                        DataTypes.STRING()),
                                Column.physical("value", DataTypes.DOUBLE())), Collections.emptyList(),
                        UniqueConstraint.primaryKey("primary key", Collections.singletonList("hash"))),
                "PARTITIONED BY (`range`)", MockTableSource.StopBehavior.STOP_POSTGRES_ON_CHECKPOINTING));

        parameters.put("testLakeSoulSinkFailOnCollectFinished", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash", DataTypes.INT().notNull()),
                                Column.physical("range1", DataTypes.DATE()), Column.physical("range2",
                                        DataTypes.STRING()),
                                Column.physical("value", DataTypes.TIMESTAMP_LTZ())), Collections.emptyList(),
                        UniqueConstraint.primaryKey("primary key", Collections.singletonList("hash"))),
                "PARTITIONED BY (`range1`, `range2`)", MockTableSource.StopBehavior.FAIL_ON_COLLECT_FINISHED));

        parameters.put("testLakeSoulSinkFailOnAssignSplitFinished", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash1", DataTypes.INT()), Column.physical("hash2",
                                        DataTypes.STRING()),
                                Column.physical("range", DataTypes.DATE()), Column.physical("value",
                                        DataTypes.BYTES())),
                        Collections.emptyList(), UniqueConstraint.primaryKey("primary key", Arrays.asList("hash1",
                        "hash2"))),
                "PARTITIONED BY (`range`)", MockTableSource.StopBehavior.FAIL_ON_ASSIGN_SPLIT_FINISHED));

        parameters.put("testLakeSoulSinkFailOnBeforeAssignSplit", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash1", DataTypes.INT()), Column.physical("hash2",
                                        DataTypes.INT()),
                                Column.physical("range1", DataTypes.STRING()), Column.physical("range2",
                                        DataTypes.BOOLEAN()),
                                Column.physical("value", DataTypes.DOUBLE())), Collections.emptyList(),
                        UniqueConstraint.primaryKey("primary key", Arrays.asList("hash1", "hash2"))),
                "PARTITIONED BY (`range1`, `range2`)", MockTableSource.StopBehavior.FAIL_ON_BEFORE_ASSIGN_SPLIT));

        parameters.put("testLakeSoulSinkWithoutPkStopPostgresOnCheckpointing", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash", DataTypes.INT()), Column.physical("range",
                                        DataTypes.STRING()),
                                Column.physical("value", DataTypes.DOUBLE())), Collections.emptyList(), null),
                "PARTITIONED BY (`range`)", MockTableSource.StopBehavior.STOP_POSTGRES_ON_CHECKPOINTING));
    }

    public static Object generateObjectWithIndexByDatatype(Integer index, RowType.RowField field) {
        int value = field.getName().contains("range") ? index / 3 : index;
        switch (field.getType().getTypeRoot().name().toLowerCase()) {
            case "integer":
            case "date":
                return value;
            case "varchar":
                return value % 2 == 0 ? StringData.fromString(String.format("'%d$", value)) : StringData.fromString("");
            case "timestamp_with_local_time_zone":
                return TimestampData.fromEpochMillis((long) value * 3600 * 24 * 1000);
            case "double":
                return Double.valueOf(index);
            case "boolean":
                return value % 2 == 0;
            case "varbinary":
                return new byte[]{index.byteValue(), 'a'};
            default:
                throw new IllegalStateException(
                        "Unexpected value: " + field.getType().getTypeRoot().name().toLowerCase());
        }
    }

    public static String generateExpectedDataWithIndexByDatatype(Integer index, Column column) {
        int value = column.getName().contains("range") ? index / 3 : index;
        switch (column.getDataType().getLogicalType().getTypeRoot().name().toLowerCase()) {
            case "integer":
                return String.valueOf(value);
            case "varchar":
                return value % 2 == 0 ? String.format("'%d$", value) : "";
            case "timestamp_with_local_time_zone":
                return DateTimeFormatter.ofPattern("yyyy-MM-dd  HH:mm:ss ")
                        .format(LocalDateTime.ofInstant(Instant.ofEpochMilli((long) value * 3600 * 24 * 1000),
                                ZoneId.of("UTC"))).replace("  ", "T").replace(" ", "Z");
            case "double":
                return String.valueOf(Double.valueOf(index));
            case "date":
                return String.format("1970-01-%02d", value + 1);
            case "boolean":
                return String.valueOf(value % 2 == 0);
            case "varbinary":
                return String.format("[%d, 97]", value);
            default:
                throw new IllegalStateException("Unexpected value: " +
                        column.getDataType().getLogicalType().getTypeRoot().name().toLowerCase());
        }
    }


    @Test
    public void testLakeSoulSinkFailOnCheckpointing() throws IOException {
        String testName = "testLakeSoulSinkFailOnCheckpointing";
        Tuple3<ResolvedSchema, String, MockTableSource.StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        indexBound = 30;
        List<String> expectedData = IntStream.range(0, indexBound).boxed().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        MockTableSource.FAIL_OPTION = Optional.of(Tuple2.of(1000, 4000));
        testLakeSoulSink(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                60 * 1000);

        List<String> actualData =
                CollectionUtil.iteratorToList(batchEnv.executeSql("SELECT * FROM test_sink").collect()).stream()
                        .map(Row::toString).sorted(Comparator.comparing(Function.identity()))
                        .collect(Collectors.toList());
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSinkFailOnCollectFinished() throws IOException {
        String testName = "testLakeSoulSinkFailOnCollectFinished";
        Tuple3<ResolvedSchema, String, MockTableSource.StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        indexBound = 30;
        List<String> expectedData = IntStream.range(0, indexBound).boxed().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        MockTableSource.FAIL_OPTION = Optional.of(Tuple2.of(1000, 4000));
        testLakeSoulSink(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                60 * 1000);

        List<String> actualData =
                CollectionUtil.iteratorToList(batchEnv.executeSql("SELECT * FROM test_sink").collect()).stream()
                        .map(Row::toString).sorted(Comparator.comparing(Function.identity()))
                        .collect(Collectors.toList());
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSinkFailOnAssignSplitFinished() throws IOException {
        String testName = "testLakeSoulSinkFailOnAssignSplitFinished";
        Tuple3<ResolvedSchema, String, MockTableSource.StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        indexBound = 30;
        List<String> expectedData = IntStream.range(0, indexBound).boxed().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        MockTableSource.FAIL_OPTION = Optional.of(Tuple2.of(1000, 4000));
        testLakeSoulSink(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                60 * 1000);

        List<String> actualData =
                CollectionUtil.iteratorToList(batchEnv.executeSql("SELECT * FROM test_sink").collect()).stream()
                        .map(Row::toString).sorted(Comparator.comparing(Function.identity()))
                        .collect(Collectors.toList());
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSinkFailOnBeforeAssignSplit() throws IOException {
        String testName = "testLakeSoulSinkFailOnBeforeAssignSplit";
        Tuple3<ResolvedSchema, String, MockTableSource.StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        indexBound = 30;
        List<String> expectedData = IntStream.range(0, indexBound).boxed().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        MockTableSource.FAIL_OPTION = Optional.of(Tuple2.of(1000, 4000));
        testLakeSoulSink(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                60 * 1000);

        List<String> actualData =
                CollectionUtil.iteratorToList(batchEnv.executeSql("SELECT * FROM test_sink").collect()).stream()
                        .map(Row::toString).sorted(Comparator.comparing(Function.identity()))
                        .collect(Collectors.toList());
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSinkStopPostgresOnCheckpointing() throws IOException {
        String testName = "testLakeSoulSinkStopPostgresOnCheckpointing";
        Tuple3<ResolvedSchema, String, MockTableSource.StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        indexBound = 40;
        List<String> expectedData = IntStream.range(0, indexBound).boxed().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        MockTableSource.FAIL_OPTION = Optional.of(Tuple2.of(5000, 4000));
        testLakeSoulSink(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                60 * 1000);

        List<String> actualData =
                CollectionUtil.iteratorToList(batchEnv.executeSql("SELECT * FROM test_sink").collect()).stream()
                        .map(Row::toString).sorted(Comparator.comparing(Function.identity()))
                        .collect(Collectors.toList());
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSinkWithoutPkStopPostgresOnCheckpointing() throws IOException {
        String testName = "testLakeSoulSinkWithoutPkStopPostgresOnCheckpointing";
        Tuple3<ResolvedSchema, String, MockTableSource.StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        indexBound = 40;
        List<String> expectedData = IntStream.range(0, indexBound).boxed().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        MockTableSource.FAIL_OPTION = Optional.of(Tuple2.of(5000, 4000));
        testLakeSoulSink(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                60 * 1000);

        List<String> actualData =
                CollectionUtil.iteratorToList(batchEnv.executeSql("SELECT * FROM test_sink").collect()).stream()
                        .map(Row::toString).sorted(Comparator.comparing(Function.identity()))
                        .collect(Collectors.toList());
        expectedData.sort(Comparator.comparing(Function.identity()));

        System.out.println(actualData);
        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    private void testLakeSoulSink(ResolvedSchema resolvedSchema, MockTableSource.StopBehavior behavior,
                                  String partitionBy, String path, int timeout) throws IOException {
        testLakeSoulCatalog.cleanForTest();
        MockLakeSoulCatalog.TestLakeSoulDynamicTableFactory testFactory =
                new MockLakeSoulCatalog.TestLakeSoulDynamicTableFactory();
        MockTableSource testTableSource =
                new MockTableSource(resolvedSchema.toPhysicalRowDataType(), "test", 2, behavior);
        testFactory.setTestSource(testTableSource);

        testLakeSoulCatalog.setTestFactory(testFactory);


        streamTableEnv.executeSql(dropSourceSql);
        streamTableEnv.executeSql(String.format(createSourceSqlFormat, resolvedSchema));


        streamTableEnv.executeSql(dropSinkSql);
        streamTableEnv.executeSql(String.format(createSinkSqlFormat, resolvedSchema, partitionBy, path, 2));

        streamTableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        streamTableEnv.getConfig().setLocalTimeZone(TimeZone.getTimeZone("UTC").toZoneId());

        TableResult tableResult = streamTableEnv.executeSql("insert into test_sink select * from test_source");
        try {
            tableResult.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            System.out.println("streaming executeSql timeout");
            tableResult.getJobClient().get().cancel();
        }
    }

    @Test
    public void testMockTableSource() throws IOException {
        testLakeSoulCatalog.cleanForTest();
        MockLakeSoulCatalog.TestLakeSoulDynamicTableFactory testFactory =
                new MockLakeSoulCatalog.TestLakeSoulDynamicTableFactory();
        ResolvedSchema resolvedSchema = new ResolvedSchema(
                Arrays.asList(Column.physical("hash", DataTypes.INT().notNull()),
                        Column.physical("range", DataTypes.STRING()), Column.physical("value", DataTypes.DOUBLE())),
                Collections.emptyList(), UniqueConstraint.primaryKey("primary key", Collections.singletonList("hash")));
        MockTableSource testTableSource = new MockTableSource(resolvedSchema.toPhysicalRowDataType(), "test", 2,
                MockTableSource.StopBehavior.FAIL_ON_COLLECT_FINISHED);
        testFactory.setTestSource(testTableSource);

        testLakeSoulCatalog.setTestFactory(testFactory);

        streamTableEnv.executeSql(String.format(createSourceSqlFormat, resolvedSchema));


        streamTableEnv.executeSql(String.format(createSinkSqlFormat, resolvedSchema, "",
                tempFolder.newFolder("testMockTableSource").getAbsolutePath(), 2));

        streamTableEnv.executeSql("DROP TABLE IF EXISTS default_catalog.default_database.test_sink");
        streamTableEnv.executeSql(
                "CREATE TABLE default_catalog.default_database.test_sink " + resolvedSchema + " WITH (" +
                        "'connector' = 'values', 'sink-insert-only' = 'false'" + ")");
        TestValuesTableFactory.clearAllData();


        TableResult tableResult = streamTableEnv.executeSql(
                "insert into default_catalog.default_database.test_sink select * from test_source");
        try {
            tableResult.await(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            System.out.println("streaming executeSql timeout");
            List<String> results = TestValuesTableFactory.getResults("test_sink");
            results.sort(Comparator.comparing(String::toString));
            System.out.println(results);
        }
    }


}
