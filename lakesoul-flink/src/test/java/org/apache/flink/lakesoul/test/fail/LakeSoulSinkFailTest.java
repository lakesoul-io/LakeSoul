package org.apache.flink.lakesoul.test.fail;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.test.LakeSoulCatalogMocks;
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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;
import org.jetbrains.annotations.Nullable;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.lakesoul.test.fail.LakeSoulSinkFailTest.MockSplitEnumerator.indexBound;
import static org.assertj.core.api.Assertions.assertThat;

public class LakeSoulSinkFailTest {


    private enum StopBehavior {

        FAIL_ON_BEFORE_ASSIGN_SPLIT,
        FAIL_ON_ASSIGN_SPLIT_FINISHED,
        FAIL_ON_CHECKPOINTING,
        FAIL_ON_COLLECT_FINISHED
    }

    static long FAIL_OVER_INTERVAL_START = 0L;
    static long FAIL_OVER_INTERVAL_END = 0L;

    private static void tryStop(StopBehavior targetBehavior, StopBehavior behavior) {
        if (targetBehavior != behavior) return;

        long current = System.currentTimeMillis();
        if (current > FAIL_OVER_INTERVAL_START && current < FAIL_OVER_INTERVAL_END) {
            String msg = "Sink fail with " + behavior + " at " + LocalDateTime.now();
            System.out.println(msg);
            throw new RuntimeException(msg);
        }
    }

    public static Map<String, Tuple3<ResolvedSchema, String, StopBehavior>> parameters;

    static String dropSourceSql = "drop table if exists test_source";
    static String createSourceSqlFormat = "create table if not exists test_source %s " +
            "with ('connector'='lakesoul', 'path'='/', 'hashBucketNum'='2')";

    static String dropSinkSql = "drop table if exists test_sink";
    static String createSinkSqlFormat = "create table if not exists test_sink %s %s" +
            "with ('connector'='lakesoul', 'path'='%s', 'hashBucketNum'='%d')";
    private static ArrayList<Integer> indexArr;
    private static final LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);
    private static StreamExecutionEnvironment streamExecEnv;
    private static StreamTableEnvironment streamTableEnv;

    private static TableEnvironment batchEnv;

    private static LakeSoulCatalogMocks.TestLakeSoulCatalog testLakeSoulCatalog;
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() {
        streamExecEnv = LakeSoulTestUtils.createStreamExecutionEnvironment(2, 500L);
        streamTableEnv = LakeSoulTestUtils.createTableEnvInStreamingMode(streamExecEnv);
        testLakeSoulCatalog = new LakeSoulCatalogMocks.TestLakeSoulCatalog();
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
                        UniqueConstraint.primaryKey("primary key", Collections.singletonList("hash"))), "PARTITIONED BY (`range`)",
                StopBehavior.FAIL_ON_CHECKPOINTING));

        parameters.put("testLakeSoulSinkFailOnCollectFinished", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash", DataTypes.INT().notNull()), Column.physical("range1",
                                        DataTypes.DATE()),
                                Column.physical("range2", DataTypes.STRING()),
                                Column.physical("value", DataTypes.TIMESTAMP_LTZ())), Collections.emptyList(),
                        UniqueConstraint.primaryKey("primary key", Collections.singletonList("hash"))),
                "PARTITIONED BY (`range1`, `range2`)", StopBehavior.FAIL_ON_COLLECT_FINISHED));

        parameters.put("testLakeSoulSinkFailOnAssignSplitFinished", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash1", DataTypes.INT()), Column.physical("hash2",
                                        DataTypes.STRING()),
                                Column.physical("range", DataTypes.DATE()), Column.physical("value",
                                        DataTypes.BYTES())),
                        Collections.emptyList(), UniqueConstraint.primaryKey("primary key", Arrays.asList("hash1",
                        "hash2"))),
                "PARTITIONED BY (`range`)", StopBehavior.FAIL_ON_ASSIGN_SPLIT_FINISHED));

        parameters.put("testLakeSoulSinkFailOnBeforeAssignSplit", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash1", DataTypes.INT()), Column.physical("hash2",
                                        DataTypes.INT()),
                                Column.physical("range1", DataTypes.STRING()), Column.physical("range2",
                                        DataTypes.BOOLEAN()),
                                Column.physical("value", DataTypes.DOUBLE())), Collections.emptyList(),
                        UniqueConstraint.primaryKey("primary key", Arrays.asList("hash1", "hash2"))),
                "PARTITIONED BY (`range1`, `range2`)", StopBehavior.FAIL_ON_BEFORE_ASSIGN_SPLIT));
    }

    public static Object generateObjectWithIndexByDatatype(Integer index, RowType.RowField field) {
        int value = field.getName().contains("range") ? index / 3 : index;
        switch (field.getType().getTypeRoot().name().toLowerCase()) {
            case "integer":
            case "date":
                return value;
            case "varchar":
                return StringData.fromString(String.format("'%d$", value));
            case "timestamp_with_local_time_zone":
                return TimestampData.fromEpochMillis((long) value * 3600 * 24 * 1000);
            case "double":
                return Double.valueOf(index);
            case "boolean":
                return value % 2 == 0;
            case "varbinary":
                return new byte[]{index.byteValue(), 'a'};
            default:
                throw new IllegalStateException("Unexpected value: " +
                        field.getType().getTypeRoot().name().toLowerCase());
        }
    }

    public static String generateExpectedDataWithIndexByDatatype(Integer index, Column column) {
        int value = column.getName().contains("range") ? index / 3 : index;
        switch (column.getDataType().getLogicalType().getTypeRoot().name().toLowerCase()) {
            case "integer":
                return String.valueOf(value);
            case "varchar":
                return String.format("'%d$", value);
            case "timestamp_with_local_time_zone":
                return DateTimeFormatter.ofPattern("yyyy-MM-dd  HH:mm:ss ").format(LocalDateTime.ofInstant(Instant.ofEpochMilli((long) value * 3600 * 24 * 1000), ZoneId.of("UTC"))).replace("  ", "T").replace(" ", "Z");
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
        Tuple3<ResolvedSchema, String, StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        indexBound = 50;
        List<String> expectedData = IntStream.range(0, indexBound).boxed().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        testLakeSoulSink(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                5 * 1000);

        List<String> actualData = CollectionUtil.iteratorToList(batchEnv.executeSql("SELECT * FROM test_sink").collect())
                .stream()
                .map(Row::toString)
                .sorted(Comparator.comparing(Function.identity()))
                .collect(Collectors.toList());
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSinkFailOnCollectFinished() throws IOException {
        String testName = "testLakeSoulSinkFailOnCollectFinished";
        Tuple3<ResolvedSchema, String, StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        indexBound = 50;
        List<String> expectedData = IntStream.range(0, indexBound).boxed().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        testLakeSoulSink(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                5 * 1000);

        List<String> actualData = CollectionUtil.iteratorToList(batchEnv.executeSql("SELECT * FROM test_sink").collect())
                .stream()
                .map(Row::toString)
                .sorted(Comparator.comparing(Function.identity()))
                .collect(Collectors.toList());
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSinkFailOnAssignSplitFinished() throws IOException {
        String testName = "testLakeSoulSinkFailOnAssignSplitFinished";
        Tuple3<ResolvedSchema, String, StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        indexBound = 30;
        List<String> expectedData = IntStream.range(0, indexBound).boxed().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        testLakeSoulSink(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                10 * 1000);

        List<String> actualData = CollectionUtil.iteratorToList(batchEnv.executeSql("SELECT * FROM test_sink").collect())
                .stream()
                .map(Row::toString)
                .sorted(Comparator.comparing(Function.identity()))
                .collect(Collectors.toList());
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSinkFailOnBeforeAssignSplit() throws IOException {
        String testName = "testLakeSoulSinkFailOnBeforeAssignSplit";
        Tuple3<ResolvedSchema, String, StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        indexBound = 50;
        List<String> expectedData = IntStream.range(0, indexBound).boxed().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        testLakeSoulSink(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                5 * 1000);

        List<String> actualData = CollectionUtil.iteratorToList(batchEnv.executeSql("SELECT * FROM test_sink").collect())
                .stream()
                .map(Row::toString)
                .sorted(Comparator.comparing(Function.identity()))
                .collect(Collectors.toList());
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    private void testLakeSoulSink(ResolvedSchema resolvedSchema, StopBehavior behavior, String partitionBy,
                                  String path, int timeout) throws IOException {
        testLakeSoulCatalog.cleanForTest();
        LakeSoulCatalogMocks.TestLakeSoulDynamicTableFactory testFactory =
                new LakeSoulCatalogMocks.TestLakeSoulDynamicTableFactory();
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

        FAIL_OVER_INTERVAL_START = 0L;
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
        LakeSoulCatalogMocks.TestLakeSoulDynamicTableFactory testFactory =
                new LakeSoulCatalogMocks.TestLakeSoulDynamicTableFactory();
        ResolvedSchema resolvedSchema = new ResolvedSchema(
                Arrays.asList(Column.physical("hash", DataTypes.INT().notNull()), Column.physical("range",
                                DataTypes.STRING()),
                        Column.physical("value", DataTypes.DOUBLE())), Collections.emptyList(),
                UniqueConstraint.primaryKey("primary key", Collections.singletonList("hash")));
        MockTableSource testTableSource =
                new MockTableSource(resolvedSchema.toPhysicalRowDataType(), "test", 2, StopBehavior.FAIL_ON_COLLECT_FINISHED);
        testFactory.setTestSource(testTableSource);

        testLakeSoulCatalog.setTestFactory(testFactory);

        streamTableEnv.executeSql(String.format(createSourceSqlFormat, resolvedSchema));


        streamTableEnv.executeSql(String.format(createSinkSqlFormat, resolvedSchema, "", tempFolder.newFolder("testMockTableSource").getAbsolutePath(), 2));

        streamTableEnv.executeSql("DROP TABLE IF EXISTS default_catalog.default_database.test_sink");
        streamTableEnv.executeSql("CREATE TABLE default_catalog.default_database.test_sink " +
                resolvedSchema +
                " WITH (" +
                "'connector' = 'values', 'sink-insert-only' = 'false'" +
                ")");
        TestValuesTableFactory.clearAllData();


        TableResult tableResult = streamTableEnv.executeSql("insert into default_catalog.default_database.test_sink select * from test_source");
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

    static class MockTableSource implements ScanTableSource {

        private final DataType dataType;
        private final String name;
        private final Integer parallelism;
        private final StopBehavior stopBehavior;

        public MockTableSource(DataType dataType, String name, Integer parallelism, StopBehavior stopBehavior) {
            this.dataType = dataType;
            this.name = name;
            this.parallelism = parallelism;
            this.stopBehavior = stopBehavior;
        }


        /**
         * Creates a copy of this instance during planning. The copy should be a deep copy of all
         * mutable members.
         */
        @Override
        public DynamicTableSource copy() {
            return new MockTableSource(dataType, name, parallelism, stopBehavior);
        }

        /**
         * Returns a string that summarizes this source for printing to a console or log.
         */
        @Override
        public String asSummaryString() {
            return null;
        }

        /**
         * Returns the set of changes that the planner can expect during runtime.
         *
         * @see RowKind
         */
        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        /**
         * Returns a provider of runtime implementation for reading the data.
         *
         * <p>There might exist different interfaces for runtime implementation which is why {@link
         * ScanRuntimeProvider} serves as the base interface. Concrete {@link ScanRuntimeProvider}
         * interfaces might be located in other Flink modules.
         *
         * <p>Independent of the provider interface, the table runtime expects that a source
         * implementation emits internal data structures (see {@link
         * RowData} for more information).
         *
         * <p>The given {@link ScanContext} offers utilities by the planner for creating runtime
         * implementation with minimal dependencies to internal data structures.
         *
         * <p>{@link SourceProvider} is the recommended core interface. {@code SourceFunctionProvider}
         * in {@code flink-table-api-java-bridge} and {@link InputFormatProvider} are available for
         * backwards compatibility.
         *
         * @param runtimeProviderContext
         * @see SourceProvider
         */
        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            return SourceProvider.of(new ExactlyOnceSource(dataType, stopBehavior));
        }
    }

    static class ExactlyOnceSource implements Source<RowData, MockSplit, Integer> {

        private final DataType dataType;
        private final StopBehavior stopBehavior;

        public ExactlyOnceSource(DataType dataType, StopBehavior stopBehavior) {
            this.dataType = dataType;
            this.stopBehavior = stopBehavior;
        }

        /**
         * Get the boundedness of this source.
         *
         * @return the boundedness of this source.
         */
        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        /**
         * Creates a new reader to read data from the splits it gets assigned. The reader starts fresh
         * and does not have any state to resume.
         *
         * @param readerContext The {@link SourceReaderContext context} for the source reader.
         * @return A new SourceReader.
         * @throws Exception The implementor is free to forward all exceptions directly. Exceptions
         *                   thrown from this method cause task failure/recovery.
         */
        @Override
        public SourceReader<RowData, MockSplit> createReader(SourceReaderContext readerContext) throws Exception {
            return new MockSourceReader(() -> new MockSplitReader(dataType, stopBehavior), (rowData, sourceOutput, split) -> {
                sourceOutput.collect(rowData);
                tryStop(StopBehavior.FAIL_ON_COLLECT_FINISHED, stopBehavior);
            }, readerContext.getConfiguration(), readerContext);
        }

        /**
         * Creates a new SplitEnumerator for this source, starting a new input.
         *
         * @param enumContext The {@link SplitEnumeratorContext context} for the split enumerator.
         * @return A new SplitEnumerator.
         * @throws Exception The implementor is free to forward all exceptions directly. * Exceptions
         *                   thrown from this method cause JobManager failure/recovery.
         */
        @Override
        public SplitEnumerator<MockSplit, Integer> createEnumerator(SplitEnumeratorContext<MockSplit> enumContext) throws Exception {
            return new MockSplitEnumerator(enumContext, stopBehavior);
        }

        /**
         * Restores an enumerator from a checkpoint.
         *
         * @param enumContext The {@link SplitEnumeratorContext context} for the restored split
         *                    enumerator.
         * @param checkpoint  The checkpoint to restore the SplitEnumerator from.
         * @return A SplitEnumerator restored from the given checkpoint.
         * @throws Exception The implementor is free to forward all exceptions directly. * Exceptions
         *                   thrown from this method cause JobManager failure/recovery.
         */
        @Override
        public SplitEnumerator<MockSplit, Integer> restoreEnumerator(SplitEnumeratorContext<MockSplit> enumContext, Integer checkpoint) throws Exception {
            return new MockSplitEnumerator(enumContext, stopBehavior, checkpoint);
        }

        /**
         * Creates a serializer for the source splits. Splits are serialized when sending them from
         * enumerator to reader, and when checkpointing the reader's current state.
         *
         * @return The serializer for the split type.
         */
        @Override
        public SimpleVersionedSerializer<MockSplit> getSplitSerializer() {
            return new SimpleVersionedSerializer<MockSplit>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(MockSplit split) throws IOException {
                    DataOutputSerializer out = new DataOutputSerializer(2);
                    out.writeInt(split.getIndex());
                    return out.getCopyOfBuffer();
                }

                @Override
                public MockSplit deserialize(int version, byte[] serialized) throws IOException {
                    DataInputDeserializer in = new DataInputDeserializer(serialized);
                    if (version == 0) {
                        return new MockSplit(in.readInt());
                    }
                    throw new IOException("Unrecognized version or corrupt state: " + version);
                }
            };
        }

        /**
         * Creates the serializer for the {@link SplitEnumerator} checkpoint. The serializer is used for
         * the result of the {@link SplitEnumerator #snapshotState()} method.
         *
         * @return The serializer for the SplitEnumerator checkpoint.
         */
        @Override
        public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
            return new SimpleVersionedSerializer<Integer>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                /**
                 * Serializes the given object. The serialization is assumed to correspond to the current
                 * serialization version (as returned by {@link #getVersion()}.
                 *
                 * @param obj The object to serialize.
                 * @return The serialized data (bytes).
                 * @throws IOException Thrown, if the serialization fails.
                 */
                @Override
                public byte[] serialize(Integer obj) throws IOException {
                    DataOutputSerializer out = new DataOutputSerializer(2);
                    out.writeInt(obj);
                    return out.getCopyOfBuffer();
                }


                @Override
                public Integer deserialize(int version, byte[] serialized) throws IOException {
                    DataInputDeserializer in = new DataInputDeserializer(serialized);
                    if (version == 0) {
                        return in.readInt();
                    }
                    throw new IOException("Unrecognized version or corrupt state: " + version);
                }
            };
        }


    }

    static class MockSplit implements SourceSplit, Serializable {

        private final int index;

        public int getIndex() {
            return index;
        }

        MockSplit(int index) {
            this.index = index;
        }

        /**
         * Get the split id of this source split.
         *
         * @return id of this source split.
         */
        @Override
        public String splitId() {
            return String.valueOf(index);
        }

        @Override
        public String toString() {
            return "MockSplit{" +
                    "index=" + index +
                    '}';
        }
    }

    static class MockSplitEnumerator implements SplitEnumerator<MockSplit, Integer> {
        private final StopBehavior stopBehavior;
        private int index;

        private final SplitEnumeratorContext<MockSplit> enumContext;
        public static int indexBound = 20;
        private final ArrayDeque<MockSplit> backSplits;

        public MockSplitEnumerator(SplitEnumeratorContext<MockSplit> enumContext, StopBehavior behavior) {
            this(enumContext, behavior, 0);
        }

        public MockSplitEnumerator(SplitEnumeratorContext<MockSplit> enumContext, StopBehavior behavior, int index) {
            this.enumContext = enumContext;
            this.index = index;
            this.stopBehavior = behavior;
            backSplits = new ArrayDeque<>();
        }

        @Override
        public void start() {
            if (FAIL_OVER_INTERVAL_START == 0L) {
                FAIL_OVER_INTERVAL_START = System.currentTimeMillis() + 200;
                FAIL_OVER_INTERVAL_END = FAIL_OVER_INTERVAL_START + 500;
            }
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            tryStop(StopBehavior.FAIL_ON_BEFORE_ASSIGN_SPLIT, stopBehavior);
            if (backSplits.isEmpty()) {
                if (index < indexBound) {
                    enumContext.assignSplit(new MockSplit(index), subtaskId);
                    index++;
                }
            } else {
                enumContext.assignSplit(backSplits.pop(), subtaskId);
            }
            tryStop(StopBehavior.FAIL_ON_ASSIGN_SPLIT_FINISHED, stopBehavior);
        }

        @Override
        public void addSplitsBack(List<MockSplit> splits, int subtaskId) {
            backSplits.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {

        }

        @Override
        public Integer snapshotState(long checkpointId) throws Exception {
            tryStop(StopBehavior.FAIL_ON_CHECKPOINTING, stopBehavior);
            return index;
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class MockSourceReader extends SingleThreadMultiplexSourceReaderBase<RowData, RowData, MockSplit, MockSplit> {

        public MockSourceReader(Supplier<SplitReader<RowData, MockSplit>> splitReaderSupplier, RecordEmitter<RowData, RowData, MockSplit> recordEmitter, Configuration config, SourceReaderContext context) {
            super(splitReaderSupplier, recordEmitter, config, context);
        }

        @Override
        public void start() {
            if (getNumberOfCurrentlyAssignedSplits() == 0) {
                context.sendSplitRequest();
            }
        }

        @Override
        protected void onSplitFinished(Map<String, MockSplit> map) {
            context.sendSplitRequest();
        }

        @Override
        protected MockSplit initializedState(MockSplit split) {
            return split;
        }

        @Override
        protected MockSplit toSplitType(String s, MockSplit split) {
            return split;
        }
    }

    static class MockSplitReader implements SplitReader<RowData, MockSplit> {
        private final ArrayDeque<MockSplit> splits;

        private final DataType dataType;
        private final StopBehavior stopBehavior;

        public MockSplitReader(DataType dataType, StopBehavior stopBehavior) {
            this.dataType = dataType;
            this.stopBehavior = stopBehavior;
            splits = new ArrayDeque<>();
        }

        @Override
        public RecordsWithSplitIds<RowData> fetch() {
            return new MockRecordsWithSplitIds(splits.pop(), dataType, stopBehavior);
        }

        @Override
        public void handleSplitsChanges(SplitsChange<MockSplit> splitsChange) {
            if (!(splitsChange instanceof SplitsAddition)) {
                throw new UnsupportedOperationException(
                        String.format("The SplitChange type of %s is not supported.", splitsChange.getClass()));
            }

            splits.addAll(splitsChange.splits());
        }

        @Override
        public void wakeUp() {

        }

        @Override
        public void close() throws Exception {

        }
    }

    static class MockRecordsWithSplitIds implements RecordsWithSplitIds<RowData> {

        final MockSplit split;

        boolean finished;

        private final RowType rowType;
        private final StopBehavior stopBehavior;

        MockRecordsWithSplitIds(MockSplit mockSplit, DataType dataType, StopBehavior stopBehavior) {
            this.rowType = (RowType) dataType.getLogicalType();
            this.stopBehavior = stopBehavior;
            split = mockSplit;
            finished = false;
        }

        /**
         * Moves to the next split. This method is also called initially to move to the first split.
         * Returns null, if no splits are left.
         */
        @Nullable
        @Override
        public String nextSplit() {
            return finished ? null : split.splitId();
        }

        /**
         * Gets the next record from the current split. Returns null if no more records are left in this
         * split.
         */
        @Nullable
        @Override
        public RowData nextRecordFromSplit() {
            RowData next = finished ? null : GenericRowData.of(rowType.getFields().stream().map(field -> generateObjectWithIndexByDatatype(split.getIndex(), field)).toArray());
            finished = true;
            return next;
        }

        /**
         * Get the finished splits.
         *
         * @return the finished splits after this RecordsWithSplitIds is returned.
         */
        @Override
        public Set<String> finishedSplits() {
            return Collections.singleton(split.splitId());
        }
    }

}
