package org.apache.flink.lakesoul.test.fail;

import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.MockLakeSoulCatalog;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.types.DataType;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class LakeSoulSourceFailTest extends AbstractTestBase {

    public static Map<String, Tuple3<ResolvedSchema, String, StopBehavior>> parameters;
    static String createSourceSqlFormat = "create table if not exists test_source %s %s" +
            "with ('connector'='lakesoul', 'path'='%s', 'hashBucketNum'='%d', " +
            "'discoveryinterval'='1000'" +
            ")";
    static String createSinkSqlFormat = "create table if not exists test_sink %s" +
            "with ('connector'='lakesoul', 'path'='/', 'hashBucketNum'='2')";
    private static ArrayList<Integer> indexArr;
    private static final LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);
    private static StreamExecutionEnvironment streamExecEnv;
    private static StreamTableEnvironment streamTableEnv;
    private static MockLakeSoulCatalog.TestLakeSoulCatalog testLakeSoulCatalog;
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() {
        streamExecEnv = LakeSoulTestUtils.createStreamExecutionEnvironment();
        streamTableEnv = LakeSoulTestUtils.createTableEnvInStreamingMode(streamExecEnv);
        testLakeSoulCatalog = new MockLakeSoulCatalog.TestLakeSoulCatalog();
        LakeSoulTestUtils.registerLakeSoulCatalog(streamTableEnv, testLakeSoulCatalog);

        indexArr = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            indexArr.add(i);
        }
        parameters = new HashMap<>();
        parameters.put("testLakeSoulSourceFailOnSinkInvokeFinished", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash", DataTypes.INT()), Column.physical("range",
                                        DataTypes.STRING()),
                                Column.physical("value", DataTypes.DOUBLE())), Collections.emptyList(),
                        UniqueConstraint.primaryKey("primary key", Collections.singletonList("hash"))), "PARTITIONED BY (`range`)",
                StopBehavior.FAIL_ON_INVOKE_FINISHED));

        parameters.put("testLakeSoulSourceFailOnSinkCheckPointing", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash", DataTypes.INT()), Column.physical("range1",
                                        DataTypes.DATE()),
                                Column.physical("range2", DataTypes.STRING()),
                                Column.physical("value", DataTypes.TIMESTAMP_LTZ())), Collections.emptyList(),
                        UniqueConstraint.primaryKey("primary key", Collections.singletonList("hash"))),
                "PARTITIONED BY (`range1`, `range2`)", StopBehavior.FAIL_ON_CHECKPOINTING));

        parameters.put("testLakeSoulSourceFailOnSinkCheckPointStarting", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash1", DataTypes.INT()), Column.physical("hash2",
                                        DataTypes.STRING()),
                                Column.physical("range", DataTypes.DATE()), Column.physical("value",
                                        DataTypes.BYTES())),
                        Collections.emptyList(), UniqueConstraint.primaryKey("primary key", Arrays.asList("hash1",
                        "hash2"))),
                "PARTITIONED BY (`range`)", StopBehavior.FAIL_ON_CHECKPOINT_STARTING));

        parameters.put("testLakeSoulSourceFailOnSinkInvokeStarting", Tuple3.of(new ResolvedSchema(
                        Arrays.asList(Column.physical("hash1", DataTypes.INT()), Column.physical("hash2",
                                        DataTypes.INT()),
                                Column.physical("range1", DataTypes.STRING()), Column.physical("range2",
                                        DataTypes.BOOLEAN()),
                                Column.physical("value", DataTypes.DOUBLE())), Collections.emptyList(),
                        UniqueConstraint.primaryKey("primary key", Arrays.asList("hash1", "hash2"))),
                "PARTITIONED BY (`range1`, `range2`)", StopBehavior.FAIL_ON_INVOKE_STARTING));

    }

    public static String generateDataWithIndexByDatatype(Integer index, Column column) {
        int value = column.getName().contains("range") ? index / 3 : index;
        switch (column.getDataType().getLogicalType().getTypeRoot().name().toLowerCase()) {
            case "integer":
                return String.valueOf(value);
            case "varchar":
                return String.format("'%d'", value);
            case "timestamp_with_local_time_zone":
                return String.format("TO_TIMESTAMP_LTZ(%d, 0)", value * 3600 * 24);
            case "double":
                return String.valueOf(Double.valueOf(index));
            case "date":
                return String.format("TO_DATE('2023-01-%02d')", value);
            case "boolean":
                return String.valueOf(value % 2 == 0);
            case "varbinary":
                return String.format("X'0%hAF'", value);
            default:
                throw new IllegalStateException("Unexpected value: " +
                        column.getDataType().getLogicalType().getTypeRoot().name().toLowerCase());
        }
    }

    public static String generateExpectedDataWithIndexByDatatype(Integer index, Column column) {
        int value = column.getName().contains("range") ? index / 3 : index;
        switch (column.getDataType().getLogicalType().getTypeRoot().name().toLowerCase()) {
            case "integer":
                return String.valueOf(value);
            case "varchar":
                return String.format("%d", value);
            case "timestamp_with_local_time_zone":
                return String.format("1970-01-%02dT00:00:00Z", value + 1);
            case "double":
                return String.valueOf(Double.valueOf(index));
            case "date":
                return value > 0 ? String.format("2023-01-%02d", value) : "null";
            case "boolean":
                return String.valueOf(value % 2 == 0);
            case "varbinary":
                return String.format("[%d, -81]", value);
            default:
                throw new IllegalStateException("Unexpected value: " +
                        column.getDataType().getLogicalType().getTypeRoot().name().toLowerCase());
        }
    }

    @Test
    public void testLakeSoulSourceFailOnSinkInvokeStarting() throws IOException {
        String testName = "testLakeSoulSourceFailOnSinkInvokeStarting";
        Tuple3<ResolvedSchema, String, StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        List<String> testData = indexArr.stream()
                .map(i -> resolvedSchema.getColumns().stream().map(col -> generateDataWithIndexByDatatype(i, col))
                        .collect(Collectors.joining(",", "(", ")"))).collect(Collectors.toList());
        List<String> expectedData = indexArr.stream().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        testLakeSoulSource(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                testData, 60);

        List<String> actualData = new ArrayList<>(ExactlyOnceRowDataPrintFunction.finalizeList);
        actualData.sort(Comparator.comparing(Function.identity()));
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSourceFailOnSinkInvokeFinished() throws IOException {
        String testName = "testLakeSoulSourceFailOnSinkInvokeFinished";
        Tuple3<ResolvedSchema, String, StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        List<String> testData = indexArr.stream()
                .map(i -> resolvedSchema.getColumns().stream().map(col -> generateDataWithIndexByDatatype(i, col))
                        .collect(Collectors.joining(",", "(", ")"))).collect(Collectors.toList());
        List<String> expectedData = indexArr.stream().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        testLakeSoulSource(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                testData, 60);

        List<String> actualData = new ArrayList<>(ExactlyOnceRowDataPrintFunction.finalizeList);
        actualData.sort(Comparator.comparing(Function.identity()));
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSourceFailOnSinkCheckPointing() throws IOException {
        String testName = "testLakeSoulSourceFailOnSinkCheckPointing";
        Tuple3<ResolvedSchema, String, StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        List<String> testData = indexArr.stream()
                .map(i -> resolvedSchema.getColumns().stream().map(col -> generateDataWithIndexByDatatype(i, col))
                        .collect(Collectors.joining(",", "(", ")"))).collect(Collectors.toList());
        List<String> expectedData = indexArr.stream().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        testLakeSoulSource(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                testData, 60);

        List<String> actualData = new ArrayList<>(ExactlyOnceRowDataPrintFunction.finalizeList);
        actualData.sort(Comparator.comparing(Function.identity()));
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    @Test
    public void testLakeSoulSourceFailOnSinkCheckPointStarting() throws IOException {
        String testName = "testLakeSoulSourceFailOnSinkCheckPointStarting";
        Tuple3<ResolvedSchema, String, StopBehavior> tuple3 = parameters.get(testName);
        ResolvedSchema resolvedSchema = tuple3.f0;

        List<String> testData = indexArr.stream()
                .map(i -> resolvedSchema.getColumns().stream().map(col -> generateDataWithIndexByDatatype(i, col))
                        .collect(Collectors.joining(",", "(", ")"))).collect(Collectors.toList());
        List<String> expectedData = indexArr.stream().map(i -> resolvedSchema.getColumns().stream()
                .map(col -> generateExpectedDataWithIndexByDatatype(i, col))
                .collect(Collectors.joining(", ", "+I[", "]"))).collect(Collectors.toList());

        testLakeSoulSource(resolvedSchema, tuple3.f2, tuple3.f1, tempFolder.newFolder(testName).getAbsolutePath(),
                testData, 60);

        List<String> actualData = new ArrayList<>(ExactlyOnceRowDataPrintFunction.finalizeList);
        actualData.sort(Comparator.comparing(Function.identity()));
        expectedData.sort(Comparator.comparing(Function.identity()));

        assertThat(actualData.toString()).isEqualTo(expectedData.toString());
    }

    public void testLakeSoulSource(ResolvedSchema resolvedSchema, StopBehavior behavior, String partitionBy,
                                   String path, List<String> testData, int timeout) throws IOException {

        testLakeSoulCatalog.cleanForTest();
        MockLakeSoulCatalog.TestLakeSoulDynamicTableFactory testFactory =
                new MockLakeSoulCatalog.TestLakeSoulDynamicTableFactory();
        TestTableSink testTableSink =
                new TestTableSink(resolvedSchema.toPhysicalRowDataType(), "test", false, 2, behavior);
        testFactory.setTestSink(testTableSink);

        testLakeSoulCatalog.setTestFactory(testFactory);


        streamTableEnv.executeSql(String.format(createSourceSqlFormat, resolvedSchema, partitionBy, path, 2));


        streamTableEnv.executeSql(String.format(createSinkSqlFormat, resolvedSchema));

        streamTableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        streamTableEnv.getConfig().setLocalTimeZone(TimeZone.getTimeZone("UTC").toZoneId());

        ExactlyOnceRowDataPrintFunction.cleanStatus();
        final TableResult execute = streamTableEnv.executeSql(
                "insert into test_sink select * from test_source");
        Thread thread = new Thread(() -> {
            try {

                execute.await(timeout, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                System.out.println("stream read end");
            } finally {
                execute.getJobClient().get().cancel();
            }
        });
        thread.start();
        TableEnvironment batchTableEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
        LakeSoulTestUtils.registerLakeSoulCatalog(batchTableEnv, lakeSoulCatalog);
        for (String value : testData) {
            System.out.println("batch insert value: " + value);
            batchTableEnv.executeSql(String.format("insert into test_source VALUES %s", value));
            try {
                int tps = 4;
                Thread.sleep(1000 / tps);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private enum StopBehavior {
        NO_FAILURE, FAIL_ON_CHECKPOINT_STARTING, FAIL_ON_CHECKPOINTING, FAIL_ON_SNAPSHOTSTATE_FINISHED,
        FAIL_ON_INVOKE_FINISHED, FAIL_ON_INVOKE_STARTING,
    }

    public static class TestTableSink implements DynamicTableSink {
        private final DataType type;
        private final String printIdentifier;
        private final boolean stdErr;
        private final @Nullable Integer parallelism;
        private final StopBehavior stopBehavior;

        private TestTableSink(DataType type, String printIdentifier, boolean stdErr, Integer parallelism,
                              StopBehavior stopBehavior) {
            this.type = type;
            this.printIdentifier = printIdentifier;
            this.stdErr = stdErr;
            this.parallelism = parallelism;
            this.stopBehavior = stopBehavior;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return requestedMode;
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(DynamicTableSink.Context context) {
            DataStructureConverter converter = context.createDataStructureConverter(type);
            return SinkFunctionProvider.of(
                    new ExactlyOnceRowDataPrintFunction(converter, printIdentifier, stdErr, stopBehavior), parallelism);
        }

        @Override
        public DynamicTableSink copy() {
            return new TestTableSink(type, printIdentifier, stdErr, parallelism, stopBehavior);
        }

        @Override
        public String asSummaryString() {
            return "Print to " + (stdErr ? "System.err" : "System.out");
        }
    }

    /**
     * Implementation of the SinkFunction converting {@link RowData} to string and passing to {@link
     * PrintSinkFunction}.
     */
    private static class ExactlyOnceRowDataPrintFunction extends RichSinkFunction<RowData>
            implements CheckpointedFunction, CheckpointListener {

        //must be static?
        public static final List<String> finalizeList = Collections.synchronizedList(new ArrayList<>());
        private static final long serialVersionUID = 1L;
        private static Long failTiming = 20 * 1000L;
        private static Long failTimeInterval = 11 * 1000L;
        private final DynamicTableSink.DataStructureConverter converter;
        private final PrintSinkOutputWriter<String> writer;
        private final Long failStartTime;
        private final Long failEndTime;
        private final StopBehavior stopBehavior;
        private transient ListState<String> checkpointedState;

        private ExactlyOnceRowDataPrintFunction(DynamicTableSink.DataStructureConverter converter,
                                                String printIdentifier, boolean stdErr, StopBehavior stopBehavior) {
            this.converter = converter;
            this.writer = new PrintSinkOutputWriter<>(printIdentifier, stdErr);
            this.stopBehavior = stopBehavior;
            failStartTime = System.currentTimeMillis() + failTiming;
            failEndTime = failStartTime + failTimeInterval;
            System.out.println("Sink will fail from " +
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(failStartTime), ZoneId.systemDefault()) + " to " +
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(failEndTime), ZoneId.systemDefault()));
        }

        public static void cleanStatus() {
            finalizeList.clear();
        }

        public static void setFailTiming(Long failTiming) {

            ExactlyOnceRowDataPrintFunction.failTiming = failTiming;
        }

        public static void setFailTimeInterval(Long failTimeInterval) {
            ExactlyOnceRowDataPrintFunction.failTimeInterval = failTimeInterval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
            writer.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
        }

        @Override
        public void invoke(RowData value, Context context) {
            tryStop(StopBehavior.FAIL_ON_INVOKE_STARTING);

            Object data = converter.toExternal(value);
            assert data != null;
            writer.write(data.toString());
            try {
                checkpointedState.add(data.toString());
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage() + "at checkpointedState.add: " + data);
            }

            tryStop(StopBehavior.FAIL_ON_INVOKE_FINISHED);
        }

        private void tryStop(StopBehavior behavior) {
            if (stopBehavior != behavior) return;

            long current = System.currentTimeMillis();
            if (current > failStartTime && current < failEndTime) {
                String msg = "Sink fail with " + stopBehavior + " at " + LocalDateTime.now();
                System.out.println(msg);
                throw new RuntimeException(msg);
            }
        }

        /**
         * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
         * was committed.
         *
         * <p>These notifications are "best effort", meaning they can sometimes be skipped. To behave
         * properly, implementers need to follow the "Checkpoint Subsuming Contract". Please see the
         * {@link CheckpointListener class-level JavaDocs} for details.
         *
         * <p>Please note that checkpoints may generally overlap, so you cannot assume that the {@code
         * notifyCheckpointComplete()} call is always for the latest prior checkpoint (or snapshot) that
         * was taken on the function/operator implementing this interface. It might be for a checkpoint
         * that was triggered earlier. Implementing the "Checkpoint Subsuming Contract" (see above)
         * properly handles this situation correctly as well.
         *
         * <p>Please note that throwing exceptions from this method will not cause the completed
         * checkpoint to be revoked. Throwing exceptions will typically cause task/job failure and
         * trigger recovery.
         *
         * @param checkpointId The ID of the checkpoint that has been completed.
         * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
         *                   the task. Not that this will NOT lead to the checkpoint being revoked.
         */
        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            System.out.println("notifyCheckpointComplete:" + checkpointId);
//                checkpointComplete=true;
        }

        /**
         * This method is called when a snapshot for a checkpoint is requested. This acts as a hook to
         * the function to ensure that all state is exposed by means previously offered through {@link
         * FunctionInitializationContext} when the Function was initialized, or offered now by {@link
         * FunctionSnapshotContext} itself.
         *
         * @param context the context for drawing a snapshot of the operator
         * @throws Exception Thrown, if state could not be created ot restored.
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            tryStop(StopBehavior.FAIL_ON_CHECKPOINT_STARTING);

            ArrayList<String> tmpList = new ArrayList<>();
            for (String string : checkpointedState.get()) {
                tmpList.add(string);
            }
            tryStop(StopBehavior.FAIL_ON_CHECKPOINTING);

            finalizeList.addAll(tmpList);
            System.out.println("finalizing state:" + tmpList);
            checkpointedState.clear();

            tryStop(StopBehavior.FAIL_ON_SNAPSHOTSTATE_FINISHED);

        }

        /**
         * This method is called when the parallel function instance is created during distributed
         * execution. Functions typically set up their state storing data structures in this method.
         *
         * @param context the context for initializing the operator
         * @throws Exception Thrown, if state could not be created ot restored.
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<String> descriptor =
                    new ListStateDescriptor<>("checkpointedState", TypeInformation.of(new TypeHint<String>() {
                    }));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        }
    }

    public static class TestSource extends TableFactoryHarness.ScanSourceBase {

    }
}
