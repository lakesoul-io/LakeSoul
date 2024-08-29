
package org.apache.flink.lakesoul.sink;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.tool.NativeOptions;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BATCH_SIZE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.MAX_ROW_GROUP_VALUE_NUMBER;

public class ArrowSinkDemo {
    static long checkpointInterval = 5 * 1000;
    static int tableNum = 8;


    public static void main(String[] args) throws Exception {

//         read data

//        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
//        tEnv.registerCatalog("lakesoul", new LakeSoulCatalog());
//        tEnv.useCatalog("lakesoul");
//        long total = 0;
//        for (int i = 0; i < tableNum; i++) {
//            List<Row> collect = CollectionUtil.iteratorToList(tEnv.executeSql("select count(*) as `rows` from `default`.`qar_table_" + i + "`").collect());
//            total += (long) collect.get(0).getField(0);
//        }
//        System.out.println(total);
//        System.exit(0);

        new LakeSoulCatalog().cleanForTest();

        Configuration conf = new Configuration();
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("512m"));
        conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("512m"));
        conf.set(NativeOptions.MEM_LIMIT, String.valueOf(1024 * 1024 * 10));
//        conf.set(TaskManagerOptions.JVM_OVERHEAD_MAX, MemorySize.parse("20m"));
//        conf.set(TaskManagerOptions.JVM_METASPACE, MemorySize.parse("512m"));
//        conf.set(ExecutionCheckpointingOptions.TOLERABLE_FAILURE_NUMBER, 2);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);

        int cols = 2000;
        int batchSize = 2000;
        int batchPerSecond = 20 * tableNum;
        int sourceParallelism = 8;
        int sinkParallelism = 4;

        int batchPerTask = 100 * tableNum;

        // TableInfo object can be reused
        List<TableInfo> sinkTableInfo = new ArrayList<>();
        for (int i = 0; i < tableNum; i++) {
            List<Field> fields = new ArrayList<>();
            for (int j = 0; j < cols; j++) {
                fields.add(new Field("f_i32_" + i + "_" + j, FieldType.nullable(new ArrowType.Int(32, true)), null));
            }
            fields.add(new Field("date", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
            fields.add(new Field("fltNum", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
            fields.add(new Field("tailNum", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
            Schema arrowSchema = new Schema(fields);
            TableInfo tableInfo = TableInfo
                    .newBuilder()
                    .setTableId("NOT_USED")
                    .setTableNamespace("default")
                    .setTableName("qar_table_" + i)
                    .setTableSchema(arrowSchema.toJson())
                    .setTablePath("file:///tmp/test_arrow_sink_" + i)
                    .setPartitions(DBUtil.formatTableInfoPartitionsField(
                            // no primary field
                            Collections.emptyList(),
                            // partition fields
                            Collections.emptyList()
//                            , Arrays.asList("date", "fltNum", "tailNum")
                    ))
                    .setProperties("{}")
                    .build();
            sinkTableInfo.add(tableInfo);
        }

        DataStreamSource<LakeSoulArrowWrapper>
                source =
                env.addSource(new ArrowDataGenSource(sinkTableInfo, cols, batchSize, batchPerSecond, batchPerTask))
                        .setParallelism(sourceParallelism);
        env.getCheckpointConfig().setCheckpointInterval(checkpointInterval);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 1000L));
        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = (Configuration) env.getConfiguration();
        int rowGroupValues = 1 * (cols + 3) * batchSize;
        System.out.println("MAX_ROW_GROUP_VALUE_NUMBER=" + rowGroupValues);
//        context.conf.set(MAX_ROW_GROUP_VALUE_NUMBER, rowGroupValues);
        context.conf.set(BATCH_SIZE, batchSize);

        LakeSoulMultiTableSinkStreamBuilder.buildArrowSink(context, source, sinkParallelism);
//        String name = "Print Sink";
//        PrintSinkFunction<LakeSoulArrowWrapper> printFunction = new PrintSinkFunction<>(name, false);
//
//        DataStreamSink<LakeSoulArrowWrapper> sink = source.addSink(printFunction).name(name).setParallelism(2);

        env.execute("Test Arrow Sink");


    }

    public static class ArrowDataGenSource extends RichParallelSourceFunction<LakeSoulArrowWrapper>
            implements CheckpointedFunction {
        private static final Logger LOG = LoggerFactory.getLogger(ArrowDataGenSource.class);

        private transient ListState<Integer> checkpointedCount;

        public int count;

        int cols;
        int batchSize;
        int batchPerSecond;
        List<String> arrowSchema;
        List<byte[]> tableInfoEncoded;
        private volatile transient boolean isRunning;
        private transient int outputSoFar = 0;

        public ArrowDataGenSource(List<TableInfo> sinkTableInfo, int cols, int batchSize, int batchPerSecond, int total) {
            this.cols = cols;
            this.batchSize = batchSize;
            this.batchPerSecond = batchPerSecond;
            arrowSchema = sinkTableInfo.stream().map(TableInfo::getTableSchema).collect(Collectors.toList());
            tableInfoEncoded = sinkTableInfo.stream().map(TableInfo::toByteArray).collect(Collectors.toList());
            count = total;
        }

        @Override
        public void run(SourceContext<LakeSoulArrowWrapper> ctx) throws Exception {
            int batchRate = batchPerSecond / getRuntimeContext().getNumberOfParallelSubtasks();
            batchRate = Math.max(1, batchRate);
            LOG.info("Batch rate: {}", batchRate);
            long nextReadTime = System.currentTimeMillis();
            List<Schema> schema = new ArrayList<>();
            for (String s : arrowSchema) {
                Schema fromJSON = Schema.fromJSON(s);
                schema.add(fromJSON);
            }
            while (isRunning) {
                for (int i = 0; i < batchRate; i++) {
                    if (isRunning) {
                        synchronized (ctx.getCheckpointLock()) {
                            LakeSoulArrowWrapper generateArrow = generateArrow(schema.get(outputSoFar % tableNum), outputSoFar);
                            outputSoFar++;
                            if (count > 0) {
                                count--;
                            } else {
                                isRunning = false;
                                break;
                            }
                            ctx.collectWithTimestamp(generateArrow, System.currentTimeMillis());
                        }
                    } else {
                        break;
                    }
                }
                nextReadTime += 1000;
                long toWaitMs = nextReadTime - System.currentTimeMillis();
                while (toWaitMs > 0) {
                    Thread.sleep(toWaitMs);
                    toWaitMs = nextReadTime - System.currentTimeMillis();
                }

            }
            Thread.sleep(checkpointInterval / 10 * 9);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void close() throws Exception {
            super.close();
            LOG.info("Closing, generated {} batches", outputSoFar);
        }

        private LakeSoulArrowWrapper generateArrow(Schema schema, int outputSoFar) {
            int tableIdx = outputSoFar % tableNum;
            try (
                    BufferAllocator allocator = ArrowUtils.getRootAllocator();
                    VectorSchemaRoot arrowBatch = VectorSchemaRoot.create(schema, allocator)
            ) {
                for (int i = 0; i < cols; i++) {
                    IntVector intVector = (IntVector) arrowBatch.getVector("f_i32_" + tableIdx + "_" + i);
                    intVector.allocateNew(batchSize);
                    for (int j = 0; j < batchSize; j++) {
                        intVector.set(j, i + j);
                    }
                }
                byte[] date;
                byte[] fltNum;
                byte[] tailNum;
                switch (outputSoFar % 3) {
//                    case 0:
//                        date = "2024-07-01".getBytes();
//                        fltNum = "1234".getBytes();
//                        tailNum = "B4567".getBytes();
//                        break;
//                    case 1:
//                        date = "2024-07-02".getBytes();
//                        fltNum = "12 5".getBytes();
//                        tailNum = "B4568".getBytes();
//                        break;
//                    case 2:
//                        date = "2024-07-01".getBytes();
//                        fltNum = "1236".getBytes();
//                        tailNum = "B4569".getBytes();
//                        break;
                    default:
                        date = "2024-07-01".getBytes();
                        fltNum = "1236".getBytes();
                        tailNum = "B4569".getBytes();
                }
                VarCharVector dateVector = (VarCharVector) arrowBatch.getVector("date");
                dateVector.allocateNew(batchSize);
                VarCharVector fltNumVector = (VarCharVector) arrowBatch.getVector("fltNum");
                fltNumVector.allocateNew(batchSize);
                VarCharVector tailNumVector = (VarCharVector) arrowBatch.getVector("tailNum");
                tailNumVector.allocateNew(batchSize);
                for (int j = 0; j < batchSize; j++) {
                    dateVector.set(j, date);
                    fltNumVector.set(j, fltNum);
                    tailNumVector.set(j, tailNum);
                }
                arrowBatch.setRowCount(batchSize);
                return new LakeSoulArrowWrapper(tableInfoEncoded.get(tableIdx), arrowBatch);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("============= Source snapshotState getCheckpointId=" + context.getCheckpointId() + " ================");
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + " snapshotState context.getCheckpointId=" + context.getCheckpointId() + ", count=" + count);
            this.checkpointedCount.clear();
            try {
                this.checkpointedCount.add(count);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LOG.info("Snapshot state, generated {} batches", outputSoFar);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            isRunning = true;
            try {
                this.checkpointedCount = context
                        .getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("count", Integer.class));

                if (context.isRestored()) {
                    for (Integer count : this.checkpointedCount.get()) {
                        this.count = count;
                    }
                }
                System.out.println("initializeState count=" + count);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }


    }
}

    