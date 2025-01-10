// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.connector;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.source.arrow.LakeSoulArrowSource;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.lakesoul.test.mock.MockLakeSoulArrowSource;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.AUTO_SCHEMA_CHANGE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.FILE_ROLLING_SIZE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.INFERRING_SCHEMA;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.MAX_ROW_GROUP_SIZE;

public class LakeSoulArrowConnectorCase extends AbstractTestBase {
    //    @Test
    public void test() throws Exception {
        int parallelism = 2;
        StreamExecutionEnvironment execEnv =
                LakeSoulTestUtils.createStreamExecutionEnvironment(parallelism, 2000L, 2000L);
        StreamTableEnvironment tableEnv = LakeSoulTestUtils.createTableEnvInStreamingMode(
                execEnv, parallelism);
        DataStreamSource<LakeSoulArrowWrapper> source =
                execEnv.addSource(new MockLakeSoulArrowSource.MockSourceFunction(100, 100L));
        String name = "Print Sink";
        PrintSinkFunction<LakeSoulArrowWrapper> printFunction = new PrintSinkFunction<>(name, false);

        Configuration conf = new Configuration();
        conf.set(LakeSoulSinkOptions.BUCKET_PARALLELISM, parallelism);
        conf.set(AUTO_SCHEMA_CHANGE, true);

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = execEnv;
        context.conf = conf;

        LakeSoulMultiTableSinkStreamBuilder.buildArrowSink(context, source);

        execEnv.execute("Test MockLakeSoulArrowSource.MockSourceFunction");
    }

    @Test
    public void testManualArrowBatch() throws Exception {

        Configuration conf = new Configuration();
        conf.set(MAX_ROW_GROUP_SIZE, 2);
        conf.set(FILE_ROLLING_SIZE, 10L);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        Schema arrowSchema = new Schema(Arrays.asList(
                // string column
                new Field("field_string", FieldType.nullable(new ArrowType.Utf8()), null),
                // signed 64 bit integer column
                new Field("field_int64", FieldType.nullable(new ArrowType.Int(64, true)), null),
                // float 32 column
                new Field("field_float32",
                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
                // date partition column
                new Field("date", FieldType.nullable(new ArrowType.Utf8()), null),
                // tail_num partition column
                new Field("tail_num", FieldType.nullable(new ArrowType.Utf8()), null)
        ));

        // TableInfo object can be reused
        TableInfo sinkTableInfo = TableInfo
                .newBuilder()
                .setTableId("NOT_USED")
                .setTableNamespace("default")
                .setTableName("qar_table")
                .setTableSchema(arrowSchema.toJson())
                .setTablePath("file:///tmp/test_arrow_sink")
                .setPartitions(DBUtil.formatTableInfoPartitionsField(
                        // no primary field
                        Collections.emptyList(),
                        // partition fields
                        Arrays.asList("date", "tail_num")))
                .setProperties("{}")
                .build();

        byte[] sinkTableInfoEncoded = sinkTableInfo.toByteArray();
        List<LakeSoulArrowWrapper> arrowBatches = new ArrayList<>();

        for (int batch = 0; batch < 10; batch++) {
            try (
                    BufferAllocator allocator = new RootAllocator();
                    VectorSchemaRoot arrowBatch = VectorSchemaRoot.create(arrowSchema, allocator)
            ) {
                int batchSize = 3;

                // create string vector
                VarCharVector strVector = (VarCharVector) arrowBatch.getVector("field_string");
                strVector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    strVector.set(i, ("David_" + batch).getBytes());
                    strVector.set(i, ("Gladis_" + batch).getBytes());
                    strVector.set(i, ("Juan_" + batch).getBytes());
                }
                strVector.setValueCount(batchSize);

                // create int64 vector
                BigIntVector bigIntVector = (BigIntVector) arrowBatch.getVector("field_int64");
                bigIntVector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    bigIntVector.set(i, i + batch);
                }
                bigIntVector.setValueCount(batchSize);

                // create float32 vector
                Float4Vector float4Vector = (Float4Vector) arrowBatch.getVector("field_float32");
                float4Vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    float4Vector.set(i, (float) i + 2.0f * (float) batch);
                }
                float4Vector.setValueCount(batchSize);

                // create date partition column vector
                VarCharVector dateVector = (VarCharVector) arrowBatch.getVector("date");
                dateVector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    dateVector.set(i, ("2024-06-0" + batch % 2).getBytes());
                    dateVector.set(i, ("2024-06-0" + batch % 2).getBytes());
                    dateVector.set(i, ("2024-06-0" + batch % 2).getBytes());
                }
                dateVector.setValueCount(batchSize);

                // create tail_num partition column vector
                VarCharVector tailNumVector = (VarCharVector) arrowBatch.getVector("tail_num");
                tailNumVector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    tailNumVector.set(i, ("B200" + batch % 2).getBytes());
                    tailNumVector.set(i, ("B200" + batch % 2).getBytes());
                    tailNumVector.set(i, ("B200" + batch % 2).getBytes());
                }
                tailNumVector.setValueCount(batchSize);

                arrowBatch.setRowCount(batchSize);

                arrowBatches.add(new LakeSoulArrowWrapper(sinkTableInfoEncoded, arrowBatch));
            }
        }

        DataStreamSource<LakeSoulArrowWrapper> source = env.fromCollection(arrowBatches);
        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = (Configuration) env.getConfiguration();
        context.conf.set(AUTO_SCHEMA_CHANGE, true);

        LakeSoulMultiTableSinkStreamBuilder.buildArrowSink(context, source);

        env.execute("Test Arrow Sink");

        // read data
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.registerCatalog("lakesoul", new LakeSoulCatalog());
        tEnv.useCatalog("lakesoul");
        tEnv.executeSql("select * from `default`.`qar_table`").print();
        tEnv.executeSql("drop table `default`.`qar_table`").print();
    }

    @Test
    public void testSchemaChange() throws Exception {
        Configuration conf = new Configuration();
        conf.set(MAX_ROW_GROUP_SIZE, 2);
        conf.set(FILE_ROLLING_SIZE, 10L);
        conf.set(AUTO_SCHEMA_CHANGE, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        {
            Schema arrowSchema = new Schema(Arrays.asList(
                    // string column
                    new Field("field_string", FieldType.nullable(new ArrowType.Utf8()), null),
                    // signed 64 bit integer column
                    new Field("field_int64", FieldType.nullable(new ArrowType.Int(64, true)), null),
                    // float 32 column
                    new Field("field_float32",
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
                    // date partition column
                    new Field("timestamp", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null)
            ));

            // TableInfo object can be reused
            TableInfo sinkTableInfo = TableInfo
                    .newBuilder()
                    .setTableId("NOT_USED")
                    .setTableNamespace("default")
                    .setTableName("qar_table")
                    .setTableSchema(arrowSchema.toJson())
                    .setTablePath("file:///tmp/test_arrow_sink")
                    .setPartitions(";")
                    .setProperties("{}")
                    .build();

            byte[] sinkTableInfoEncoded = sinkTableInfo.toByteArray();
            List<LakeSoulArrowWrapper> arrowBatches = new ArrayList<>();

            try (
                    BufferAllocator allocator = new RootAllocator();
                    VectorSchemaRoot arrowBatch = VectorSchemaRoot.create(arrowSchema, allocator)
            ) {
                int batchSize = 3;

                // create string vector
                VarCharVector strVector = (VarCharVector) arrowBatch.getVector("field_string");
                strVector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    strVector.set(i, ("" + i).getBytes());
                    strVector.set(i, ("" + i).getBytes());
                    strVector.set(i, ("" + i).getBytes());
                }
                strVector.setValueCount(batchSize);

                // create int64 vector
                BigIntVector bigIntVector = (BigIntVector) arrowBatch.getVector("field_int64");
                bigIntVector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    bigIntVector.set(i, i + i);
                }
                bigIntVector.setValueCount(batchSize);

                // create float32 vector
                Float4Vector float4Vector = (Float4Vector) arrowBatch.getVector("field_float32");
                float4Vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    float4Vector.set(i, (float) i + 2.0f * (float) i);
                }
                float4Vector.setValueCount(batchSize);

                // create time stamp vector
                TimeStampMilliTZVector timestampVector = (TimeStampMilliTZVector) arrowBatch.getVector("timestamp");
                timestampVector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    timestampVector.set(i, Instant.now().toEpochMilli());
                }
                timestampVector.setValueCount(batchSize);

                arrowBatch.setRowCount(batchSize);

                arrowBatches.add(new LakeSoulArrowWrapper(sinkTableInfoEncoded, arrowBatch));
            }

            DataStreamSource<LakeSoulArrowWrapper> source = env.fromCollection(arrowBatches);
            LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
            context.env = env;
            context.conf = (Configuration) env.getConfiguration();
            context.conf.set(AUTO_SCHEMA_CHANGE, true);

            LakeSoulMultiTableSinkStreamBuilder.buildArrowSink(context, source);

            env.execute("Test Arrow Sink");

            // read data
            TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
            tEnv.registerCatalog("lakesoul", new LakeSoulCatalog());
            tEnv.useCatalog("lakesoul");
            tEnv.executeSql("desc `default`.`qar_table`").print();
            tEnv.executeSql("select * from `default`.`qar_table`").print();
        }

        // auto change schema
        {
            Schema arrowSchema = new Schema(Arrays.asList(
                    // string column
                    new Field("field_string", FieldType.nullable(new ArrowType.Int(64, true)), null),
                    // signed 64 bit integer column
                    new Field("field_int64", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                    // float 32 column
                    new Field("field_float32",
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                    // date partition column
                    new Field("timestamp", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null)
            ));

            // TableInfo object can be reused
            TableInfo sinkTableInfo = TableInfo
                    .newBuilder()
                    .setTableId("NOT_USED")
                    .setTableNamespace("default")
                    .setTableName("qar_table")
                    .setTableSchema(arrowSchema.toJson())
                    .setTablePath("file:///tmp/test_arrow_sink")
                    .setPartitions(";")
                    .setProperties("{}")
                    .build();

            byte[] sinkTableInfoEncoded = sinkTableInfo.toByteArray();
            List<LakeSoulArrowWrapper> arrowBatches = new ArrayList<>();

            try (
                    BufferAllocator allocator = new RootAllocator();
                    VectorSchemaRoot arrowBatch = VectorSchemaRoot.create(arrowSchema, allocator)
            ) {
                int batchSize = 3;

                // create string vector
                BigIntVector strVector = (BigIntVector) arrowBatch.getVector("field_string");
                strVector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    strVector.set(i, i + 15);
                    strVector.set(i, i + 35);
                    strVector.set(i, i + 55);
                }
                strVector.setValueCount(batchSize);

                // create int64 vector
                VarCharVector bigIntVector = (VarCharVector) arrowBatch.getVector("field_int64");
                bigIntVector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    bigIntVector.set(i, ("" + (i + i)).getBytes());
                }
                bigIntVector.setValueCount(batchSize);

                // create float32 vector
                Float8Vector float4Vector = (Float8Vector) arrowBatch.getVector("field_float32");
                float4Vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    float4Vector.set(i, (double) i + 2.0f * (double) i);
                }
                float4Vector.setValueCount(batchSize);

                // create time stamp vector
                TimeStampMilliVector timestampVector = (TimeStampMilliVector) arrowBatch.getVector("timestamp");
                timestampVector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    timestampVector.set(i, Instant.now().toEpochMilli());
                }
                timestampVector.setValueCount(batchSize);

                arrowBatch.setRowCount(batchSize);

                arrowBatches.add(new LakeSoulArrowWrapper(sinkTableInfoEncoded, arrowBatch));
            }

            DataStreamSource<LakeSoulArrowWrapper> source = env.fromCollection(arrowBatches);
            LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
            context.env = env;
            context.conf = (Configuration) env.getConfiguration();
            context.conf.set(AUTO_SCHEMA_CHANGE, true);

            LakeSoulMultiTableSinkStreamBuilder.buildArrowSink(context, source);

            env.execute("Test Arrow Sink");
            // read data
            TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
            tEnv.registerCatalog("lakesoul", new LakeSoulCatalog());
            tEnv.useCatalog("lakesoul");
            tEnv.executeSql("desc `default`.`qar_table`").print();
            tEnv.executeSql("select * from `default`.`qar_table`").print();
        }
    }

    public static void main(String[] args) throws Exception {

        int parallelism = 2;

        StreamExecutionEnvironment
                execEnv =
                LakeSoulTestUtils.createStreamExecutionEnvironment(parallelism, 2000L, 2000L);

        Configuration conf = new Configuration();
        conf.set(INFERRING_SCHEMA, true);
        DataStreamSource<LakeSoulArrowWrapper> source = execEnv.fromSource(
                LakeSoulArrowSource.create(
                        "default",
                        MockLakeSoulArrowSource.MockSourceFunction.tableName,
                        conf
                ),
                WatermarkStrategy.noWatermarks(),
                "LakeSoul Arrow Source"
        );

        String name = "Print Sink";
        PrintSinkFunction<LakeSoulArrowWrapper> printFunction = new PrintSinkFunction<>(name, false);

        DataStreamSink<LakeSoulArrowWrapper> sink = source.addSink(printFunction).name(name);
        execEnv.execute("Test MockLakeSoulArrowSource.MockSourceFunction");

    }
}
