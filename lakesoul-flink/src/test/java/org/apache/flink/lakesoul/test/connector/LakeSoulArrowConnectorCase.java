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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.lakesoul.test.mock.MockLakeSoulArrowSource;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.util.*;

import static org.apache.flink.lakesoul.metadata.LakeSoulCatalog.TABLE_ID_PREFIX;

public class LakeSoulArrowConnectorCase extends AbstractTestBase {
    @Test
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

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = execEnv;
        context.conf = conf;

        LakeSoulMultiTableSinkStreamBuilder.buildArrowSink(context, source);

        execEnv.execute("Test MockLakeSoulArrowSource.MockSourceFunction");
    }

    @Test
    public void testManualArrowBatch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Schema arrowSchema = new Schema(Arrays.asList(
                // string column
                new Field("field_string", FieldType.nullable(new ArrowType.Utf8()), null),
                // signed 64 bit integer column
                new Field("field_int64", FieldType.nullable(new ArrowType.Int(64, true)), null),
                // float 32 column
                new Field("field_float32", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
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

        for (int batch = 0; batch < 2; batch++) {
            try (
                    BufferAllocator allocator = new RootAllocator();
                    VectorSchemaRoot arrowBatch = VectorSchemaRoot.create(arrowSchema, allocator)
            ) {
                int batchSize = 3;

                // create string vector
                VarCharVector strVector = (VarCharVector) arrowBatch.getVector("field_string");
                strVector.allocateNew(batchSize);
                strVector.set(0, ("David_" + batch).getBytes());
                strVector.set(1, ("Gladis_" + batch).getBytes());
                strVector.set(2, ("Juan_" + batch).getBytes());
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
                dateVector.set(0, ("2024-06-0" + batch).getBytes());
                dateVector.set(1, ("2024-06-0" + batch).getBytes());
                dateVector.set(2, ("2024-06-0" + batch).getBytes());
                dateVector.setValueCount(batchSize);

                // create tail_num partition column vector
                VarCharVector tailNumVector = (VarCharVector) arrowBatch.getVector("tail_num");
                tailNumVector.allocateNew(batchSize);
                tailNumVector.set(0, ("B200" + batch).getBytes());
                tailNumVector.set(1, ("B200" + batch).getBytes());
                tailNumVector.set(2, ("B200" + batch).getBytes());
                tailNumVector.setValueCount(batchSize);

                arrowBatch.setRowCount(batchSize);

                arrowBatches.add(new LakeSoulArrowWrapper(sinkTableInfoEncoded, arrowBatch));
            }
        }

        DataStreamSource<LakeSoulArrowWrapper> source = env.fromCollection(arrowBatches);
        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        Configuration conf = new Configuration();
        context.conf = conf;

        LakeSoulMultiTableSinkStreamBuilder.buildArrowSink(context, source);

        env.execute("Test Arrow Sink");

        // read data
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.registerCatalog("lakesoul", new LakeSoulCatalog());
        tEnv.useCatalog("lakesoul");
        tEnv.executeSql("select * from `default`.`qar_table`").print();
    }
}
