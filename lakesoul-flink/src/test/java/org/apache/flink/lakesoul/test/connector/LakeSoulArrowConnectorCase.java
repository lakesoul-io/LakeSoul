// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.lakesoul.test.mock.MockLakeSoulArrowSource;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

public class LakeSoulArrowConnectorCase extends AbstractTestBase {
    @Test
    public void test() throws Exception {
        int parallelism = 2;
        StreamExecutionEnvironment execEnv = LakeSoulTestUtils.createStreamExecutionEnvironment(parallelism, 2000L, 2000L);
        StreamTableEnvironment tableEnv = LakeSoulTestUtils.createTableEnvInStreamingMode(
                execEnv, parallelism);
        DataStreamSource<LakeSoulArrowWrapper> source = execEnv.addSource(new MockLakeSoulArrowSource.MockSourceFunction(100, 100L));
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
}
