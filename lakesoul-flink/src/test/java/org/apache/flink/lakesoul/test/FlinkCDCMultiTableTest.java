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

package org.apache.flink.lakesoul.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.JsonSourceRecord;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FlinkCDCMultiTableTest {

    Configuration conf = new Configuration();

    StreamExecutionEnvironment env;

    MiniClusterWithClientResource flinkCluster;

    @Before
    public void before() throws Exception {

        conf.set(LakeSoulSinkOptions.USE_CDC, true)
            .set(LakeSoulSinkOptions.SOURCE_PARALLELISM, 4)
            .set(LakeSoulSinkOptions.BUCKET_PARALLELISM, 2)
            .set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true)
            .set(RestOptions.ADDRESS, "localhost")
            .set(WebOptions.SUBMIT_ENABLE, true)
            .set(RestOptions.PORT, 8081);
//        flinkCluster =
//                new MiniClusterWithClientResource(
//                        new MiniClusterResourceConfiguration.Builder()
//                                .setNumberSlotsPerTaskManager(2)
//                                .setNumberTaskManagers(1)
//                                .setConfiguration(conf)
//                                .build());
//        flinkCluster.before();
//        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointStorage("file:/tmp/localState");
        config.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakesoul", lakesoulCatalog);
        tEnvs.useCatalog("lakesoul");

        tEnvs.executeSql("CREATE DATABASE IF NOT EXISTS test_cdc");

        tEnvs.executeSql(
                "CREATE TABLE IF NOT EXISTS test_cdc.mysql_test_1 ( id int," +
                " name string," +
                " dt int," +
                " date1 date," +
                " ts timestamp, " +
                "primary key (id) NOT ENFORCED ) " +
                " with ('connector' = 'lakesoul'," +
                "'format'='parquet','path'='" +
                "/tmp/lakesoul/test_cdc/mysql_test_1" + "'," +
                "'use_cdc'='true'," +
                "'hashBucketNum'='2')");

        tEnvs.executeSql(
                "CREATE TABLE IF NOT EXISTS test_cdc.mysql_test_2 ( id int," +
                " name string," +
                " dt int," +
                " new_col string, " +
                "primary key (id) NOT ENFORCED ) " +
                " with ('connector' = 'lakesoul'," +
                "'format'='parquet','path'='" +
                "/tmp/lakesoul/test_cdc/mysql_test_2" + "'," +
                "'use_cdc'='true'," +
                "'hashBucketNum'='2')");
    }

    @After
    public void after() {
//        flinkCluster.after();
    }

    @Test
    public void test() throws Exception {

        MySqlSourceBuilder<JsonSourceRecord> sourceBuilder = MySqlSource.<JsonSourceRecord>builder()
            .hostname("localhost")
            .port(3306)
            .databaseList("test_cdc") // set captured // database
            .tableList("test_cdc.*") // set captured table
            .username("root")
            .password("root");

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.sourceBuilder = sourceBuilder;
        context.conf = conf;

        LakeSoulMultiTableSinkStreamBuilder builder = new LakeSoulMultiTableSinkStreamBuilder(context);

        DataStreamSource<JsonSourceRecord> source = builder.buildMultiTableSource();

        Tuple2<DataStream<JsonSourceRecord>, DataStream<JsonSourceRecord>> streams = builder.buildCDCAndDDLStreamsFromSource(source);

        DataStream<JsonSourceRecord> stream = builder.buildHashPartitionedCDCStream(streams.f0);

        DataStreamSink<JsonSourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
        DataStreamSink<JsonSourceRecord> ddlSink = builder.buildLakeSoulDDLSink(streams.f1);
        env.execute("test");

//        StreamGraph sg = env.getStreamGraph();
        // Note:
//        sg.setSavepointRestoreSettings(SavepointRestoreSettings
//             .forPath("/tmp/localState/1a4d2ab3705489acb671e44212c745b2/chk-1/"));

//        flinkCluster.getMiniCluster().executeJobBlocking(sg.getJobGraph());

//        env.execute("Flink Multi Table CDC Sink To LakeSoul");
    }
}
