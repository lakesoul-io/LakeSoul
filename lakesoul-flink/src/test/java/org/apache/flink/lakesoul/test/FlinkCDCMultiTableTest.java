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
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.JsonSourceRecord;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;

public class FlinkCDCMultiTableTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration()
                .set(LakeSoulSinkOptions.USE_CDC, true)
                .set(LakeSoulSinkOptions.BUCKET_PARALLELISM, 2);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(3000);
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakesoul", lakesoulCatalog);
        tEnvs.useCatalog("lakesoul");

        tEnvs.executeSql(
                "CREATE TABLE mysql_test_1 ( id int," +
                " name string," +
                " dt int," +
                " date1 date," +
                " ts timestamp, " +
                "primary key (id) NOT ENFORCED ) " +
                " with ('connector' = 'lakeSoul'," +
                "'format'='parquet','path'='" +
                "/tmp/lakesoul/test_cdc/mysql_test_1" + "'," +
                "'useCDC'='true'," +
                "'hashBucketNum'='2')");

        tEnvs.executeSql(
                "CREATE TABLE mysql_test_2 ( id int," +
                " name string," +
                " dt int," +
                " new_col string, " +
                "primary key (id) NOT ENFORCED ) " +
                " with ('connector' = 'lakeSoul'," +
                "'format'='parquet','path'='" +
                "/tmp/lakesoul/test_cdc/mysql_test_2" + "'," +
                "'useCDC'='true'," +
                "'hashBucketNum'='2')");

        MySqlSourceBuilder<JsonSourceRecord> sourceBuilder = MySqlSource.<JsonSourceRecord>builder()
                                                                        .hostname("localhost")
                                                                        .port(3306)
                                                                        .databaseList("test_cdc") // set captured
                                                                        // database
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

        DataStreamSink<JsonSourceRecord> sink = builder.buildSink(stream);

        builder.printStream(streams.f1, "Print DDL Stream");

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
