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

package org.apache.flink.lakeSoul.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakeSoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakeSoul.types.JsonSourceRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCMultiTableTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(3000);

        MySqlSourceBuilder<JsonSourceRecord> sourceBuilder = MySqlSource.<JsonSourceRecord>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test_cdc") // set captured database
                .tableList("test_cdc.*") // set captured table
                .username("root")
                .password("root");

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.sourceBuilder = sourceBuilder;

        LakeSoulMultiTableSinkStreamBuilder builder = new LakeSoulMultiTableSinkStreamBuilder(context);

        DataStreamSource<JsonSourceRecord> source = builder.buildMultiTableSource();

        Tuple2<DataStream<JsonSourceRecord>, DataStream<JsonSourceRecord>> streams = builder.buildCDCAndDDLStreamsFromSource(source);

        builder.printStream(streams.f0, "Print CDC Stream");
        builder.printStream(streams.f1, "Print DDL Stream");

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
