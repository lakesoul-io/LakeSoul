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

package org.apache.flink.lakeSoul.sink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.lakeSoul.types.JsonDebeziumDeserializationSchema;
import org.apache.flink.lakeSoul.types.JsonSourceRecord;
import org.apache.flink.lakeSoul.types.SourceRecordJsonSerde;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.SCHEMA_CHANGE_EVENT_KEY_NAME;

public class LakeSoulMultiTableSinkStreamBuilder {

    public static final class Context {
        public StreamExecutionEnvironment env;

        public MySqlSourceBuilder<JsonSourceRecord> sourceBuilder;

        int sourceParallelism = 16;
    }

    private MySqlSource<JsonSourceRecord> mySqlSource;

    private final Context context;

    public LakeSoulMultiTableSinkStreamBuilder(Context context) {
        this.context = context;
    }

    public DataStreamSource<JsonSourceRecord> buildMultiTableSource() {
        context.sourceBuilder.includeSchemaChanges(true);
        context.sourceBuilder.scanNewlyAddedTableEnabled(true);
        context.sourceBuilder.deserializer(new JsonDebeziumDeserializationSchema());
        mySqlSource = context.sourceBuilder.build();
        return context.env
                .fromSource(this.mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(context.sourceParallelism);
    }

    /**
     * Create two DataStreams. First one contains all records of table changes,
     * second one contains all DDL records.
     */
    public Tuple2<DataStream<JsonSourceRecord>, DataStream<JsonSourceRecord>> buildCDCAndDDLStreamsFromSource(
         DataStreamSource<JsonSourceRecord> source
    ) {
        final OutputTag<JsonSourceRecord> outputTag = new OutputTag<>("ddl-side-output") {};

        SingleOutputStreamOperator<JsonSourceRecord> cdcStream = source.process(
                new ProcessFunction<JsonSourceRecord, JsonSourceRecord>() {
                    @Override
                    public void processElement(
                            JsonSourceRecord value,
                            ProcessFunction<JsonSourceRecord, JsonSourceRecord>.Context ctx,
                            Collector<JsonSourceRecord> out) {
                        SchemaAndValue key = value.getKey(SourceRecordJsonSerde.getInstance());
                        Schema keySchema = key.schema();
                        assert keySchema != null;
                        if (SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name())) {
                            ctx.output(outputTag, value);
                        } else {
                            out.collect(value.fillPrimaryKeys(keySchema));
                        }
                    }
                }).name("cdc-stream");

        DataStream<JsonSourceRecord> ddlStream = cdcStream.getSideOutput(outputTag);

        return Tuple2.of(cdcStream, ddlStream);
    }

    public DataStreamSink<JsonSourceRecord> printStream(DataStream<JsonSourceRecord> stream, String name) {
        PrintSinkFunction<JsonSourceRecord> printFunction = new PrintSinkFunction<>(name, false);
        return stream.addSink(printFunction).name(name);
    }
}
