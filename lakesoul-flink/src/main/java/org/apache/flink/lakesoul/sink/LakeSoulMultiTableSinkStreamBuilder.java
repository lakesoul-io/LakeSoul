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

package org.apache.flink.lakesoul.sink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.SCHEMA_CHANGE_EVENT_KEY_NAME;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;

public class LakeSoulMultiTableSinkStreamBuilder {

    public static final class Context {
        public StreamExecutionEnvironment env;

        public MySqlSourceBuilder<JsonSourceRecord> sourceBuilder;

        public Configuration conf;
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
                .setParallelism(context.conf.getInteger(LakeSoulSinkOptions.SOURCE_PARALLELISM));
    }

    /**
     * Create two DataStreams. First one contains all records of table changes,
     * second one contains all DDL records.
     */
    public Tuple2<DataStream<JsonSourceRecord>, DataStream<JsonSourceRecord>> buildCDCAndDDLStreamsFromSource(
         DataStreamSource<JsonSourceRecord> source
    ) {
        final OutputTag<JsonSourceRecord> outputTag = new OutputTag<JsonSourceRecord>("ddl-side-output") {};

        SingleOutputStreamOperator<JsonSourceRecord> cdcStream = source.process(
                new JsonSourceRecordSplitProcessFunction(
                        outputTag, context.conf.getString(WAREHOUSE_PATH)))
                                                                       .name("cdc-dml-stream")
                .setParallelism(context.conf.getInteger(BUCKET_PARALLELISM));

        DataStream<JsonSourceRecord> ddlStream = cdcStream.getSideOutput(outputTag);

        return Tuple2.of(cdcStream, ddlStream);
    }

    public DataStream<JsonSourceRecord> buildHashPartitionedCDCStream(DataStream<JsonSourceRecord> stream) {
        LakeSoulRecordConvert convert = new LakeSoulRecordConvert(context.conf.getBoolean(USE_CDC), context.conf.getString(SERVER_TIME_ZONE));
        return stream.partitionCustom(new HashPartitioner(), convert::computeJsonRecordPrimaryKeyHash);
    }

    public DataStreamSink<JsonSourceRecord> buildLakeSoulDMLSink(DataStream<JsonSourceRecord> stream) {
        LakeSoulRollingPolicyImpl rollingPolicy = new LakeSoulRollingPolicyImpl(
                context.conf.getLong(FILE_ROLLING_SIZE), context.conf.getLong(FILE_ROLLING_TIME));
        OutputFileConfig fileNameConfig = OutputFileConfig.builder()
                                                          .withPartSuffix(".parquet")
                                                          .build();
        LakeSoulMultiTablesSink<JsonSourceRecord> sink = LakeSoulMultiTablesSink.forMultiTablesBulkFormat(context.conf)
           .withBucketCheckInterval(context.conf.getLong(BUCKET_CHECK_INTERVAL))
           .withRollingPolicy(rollingPolicy)
           .withOutputFileConfig(fileNameConfig)
           .build();
        return stream.sinkTo(sink).name("LakeSoul MultiTable DML Sink")
                     .setParallelism(context.conf.getInteger(BUCKET_PARALLELISM));
    }

    public DataStreamSink<JsonSourceRecord> buildLakeSoulDDLSink(DataStream<JsonSourceRecord> stream) {
        return stream.addSink(new LakeSoulDDLSink()).name("LakeSoul MultiTable DDL Sink")
                .setParallelism(context.conf.getInteger(BUCKET_PARALLELISM));
    }

    public DataStreamSink<JsonSourceRecord> printStream(DataStream<JsonSourceRecord> stream, String name) {
        PrintSinkFunction<JsonSourceRecord> printFunction = new PrintSinkFunction<>(name, false);
        return stream.addSink(printFunction).name(name);
    }

    private static class JsonSourceRecordSplitProcessFunction extends ProcessFunction<JsonSourceRecord, JsonSourceRecord> {
        private final OutputTag<JsonSourceRecord> outputTag;

        private final String basePath;

        public JsonSourceRecordSplitProcessFunction(OutputTag<JsonSourceRecord> outputTag, String basePath) {
            this.outputTag = outputTag;
            this.basePath = basePath;
        }

        @Override
        public void processElement(
                JsonSourceRecord value,
                ProcessFunction<JsonSourceRecord, JsonSourceRecord>.Context ctx,
                Collector<JsonSourceRecord> out) {

            SchemaAndValue key = value.getKey(SourceRecordJsonSerde.getInstance());
            Schema keySchema = key.schema();
            assert keySchema != null;
            if (SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name())) {
                // side output DDL records
                ctx.output(outputTag, value);
            } else {
                // fill table location
                value.setTableLocation(
                        new Path(
                                new Path(basePath,
                                        value.getTableId().schema()),
                                value.getTableId().table()).toString());
                // fill primary key
                out.collect(value.fillPrimaryKeys(keySchema));
            }
        }
    }
}
