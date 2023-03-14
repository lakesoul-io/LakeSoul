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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.BinaryDebeziumDeserializationSchema;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;

public class LakeSoulMultiTableSinkStreamBuilder {

    public static final class Context {
        public StreamExecutionEnvironment env;

        public MySqlSourceBuilder<BinarySourceRecord> sourceBuilder;

        public Configuration conf;
    }

    private MySqlSource<BinarySourceRecord> mySqlSource;

    private final Context context;

    private final LakeSoulRecordConvert convert;

    public LakeSoulMultiTableSinkStreamBuilder(Context context) {
        this.context = context;
        this.convert = new LakeSoulRecordConvert(context.conf.getBoolean(USE_CDC), context.conf.getString(SERVER_TIME_ZONE));
    }

    public DataStreamSource<BinarySourceRecord> buildMultiTableSource() {
        context.env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));
        context.env.getConfig().registerTypeWithKryoSerializer(RowType.class, JavaSerializer.class);
        context.sourceBuilder.includeSchemaChanges(true);
        context.sourceBuilder.scanNewlyAddedTableEnabled(true);
        context.sourceBuilder.deserializer(new BinaryDebeziumDeserializationSchema(this.convert));
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("allowPublicKeyRetrieval", "true");
        jdbcProperties.put("useSSL", "false");
        context.sourceBuilder.jdbcProperties(jdbcProperties);
        mySqlSource = context.sourceBuilder.build();
        return context.env
                .fromSource(this.mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(context.conf.getInteger(LakeSoulSinkOptions.SOURCE_PARALLELISM));
    }

    /**
     * Create two DataStreams. First one contains all records of table changes,
     * second one contains all DDL records.
     */
    public Tuple2<DataStream<BinarySourceRecord>, DataStream<BinarySourceRecord>> buildCDCAndDDLStreamsFromSource(
         DataStreamSource<BinarySourceRecord> source
    ) {
        final OutputTag<BinarySourceRecord> outputTag = new OutputTag<BinarySourceRecord>("ddl-side-output") {};

        SingleOutputStreamOperator<BinarySourceRecord> cdcStream = source.process(
                new BinarySourceRecordSplitProcessFunction(
                        outputTag, context.conf.getString(WAREHOUSE_PATH)))
                                                                       .name("cdc-dml-stream")
                .setParallelism(context.conf.getInteger(BUCKET_PARALLELISM));

        DataStream<BinarySourceRecord> ddlStream = cdcStream.getSideOutput(outputTag);

        return Tuple2.of(cdcStream, ddlStream);
    }

    public DataStream<BinarySourceRecord> buildHashPartitionedCDCStream(DataStream<BinarySourceRecord> stream) {
        return stream.partitionCustom(new HashPartitioner(), convert::computeBinarySourceRecordPrimaryKeyHash);
    }

    public DataStreamSink<BinarySourceRecord> buildLakeSoulDMLSink(DataStream<BinarySourceRecord> stream) {
        LakeSoulRollingPolicyImpl rollingPolicy = new LakeSoulRollingPolicyImpl(
                context.conf.getLong(FILE_ROLLING_SIZE), context.conf.getLong(FILE_ROLLING_TIME));
        OutputFileConfig fileNameConfig = OutputFileConfig.builder()
                                                          .withPartSuffix(".parquet")
                                                          .build();
        LakeSoulMultiTablesSink<BinarySourceRecord> sink = LakeSoulMultiTablesSink.forMultiTablesBulkFormat(context.conf)
           .withBucketCheckInterval(context.conf.getLong(BUCKET_CHECK_INTERVAL))
           .withRollingPolicy(rollingPolicy)
           .withOutputFileConfig(fileNameConfig)
           .build();
        return stream.sinkTo(sink).name("LakeSoul MultiTable DML Sink")
                     .setParallelism(context.conf.getInteger(BUCKET_PARALLELISM));
    }

    public DataStreamSink<BinarySourceRecord> buildLakeSoulDDLSink(DataStream<BinarySourceRecord> stream) {
        return stream.addSink(new LakeSoulDDLSink()).name("LakeSoul MultiTable DDL Sink")
                .setParallelism(context.conf.getInteger(BUCKET_PARALLELISM));
    }

    public DataStreamSink<BinarySourceRecord> printStream(DataStream<BinarySourceRecord> stream, String name) {
        PrintSinkFunction<BinarySourceRecord> printFunction = new PrintSinkFunction<>(name, false);
        return stream.addSink(printFunction).name(name);
    }

    private static class BinarySourceRecordSplitProcessFunction extends ProcessFunction<BinarySourceRecord, BinarySourceRecord> {
        private final OutputTag<BinarySourceRecord> outputTag;

        private final String basePath;

        public BinarySourceRecordSplitProcessFunction(OutputTag<BinarySourceRecord> outputTag, String basePath) {
            this.outputTag = outputTag;
            this.basePath = basePath;
        }

        @Override
        public void processElement(
                BinarySourceRecord value,
                ProcessFunction<BinarySourceRecord, BinarySourceRecord>.Context ctx,
                Collector<BinarySourceRecord> out) {
            if (value.isDDLRecord()) {
                // side output DDL records
                ctx.output(outputTag, value);
            } else {
                // fill table location
                value.setTableLocation(
                        new Path(
                                new Path(basePath,
                                        value.getTableId().schema()),
                                value.getTableId().table()).toString());
                out.collect(value);
            }
        }
    }
}
