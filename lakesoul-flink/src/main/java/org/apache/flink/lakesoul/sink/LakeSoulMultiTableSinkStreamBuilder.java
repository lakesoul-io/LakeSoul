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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.types.logical.RowType;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BUCKET_CHECK_INTERVAL;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BUCKET_PARALLELISM;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.FILE_ROLLING_SIZE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.FILE_ROLLING_TIME;

public class LakeSoulMultiTableSinkStreamBuilder {

    public static final class Context {
        public StreamExecutionEnvironment env;
        public Configuration conf;
    }

    private Source source;

    private final Context context;

    private final LakeSoulRecordConvert convert;

    public LakeSoulMultiTableSinkStreamBuilder(Source source, Context context, LakeSoulRecordConvert convert) {
        this.source = source;
        this.context = context;
        this.convert = convert;
    }

    public DataStreamSource<BinarySourceRecord> buildMultiTableSource(String SourceName) {
        context.env.getConfig().registerTypeWithKryoSerializer(RowType.class, JavaSerializer.class);

        return context.env
                .fromSource(this.source, WatermarkStrategy.noWatermarks(), SourceName)
                .setParallelism(context.conf.getInteger(LakeSoulSinkOptions.SOURCE_PARALLELISM));
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

    public DataStreamSink<BinarySourceRecord> printStream(DataStream<BinarySourceRecord> stream, String name) {
        PrintSinkFunction<BinarySourceRecord> printFunction = new PrintSinkFunction<>(name, false);
        return stream.addSink(printFunction).name(name);
    }
}
