// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;

public class LakeSoulMultiTableSinkStreamBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulMultiTableSinkStreamBuilder.class);

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

    private class HashGen implements KeySelector<BinarySourceRecord, Long> {
        private static final long serialVersionUID = -4298875987882891700L;
        private final int hashBucketNum;
        private final int parallelism;

        public HashGen(int hashBucketNum, int parallelism) {
            this.hashBucketNum = hashBucketNum;
            this.parallelism = parallelism;
        }

        @Override
        public Long getKey(BinarySourceRecord binarySourceRecord) throws Exception {
            return convert.computeBinarySourceRecordPrimaryKeyHash(binarySourceRecord, hashBucketNum, parallelism);
        }
    }

    public DataStream<BinarySourceRecord> buildHashPartitionedCDCStream(DataStream<BinarySourceRecord> stream) {
        boolean dynamicBucketing = context.conf.get(DYNAMIC_BUCKETING);
        int parallelism = context.conf.get(DEFAULT_PARALLELISM);
        int hashBucketNum = context.conf.get(HASH_BUCKET_NUM);
        LOG.info("Building CDC stream partition for parallelism {}, dynamic bucket {}",
                parallelism, dynamicBucketing);
        return stream.partitionCustom(new HashPartitioner(hashBucketNum),
                new HashGen(hashBucketNum, parallelism));
    }

    public DataStreamSink<BinarySourceRecord> buildLakeSoulDMLSink(DataStream<BinarySourceRecord> stream) {
        Boolean dynamicBucketing = context.conf.get(DYNAMIC_BUCKETING);
        if (!context.conf.contains(AUTO_SCHEMA_CHANGE)) {
            context.conf.set(AUTO_SCHEMA_CHANGE, true);
        }
        LakeSoulRollingPolicyImpl<RowData> rollingPolicy = new LakeSoulRollingPolicyImpl<>(
                context.conf.getLong(FILE_ROLLING_SIZE), context.conf.getLong(FILE_ROLLING_TIME));
        OutputFileConfig fileNameConfig = OutputFileConfig.builder()
                .withPartSuffix(".parquet")
                .build();
        LakeSoulMultiTablesSink<BinarySourceRecord, RowData>
                sink =
                LakeSoulMultiTablesSink.forMultiTablesBulkFormat(context.conf)
                        .withBucketCheckInterval(context.conf.getLong(BUCKET_CHECK_INTERVAL))
                        .withRollingPolicy(rollingPolicy)
                        .withOutputFileConfig(fileNameConfig)
                        .build();
        if (dynamicBucketing) {
            return stream.sinkTo(sink).name("LakeSoul MultiTable DML Sink");
        } else {
            return stream.sinkTo(sink).name("LakeSoul MultiTable DML Sink")
                    .setParallelism(context.conf.getInteger(BUCKET_PARALLELISM));
        }
    }

    public static DataStreamSink<LakeSoulArrowWrapper> buildArrowSink(Context context,
                                                                      DataStream<LakeSoulArrowWrapper> stream) {
        return buildArrowSink(context, stream, 1);
    }

    public static DataStreamSink<LakeSoulArrowWrapper> buildArrowSink(Context context,
                                                                      DataStream<LakeSoulArrowWrapper> stream,
                                                                      int parallelism
    ) {
        context.conf.set(DYNAMIC_BUCKETING, true);
        LakeSoulRollingPolicyImpl<LakeSoulArrowWrapper> rollingPolicy = new LakeSoulRollingPolicyImpl<>(
                context.conf.getLong(FILE_ROLLING_SIZE), context.conf.getLong(FILE_ROLLING_TIME));
        OutputFileConfig fileNameConfig = OutputFileConfig.builder()
                .withPartSuffix(".parquet")
                .build();
        LakeSoulMultiTablesSink<LakeSoulArrowWrapper, LakeSoulArrowWrapper>
                sink =
                LakeSoulMultiTablesSink.forMultiTablesArrowFormat(context.conf)
                        .withBucketCheckInterval(context.conf.getLong(BUCKET_CHECK_INTERVAL))
                        .withRollingPolicy(rollingPolicy)
                        .withOutputFileConfig(fileNameConfig)
                        .build();
        return stream.sinkTo(sink).name("LakeSoul MultiTable Arrow Sink").setParallelism(parallelism);
    }

    public DataStreamSink<BinarySourceRecord> printStream(DataStream<BinarySourceRecord> stream, String name) {
        PrintSinkFunction<BinarySourceRecord> printFunction = new PrintSinkFunction<>(name, false);
        return stream.addSink(printFunction).name(name);
    }
}
