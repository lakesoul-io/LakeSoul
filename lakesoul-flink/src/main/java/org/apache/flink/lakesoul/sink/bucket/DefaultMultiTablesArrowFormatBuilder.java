// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.DefaultLakeSoulWriterBucketFactory;
import org.apache.flink.lakesoul.sink.writer.arrow.LakeSoulArrowWriterBucketFactory;
import org.apache.flink.lakesoul.sink.writer.arrow.LakeSoulArrowMultiTableSinkWriter;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;

import java.io.IOException;

public class DefaultMultiTablesArrowFormatBuilder
        extends BulkFormatBuilder<LakeSoulArrowWrapper, LakeSoulArrowWrapper, DefaultMultiTablesArrowFormatBuilder> {

    protected final LakeSoulArrowWriterBucketFactory arrowBucketFactory;

    public DefaultMultiTablesArrowFormatBuilder(Path basePath, Configuration conf) {
        super(basePath, conf, new DefaultLakeSoulWriterBucketFactory(conf));
        arrowBucketFactory = new LakeSoulArrowWriterBucketFactory();
    }

    @Override
    public AbstractLakeSoulMultiTableSinkWriter<LakeSoulArrowWrapper, LakeSoulArrowWrapper> createWriter(Sink.InitContext context, int subTaskId) throws IOException {
        return new LakeSoulArrowMultiTableSinkWriter(
                subTaskId,
                context.metricGroup(),
                arrowBucketFactory,
                super.rollingPolicy,
                super.outputFileConfig,
                context.getProcessingTimeService(),
                super.bucketCheckInterval,
                super.conf);
    }
}