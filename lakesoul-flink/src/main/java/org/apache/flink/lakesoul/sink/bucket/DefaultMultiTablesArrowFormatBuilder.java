// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.arrow.LakeSoulArrowMultiTableSinkWriter;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;

import java.io.IOException;

public class DefaultMultiTablesArrowFormatBuilder
        extends BulkFormatBuilder<LakeSoulArrowWrapper, DefaultMultiTablesArrowFormatBuilder> {


    public DefaultMultiTablesArrowFormatBuilder(Path basePath, Configuration conf) {
        super(basePath, conf);
    }

    @Override
    public AbstractLakeSoulMultiTableSinkWriter<LakeSoulArrowWrapper> createWriter(Sink.InitContext context, int subTaskId) throws IOException {
        System.out.println("createWriter");
        return new LakeSoulArrowMultiTableSinkWriter(subTaskId,
                context.metricGroup(),
                super.bucketFactory,
                super.rollingPolicy,
                super.outputFileConfig,
                context.getProcessingTimeService(),
                super.bucketCheckInterval,
                super.conf);
    }
}