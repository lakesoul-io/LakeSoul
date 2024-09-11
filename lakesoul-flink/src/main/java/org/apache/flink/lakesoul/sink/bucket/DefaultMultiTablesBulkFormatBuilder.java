// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.DefaultLakeSoulWriterBucketFactory;
import org.apache.flink.lakesoul.sink.writer.LakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.table.data.RowData;

public class DefaultMultiTablesBulkFormatBuilder
        extends BulkFormatBuilder<BinarySourceRecord, RowData, DefaultMultiTablesBulkFormatBuilder> {

    public DefaultMultiTablesBulkFormatBuilder(Path basePath, Configuration conf) {
        super(basePath, conf, new DefaultLakeSoulWriterBucketFactory(conf));
    }

    @Override
    public AbstractLakeSoulMultiTableSinkWriter<BinarySourceRecord, RowData> createWriter(Sink.InitContext context, int subTaskId) {
        int hashBucketNum = conf.getInteger(LakeSoulSinkOptions.HASH_BUCKET_NUM);
        int hashBucketId = hashBucketNum == -1 ? subTaskId : subTaskId % hashBucketNum;
        System.out.printf("DefaultMultiTablesBulkFormatBuilder::createWriter, subTaskId=%d, hashBucketId=%d\n", subTaskId, hashBucketId);
        return new LakeSoulMultiTableSinkWriter(
                hashBucketId,
                context.metricGroup(),
                super.bucketFactory,
                super.rollingPolicy,
                super.outputFileConfig,
                context.getProcessingTimeService(),
                super.bucketCheckInterval,
                super.conf
        );
    }
}
