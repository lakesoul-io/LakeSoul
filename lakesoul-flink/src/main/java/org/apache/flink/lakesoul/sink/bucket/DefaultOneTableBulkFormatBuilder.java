// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.DefaultLakeSoulWriterBucketFactory;
import org.apache.flink.lakesoul.sink.writer.LakeSoulRowDataOneTableSinkWriter;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * Builder for the vanilla {@link LakeSoulMultiTablesSink} using a bulk format.
 */
public final class DefaultOneTableBulkFormatBuilder
        extends BulkFormatBuilder<RowData, RowData, DefaultOneTableBulkFormatBuilder> {

    private static final long serialVersionUID = 7493169281036370228L;

    private final TableSchemaIdentity identity;

    public DefaultOneTableBulkFormatBuilder(
            TableSchemaIdentity identity,
            Path basePath, Configuration conf) {
        super(basePath, conf, new DefaultLakeSoulWriterBucketFactory(conf));
        this.identity = identity;
    }
    public TableSchemaIdentity getIdentity(){
        return this.identity;
    }

    @Override
    public AbstractLakeSoulMultiTableSinkWriter<RowData, RowData> createWriter(Sink.InitContext context, int subTaskId) throws
            IOException {
        int hashBucketNum = conf.getInteger(LakeSoulSinkOptions.HASH_BUCKET_NUM);
        int hashBucketId = hashBucketNum == -1 ? subTaskId : subTaskId % hashBucketNum;
        System.out.printf("DefaultOneTableBulkFormatBuilder::createWriter, subTaskId=%d, hashBucketId=%d\n", subTaskId, hashBucketId);
        return new LakeSoulRowDataOneTableSinkWriter(
                hashBucketId,
                identity,
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
