// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.LakeSoulRowDataOneTableSinkWriter;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.table.data.RowData;

/**
 * Builder for the vanilla {@link LakeSoulMultiTablesSink} using a bulk format.
 */
public final class DefaultOneTableBulkFormatBuilder
        extends BulkFormatBuilder<RowData, DefaultOneTableBulkFormatBuilder> {

    private static final long serialVersionUID = 7493169281036370228L;

    private final TableSchemaIdentity identity;

    public DefaultOneTableBulkFormatBuilder(
            TableSchemaIdentity identity,
            Path basePath, Configuration conf) {
        super(basePath, conf);
        this.identity = identity;
    }

    @Override
    public AbstractLakeSoulMultiTableSinkWriter<RowData> createWriter(Sink.InitContext context, int subTaskId) {
        return new LakeSoulRowDataOneTableSinkWriter(
                subTaskId,
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
