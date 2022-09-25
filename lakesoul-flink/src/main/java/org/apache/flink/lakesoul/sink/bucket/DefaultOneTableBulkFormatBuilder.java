/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

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
                context.getUserCodeClassLoader().asClassLoader(),
                super.conf
                );
    }
}
