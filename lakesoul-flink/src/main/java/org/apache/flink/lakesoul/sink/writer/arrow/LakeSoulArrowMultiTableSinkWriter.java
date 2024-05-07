// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer.arrow;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucketFactory;
import org.apache.flink.lakesoul.sink.writer.TableSchemaWriterCreator;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LakeSoulArrowMultiTableSinkWriter extends AbstractLakeSoulMultiTableSinkWriter<LakeSoulArrowWrapper> {

    private final ConcurrentHashMap<TableSchemaIdentity, TableSchemaWriterCreator> perTableSchemaWriterCreator;

    private final Configuration conf;

    public LakeSoulArrowMultiTableSinkWriter(int subTaskId,
                                             SinkWriterMetricGroup metricGroup,
                                             LakeSoulWriterBucketFactory bucketFactory,
                                             RollingPolicy<RowData, String> rollingPolicy,
                                             OutputFileConfig outputFileConfig,
                                             Sink.ProcessingTimeService processingTimeService,
                                             long bucketCheckInterval,
                                             Configuration conf) {
        super(subTaskId, metricGroup, bucketFactory, rollingPolicy, outputFileConfig, processingTimeService,
                bucketCheckInterval, conf);
        this.conf = conf;
        this.perTableSchemaWriterCreator = new ConcurrentHashMap<>();
    }

    @Override
    protected TableSchemaWriterCreator getOrCreateTableSchemaWriterCreator(TableSchemaIdentity identity) {
        return null;
    }

    @Override
    protected List<Tuple2<TableSchemaIdentity, RowData>> extractTableSchemaAndRowData(LakeSoulArrowWrapper element) throws Exception {
        return null;
    }

    @Override
    public void write(LakeSoulArrowWrapper element, Context context) throws IOException {
        super.write(element, context);
    }
}
