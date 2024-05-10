// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucket;
import org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucketFactory;
import org.apache.flink.lakesoul.sink.writer.TableSchemaWriterCreator;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.DYNAMIC_BUCKET;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.DYNAMIC_BUCKETING;

public class LakeSoulArrowMultiTableSinkWriter extends AbstractLakeSoulMultiTableSinkWriter<LakeSoulArrowWrapper> {

    private final ConcurrentHashMap<TableSchemaIdentity, TableSchemaWriterCreator> perTableSchemaWriterCreator;

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
        this.perTableSchemaWriterCreator = new ConcurrentHashMap<>();
    }

    @Override
    protected TableSchemaWriterCreator getOrCreateTableSchemaWriterCreator(TableSchemaIdentity identity) {
        System.out.println("getOrCreateTableSchemaWriterCreator");
        System.out.println(identity);
        return perTableSchemaWriterCreator.computeIfAbsent(identity, identity1 -> {
            try {
                return TableSchemaWriterCreator.create(identity1.tableId, identity1.rowType,
                        identity1.tableLocation, identity1.primaryKeys,
                        identity1.partitionKeyList, conf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected List<Tuple2<TableSchemaIdentity, RowData>> extractTableSchemaAndRowData(LakeSoulArrowWrapper element) throws Exception {
        System.out.println("extractTableSchemaAndRowData");
        return null;
    }

    @Override
    public void write(LakeSoulArrowWrapper element, Context context) throws IOException {
        // setting the values in the bucketer context
        bucketerContext.update(
                context.timestamp(),
                context.currentWatermark(),
                processingTimeService.getCurrentProcessingTime());

        TableSchemaIdentity identity = null;
        TableSchemaWriterCreator creator = getOrCreateTableSchemaWriterCreator(identity);

        System.out.println("LakeSoulArrowMultiTableSinkWriter.write");
        System.out.println(conf);
        if (conf.get(DYNAMIC_BUCKETING)) {

            final Path bucketPath = creator.tableLocation;
            BucketWriter<VectorSchemaRoot, String> bucketWriter = creator.createArrowBucketWriter();

            element.withDecoded((recordBatch) -> {

                System.out.println(recordBatch.contentToTSVString());
            });
        } else {
            throw new RuntimeException("Static Bucketing Not Support");
        }

        System.out.println("LakeSoulArrowMultiTableSinkWriter.write()");
        System.out.println(element);
    }
}
