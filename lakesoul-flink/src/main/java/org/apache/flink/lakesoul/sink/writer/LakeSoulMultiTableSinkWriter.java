// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.lakesoul.types.LakeSoulRowDataWrapper;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LakeSoulMultiTableSinkWriter extends AbstractLakeSoulMultiTableSinkWriter<BinarySourceRecord, RowData> {

    private final ConcurrentHashMap<TableSchemaIdentity, TableSchemaWriterCreator> perTableSchemaWriterCreator;

    private final Configuration conf;

    public LakeSoulMultiTableSinkWriter(int subTaskId,
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

    private TableSchemaIdentity getIdentity(RowType rowType, BinarySourceRecord element) {
        return new TableSchemaIdentity(
                element.getTableId(),
                rowType,
                element.getTableLocation(),
                element.getPrimaryKeys(),
                element.getPartitionKeys(),
                element.getData().getUseCDC(),
                element.getData().getCdcColumn());
    }

    @Override
    protected TableSchemaWriterCreator getOrCreateTableSchemaWriterCreator(TableSchemaIdentity identity) {
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
    protected List<Tuple2<TableSchemaIdentity, RowData>> extractTableSchemaAndRowData(BinarySourceRecord element) throws Exception {
        LakeSoulRowDataWrapper wrapper = element.getData();
        List<Tuple2<TableSchemaIdentity, RowData>> list = new ArrayList<>();
        if (wrapper.getBefore() != null && wrapper.getBeforeType() != null) {
            list.add(Tuple2.of(getIdentity(wrapper.getBeforeType(), element), wrapper.getBefore()));
        }
        if (wrapper.getAfter() != null && wrapper.getAfterType() != null) {
            list.add(Tuple2.of(getIdentity(wrapper.getAfterType(), element), wrapper.getAfter()));
        }
        return list;
    }

    @Override
    protected long getDataDmlTsMs(BinarySourceRecord element) {
        return element.getData().getTsMs();
    }
}
