// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.USE_CDC;

public class LakeSoulRowDataOneTableSinkWriter extends AbstractLakeSoulMultiTableSinkWriter<RowData> {

    private final LakeSoulRecordConvert converter;

    private final TableSchemaIdentity identity;
    private final RowData.FieldGetter[] fieldGetters;

    public LakeSoulRowDataOneTableSinkWriter(
            int subTaskId,
            TableSchemaIdentity identity,
            SinkWriterMetricGroup metricGroup,
            LakeSoulWriterBucketFactory bucketFactory,
            RollingPolicy<RowData, String> rollingPolicy,
            OutputFileConfig outputFileConfig,
            Sink.ProcessingTimeService processingTimeService,
            long bucketCheckInterval,
            Configuration conf) {
        super(subTaskId, metricGroup, bucketFactory, rollingPolicy, outputFileConfig,
                processingTimeService, bucketCheckInterval, conf);
        this.converter = new LakeSoulRecordConvert(conf, conf.getString(SERVER_TIME_ZONE));
        this.identity = identity;
        this.fieldGetters =
                IntStream.range(0, this.identity.rowType.getFieldCount())
                        .mapToObj(
                                i ->
                                        RowData.createFieldGetter(
                                                identity.rowType.getTypeAt(i), i))
                        .toArray(RowData.FieldGetter[]::new);
        this.identity.rowType = converter.toFlinkRowTypeCDC(this.identity.rowType);
    }

    @Override
    protected List<Tuple2<TableSchemaIdentity, RowData>> extractTableSchemaAndRowData(RowData element) {
        return Collections.singletonList(Tuple2.of(identity, converter.addCDCKindField(element, this.fieldGetters)));
    }
}
