package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.types.JsonSourceRecord;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.lakesoul.types.LakeSoulRowDataWrapper;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

public class LakeSoulMultiTableSinkWriter extends AbstractLakeSoulMultiTableSinkWriter<JsonSourceRecord> {

    private final LakeSoulRecordConvert converter = new LakeSoulRecordConvert();

    public LakeSoulMultiTableSinkWriter(int subTaskId,
                                        SinkWriterMetricGroup metricGroup,
                                        FileWriterBucketFactory bucketFactory,
                                        RollingPolicy<RowData, String> rollingPolicy,
                                        OutputFileConfig outputFileConfig,
                                        Sink.ProcessingTimeService processingTimeService,
                                        long bucketCheckInterval,
                                        ClassLoader userClassLoader,
                                        Configuration conf) {
        super(subTaskId, metricGroup, bucketFactory, rollingPolicy, outputFileConfig, processingTimeService,
              bucketCheckInterval, userClassLoader, conf);
    }

    private TableSchemaIdentity getIdentity(RowType rowType, JsonSourceRecord element) {
        return new TableSchemaIdentity(
                element.getTableId(),
                rowType,
                element.getTableLocation(),
                element.getPrimaryKeys(),
                element.getPartitionKeys()
        );
    }

    @Override
    protected List<Tuple2<TableSchemaIdentity, RowData>> extractTableSchemaAndRowData(JsonSourceRecord element) throws Exception {
        LakeSoulRowDataWrapper wrapper = converter.toLakeSoulDataType(element);
        if (wrapper.getOp().equals("insert")) {
            return List.of(Tuple2.of(getIdentity(wrapper.getAfterType(), element), wrapper.getAfter()));
        } else if (wrapper.getOp().equals("delete")) {
            return List.of(Tuple2.of(getIdentity(wrapper.getBeforeType(), element), wrapper.getBefore()));
        } else {
            return List.of(Tuple2.of(getIdentity(wrapper.getBeforeType(), element), wrapper.getBefore()),
                           Tuple2.of(getIdentity(wrapper.getAfterType(), element), wrapper.getAfter()));
        }
    }

}
