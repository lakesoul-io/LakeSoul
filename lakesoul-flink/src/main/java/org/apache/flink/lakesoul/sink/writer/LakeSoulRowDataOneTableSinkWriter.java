package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;

import java.util.List;

public class LakeSoulRowDataOneTableSinkWriter extends AbstractLakeSoulMultiTableSinkWriter<RowData> {

    private final TableSchemaIdentity identity;

    public LakeSoulRowDataOneTableSinkWriter(
            int subTaskId,
            TableSchemaIdentity identity,
            SinkWriterMetricGroup metricGroup,
            FileWriterBucketFactory bucketFactory,
            RollingPolicy<RowData, String> rollingPolicy,
            OutputFileConfig outputFileConfig,
            Sink.ProcessingTimeService processingTimeService,
            long bucketCheckInterval,
            ClassLoader userClassLoader,
            Configuration conf) {
        super(subTaskId, metricGroup, bucketFactory, rollingPolicy, outputFileConfig, processingTimeService, bucketCheckInterval
                , userClassLoader, conf);
        this.identity = identity;
    }

    @Override
    protected List<Tuple2<TableSchemaIdentity, RowData>> extractTableSchemaAndRowData(RowData element) {
        return List.of(Tuple2.of(identity, element));
    }
}
