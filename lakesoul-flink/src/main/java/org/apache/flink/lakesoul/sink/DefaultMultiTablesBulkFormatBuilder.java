package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.LakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.types.JsonSourceRecord;

public class DefaultMultiTablesBulkFormatBuilder
        extends BulkFormatBuilder<JsonSourceRecord, DefaultMultiTablesBulkFormatBuilder> {

    public DefaultMultiTablesBulkFormatBuilder(Path basePath, Configuration conf) {
        super(basePath, conf);
    }

    @Override
    AbstractLakeSoulMultiTableSinkWriter<JsonSourceRecord> createWriter(Sink.InitContext context, int subTaskId) {
        return new LakeSoulMultiTableSinkWriter(
                subTaskId,
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
