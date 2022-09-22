package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.lakesoul.sink.committer.FileCommitter;
import org.apache.flink.lakesoul.sink.writer.*;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.BulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

import static org.apache.flink.formats.parquet.ParquetFileFormatFactory.UTC_TIMEZONE;
import static org.apache.flink.lakesoul.sink.writer.TableSchemaWriterCreator.getParquetConfiguration;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder for configuring the sink for bulk-encoding formats
 */
public abstract class BulkFormatBuilder<IN, T extends BulkFormatBuilder<IN, T>>
        extends BucketsBuilder<IN, T> {

    protected final Path basePath;

    protected BulkWriter.Factory<RowData> writerFactory;

    protected long bucketCheckInterval;

    protected final FileWriterBucketFactory bucketFactory;

    protected CheckpointRollingPolicy<RowData, String> rollingPolicy;

    protected OutputFileConfig outputFileConfig;

    protected Configuration conf;

    protected BulkFormatBuilder(Path basePath, Configuration conf) {
        this(
                basePath,
                conf,
                DEFAULT_BUCKET_CHECK_INTERVAL,
                OnCheckpointRollingPolicy.build(),
                new DefaultFileWriterBucketFactory(),
                OutputFileConfig.builder().build());
    }

    protected BulkFormatBuilder(
            Path basePath,
            Configuration conf,
            long bucketCheckInterval,
            CheckpointRollingPolicy<RowData, String> policy,
            FileWriterBucketFactory bucketFactory,
            OutputFileConfig outputFileConfig) {
        this.basePath = basePath;
        this.conf = conf;
        this.bucketCheckInterval = bucketCheckInterval;
        this.rollingPolicy = checkNotNull(policy);
        this.bucketFactory = checkNotNull(bucketFactory);
        this.outputFileConfig = checkNotNull(outputFileConfig);
        this.writerFactory = ParquetRowDataBuilder.createWriterFactory(
                getFakeRowType(),
                getParquetConfiguration(conf),
                conf.get(UTC_TIMEZONE));
    }

    private RowType getFakeRowType() {
        return RowType.of(true, new IntType(true));
    }

    public T withBucketCheckInterval(final long interval) {
        this.bucketCheckInterval = interval;
        return self();
    }

    public T withRollingPolicy(CheckpointRollingPolicy<RowData, String> rollingPolicy) {
        this.rollingPolicy = checkNotNull(rollingPolicy);
        return self();
    }

    public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
        this.outputFileConfig = outputFileConfig;
        return self();
    }

    /**
     * Creates the actual sink.
     */
    public LakeSoulMultiTablesSink<IN> build() {
        return new LakeSoulMultiTablesSink<>(this);
    }

    @Override
    abstract AbstractLakeSoulMultiTableSinkWriter<IN> createWriter(Sink.InitContext context, int subTaskId);

    @Override
    FileCommitter createCommitter() throws IOException {
        return new FileCommitter(createBucketWriter());
    }

    @Override
    SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer()
            throws IOException {
        BucketWriter<RowData, String> bucketWriter = createBucketWriter();

        return new FileWriterBucketStateSerializer(
                bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
                bucketWriter.getProperties().getPendingFileRecoverableSerializer());
    }

    @Override
    SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer()
            throws IOException {
        BucketWriter<RowData, String> bucketWriter = createBucketWriter();

        return new FileSinkCommittableSerializer(
                bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
    }

    private BucketWriter<RowData, String> createBucketWriter() throws IOException {
        return new BulkBucketWriter<>(
                FileSystem.get(basePath.toUri()).createRecoverableWriter(), writerFactory);
    }
}
