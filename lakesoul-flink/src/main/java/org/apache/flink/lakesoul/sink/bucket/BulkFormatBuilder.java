// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.committer.LakeSoulSinkCommitter;
import org.apache.flink.lakesoul.sink.committer.LakeSoulSinkGlobalCommitter;
import org.apache.flink.lakesoul.sink.state.*;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.DefaultLakeSoulWriterBucketFactory;
import org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucketFactory;
import org.apache.flink.lakesoul.sink.writer.NativeBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder for configuring the sink for bulk-encoding formats
 */
public abstract class BulkFormatBuilder<IN, T extends BulkFormatBuilder<IN, T>>
        extends BucketsBuilder<IN, T> {

    protected final Path basePath;

    protected long bucketCheckInterval;

    protected final LakeSoulWriterBucketFactory bucketFactory;

    protected CheckpointRollingPolicy<RowData, String> rollingPolicy;

    protected OutputFileConfig outputFileConfig;

    protected Configuration conf;

    protected BulkFormatBuilder(Path basePath, Configuration conf) {
        this(
                basePath,
                conf,
                DEFAULT_BUCKET_CHECK_INTERVAL,
                OnCheckpointRollingPolicy.build(),
                new DefaultLakeSoulWriterBucketFactory(),
                OutputFileConfig.builder().build());
    }

    protected BulkFormatBuilder(
            Path basePath,
            Configuration conf,
            long bucketCheckInterval,
            CheckpointRollingPolicy<RowData, String> policy,
            LakeSoulWriterBucketFactory bucketFactory,
            OutputFileConfig outputFileConfig) {
        this.basePath = basePath;
        this.conf = conf;
        this.bucketCheckInterval = bucketCheckInterval;
        this.rollingPolicy = checkNotNull(policy);
        this.bucketFactory = checkNotNull(bucketFactory);
        this.outputFileConfig = checkNotNull(outputFileConfig);
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
    public abstract AbstractLakeSoulMultiTableSinkWriter<IN> createWriter(Sink.InitContext context, int subTaskId) throws IOException;

    @Override
    public LakeSoulSinkCommitter createCommitter() throws IOException {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulWriterBucketState> getWriterStateSerializer()
            throws IOException {
        return new LakeSoulWriterBucketStateSerializer(
                NativeBucketWriter.NativePendingFileRecoverableSerializer.INSTANCE);
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulMultiTableSinkCommittable> getCommittableSerializer()
            throws IOException {
        return new LakeSoulSinkCommittableSerializer(
                NativeBucketWriter.NativePendingFileRecoverableSerializer.INSTANCE);
    }

    @Override
    public LakeSoulSinkGlobalCommitter createGlobalCommitter() throws IOException {
        return new LakeSoulSinkGlobalCommitter(conf);
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulMultiTableSinkGlobalCommittable> getGlobalCommittableSerializer() throws IOException {
        return new LakeSoulSinkGlobalCommittableSerializer(LakeSoulSinkCommittableSerializer.INSTANCE);
    }
}
