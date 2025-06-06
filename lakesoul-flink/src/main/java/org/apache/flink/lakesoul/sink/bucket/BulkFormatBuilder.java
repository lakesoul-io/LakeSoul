// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.committer.LakeSoulSinkCommitter;
import org.apache.flink.lakesoul.sink.committer.LakeSoulSinkGlobalCommitter;
import org.apache.flink.lakesoul.sink.state.*;
import org.apache.flink.lakesoul.sink.writer.*;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder for configuring the sink for bulk-encoding formats
 */
public abstract class BulkFormatBuilder<IN, OUT, T extends BulkFormatBuilder<IN, OUT, T>>
        extends BucketsBuilder<IN, OUT, T> {

    private static final long serialVersionUID = 5528527971847083823L;
    protected final Path basePath;

    protected long bucketCheckInterval;

    protected final LakeSoulWriterBucketFactory bucketFactory;

    protected CheckpointRollingPolicy<OUT, String> rollingPolicy;

    protected OutputFileConfig outputFileConfig;

    protected Configuration conf;

    protected BulkFormatBuilder(Path basePath, Configuration conf, LakeSoulWriterBucketFactory bucketFactory) {
        this(
                basePath,
                conf,
                DEFAULT_BUCKET_CHECK_INTERVAL,
                OnCheckpointRollingPolicy.build(),
                bucketFactory,
                OutputFileConfig.builder().build());
    }

    protected BulkFormatBuilder(
            Path basePath,
            Configuration conf,
            long bucketCheckInterval,
            CheckpointRollingPolicy<OUT, String> policy,
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

    public T withRollingPolicy(CheckpointRollingPolicy<OUT, String> rollingPolicy) {
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
    public LakeSoulMultiTablesSink<IN, OUT> build() {
        return new LakeSoulMultiTablesSink<>(this);
    }

    @Override
    public abstract AbstractLakeSoulMultiTableSinkWriter<IN, OUT> createWriter(Sink.InitContext context, int subTaskId) throws IOException;

    @Override
    public LakeSoulSinkCommitter createCommitter() throws IOException {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulWriterBucketState> getWriterStateSerializer()
            throws IOException {
        return new LakeSoulWriterBucketStateSerializer(
                NativeParquetWriter.NativePendingFileRecoverableSerializer.INSTANCE);
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulMultiTableSinkCommittable> getCommittableSerializer()
            throws IOException {
        return new LakeSoulSinkCommittableSerializer(
                NativeParquetWriter.NativePendingFileRecoverableSerializer.INSTANCE);
    }

    @Override
    public LakeSoulSinkGlobalCommitter createGlobalCommitter() {
        return new LakeSoulSinkGlobalCommitter(conf);
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulMultiTableSinkGlobalCommittable> getGlobalCommittableSerializer() throws IOException {
        return new LakeSoulSinkGlobalCommittableSerializer(LakeSoulSinkCommittableSerializer.INSTANCE);
    }
}
