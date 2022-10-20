/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.committer.LakeSoulSinkCommitter;
import org.apache.flink.lakesoul.sink.state.LakeSoulSinkCommittableSerializer;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketStateSerializer;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
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
    public abstract AbstractLakeSoulMultiTableSinkWriter<IN> createWriter(Sink.InitContext context, int subTaskId);

    @Override
    public LakeSoulSinkCommitter createCommitter() throws IOException {
        return new LakeSoulSinkCommitter(createBucketWriter());
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulWriterBucketState> getWriterStateSerializer()
            throws IOException {
        BucketWriter<RowData, String> bucketWriter = createBucketWriter();

        return new LakeSoulWriterBucketStateSerializer(
                bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulMultiTableSinkCommittable> getCommittableSerializer()
            throws IOException {
        BucketWriter<RowData, String> bucketWriter = createBucketWriter();

        return new LakeSoulSinkCommittableSerializer(
                bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
    }

    private BucketWriter<RowData, String> createBucketWriter() throws IOException {
        return new BulkBucketWriter<>(
                FileSystem.get(basePath.toUri()).createRecoverableWriter(), writerFactory);
    }
}
