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

package org.apache.flink.lakesoul.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.fileSystem.*;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;

public class LakesoulFileSink <IN> extends RichSinkFunction<IN>
        implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;

    // ------------------------ configuration fields --------------------------

    private  long bucketCheckInterval;

    private  LakeSoulBucketsBuilder<IN, ?, ? extends LakeSoulBucketsBuilder<IN, ?, ?>> bucketsBuilder;

    // --------------------------- runtime fields -----------------------------

    private transient LakeSoulFileSinkHelper<IN> helper;

    /**
     * Creates a new {@code LakesoulFileSink} that writes files to the given base directory with
     * the give buckets properties.
     */
    public LakesoulFileSink(
            LakeSoulBucketsBuilder<IN, ?, ? extends LakeSoulBucketsBuilder<IN, ?, ?>> bucketsBuilder,
            long bucketCheckInterval) {

        Preconditions.checkArgument(bucketCheckInterval > 0L);

        this.bucketsBuilder = Preconditions.checkNotNull(bucketsBuilder);
        this.bucketCheckInterval = bucketCheckInterval;
    }

    // ------------------------------------------------------------------------

    // --------------------------- Sink Builders  -----------------------------

    public static <IN> DefaultRowFormatBuilder<IN> forRowFormat(
            final Path basePath, final Encoder<IN> encoder) {
        return new DefaultRowFormatBuilder<>(basePath, encoder, new DateTimeBucketAssigner<>());
    }

    public static <IN> LakesoulFileSink.DefaultBulkFormatBuilder<IN> forBulkFormat(
            final Path basePath, final BulkWriter.Factory<IN> writerFactory) {
        return new LakesoulFileSink.DefaultBulkFormatBuilder<>(
                basePath, writerFactory, new DateTimeBucketAssigner<>());
    }

    /** A builder for configuring the sink for row-wise encoding formats. */
    @PublicEvolving
    public static class RowFormatBuilder<IN, BucketID, T extends LakesoulFileSink.RowFormatBuilder<IN, BucketID, T>>
            extends LakeSoulBucketsBuilder<IN, BucketID, T> {

        private static final long serialVersionUID = 1L;

        private long bucketCheckInterval;

        private final Path basePath;

        private Encoder<IN> encoder;

        private BucketAssigner<IN, BucketID> bucketAssigner;

        private LakesoulTableSink.LakesoulRollingPolicy<IN, BucketID> rollingPolicy;

        private LakeSoulBucketFactory<IN, BucketID> bucketFactory;

        private OutputFileConfig outputFileConfig;

        protected RowFormatBuilder(
                Path basePath, Encoder<IN> encoder, BucketAssigner<IN, BucketID> bucketAssigner) {
            this(
                    basePath,
                    encoder,
                    bucketAssigner,
                    new LakesoulTableSink.LakesoulRollingPolicy(false),
                    DEFAULT_BUCKET_CHECK_INTERVAL,
                    new LakeSoulBucketFactoryImpl<>(),
                    OutputFileConfig.builder().build());
        }

        protected RowFormatBuilder(
                Path basePath,
                Encoder<IN> encoder,
                BucketAssigner<IN, BucketID> assigner,
                LakesoulTableSink.LakesoulRollingPolicy<IN, BucketID> policy,
                long bucketCheckInterval,
                LakeSoulBucketFactory<IN, BucketID> bucketFactory,
                OutputFileConfig outputFileConfig) {
            this.basePath = Preconditions.checkNotNull(basePath);
            this.encoder = Preconditions.checkNotNull(encoder);
            this.bucketAssigner = Preconditions.checkNotNull(assigner);
            this.rollingPolicy = Preconditions.checkNotNull(policy);
            this.bucketCheckInterval = bucketCheckInterval;
            this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
            this.outputFileConfig = Preconditions.checkNotNull(outputFileConfig);
        }

        public long getBucketCheckInterval() {
            return bucketCheckInterval;
        }

        public T withBucketCheckInterval(final long interval) {
            this.bucketCheckInterval = interval;
            return self();
        }

        public T withBucketAssigner(final BucketAssigner<IN, BucketID> assigner) {
            this.bucketAssigner = Preconditions.checkNotNull(assigner);
            return self();
        }

        public T withRollingPolicy(final LakesoulTableSink.LakesoulRollingPolicy<IN, BucketID> policy) {
            this.rollingPolicy = Preconditions.checkNotNull(policy);
            return self();
        }

        public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
            this.outputFileConfig = outputFileConfig;
            return self();
        }

        public <ID>
        LakesoulFileSink.RowFormatBuilder<IN, ID, ? extends LakesoulFileSink.RowFormatBuilder<IN, ID, ?>>
        withNewBucketAssignerAndPolicy(
                final BucketAssigner<IN, ID> assigner,
                final LakesoulTableSink.LakesoulRollingPolicy<IN, ID> policy) {
           return new LakesoulFileSink.RowFormatBuilder(
                    basePath,
                    encoder,
                    Preconditions.checkNotNull(assigner),
                    Preconditions.checkNotNull(policy),
                    bucketCheckInterval,
                    new LakeSoulBucketFactoryImpl<>(),
                    outputFileConfig);
        }

        /** Creates the actual sink. */
        public LakesoulFileSink<IN> build() {
            return new LakesoulFileSink<>(this, bucketCheckInterval);
        }

        @VisibleForTesting
        T withBucketFactory(final LakeSoulBucketFactory<IN, BucketID> factory) {
            this.bucketFactory = Preconditions.checkNotNull(factory);
            return self();
        }

        @Internal
        @Override
        public BucketWriter<IN, BucketID> createBucketWriter() throws IOException {
            return new RowWiseBucketWriter<>(
                    FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder);
        }

        @Internal
        @Override
        public LakeSoulBuckets<IN, BucketID> createBuckets(int subtaskIndex) throws IOException {
            return new LakeSoulBuckets<>(
                    basePath,
                    bucketAssigner,
                    bucketFactory,
                    createBucketWriter(),
                    rollingPolicy,
                    subtaskIndex,
                    outputFileConfig);
        }
    }

    public static final class DefaultRowFormatBuilder<IN>
            extends RowFormatBuilder<IN, String, DefaultRowFormatBuilder<IN>> {
        private static final long serialVersionUID = -8503344257202146718L;

        private DefaultRowFormatBuilder(
                Path basePath, Encoder<IN> encoder, BucketAssigner<IN, String> bucketAssigner) {
            super(basePath, encoder, bucketAssigner);
        }
    }

    /** A builder for configuring the sink for bulk-encoding formats, e.g. Parquet/ORC. */
    @PublicEvolving
    public static class BulkFormatBuilder<IN, BucketID, T extends LakeSoulBucketsBuilder<IN, BucketID, T>>
            extends LakeSoulBucketsBuilder<IN, BucketID, T> {

        private static final long serialVersionUID = 1L;

        private long bucketCheckInterval;

        private final Path basePath;

        private BulkWriter.Factory<IN> writerFactory;

        private BucketAssigner<IN, BucketID> bucketAssigner;

        private LakesoulTableSink.LakesoulRollingPolicy<IN, BucketID> rollingPolicy;

        private LakeSoulBucketFactory<IN, BucketID> bucketFactory;

        private OutputFileConfig outputFileConfig;

        protected BulkFormatBuilder(
                Path basePath,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, BucketID> assigner) {
            this(
                    basePath,
                    writerFactory,
                    assigner,
                    new LakesoulTableSink.LakesoulRollingPolicy<>(false),
                    DEFAULT_BUCKET_CHECK_INTERVAL,
                    new LakeSoulBucketFactoryImpl<>(),
                    OutputFileConfig.builder().build());
        }

        public BulkFormatBuilder(
                Path basePath,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, BucketID> assigner,
                LakesoulTableSink.LakesoulRollingPolicy<IN, BucketID> policy,
                long bucketCheckInterval,
                LakeSoulBucketFactory<IN, BucketID> bucketFactory,
                OutputFileConfig outputFileConfig) {
            this.basePath = Preconditions.checkNotNull(basePath);
            this.writerFactory = writerFactory;
            this.bucketAssigner = Preconditions.checkNotNull(assigner);
            this.rollingPolicy = Preconditions.checkNotNull(policy);
            this.bucketCheckInterval = bucketCheckInterval;
            this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
            this.outputFileConfig = Preconditions.checkNotNull(outputFileConfig);
        }

        public long getBucketCheckInterval() {
            return bucketCheckInterval;
        }

        public T withBucketCheckInterval(long interval) {
            this.bucketCheckInterval = interval;
            return self();
        }

        public T withBucketAssigner(BucketAssigner<IN, BucketID> assigner) {
            this.bucketAssigner = Preconditions.checkNotNull(assigner);
            return self();
        }

        public T withRollingPolicy(LakesoulTableSink.LakesoulRollingPolicy<IN, BucketID> rollingPolicy) {
            this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
            return self();
        }

        @VisibleForTesting
        T withBucketFactory(final LakeSoulBucketFactory<IN, BucketID> factory) {
            this.bucketFactory = Preconditions.checkNotNull(factory);
            return self();
        }

        public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
            this.outputFileConfig = outputFileConfig;
            return self();
        }

//        public <ID> BulkFormatBuilder<IN, ID, ? extends BulkFormatBuilder<IN, ID, ?>> withNewBucketAssigner(final BucketAssigner<IN, ID> assigner) {
//            return new LakesoulFileSink.BulkFormatBuilder(
//                    basePath,
//                    writerFactory,
//                    Preconditions.checkNotNull(assigner),
//                    rollingPolicy,
//                    bucketCheckInterval,
//                    new LakeSoulBucketFactoryImpl<>(),
//                    outputFileConfig);
//        }

        /** Creates the actual sink. */
        public LakesoulFileSink<IN> build() {
            return new LakesoulFileSink<>(this, bucketCheckInterval);
        }

        @Internal
        @Override
        public BucketWriter<IN, BucketID> createBucketWriter() throws IOException {
            return new BulkBucketWriter<>(
                    FileSystem.get(basePath.toUri()).createRecoverableWriter(), writerFactory);
        }

        @Internal
        @Override
        public LakeSoulBuckets<IN, BucketID> createBuckets(int subtaskIndex) throws IOException {
            return new LakeSoulBuckets<>(
                    basePath,
                    bucketAssigner,
                    bucketFactory,
                    createBucketWriter(),
                    rollingPolicy,
                    subtaskIndex,
                    outputFileConfig);
        }
    }

    /**
     * Builder for the vanilla {@link LakesoulFileSink} using a bulk format.
     *
     * @param <IN> record type
     */
    public static final class DefaultBulkFormatBuilder<IN>
            extends LakesoulFileSink.BulkFormatBuilder<IN, String, LakesoulFileSink.DefaultBulkFormatBuilder<IN>> {

        private static final long serialVersionUID = 7493169281036370228L;

        private DefaultBulkFormatBuilder(
                Path basePath,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, String> assigner) {
            super(basePath, writerFactory, assigner);
        }
    }

    // --------------------------- Sink Methods -----------------------------

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.helper =
                new LakeSoulFileSinkHelper<>(
                        bucketsBuilder.createBuckets(getRuntimeContext().getIndexOfThisSubtask()),
                        context.isRestored(),
                        context.getOperatorStateStore(),
                        ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService(),
                        bucketCheckInterval);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        this.helper.commitUpToCheckpoint(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(helper != null, "sink has not been initialized");
        this.helper.snapshotState(context.getCheckpointId());
    }

    @Override
    public void invoke(IN value, SinkFunction.Context context) throws Exception {
        System.out.println("=====================================help.write================");
        this.helper.onElement(
                value,
                context.currentProcessingTime(),
                context.timestamp(),
                context.currentWatermark());
    }

    @Override
    public void close() throws Exception {
        if (this.helper != null) {
            this.helper.close();
        }
    }
}

