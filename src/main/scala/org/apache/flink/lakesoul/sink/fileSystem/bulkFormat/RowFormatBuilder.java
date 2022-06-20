package org.apache.flink.lakesoul.sink.fileSystem.bulkFormat;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulRollingPolicyImpl;
import org.apache.flink.lakesoul.sink.LakesoulFileSink;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulBucketFactory;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulBucketFactoryImpl;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulBuckets;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulBucketsBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;


public class RowFormatBuilder<IN, BucketID, T extends RowFormatBuilder<IN, BucketID, T>>
        extends LakeSoulBucketsBuilder<IN, BucketID, T> {

    private static final long serialVersionUID = 1L;

    private long bucketCheckInterval;

    private final Path basePath;

    private Encoder<IN> encoder;

    private BucketAssigner<IN, BucketID> bucketAssigner;

    private LakeSoulRollingPolicyImpl<IN, BucketID> rollingPolicy;

    private LakeSoulBucketFactory<IN, BucketID> bucketFactory;

    private OutputFileConfig outputFileConfig;

    protected RowFormatBuilder(
            Path basePath, Encoder<IN> encoder, BucketAssigner<IN, BucketID> bucketAssigner) {
        this(
                basePath,
                encoder,
                bucketAssigner,
                new LakeSoulRollingPolicyImpl(false),
                DEFAULT_BUCKET_CHECK_INTERVAL,
                new LakeSoulBucketFactoryImpl<>(),
                OutputFileConfig.builder().build());
    }

    protected RowFormatBuilder(
            Path basePath,
            Encoder<IN> encoder,
            BucketAssigner<IN, BucketID> assigner,
            LakeSoulRollingPolicyImpl<IN, BucketID> policy,
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

    public T withRollingPolicy(final LakeSoulRollingPolicyImpl<IN, BucketID> policy) {
        this.rollingPolicy = Preconditions.checkNotNull(policy);
        return self();
    }

    public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
        this.outputFileConfig = outputFileConfig;
        return self();
    }

    public <ID> RowFormatBuilder<IN, ID, ? extends RowFormatBuilder<IN, ID, ?>>
    withNewBucketAssignerAndPolicy(
            final BucketAssigner<IN, ID> assigner,
            final LakeSoulRollingPolicyImpl<IN, ID> policy) {
        return new RowFormatBuilder(
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

    //        @VisibleForTesting
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
