package org.apache.flink.lakesoul.sink.fileSystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;



import javax.annotation.Nullable;
import java.io.IOException;

public class LakeSoulBucketFactoryImpl <IN, BucketID> implements LakeSoulBucketFactory<IN, BucketID> {
    private static final long serialVersionUID = 1L;

    public LakeSoulBucketFactoryImpl() {
    }

    @Override
    public LakeSoulBucket<IN, BucketID> getNewBucket(int subtaskIndex, BucketID bucketId, Path bucketPath, long initialPartCounter, BucketWriter<IN, BucketID> bucketWriter, LakeSoulRollingPolicyImpl<IN, BucketID> rollingPolicy, @Nullable FileLifeCycleListener<BucketID> fileListener, OutputFileConfig outputFileConfig ) {
        return LakeSoulBucket.getNew(subtaskIndex, bucketId, bucketPath, initialPartCounter, bucketWriter, rollingPolicy, fileListener, outputFileConfig);
    }

    @Override
    public LakeSoulBucket<IN, BucketID> restoreBucket(int subtaskIndex, long initialPartCounter, BucketWriter<IN, BucketID> bucketWriter, LakeSoulRollingPolicyImpl<IN, BucketID> rollingPolicy, BucketState<BucketID> bucketState, @Nullable FileLifeCycleListener<BucketID> fileListener, OutputFileConfig outputFileConfig) throws IOException {
        return LakeSoulBucket.restore(subtaskIndex, initialPartCounter, bucketWriter, rollingPolicy, bucketState, fileListener, outputFileConfig);
    }
}
