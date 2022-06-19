package org.apache.flink.lakesoul.sink.fileSystem;

public interface LakeSoulBucketLifeCycleListener <IN, BucketID> {
    void bucketCreated(LakeSoulBucket<IN, BucketID> var1);

    void bucketInactive(LakeSoulBucket<IN, BucketID> var1);
}