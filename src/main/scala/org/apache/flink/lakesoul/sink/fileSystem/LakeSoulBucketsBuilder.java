package org.apache.flink.lakesoul.sink.fileSystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;

import java.io.IOException;
import java.io.Serializable;

public abstract class LakeSoulBucketsBuilder<IN, BucketID, T extends LakeSoulBucketsBuilder<IN, BucketID, T>>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

    @SuppressWarnings("unchecked")
    protected T self() {
        return (T) this;
    }

    @Internal
    public abstract BucketWriter<IN, BucketID> createBucketWriter() throws IOException;

    @Internal
    public abstract LakeSoulBuckets<IN, BucketID> createBuckets(final int subtaskIndex)
            throws IOException;
}