package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.sink.committer.FileCommitter;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.FileWriterBucketState;

import java.io.IOException;
import java.io.Serializable;

/**
 * The base abstract class for the {@link LakeSoulMultiTablesSink.BulkFormatBuilder}.
 */
abstract class BucketsBuilder<IN, T extends BucketsBuilder<IN, T>>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

    @SuppressWarnings("unchecked")
    protected T self() {
        return (T) this;
    }

    abstract AbstractLakeSoulMultiTableSinkWriter<IN> createWriter(final Sink.InitContext context, int subTaskId) throws IOException;

    abstract FileCommitter createCommitter() throws IOException;

    abstract SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer()
            throws IOException;

    abstract SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer()
            throws IOException;
}
