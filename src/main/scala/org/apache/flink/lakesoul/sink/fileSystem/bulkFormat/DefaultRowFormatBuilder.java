package org.apache.flink.lakesoul.sink.fileSystem.bulkFormat;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;


public final class DefaultRowFormatBuilder<IN>
        extends RowFormatBuilder<IN, String, DefaultRowFormatBuilder<IN>> {
    private static final long serialVersionUID = -8503344257202146718L;

    public DefaultRowFormatBuilder(
            Path basePath, Encoder<IN> encoder, BucketAssigner<IN, String> bucketAssigner) {
        super(basePath, encoder, bucketAssigner);
    }
}
