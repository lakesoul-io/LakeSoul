package org.apache.flink.lakesoul.sink.fileSystem.bulkFormat;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;

public final class DefaultBulkFormatBuilder<IN>
        extends BulkFormatBuilder<IN, String, DefaultBulkFormatBuilder<IN>> {

    private static final long serialVersionUID = 7493169281036370228L;

    public DefaultBulkFormatBuilder(
            Path basePath,
            BulkWriter.Factory<IN> writerFactory,
            BucketAssigner<IN, String> assigner) {
        super(basePath, writerFactory, assigner);
    }
}

