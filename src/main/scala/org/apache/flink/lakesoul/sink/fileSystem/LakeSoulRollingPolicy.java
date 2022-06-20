package org.apache.flink.lakesoul.sink.fileSystem;

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;

import java.io.IOException;

public interface LakeSoulRollingPolicy<IN,BucketID> extends RollingPolicy<IN, BucketID> {
    boolean shouldRoll(PartFileInfo<BucketID> var1, long var2) throws IOException;
}
