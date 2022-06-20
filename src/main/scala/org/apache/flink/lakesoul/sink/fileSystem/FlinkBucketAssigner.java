package org.apache.flink.lakesoul.sink.fileSystem;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.tools.FlinkUtil;
import org.apache.flink.lakesoul.tools.LakeSoulKeyGen;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.PartitionComputer;

public class FlinkBucketAssigner implements BucketAssigner<RowData, String> {

    private final PartitionComputer<RowData> computer;


    public FlinkBucketAssigner(PartitionComputer<RowData> computer) {
        this.computer = computer;
    }

    @Override
    public String getBucketId(RowData element, Context context) {
        try {
            return FlinkUtil.generatePartitionPath(
                    computer.generatePartValues(element));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}