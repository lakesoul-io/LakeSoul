package org.apache.flink.lakesoul.sink.fileSystem;


import org.apache.flink.lakesoul.tools.LakeSoulKeyGen;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.table.data.RowData;
import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.DEFAULT_BUCKET_ROLLING_SIZE;
import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.DEFAULT_BUCKET_ROLLING_TIME;


public class LakeSoulRollingPolicyImpl<IN, BucketID> implements LakeSoulRollingPolicy<RowData, String> {

    private boolean rollOnCheckpoint;

    private LakeSoulKeyGen keyGen;

    private long rollingSize;

    private long rollingTime;

    public LakeSoulKeyGen getKeyGen() {
        return keyGen;
    }

    public void setKeyGen(LakeSoulKeyGen keyGen) {
        this.keyGen = keyGen;
    }

    public LakeSoulRollingPolicyImpl(boolean rollOnCheckpoint, long rollingSize, long rollingTime) {
        this.rollOnCheckpoint = rollOnCheckpoint;
        this.rollingSize = rollingSize;
        this.rollingTime = rollingTime;
    }

    public LakeSoulRollingPolicyImpl(boolean rollOnCheckpoint) {
        this.rollingSize = DEFAULT_BUCKET_ROLLING_SIZE;
        this.rollingTime = DEFAULT_BUCKET_ROLLING_TIME;
        this.rollOnCheckpoint = rollOnCheckpoint;
    }

    @Override
    public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
        return this.rollOnCheckpoint;
    }

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element) {
        return false;
    }

    @Override
    public boolean shouldRollOnProcessingTime(
            PartFileInfo<String> partFileState, long currentTime) {
        //TODO set time
        return currentTime - partFileState.getLastUpdateTime() > rollingTime;
    }

    public boolean shouldRollOnMaxSize(long size) {
        return size > rollingSize;
    }

    @Override
    public boolean shouldRoll(PartFileInfo<String> partFileState, long currentTime) {
        return false;
    }

    public long getRollingSize() {
        return rollingSize;
    }

    public void setRollingSize(long rollingSize) {
        this.rollingSize = rollingSize;
    }


    public long getRollingTime() {
        return rollingTime;
    }

    public void setRollingTime(long rollingTime) {
        this.rollingTime = rollingTime;
    }

    public boolean isRollOnCheckpoint() {
        return rollOnCheckpoint;
    }

    public void setRollOnCheckpoint(boolean rollOnCheckpoint) {
        this.rollOnCheckpoint = rollOnCheckpoint;
    }
}
