package org.apache.flink.lakesoul.sink;

import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulRollingPolicy;
import org.apache.flink.lakesoul.tools.LakeSoulKeyGen;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.table.data.RowData;

import java.io.IOException;


public class LakeSoulRollingPolicyImpl<IN,BucketID> implements LakeSoulRollingPolicy<RowData, String> {

    private  boolean rollOnCheckpoint;

    private LakeSoulKeyGen keygen;


    public LakeSoulKeyGen getKeygen() {
        return keygen;
    }

    public void setKeygen(LakeSoulKeyGen keygen) {
        this.keygen = keygen;
    }

    public LakeSoulRollingPolicyImpl(boolean rollOnCheckpoint) {
        this.rollOnCheckpoint = rollOnCheckpoint;
    }

    @Override
    public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
        boolean rollOnCheckpoint = this.rollOnCheckpoint;
        return rollOnCheckpoint;
    }

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element)
            throws IOException {
        return false;
    }

    @Override
    public boolean shouldRollOnProcessingTime(
            PartFileInfo<String> partFileState, long currentTime) {
        //TODO set time
        return currentTime - partFileState.getLastUpdateTime() > 100000;
    }

    public boolean shouldRollOnMaxSize(long size){
        //TODO set time
        return size > 2;
    }

    @Override
    public boolean shouldRoll(PartFileInfo<String> partFileState, long currentTime , long size) throws IOException {
        return shouldRollOnProcessingTime(partFileState,currentTime)||shouldRollOnMaxSize(size);
    }
}
