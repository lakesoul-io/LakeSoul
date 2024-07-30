// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink;

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;

import java.io.IOException;

public class LakeSoulRollingPolicyImpl<In> extends CheckpointRollingPolicy<In, String> {

    private boolean rollOnCheckpoint;

    private long rollingSize;

    private long rollingTime;

    public LakeSoulRollingPolicyImpl(long rollingSize, long rollingTime) {
        this.rollOnCheckpoint = true;
        this.rollingSize = rollingSize;
        this.rollingTime = rollingTime;
    }

    @Override
    public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
        return this.rollOnCheckpoint;
    }

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, In element) throws IOException {
        return partFileState.getSize() >= this.rollingSize;
    }

    @Override
    public boolean shouldRollOnProcessingTime(
            PartFileInfo<String> partFileState, long currentTime) {
        return currentTime - partFileState.getLastUpdateTime() > rollingTime;
    }

    public boolean shouldRollOnMaxSize(long size) {
        return size > rollingSize;
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
