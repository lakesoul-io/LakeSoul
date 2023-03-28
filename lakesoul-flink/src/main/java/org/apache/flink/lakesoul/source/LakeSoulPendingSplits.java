package org.apache.flink.lakesoul.source;

import java.util.List;

public class LakeSoulPendingSplits {

    /**
     * Split to read for both batch and streaming
     */
    private final List<LakeSoulSplit> splits;

    /**
     * Already discovered lastest version's timestamp
     * For streaming only
     */
    private final long lastReadTimestamp;

    public LakeSoulPendingSplits(List<LakeSoulSplit> splits, long lastReadTimestamp) {
        this.splits = splits;
        this.lastReadTimestamp = lastReadTimestamp;
    }
}
