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

    private final String tableid;
    private final String parDesc;
    private final long discoverInterval;
    public LakeSoulPendingSplits(List<LakeSoulSplit> splits, long lastReadTimestamp,String tableid,String parDesc,long discoverInterval) {
        this.splits = splits;
        this.lastReadTimestamp = lastReadTimestamp;
        this.tableid = tableid;
        this.parDesc = parDesc;
        this.discoverInterval = discoverInterval;
    }

    public List<LakeSoulSplit> getSplits() {
        return splits;
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    public String getTableid() {
        return tableid;
    }

    public String getParDesc() {
        return parDesc;
    }

    public long getDiscoverInterval() {
        return discoverInterval;
    }
}
