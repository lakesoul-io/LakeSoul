/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
    private final int hashBucketNum;
    public LakeSoulPendingSplits(List<LakeSoulSplit> splits, long lastReadTimestamp, String tableid, String parDesc, long discoverInterval, int hashBucketNum) {
        this.splits = splits;
        this.lastReadTimestamp = lastReadTimestamp;
        this.tableid = tableid;
        this.parDesc = parDesc;
        this.discoverInterval = discoverInterval;
        this.hashBucketNum = hashBucketNum;
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

    public int getHashBucketNum() {
        return hashBucketNum;
    }

    @Override
    public String toString() {
        return "LakeSoulPendingSplits{" + "splits=" + splits + ", lastReadTimestamp=" + lastReadTimestamp +
                ", tableid='" + tableid + '\'' + ", parDesc='" + parDesc + '\'' + ", discoverInterval=" +
                discoverInterval + ", hashBucketNum=" + hashBucketNum + '}';
    }
}
