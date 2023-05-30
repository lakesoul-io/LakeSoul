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

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.jetty.util.ConcurrentHashSet;

import javax.annotation.Nullable;
import java.io.Console;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class LakeSoulDynamicSplitEnumerator implements SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> {
    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulDynamicSplitEnumerator.class);

    private final SplitEnumeratorContext<LakeSoulSplit> context;

    private final LakeSoulSimpleSplitAssigner splitAssigner;
    private final long discoveryInterval;
    String tid;
    private long startTime;
    private long nextStartTime;
    private String parDesc;
    private ConcurrentHashSet<Integer> taskIdsAwaitingSplit;

    private ConcurrentHashMap<Integer, ArrayList<LakeSoulSplit>> splits;

    public LakeSoulDynamicSplitEnumerator(SplitEnumeratorContext<LakeSoulSplit> context, LakeSoulSimpleSplitAssigner splitAssigner, long discoveryInterval, long startTime, String tid, String parDesc) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.discoveryInterval = discoveryInterval;
        this.tid = tid;
        this.startTime = startTime;
        this.parDesc = parDesc;
        this.taskIdsAwaitingSplit = new ConcurrentHashSet<>();
        this.splits = new ConcurrentHashMap<>(100);
    }

    @Override
    public void start() {
        context.callAsync(
                () -> this.enumerateSplits(tid, parDesc),
                this::processDiscoveredSplits,
                discoveryInterval,
                discoveryInterval);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }
        ArrayList<LakeSoulSplit> al = this.splits.get(subtaskId);
        if (al != null && al.size() > 0) {
            context.assignSplit(al.remove(0), subtaskId);
            taskIdsAwaitingSplit.remove(subtaskId);
        } else {
            taskIdsAwaitingSplit.add(subtaskId);
        }

    }

    private int getSubtaskIdForSplit(LakeSoulSplit lss) {
        Set<Integer> allSubTaskIds = context.registeredReaders().keySet();
        int bucketid = lss.getBucketId();
        int taskIdsSize = allSubTaskIds.size();
        if (taskIdsSize == 1) {
            return allSubTaskIds.stream().findFirst().get();
        }
        if (bucketid < taskIdsSize) {
            return bucketid;
        } else {
            return bucketid % taskIdsSize;
        }

    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    }

    @Override
    public void addSplitsBack(List<LakeSoulSplit> splits, int subtaskId) {
        LOG.info("Add split back: {}", splits);
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
    }

    @Override
    public LakeSoulPendingSplits snapshotState(long checkpointId) throws Exception {
        LOG.info("LakeSoulDynamicSplitEnumerator snapshotState");
        return new LakeSoulPendingSplits(splitAssigner.remainingSplits(), this.nextStartTime, this.tid, this.parDesc, this.discoveryInterval);
    }

    @Override
    public void close() throws IOException {

    }

    private void processDiscoveredSplits(Collection<LakeSoulSplit> splits, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            return;
        }
        for (int item : taskIdsAwaitingSplit) {
            ArrayList<LakeSoulSplit> al = this.splits.get(item);
            if (al != null && al.size() > 0) {
                context.assignSplit(al.remove(0), item);
                taskIdsAwaitingSplit.remove(item);
            }
        }
    }

    public Collection<LakeSoulSplit> enumerateSplits(String tid, String parDesc) {
        this.nextStartTime = MetaVersion.getLastedTimestamp(tid, parDesc) + 1;
        DataFileInfo[] dfinfos = DataOperation.getIncrementalPartitionDataInfo(tid, parDesc, this.startTime, this.nextStartTime, "incremental");
        int capacity = 100;
        int i = 0;
        Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition = FlinkUtil.splitDataInfosToRangeAndHashPartition(tid, dfinfos);
        for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
            for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                Integer bucketNum = split.getKey();
                if (!this.splits.containsKey(bucketNum)) {
                    this.splits.put(bucketNum, new ArrayList<>());
                }
                this.splits.get(bucketNum).add(new LakeSoulSplit(i + "", split.getValue(), 0, split.getKey()));
            }
        }
        this.startTime = this.nextStartTime;
        return null;
    }
}
