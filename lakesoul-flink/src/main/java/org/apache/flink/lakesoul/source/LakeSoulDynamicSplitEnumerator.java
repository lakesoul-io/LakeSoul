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
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class LakeSoulDynamicSplitEnumerator implements SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> {
    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulDynamicSplitEnumerator.class);

    private final SplitEnumeratorContext<LakeSoulSplit> context;

    private final LakeSoulDynSplitAssigner splitAssigner;
    private final long discoveryInterval;
    private final String parDesc;
    private final Set<Integer> taskIdsAwaitingSplit;
    String tid;
    private long startTime;
    private long nextStartTime;
    private int hashBucketNum = -1;


    public LakeSoulDynamicSplitEnumerator(SplitEnumeratorContext<LakeSoulSplit> context,
                                          LakeSoulDynSplitAssigner splitAssigner, long discoveryInterval,
                                          long startTime, String tid, String parDesc, String hashBucketNum) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.discoveryInterval = discoveryInterval;
        this.tid = tid;
        this.startTime = startTime;
        this.parDesc = parDesc;
        this.hashBucketNum = Integer.parseInt(hashBucketNum);
        this.taskIdsAwaitingSplit = Sets.newConcurrentHashSet();
    }

    @Override
    public void start() {
        context.callAsync(() -> this.enumerateSplits(tid, parDesc), this::processDiscoveredSplits, discoveryInterval,
                discoveryInterval);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }
        int tasksSize = context.registeredReaders().size();
        Optional<LakeSoulSplit> al = this.splitAssigner.getNext(subtaskId, tasksSize);
        if (al.isPresent()) {
            context.assignSplit(al.get(), subtaskId);
            taskIdsAwaitingSplit.remove(subtaskId);
        } else {
            taskIdsAwaitingSplit.add(subtaskId);
        }

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
        LakeSoulPendingSplits pendingSplits =
                new LakeSoulPendingSplits(splitAssigner.remainingSplits(), this.nextStartTime, this.tid, this.parDesc,
                        this.discoveryInterval, this.hashBucketNum);
        LOG.info("LakeSoulDynamicSplitEnumerator snapshotState {}", pendingSplits);
        return pendingSplits;
    }

    @Override
    public void close() throws IOException {

    }

    private void processDiscoveredSplits(Collection<LakeSoulSplit> splits, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            return;
        }
        LOG.info("Process discovered splits {}", splits);
        int tasksSize = context.registeredReaders().size();
        this.splitAssigner.addSplits(splits);
        for (Integer item : taskIdsAwaitingSplit) {
            Optional<LakeSoulSplit> al = this.splitAssigner.getNext(item, tasksSize);
            if (al.isPresent()) {
                context.assignSplit(al.get(), item);
                taskIdsAwaitingSplit.remove(item);
            }
        }
    }

    public Collection<LakeSoulSplit> enumerateSplits(String tid, String parDesc) {
        this.nextStartTime = MetaVersion.getLastedTimestamp(tid, parDesc) + 1;
        DataFileInfo[] dfinfos =
                DataOperation.getIncrementalPartitionDataInfo(tid, parDesc, this.startTime, this.nextStartTime,
                        "incremental");
        LOG.info("Found new data info {}", (Object) dfinfos);
        int capacity = 100;
        ArrayList<LakeSoulSplit> splits = new ArrayList<>(capacity);
        int i = 0;
        Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition =
                FlinkUtil.splitDataInfosToRangeAndHashPartition(tid, dfinfos);
        for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
            for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                splits.add(new LakeSoulSplit(String.valueOf(i), split.getValue(), 0, split.getKey()));
            }
        }
        this.startTime = this.nextStartTime;
        return splits;
    }
}
