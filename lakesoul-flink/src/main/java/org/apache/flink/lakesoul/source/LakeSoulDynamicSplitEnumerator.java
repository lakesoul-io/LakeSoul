package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;


public class LakeSoulDynamicSplitEnumerator implements SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> {
    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulDynamicSplitEnumerator.class);

    private final SplitEnumeratorContext<LakeSoulSplit> context;

    private final LakeSoulSimpleSplitAssigner splitAssigner;
    private final long discoveryInterval;
    String tid;
    private long startTime;
    private long nextStartTime;
    private String parDesc;

    private Set<Integer> taskIdsAwaitingSplit;

    public LakeSoulDynamicSplitEnumerator(SplitEnumeratorContext<LakeSoulSplit> context, LakeSoulSimpleSplitAssigner splitAssigner, long discoveryInterval, long startTime, String tid, String parDesc) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.discoveryInterval = discoveryInterval;
        this.tid = tid;
        this.startTime = startTime;
        this.parDesc = parDesc;
        this.taskIdsAwaitingSplit = new HashSet<>();
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

        final Optional<LakeSoulSplit> nextSplit = splitAssigner.getNext();
        if (nextSplit.isPresent()) {
            final LakeSoulSplit split = nextSplit.get();
            context.assignSplit(split, subtaskId);
            LOG.info("Assigned split to subtask {} : {}", subtaskId, split);
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
        Set<Integer> allSubTaskIds = context.registeredReaders().keySet();
        for (Integer subTaskId : allSubTaskIds) {
            // if the subTask that requested another split has failed in the meantime, remove
            // it from the list of waiting taskIds
            if (!taskIdsAwaitingSplit.contains(subTaskId)) {
                taskIdsAwaitingSplit.add(subTaskId);
            } else {
                final Optional<LakeSoulSplit> nextSplit = splitAssigner.getNext();
                if (nextSplit.isPresent()) {
                    context.assignSplit(nextSplit.get(), subTaskId);
                    taskIdsAwaitingSplit.remove(subTaskId);
                }
            }
        }
        splitAssigner.addSplits(splits);
    }

    public Collection<LakeSoulSplit> enumerateSplits(String tid, String parDesc) {
        this.nextStartTime = MetaVersion.getLastedTimestamp(tid, parDesc) + 1;
        DataFileInfo[] dfinfos = DataOperation.getIncrementalPartitionDataInfo(tid, parDesc, this.startTime, this.nextStartTime, "incremental");
        int capacity = 100;
        ArrayList<LakeSoulSplit> splits = new ArrayList<>(capacity);
        int i = 0;
        Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition = FlinkUtil.splitDataInfosToRangeAndHashPartition(tid, dfinfos);
        // todo:流式增量读split取消分组，不需要支持 merge on read
        for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
            for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                splits.add(new LakeSoulSplit(i + "", split.getValue(), 0));
            }
        }
        this.startTime = this.nextStartTime;
        return splits;
    }
}
