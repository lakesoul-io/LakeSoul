package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
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

    public LakeSoulDynamicSplitEnumerator(SplitEnumeratorContext<LakeSoulSplit> context, LakeSoulSimpleSplitAssigner splitAssigner, long discoveryInterval, long startTime, String tid, String parDesc) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.discoveryInterval = discoveryInterval;
        this.tid = tid;
        this.startTime = startTime;
        this.parDesc = parDesc;
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
        splitAssigner.addSplits(splits);
    }

    public Collection<LakeSoulSplit> enumerateSplits(String tid, String parDesc)
            throws IOException {
        this.nextStartTime = MetaVersion.getLastedTimestamp(tid, "") + 1;
        DataFileInfo[] dfinfos = (DataFileInfo[]) DataOperation.getSinglePartitionIncrementalDataInfos(tid, parDesc, this.startTime, this.nextStartTime).array();
        int capacity = 100;
        ArrayList<LakeSoulSplit> splits = new ArrayList<>(capacity);
        int i = 0;
        Map<String, Map<String, List<Path>>> splitByRangeAndHashPartition = FlinkUtil.splitDataInfosToRangeAndHashPartition(dfinfos);
        for (Map.Entry<String, Map<String, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
            for (Map.Entry<String, List<Path>> split : entry.getValue().entrySet()) {
                splits.add(new LakeSoulSplit(i + "", split.getValue(),0));
            }
        }
        this.startTime = this.nextStartTime;
        return splits;
    }
}
