// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class LakeSoulStaticSplitEnumerator implements SplitEnumerator<LakeSoulPartitionSplit, LakeSoulPendingSplits> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulStaticSplitEnumerator.class);

    private final SplitEnumeratorContext<LakeSoulPartitionSplit> context;

    private final LakeSoulSimpleSplitAssigner splitAssigner;

    public LakeSoulStaticSplitEnumerator(SplitEnumeratorContext<LakeSoulPartitionSplit> context,
                                         LakeSoulSimpleSplitAssigner splitAssigner) {
        this.context = context;
        this.splitAssigner = splitAssigner;
    }

    @Override
    public void start() {
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        final Optional<LakeSoulPartitionSplit> nextSplit = splitAssigner.getNext();
        if (nextSplit.isPresent()) {
            final LakeSoulPartitionSplit split = nextSplit.get();
            context.assignSplit(split, subtaskId);
            LOG.info("Assigned split to subtask {} : {}", subtaskId, split);
        } else {
            context.signalNoMoreSplits(subtaskId);
            LOG.info("No more splits available for subtask {}", subtaskId);
        }
    }

    @Override
    public synchronized void addSplitsBack(List<LakeSoulPartitionSplit> splits, int subtaskId) {
        LOG.info("Add split back: {}", splits);
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
    }

    @Override
    public synchronized LakeSoulPendingSplits snapshotState(long checkpointId) throws Exception {
        LOG.info("LakeSoulStaticSplitEnumerator snapshotState");
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}
