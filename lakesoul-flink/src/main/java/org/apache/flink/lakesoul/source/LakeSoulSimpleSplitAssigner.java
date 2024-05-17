// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import java.util.*;

public class LakeSoulSimpleSplitAssigner {
    private final List<LakeSoulPartitionSplit> splits;

    public LakeSoulSimpleSplitAssigner(Collection<LakeSoulPartitionSplit> splits) {
        this.splits = new LinkedList<>(splits);
    }

    public LakeSoulSimpleSplitAssigner() {
        this.splits = new LinkedList<>();
    }

    public Optional<LakeSoulPartitionSplit> getNext() {
        final int size = splits.size();
        return size == 0 ? Optional.empty() : Optional.of(splits.remove(0));
    }

    public void addSplits(Collection<LakeSoulPartitionSplit> newSplits) {
        splits.addAll(newSplits);
    }

    public List<LakeSoulPartitionSplit> remainingSplits() {
        return splits;
    }

    @Override
    public String toString() {
        return "LakeSoulSimpleSplitAssigner " + splits;
    }
}
