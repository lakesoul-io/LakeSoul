// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import java.util.*;

public class LakeSoulSimpleSplitAssigner {
    private final List<LakeSoulSplit> splits;

    public LakeSoulSimpleSplitAssigner(Collection<LakeSoulSplit> splits) {
        this.splits = new LinkedList<>(splits);
    }
    public LakeSoulSimpleSplitAssigner() {
        this.splits = new LinkedList<>();
    }

    public Optional<LakeSoulSplit> getNext() {
        final int size = splits.size();
        return size == 0 ? Optional.empty() : Optional.of(splits.remove(0));
    }
    public void addSplits(Collection<LakeSoulSplit> newSplits) {
        splits.addAll(newSplits);
    }

    public List<LakeSoulSplit> remainingSplits() {
        return splits;
    }

    @Override
    public String toString() {
        return "LakeSoulSimpleSplitAssigner " + splits;
    }
}
