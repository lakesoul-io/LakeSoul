package org.apache.flink.lakesoul.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class LakeSoulSimpleSplitAssigner {
    private final ArrayList<LakeSoulSplit> splits;

    public LakeSoulSimpleSplitAssigner(Collection<LakeSoulSplit> splits) {
        this.splits = new ArrayList<>(splits);
    }
    public LakeSoulSimpleSplitAssigner() {
        this.splits = new ArrayList<>();
    }

    public Optional<LakeSoulSplit> getNext() {
        final int size = splits.size();
        return size == 0 ? Optional.empty() : Optional.of(splits.remove(size - 1));
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
