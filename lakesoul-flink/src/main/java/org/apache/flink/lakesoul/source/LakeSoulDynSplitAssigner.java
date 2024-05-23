// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import java.util.*;

public class LakeSoulDynSplitAssigner {
    private final HashMap<Integer, ArrayList<LakeSoulPartitionSplit>> splits;
    private int hashBucketNum = -1;

    public LakeSoulDynSplitAssigner(Collection<LakeSoulPartitionSplit> splits, String hashBucketNum) {
        this.hashBucketNum = Integer.valueOf(hashBucketNum);
        this.splits = new HashMap<>(100);
        addSplitsFromCollection(splits);
    }

    private void addSplitsFromCollection(Collection<LakeSoulPartitionSplit> splitsCol) {
        if (splitsCol == null && splitsCol.size() == 0) {
            return;
        }
        for (LakeSoulPartitionSplit lss : splitsCol) {
            if (!this.splits.containsKey(lss.getBucketId())) {
                this.splits.put(lss.getBucketId(), new ArrayList<>());
            }
            this.splits.get(lss.getBucketId()).add(lss);
        }
    }

    public LakeSoulDynSplitAssigner(String hashBucketNum) {
        this.hashBucketNum = Integer.valueOf(hashBucketNum);
        this.splits = new HashMap<>(100);
    }


    public Optional<LakeSoulPartitionSplit> getNext(int taskId, int tasksNum) {
        final int size = splits.size();
        if (size > 0) {
            if (-1 == this.hashBucketNum) {
                Collection<ArrayList<LakeSoulPartitionSplit>> all = this.splits.values();
                for (ArrayList<LakeSoulPartitionSplit> al : all) {
                    if (al.size() > 0) {
                        return Optional.of(al.remove(0));
                    }
                }
                return Optional.empty();
            } else {
                if (this.hashBucketNum <= tasksNum) {
                    ArrayList<LakeSoulPartitionSplit> taskSplits = this.splits.get(taskId);
                    return (taskSplits == null || taskSplits.size() == 0) ? Optional.empty() : Optional.of(taskSplits.remove(0));
                } else {
                    for (int i = taskId; i < this.hashBucketNum; i += tasksNum) {
                        ArrayList<LakeSoulPartitionSplit> splits = this.splits.get(i);
                        if (splits != null && splits.size() > 0) {
                            return Optional.of(splits.remove(0));
                        }
                    }
                    return Optional.empty();
                }

            }
        } else {
            return Optional.empty();
        }

    }

    public void addSplits(Collection<LakeSoulPartitionSplit> newSplits) {
        addSplitsFromCollection(newSplits);
    }

    public List<LakeSoulPartitionSplit> remainingSplits() {
        ArrayList<LakeSoulPartitionSplit> als = new ArrayList<>(100);
        for (ArrayList al : this.splits.values()) {
            als.addAll(al);
        }
        return als;
    }

    @Override
    public String toString() {
        return "LakeSoulDynSplitAssigner " + splits;
    }

}
