// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import java.util.*;

public class LakeSoulDynSplitAssigner {
    private final HashMap<Integer, ArrayList<LakeSoulSplit>> splits;
    private int hashBucketNum = -1;

    public LakeSoulDynSplitAssigner(Collection<LakeSoulSplit> splits, String hashBucketNum) {
        this.hashBucketNum = Integer.valueOf(hashBucketNum);
        this.splits = new HashMap<>(100);
        addSplitsFromCollection(splits);
    }

    private void addSplitsFromCollection(Collection<LakeSoulSplit> splitsCol) {
        if (splitsCol == null && splitsCol.size() == 0) {
            return;
        }
        for (LakeSoulSplit lss : splitsCol) {
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



    public Optional<LakeSoulSplit> getNext(int taskId, int tasksNum) {
        final int size = splits.size();
        if (size > 0) {
            if (-1 == this.hashBucketNum) {
                Collection<ArrayList<LakeSoulSplit>> all = this.splits.values();
                for (ArrayList<LakeSoulSplit> al : all) {
                    if (al.size() > 0) {
                        return Optional.of(al.remove(0));
                    }
                }
                return Optional.empty();
            } else {
                if (this.hashBucketNum <= tasksNum) {
                    ArrayList<LakeSoulSplit> taskSplits = this.splits.get(taskId);
                    return (taskSplits == null || taskSplits.size() == 0) ? Optional.empty() : Optional.of(taskSplits.remove(0));
                } else {
                    for (int i = taskId; i < this.hashBucketNum; i += tasksNum) {
                        ArrayList<LakeSoulSplit> splits = this.splits.get(i);
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

    public void addSplits(Collection<LakeSoulSplit> newSplits) {
        addSplitsFromCollection(newSplits);
    }

    public List<LakeSoulSplit> remainingSplits() {
        ArrayList<LakeSoulSplit> als = new ArrayList<>(100);
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
