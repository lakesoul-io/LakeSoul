// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wrapper class for both type of global committables in {@link LakeSoulMultiTablesSink}. One committable might be
 * either
 * one or more pending files to commit, or one in-progress file to clean up.
 */
public class LakeSoulMultiTableSinkGlobalCommittable implements Serializable {

    static final long serialVersionUID = 42L;

    private final Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> groupedCommitables;

    public LakeSoulMultiTableSinkGlobalCommittable(
            Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> groupedCommitables) {
        groupedCommitables.forEach((key, disorderedCommitables) -> {
            disorderedCommitables.sort(LakeSoulMultiTableSinkCommittable::compareTo);
            List<LakeSoulMultiTableSinkCommittable> mergedCommittables = new ArrayList<>();
            for (LakeSoulMultiTableSinkCommittable committable : disorderedCommitables) {
                if (mergedCommittables.isEmpty()) {
                    mergedCommittables.add(committable);
                } else {
                    LakeSoulMultiTableSinkCommittable tail = mergedCommittables.get(mergedCommittables.size() - 1);
                    if (tail.getCreationTime() == committable.getCreationTime()) {
                        tail.merge(committable);
                    } else {
                        mergedCommittables.add(committable);
                    }
                }
            }
            groupedCommitables.put(key, mergedCommittables);
        });

        this.groupedCommitables = groupedCommitables;
    }

    public static LakeSoulMultiTableSinkGlobalCommittable fromLakeSoulMultiTableSinkGlobalCommittable(
            List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittables) {
        Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> groupedCommitables =
                new HashMap<>();
        globalCommittables.forEach(globalCommittable -> globalCommittable.getGroupedCommitables().forEach(
                (key, value) -> groupedCommitables.computeIfAbsent(key, tuple2 -> new ArrayList<>()).addAll(value)));
        return new LakeSoulMultiTableSinkGlobalCommittable(groupedCommitables);
    }

    public static LakeSoulMultiTableSinkGlobalCommittable fromLakeSoulMultiTableSinkCommittable(
            List<LakeSoulMultiTableSinkCommittable> committables) {
        Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> groupedCommitables =
                new HashMap<>();
        committables.forEach(committable -> groupedCommitables.computeIfAbsent(
                        Tuple2.of(committable.getIdentity(), committable.getBucketId()), tuple2 -> new ArrayList<>())
                .add(committable));
        return new LakeSoulMultiTableSinkGlobalCommittable(groupedCommitables);
    }


    public Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> getGroupedCommitables() {
        return groupedCommitables;
    }

    @Override
    public String toString() {
        return "LakeSoulMultiTableSinkGlobalCommittable{" + "groupedCommitables=" + groupedCommitables + '}';
    }
}
