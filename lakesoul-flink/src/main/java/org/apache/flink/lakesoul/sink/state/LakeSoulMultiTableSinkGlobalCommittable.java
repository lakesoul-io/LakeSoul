// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;

import java.io.Serializable;
import java.util.*;

/**
 * Wrapper class for both type of global committables in {@link LakeSoulMultiTablesSink}. One committable might be
 * either
 * one or more pending files to commit, or one in-progress file to clean up.
 */
public class LakeSoulMultiTableSinkGlobalCommittable implements Serializable {

    static final long serialVersionUID = 42L;

    private final List<Tuple2<TableSchemaIdentity, List<LakeSoulMultiTableSinkCommittable>>> groupedCommittable;

    public LakeSoulMultiTableSinkGlobalCommittable(
            List<LakeSoulMultiTableSinkCommittable> committableList, boolean isBounded) {
        committableList.sort(LakeSoulMultiTableSinkCommittable::compareTo);
        groupedCommittable = new ArrayList<>();
        for (LakeSoulMultiTableSinkCommittable committable : committableList) {
            if (groupedCommittable.isEmpty()) {
                groupedCommittable.add(Tuple2.of(committable.getIdentity(), new ArrayList<>()));
                groupedCommittable.get(0).f1.add(committable);
            } else {
                Tuple2<TableSchemaIdentity, List<LakeSoulMultiTableSinkCommittable>>
                        lastTuple = groupedCommittable.get(groupedCommittable.size() - 1);
                if (lastTuple.f0.equals(committable.getIdentity())) {
                    LakeSoulMultiTableSinkCommittable lastCommittable = lastTuple.f1.get(lastTuple.f1.size() - 1);
                    if (lastCommittable.getCreationTime() == committable.getCreationTime()) {
                        lastCommittable.merge(committable);
                    } else {
                        lastTuple.f1.add(committable);
                    }
                } else {
                    groupedCommittable.add(Tuple2.of(committable.getIdentity(), new ArrayList<>()));
                    groupedCommittable.get(groupedCommittable.size() - 1).f1.add(committable);
                }
            }
        }
    }

    public static LakeSoulMultiTableSinkGlobalCommittable fromLakeSoulMultiTableSinkGlobalCommittable(
            List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittableList, boolean isBounded) {
        List<LakeSoulMultiTableSinkCommittable> committableList = new ArrayList<>();
        for (LakeSoulMultiTableSinkGlobalCommittable globalCommittable : globalCommittableList) {
            globalCommittable.getGroupedCommittable().forEach(t2 ->
                    committableList.addAll(t2.f1)
            );
        }
        return fromLakeSoulMultiTableSinkCommittable(committableList, isBounded);
//        Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> groupedCommitables =
//                new HashMap<>();
//        globalCommittables.forEach(globalCommittable -> globalCommittable.getGroupedCommittable().forEach(
//                (key, value) -> groupedCommitables.computeIfAbsent(key, tuple2 -> new ArrayList<>()).addAll(value)));
//        return new LakeSoulMultiTableSinkGlobalCommittable(groupedCommitables, isBounded);
    }

    public static LakeSoulMultiTableSinkGlobalCommittable fromLakeSoulMultiTableSinkCommittable(
            List<LakeSoulMultiTableSinkCommittable> committableList, boolean isBounded) {
        return new LakeSoulMultiTableSinkGlobalCommittable(committableList, isBounded);
    }


    public List<Tuple2<TableSchemaIdentity, List<LakeSoulMultiTableSinkCommittable>>> getGroupedCommittable() {
        return groupedCommittable;
    }

    @Override
    public String toString() {
        return "LakeSoulMultiTableSinkGlobalCommittable{" + "groupedCommitables=" + groupedCommittable + '}';
    }
}
