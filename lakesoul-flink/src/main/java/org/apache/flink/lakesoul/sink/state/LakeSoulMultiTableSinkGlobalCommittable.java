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

    private final Map<TableSchemaIdentity, List<LakeSoulMultiTableSinkCommittable>> groupedCommittable;

    public LakeSoulMultiTableSinkGlobalCommittable(
            List<LakeSoulMultiTableSinkCommittable> committableList, boolean isBounded) {
        committableList.sort(LakeSoulMultiTableSinkCommittable::compareTo);
        groupedCommittable = new HashMap<>();
        for (LakeSoulMultiTableSinkCommittable committable : committableList) {
            List<LakeSoulMultiTableSinkCommittable> groupedCommittableList = groupedCommittable.computeIfAbsent(
                    committable.getIdentity(),
                    bucketId -> new ArrayList()
            );
            if (groupedCommittableList.isEmpty()) {
                groupedCommittableList.add(committable);
            } else {
                LakeSoulMultiTableSinkCommittable last = groupedCommittableList.get(groupedCommittableList.size() - 1);
                if (isBounded || last.getCreationTime() == committable.getCreationTime()) {
                    last.merge(committable);
                } else {
                    groupedCommittableList.add(committable);
                }
            }
        }
    }

    public static LakeSoulMultiTableSinkGlobalCommittable fromLakeSoulMultiTableSinkGlobalCommittable(
            List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittableList, boolean isBounded) {
        List<LakeSoulMultiTableSinkCommittable> committableList = new ArrayList<>();
        for (LakeSoulMultiTableSinkGlobalCommittable globalCommittable : globalCommittableList) {
            globalCommittable.getGroupedCommittable().values().forEach(committableList::addAll);
        }
        return fromLakeSoulMultiTableSinkCommittable(committableList, isBounded);
    }

    public static LakeSoulMultiTableSinkGlobalCommittable fromLakeSoulMultiTableSinkCommittable(
            List<LakeSoulMultiTableSinkCommittable> committableList, boolean isBounded) {
        return new LakeSoulMultiTableSinkGlobalCommittable(committableList, isBounded);
    }


    public Map<TableSchemaIdentity, List<LakeSoulMultiTableSinkCommittable>> getGroupedCommittable() {
        return groupedCommittable;
    }

    @Override
    public String toString() {
        return "LakeSoulMultiTableSinkGlobalCommittable{" + "groupedCommitables=" + groupedCommittable + '}';
    }
}
