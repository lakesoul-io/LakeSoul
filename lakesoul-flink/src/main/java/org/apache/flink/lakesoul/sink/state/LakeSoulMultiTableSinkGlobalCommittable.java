/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucket;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

/**
 * Wrapper class for both type of global committables in {@link LakeSoulMultiTablesSink}. One committable might be either
 * one or more pending files to commit, or one in-progress file to clean up.
 */
public class LakeSoulMultiTableSinkGlobalCommittable implements Serializable {

    static final long serialVersionUID = 42L;

    private final Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> groupedCommitables;

    public LakeSoulMultiTableSinkGlobalCommittable(Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> groupedCommitables) {
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

    public static LakeSoulMultiTableSinkGlobalCommittable fromLakeSoulMultiTableSinkGlobalCommittable(List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittables) {
        Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> groupedCommitables = new HashMap<>();
        globalCommittables.forEach(globalCommittable -> globalCommittable.getGroupedCommitables().forEach((key, value) -> groupedCommitables.computeIfAbsent(key, tuple2 -> new ArrayList<>()).addAll(value)));
        return new LakeSoulMultiTableSinkGlobalCommittable(groupedCommitables);
    }

    public static LakeSoulMultiTableSinkGlobalCommittable fromLakeSoulMultiTableSinkCommittable(List<LakeSoulMultiTableSinkCommittable> committables) {
        Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> groupedCommitables = new HashMap<>();
        committables.forEach(committable -> groupedCommitables.computeIfAbsent(Tuple2.of(committable.getIdentity(), committable.getBucketId()), tuple2 -> new ArrayList<>()).add(committable));
        return new LakeSoulMultiTableSinkGlobalCommittable(groupedCommitables);
    }


    public Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> getGroupedCommitables() {
        return groupedCommitables;
    }

}
