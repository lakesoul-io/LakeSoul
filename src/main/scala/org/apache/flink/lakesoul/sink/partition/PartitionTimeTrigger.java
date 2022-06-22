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

package org.apache.flink.lakesoul.sink.partition;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.util.StringUtils;

import java.util.*;

public class PartitionTimeTrigger implements PartitionTrigger {

    private static final ListStateDescriptor<List<String>> PENDING_PARTITIONS_STATE_DESC =
            new ListStateDescriptor<>(
                    "pending-partitions", new ListSerializer<> ( StringSerializer.INSTANCE));

    private static final ListStateDescriptor<Map<Long, Long>> WATERMARKS_STATE_DESC =
            new ListStateDescriptor<>(
                    "checkpoint-id-to-watermark",
                    new MapSerializer<> ( LongSerializer.INSTANCE, LongSerializer.INSTANCE));


    private final ListState<List<String>> pendingPartitionsState;
    private final Set<String> pendingPartitions;

    private final ListState<Map<Long, Long>> watermarksState;
    private final TreeMap<Long, Long> watermarks;

    public PartitionTimeTrigger(
            boolean isRestored,
            OperatorStateStore stateStore)
            throws Exception {
        this.pendingPartitionsState = stateStore.getListState(PENDING_PARTITIONS_STATE_DESC);
        this.pendingPartitions = new HashSet<> ();
        if (isRestored) {
            pendingPartitions.addAll(pendingPartitionsState.get().iterator().next());
        }


        this.watermarksState = stateStore.getListState(WATERMARKS_STATE_DESC);
        this.watermarks = new TreeMap<>();
        if (isRestored) {
            watermarks.putAll(watermarksState.get().iterator().next());
        }
    }

    @Override
    public void addPartition(String partition) {
        if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
            this.pendingPartitions.add(partition);
        }
    }

    @Override
    public List<String> committablePartitions(long checkpointId) {
        if (!watermarks.containsKey(checkpointId)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Checkpoint(%d) has not been snapshot. The watermark information is: %s.",
                            checkpointId, watermarks));
        }

        watermarks.headMap(checkpointId, true).clear();

        List<String> needCommit = new ArrayList<>();
        Iterator<String> iter = pendingPartitions.iterator();
        while (iter.hasNext()) {
            String partition = iter.next();
            needCommit.add(partition);
            iter.remove();

        }
        return needCommit;
    }


    @Override
    public void snapshotState(long checkpointId, long watermark) throws Exception {
        pendingPartitionsState.clear();
        pendingPartitionsState.add(new ArrayList<>(pendingPartitions));

        watermarks.put(checkpointId, watermark);
        watermarksState.clear();
        watermarksState.add(new HashMap<>(watermarks));
    }

    @Override
    public List<String> endInput() {
        ArrayList<String> partitions = new ArrayList<>(pendingPartitions);
        pendingPartitions.clear();
        return partitions;
    }
}
