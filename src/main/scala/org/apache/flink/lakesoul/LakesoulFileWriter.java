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

package org.apache.flink.lakesoul;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.filesystem.stream.PartitionCommitPredicate;

import java.util.*;

public class LakesoulFileWriter<IN> extends LakesoulAbstractStreamingWriter<IN, DataInfo> {

    private static final long serialVersionUID = 2L;

    private  List<String> partitionKeys;
    private  Configuration conf;
    private OutputFileConfig outputFileConfig;

    private transient Set<String> currentNewPartitions;
    private transient TreeMap<Long, Set<String>> newPartitions;
    private transient Set<String> committablePartitions;
    private transient Map<String, Long> inProgressPartitions;

    private transient PartitionCommitPredicate partitionCommitPredicate;

    public LakesoulFileWriter(
                              long bucketCheckInterval,
                              LakesoulFileSink.BucketsBuilder<
                                      IN, String, ? extends LakesoulFileSink.BucketsBuilder<IN, String, ?>>
                                      bucketsBuilder,
                              List<String> partitionKeys,
                              Configuration conf) {
        super(bucketCheckInterval, bucketsBuilder);
        this.partitionKeys = partitionKeys;
        this.conf = conf;
    }

    public LakesoulFileWriter(long bucketCheckInterval, LakesoulFileSink.BucketsBuilder<IN, String, ? extends LakesoulFileSink.BucketsBuilder<IN, ?, ?>> bucketsBuilder, List<String> partitionKeys, Configuration conf, OutputFileConfig outputFileConf) {
        super(bucketCheckInterval, bucketsBuilder);
        this.partitionKeys = partitionKeys;
        this.conf = conf;
        this.outputFileConfig=outputFileConf;
    }


    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        if (isPartitionCommitTriggerEnabled()) {
            partitionCommitPredicate =
                    PartitionCommitPredicate.create(conf, getUserCodeClassloader(), partitionKeys);
        }

        currentNewPartitions = new HashSet<>();
        newPartitions = new TreeMap<>();
        committablePartitions = new HashSet<>();
        inProgressPartitions = new HashMap<>();
        super.initializeState(context);
    }

    @Override
    protected void partitionCreated(String partition) {
        currentNewPartitions.add(partition);
        inProgressPartitions.putIfAbsent(
                partition, getProcessingTimeService().getCurrentProcessingTime());
    }

    @Override
    protected void partitionInactive(String partition) {
        committablePartitions.add(partition);
        inProgressPartitions.remove(partition);
    }

    @Override
    protected void onPartFileOpened(String s, Path newPath) {}

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        closePartFileForPartitions();
        super.snapshotState(context);
        newPartitions.put(context.getCheckpointId(), new HashSet<>(currentNewPartitions));
        currentNewPartitions.clear();
    }

    private boolean isPartitionCommitTriggerEnabled() {
        // when partition keys and partition commit policy exist,
        // the partition commit trigger is enabled
        return true;
    }

    /** Close in-progress part file when partition is committable. */
    private void closePartFileForPartitions() throws Exception {
        if (partitionCommitPredicate != null) {
            final Iterator<Map.Entry<String, Long>> iterator =
                    inProgressPartitions.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> entry = iterator.next();
                String partition = entry.getKey();
                Long creationTime = entry.getValue();
                PartitionCommitPredicate.PredicateContext predicateContext =
                        PartitionCommitPredicate.createPredicateContext(
                                partition,
                                creationTime,
                                processingTimeService.getCurrentProcessingTime(),
                                currentWatermark);
                if (partitionCommitPredicate.isPartitionCommittable(predicateContext)) {
                    // if partition is committable, close in-progress part file in this partition
                    buckets.closePartFileForBucket(partition);
                    iterator.remove();
                }
            }
        }
    }

    @Override
    protected void commitUpToCheckpoint(long checkpointId) throws Exception {
        super.commitUpToCheckpoint(checkpointId);

        NavigableMap<Long, Set<String>> headPartitions =
                this.newPartitions.headMap(checkpointId, true);
        Set<String> partitions = new HashSet<>(committablePartitions);
        committablePartitions.clear();
        headPartitions.values().forEach(partitions::addAll);
        headPartitions.clear();
        String taskPathPre=outputFileConfig.getPartPrefix ()+"-";
        output.collect(
                new StreamRecord<>(
                        new DataInfo(
                                checkpointId,
                                getRuntimeContext().getIndexOfThisSubtask(),
                                getRuntimeContext().getNumberOfParallelSubtasks(),
                                new ArrayList<>(partitions),
                                taskPathPre)
                                )
        );
    }
}

