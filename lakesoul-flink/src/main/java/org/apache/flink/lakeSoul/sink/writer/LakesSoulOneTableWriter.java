/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakeSoul.sink.writer;

import org.apache.flink.lakeSoul.metaData.DataFileMetaData;
import org.apache.flink.lakeSoul.sink.bucket.*;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.filesystem.stream.PartitionCommitPredicate;

import java.io.Serializable;
import java.util.*;

public class LakesSoulOneTableWriter<IN> implements Serializable {

  private static final long serialVersionUID = 1L;
  private final long bucketCheckInterval;
  private final LakeSoulBucketsBuilder<IN, String, ? extends LakeSoulBucketsBuilder<IN, String, ?>> bucketsBuilder;
  protected transient LakeSoulBuckets<IN, String> buckets;
  private transient LakeSoulFileSinkHelper<IN> helper;
  protected transient long currentWatermark;
  protected transient Set<String> currentNewBuckets;
  protected transient Map<String, Long> inProgressBuckets;
  protected transient Set<String> committableBuckets;
  protected transient TreeMap<Long, Set<String>> newBuckets;

  private final StreamingRuntimeContext runtimeContext;

  List<byte[]> bucketStates;

  List<Long> maxPartCountersStates;
  private final transient PartitionCommitPredicate partitionCommitPredicate;

  public LakesSoulOneTableWriter(long bucketCheckInterval,
                                 LakeSoulBucketsBuilder<IN, String, ? extends LakeSoulBucketsBuilder<IN, String, ?>> bucketsBuilder,
                                 PartitionCommitPredicate partitionCommitPredicate,
                                 StreamingRuntimeContext runtimeContext) {
    this.bucketCheckInterval = bucketCheckInterval;
    this.bucketsBuilder = bucketsBuilder;
    this.bucketStates = new ArrayList<>();
    this.maxPartCountersStates = new ArrayList<>();
    this.partitionCommitPredicate = partitionCommitPredicate;
    this.runtimeContext = runtimeContext;
  }

  protected  void partitionCreated(String partition, long currentProcessingTime){
    this.currentNewBuckets.add(partition);
    this.inProgressBuckets.putIfAbsent(partition, currentProcessingTime);
  }

  protected void partitionInactive(String partition){
    this.committableBuckets.add(partition);
    this.inProgressBuckets.remove(partition);
  }

  /**
   * Commit up to this checkpoint id.
   */
  public void commitUpToCheckpoint(long checkpointId) throws Exception {
    helper.commitUpToCheckpoint(checkpointId);
  }

  protected void initializeState(boolean isRestored, int taskId) throws Exception {
    this.currentNewBuckets = new HashSet<>();
    this.committableBuckets = new HashSet<>();
    this.inProgressBuckets = new HashMap<>();
    this.newBuckets = new TreeMap<>();

    buckets = bucketsBuilder.createBuckets(taskId);
    // Set listener before the initialization of LakeSoulBuckets.
    buckets.setBucketLifeCycleListener(
            new LakeSoulBucketLifeCycleListener<>() {
              @Override
              public void bucketCreated(LakeSoulBucket<IN, String> bucket) {
                LakesSoulOneTableWriter.this.partitionCreated(bucket.getBucketId(),
                        runtimeContext.getProcessingTimeService().getCurrentProcessingTime());
              }

              @Override
              public void bucketInactive(LakeSoulBucket<IN, String> bucket) {
                LakesSoulOneTableWriter.this.partitionInactive(bucket.getBucketId());
              }
            });

    helper =
            new LakeSoulFileSinkHelper<>(
                    buckets,
                    isRestored,
                    runtimeContext.getProcessingTimeService(),
                    bucketCheckInterval,
                    this.bucketStates,
                    this.maxPartCountersStates
                    );

    currentWatermark = Long.MIN_VALUE;
  }

  public void initializeState(boolean isRestored,
                              List<byte[]> bucketStatesRestored,
                              List<Long> maxPartCountersStatesRestored) throws Exception {
    bucketStates.clear();
    maxPartCountersStates.clear();
    if (isRestored) {
      bucketStates.addAll(bucketStatesRestored);
      maxPartCountersStates.addAll(maxPartCountersStatesRestored);
    }
    initializeState(isRestored, runtimeContext.getIndexOfThisSubtask());
  }

  public void processElement(StreamRecord<IN> element) throws Exception {
    helper.onElement(
        element.getValue(),
        runtimeContext.getProcessingTimeService().getCurrentProcessingTime(),
        element.hasTimestamp() ? element.getTimestamp() : null,
        currentWatermark);
  }

  public void processWatermark(Watermark mark) {
    currentWatermark = mark.getTimestamp();
  }

  public void snapshotState(StateSnapshotContext context) throws Exception {
    closePartFileForPartitions();
    helper.snapshotState(context.getCheckpointId());
    this.newBuckets.put(context.getCheckpointId(), new HashSet<>(currentNewBuckets));
    this.currentNewBuckets.clear();
  }

  private void closePartFileForPartitions() {
    if (partitionCommitPredicate != null) {
      inProgressBuckets.forEach((BucketID, creationTime) -> {
        PartitionCommitPredicate.PredicateContext predicateContexts = PartitionCommitPredicate.createPredicateContext(
                BucketID, creationTime, runtimeContext.getProcessingTimeService().getCurrentProcessingTime(),
                currentWatermark);
        if (partitionCommitPredicate.isPartitionCommittable(predicateContexts)) {
          try {
            buckets.closePartFileForBucket(BucketID);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }
  }

  public void endInput(Output<StreamRecord<DataFileMetaData>> output) throws Exception {
    buckets.onProcessingTime(Long.MAX_VALUE);
    helper.snapshotState(Long.MAX_VALUE);
    output.emitWatermark(new Watermark(Long.MAX_VALUE));
    commitUpToCheckpoint(Long.MAX_VALUE);
  }

  public void close() {
    if (helper != null) {
      helper.close();
    }
  }

  public void finish() {
  }

  public Set<String> getCommittableBuckets() {
    return committableBuckets;
  }

  public List<byte[]> getBucketStates() {
    return bucketStates;
  }

  public List<Long> getMaxPartCountersStates() {
    return maxPartCountersStates;
  }

  public TreeMap<Long, Set<String>> getNewBuckets() {
    return newBuckets;
  }
}
