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

package org.apache.flink.lakeSoul.sink.fileSystem;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;

public class LakeSoulFileSinkHelper<IN> implements ProcessingTimeCallback {
  private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC;
  private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC;
  private final long bucketCheckInterval;
  private final ProcessingTimeService procTimeService;
  private final LakeSoulBuckets<IN, ?> buckets;
  private final ListState<byte[]> bucketStates;
  private final ListState<Long> maxPartCountersState;

  public LakeSoulFileSinkHelper(LakeSoulBuckets<IN, ?> buckets, boolean isRestored, OperatorStateStore stateStore, ProcessingTimeService procTimeService, long bucketCheckInterval) throws Exception {
    this.bucketCheckInterval = bucketCheckInterval;
    this.buckets = buckets;
    this.bucketStates = stateStore.getListState(BUCKET_STATE_DESC);
    this.maxPartCountersState = stateStore.getUnionListState(MAX_PART_COUNTER_STATE_DESC);
    this.procTimeService = procTimeService;
    if (isRestored) {
      buckets.initializeState(this.bucketStates, this.maxPartCountersState);
    }

    long currentProcessingTime = procTimeService.getCurrentProcessingTime();
    procTimeService.registerTimer(currentProcessingTime + bucketCheckInterval, this);


  }

  public void commitUpToCheckpoint(long checkpointId) throws Exception {
    this.buckets.commitUpToCheckpoint(checkpointId);
  }

  public void snapshotState(long checkpointId) throws Exception {
    this.buckets.snapshotState(checkpointId, this.bucketStates, this.maxPartCountersState);
  }

  @Override
  public void onProcessingTime(long timestamp) throws Exception {
    long currentTime = this.procTimeService.getCurrentProcessingTime();
    this.buckets.onProcessingTime(currentTime);
    this.procTimeService.registerTimer(currentTime + this.bucketCheckInterval, this);
  }

  public void onElement(IN value, long currentProcessingTime, @Nullable Long elementTimestamp, long currentWatermark) throws Exception {
    this.buckets.onElement(value, currentProcessingTime, elementTimestamp, currentWatermark);
  }

  public void close() {
    this.buckets.close();
  }

  static {
    BUCKET_STATE_DESC = new ListStateDescriptor("bucket-states", BytePrimitiveArraySerializer.INSTANCE);
    MAX_PART_COUNTER_STATE_DESC = new ListStateDescriptor("max-part-counter", LongSerializer.INSTANCE);
  }
}

