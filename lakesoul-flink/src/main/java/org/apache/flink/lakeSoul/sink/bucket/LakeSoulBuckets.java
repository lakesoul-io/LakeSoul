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

package org.apache.flink.lakeSoul.sink.bucket;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.FileLifeCycleListener;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LakeSoulBuckets<IN, BucketID> {
  private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.streaming.api.functions.sink.filesystem.Buckets.class);
  private final Path basePath;
  private final LakeSoulBucketFactory<IN, BucketID> bucketFactory;
  private final BucketAssigner<IN, BucketID> bucketAssigner;
  private final BucketWriter<IN, BucketID> bucketWriter;
  private final LakeSoulRollingPolicyImpl rollingPolicy;
  private final int subtaskIndex;
  private final BucketContext bucketContext;
  private final Map<BucketID, LakeSoulBucket<IN, BucketID>> activeBuckets;
  private long maxPartCounter;
  private final OutputFileConfig outputFileConfig;
  @Nullable
  private LakeSoulBucketLifeCycleListener<IN, BucketID> bucketLifeCycleListener;
  @Nullable
  private FileLifeCycleListener<BucketID> fileLifeCycleListener;
  private final BucketStateSerializer<BucketID> bucketStateSerializer;

  public LakeSoulBuckets(Path basePath, BucketAssigner<IN, BucketID> bucketAssigner, LakeSoulBucketFactory<IN, BucketID> bucketFactory, BucketWriter<IN, BucketID> bucketWriter,
                         LakeSoulRollingPolicyImpl rollingPolicy, int subtaskIndex, OutputFileConfig outputFileConfig) {
    this.basePath = Preconditions.checkNotNull(basePath);
    this.bucketAssigner = Preconditions.checkNotNull(bucketAssigner);
    this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
    this.bucketWriter = Preconditions.checkNotNull(bucketWriter);
    this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
    this.subtaskIndex = subtaskIndex;
    this.outputFileConfig = Preconditions.checkNotNull(outputFileConfig);
    this.activeBuckets = new HashMap<>();
    this.bucketContext = new BucketContext();
    this.bucketStateSerializer = new BucketStateSerializer<>(
            bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
            bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
        bucketAssigner.getSerializer());
    this.maxPartCounter = 0L;

  }

  public void setBucketLifeCycleListener(LakeSoulBucketLifeCycleListener<IN, BucketID> bucketLifeCycleListener) {
    this.bucketLifeCycleListener = Preconditions.checkNotNull(bucketLifeCycleListener);
  }

  public void setFileLifeCycleListener(FileLifeCycleListener<BucketID> fileLifeCycleListener) {
    this.fileLifeCycleListener = Preconditions.checkNotNull(fileLifeCycleListener);
  }

  public void initializeState(List<byte[]> bucketStates, List<Long> partCounterState) throws Exception {
    this.initializePartCounter(partCounterState);
    LOG.info("Subtask {} initializing its state (max part counter={}).", this.subtaskIndex, this.maxPartCounter);
    this.initializeActiveBuckets(bucketStates);
  }

  private void initializePartCounter(List<Long> partCounterState) throws Exception {
    long maxCounter = 0L;

    long partCounter;
    for (Iterator<Long> var4 = partCounterState.iterator(); var4.hasNext(); maxCounter = Math.max(partCounter, maxCounter)) {
      partCounter = var4.next();
    }

    this.maxPartCounter = maxCounter;
  }

  private void initializeActiveBuckets(List<byte[]> bucketStates) throws Exception {
    for (byte[] serializedRecoveredState : bucketStates) {
      BucketState<BucketID> recoveredState = SimpleVersionedSerialization.readVersionAndDeSerialize(
              this.bucketStateSerializer, serializedRecoveredState);
      this.handleRestoredBucketState(recoveredState);
    }
  }

  private void handleRestoredBucketState(BucketState<BucketID> recoveredState) throws Exception {
    BucketID bucketId = recoveredState.getBucketId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Subtask {} restoring: {}", this.subtaskIndex, recoveredState);
    }

    LakeSoulBucket<IN, BucketID> restoredLakeSoulBucket =
        this.bucketFactory.restoreBucket(this.subtaskIndex, this.maxPartCounter, this.bucketWriter, this.rollingPolicy, recoveredState, this.fileLifeCycleListener, this.outputFileConfig);

    this.updateActiveBucketId(bucketId, restoredLakeSoulBucket);
  }

  private void updateActiveBucketId(BucketID bucketId, LakeSoulBucket<IN, BucketID> restoredLakeSoulBucket) throws IOException {
    if (!restoredLakeSoulBucket.isActive()) {
      this.notifyBucketInactive(restoredLakeSoulBucket);
    } else {
      LakeSoulBucket<IN, BucketID> lakeSoulBucket = this.activeBuckets.get(bucketId);
      if (lakeSoulBucket != null) {
        lakeSoulBucket.merge(restoredLakeSoulBucket);
      } else {
        this.activeBuckets.put(bucketId, restoredLakeSoulBucket);
      }

    }
  }

  public void commitUpToCheckpoint(long checkpointId) throws IOException {
    Iterator<Map.Entry<BucketID, LakeSoulBucket<IN, BucketID>>> activeBucketIt = this.activeBuckets.entrySet().iterator();
    LOG.info("Subtask {} received completion notification for checkpoint with id={}.", this.subtaskIndex, checkpointId);

    while (activeBucketIt.hasNext()) {
      LakeSoulBucket<IN, BucketID> lakeSoulBucket = activeBucketIt.next().getValue();
      lakeSoulBucket.onSuccessfulCompletionOfCheckpoint(checkpointId);
      if (!lakeSoulBucket.isActive()) {
        activeBucketIt.remove();
        this.notifyBucketInactive(lakeSoulBucket);
      }
    }

  }

  public void snapshotState(long checkpointId, List<byte[]> bucketStatesContainer, List<Long> partCounterStateContainer) throws Exception {
    Preconditions.checkState(this.bucketWriter != null && this.bucketStateSerializer != null, "sink has not been initialized");
    LOG.info("Subtask {} checkpointing for checkpoint with id={} (max part counter={}).", this.subtaskIndex, checkpointId, this.maxPartCounter);
    bucketStatesContainer.clear();
    partCounterStateContainer.clear();
    this.snapshotActiveBuckets(checkpointId, bucketStatesContainer);
    partCounterStateContainer.add(this.maxPartCounter);
  }

  private void snapshotActiveBuckets(long checkpointId, List<byte[]> bucketStatesContainer) throws Exception {
    for (LakeSoulBucket<IN, BucketID> lakeSoulBucket : this.activeBuckets.values()) {
      BucketState<BucketID> bucketState = lakeSoulBucket.onReceptionOfCheckpoint(checkpointId);
      byte[] serializedBucketState = SimpleVersionedSerialization.writeVersionAndSerialize(this.bucketStateSerializer, bucketState);
      bucketStatesContainer.add(serializedBucketState);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Subtask {} checkpointing: {}", this.subtaskIndex, bucketState);
      }
    }
  }

  public LakeSoulBucket<IN, BucketID> onElement(IN value,
                                                long currentProcessingTime,
                                                @Nullable Long elementTimestamp,
                                                long currentWatermark) throws Exception {
    this.bucketContext.update(elementTimestamp, currentWatermark, currentProcessingTime);
    BucketID bucketId = this.bucketAssigner.getBucketId(value, this.bucketContext);
    LakeSoulBucket<IN, BucketID> lakeSoulBucket = this.getOrCreateBucketForBucketId(bucketId);
    lakeSoulBucket.write(value, currentProcessingTime);
    this.maxPartCounter = Math.max(this.maxPartCounter, lakeSoulBucket.getPartCounter());
    return lakeSoulBucket;
  }

  private LakeSoulBucket<IN, BucketID> getOrCreateBucketForBucketId(BucketID bucketId) throws IOException {
    LakeSoulBucket<IN, BucketID> lakeSoulBucket = this.activeBuckets.get(bucketId);
    if (lakeSoulBucket == null) {
      Path bucketPath = this.assembleBucketPath(bucketId);
      lakeSoulBucket =
          this.bucketFactory.getNewBucket(this.subtaskIndex, bucketId, bucketPath,
                  this.maxPartCounter, this.bucketWriter,
                  this.rollingPolicy, this.fileLifeCycleListener, this.outputFileConfig);
      this.activeBuckets.put(bucketId, lakeSoulBucket);
      this.notifyBucketCreate(lakeSoulBucket);
    }

    return lakeSoulBucket;
  }

  public void onProcessingTime(long timestamp) throws Exception {

    for (LakeSoulBucket<IN, BucketID> inBucketIDLakeSoulBucket : this.activeBuckets.values()) {
      inBucketIDLakeSoulBucket.onProcessingTime(timestamp);
    }

  }

  public void closePartFileForBucket(BucketID bucketID) throws Exception {
    LakeSoulBucket<IN, BucketID> lakeSoulBucket = this.activeBuckets.get(bucketID);
    if (lakeSoulBucket != null) {
      lakeSoulBucket.closePartFile();
    }

  }

  public void close() {
    if (this.activeBuckets != null) {
      this.activeBuckets.values().forEach(LakeSoulBucket::disposePartFile);
    }

  }

  private Path assembleBucketPath(BucketID bucketId) {
    String child = bucketId.toString();
    return "".equals(child) ? this.basePath : new Path(this.basePath, child);
  }

  private void notifyBucketCreate(LakeSoulBucket<IN, BucketID> lakeSoulBucket) {
    if (this.bucketLifeCycleListener != null) {
      this.bucketLifeCycleListener.bucketCreated(lakeSoulBucket);
    }

  }

  private void notifyBucketInactive(LakeSoulBucket<IN, BucketID> lakeSoulBucket) {
    if (this.bucketLifeCycleListener != null) {
      this.bucketLifeCycleListener.bucketInactive(lakeSoulBucket);
    }

  }

  @VisibleForTesting
  public long getMaxPartCounter() {
    return this.maxPartCounter;
  }

  @VisibleForTesting
  Map<BucketID, LakeSoulBucket<IN, BucketID>> getActiveBuckets() {
    return this.activeBuckets;
  }

  private static final class BucketContext implements org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context {
    @Nullable
    private Long elementTimestamp;
    private long currentWatermark;
    private long currentProcessingTime;

    private BucketContext() {
      this.elementTimestamp = null;
      this.currentWatermark = -9223372036854775808L;
      this.currentProcessingTime = -9223372036854775808L;
    }

    void update(@Nullable Long elementTimestamp, long watermark, long processingTime) {
      this.elementTimestamp = elementTimestamp;
      this.currentWatermark = watermark;
      this.currentProcessingTime = processingTime;
    }

    @Override
    public long currentProcessingTime() {
      return this.currentProcessingTime;
    }

    @Override
    public long currentWatermark() {
      return this.currentWatermark;
    }

    @Override
    @Nullable
    public Long timestamp() {
      return this.elementTimestamp;
    }
  }

}
