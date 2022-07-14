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

package org.apache.flink.lakeSoul.sink;

import org.apache.flink.core.fs.Path;
import org.apache.flink.lakeSoul.sink.fileSystem.LakeSoulBucket;
import org.apache.flink.lakeSoul.sink.fileSystem.LakeSoulBucketLifeCycleListener;
import org.apache.flink.lakeSoul.sink.fileSystem.LakeSoulBuckets;
import org.apache.flink.lakeSoul.sink.fileSystem.LakeSoulBucketsBuilder;
import org.apache.flink.lakeSoul.sink.fileSystem.LakeSoulFileSinkHelper;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public abstract class LakesSoulAbstractStreamingWriter<IN, OUT> extends AbstractStreamOperator<OUT>
    implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private final long bucketCheckInterval;
  private LakeSoulBucketsBuilder<IN, String, ? extends LakeSoulBucketsBuilder<IN, String, ?>> bucketsBuilder;
  protected transient LakeSoulBuckets<IN, String> buckets;
  private transient LakeSoulFileSinkHelper<IN> helper;
  protected transient long currentWatermark;

  public LakesSoulAbstractStreamingWriter(long bucketCheckInterval,
                                          LakeSoulBucketsBuilder<IN, String, ? extends LakeSoulBucketsBuilder<IN, String, ?>> bucketsBuilder) {
    this.bucketCheckInterval = bucketCheckInterval;
    this.bucketsBuilder = bucketsBuilder;
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  protected abstract void partitionCreated(String partition);

  protected abstract void partitionInactive(String partition);

  protected abstract void onPartFileOpened(String partition, Path newPath);

  /**
   * Commit up to this checkpoint id.
   */
  protected void commitUpToCheckpoint(long checkpointId) throws Exception {
    helper.commitUpToCheckpoint(checkpointId);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    buckets = bucketsBuilder.createBuckets(getRuntimeContext().getIndexOfThisSubtask());

    // Set listener before the initialization of LakeSoulBuckets.
    buckets.setBucketLifeCycleListener(
        new LakeSoulBucketLifeCycleListener<IN, String>() {
          @Override
          public void bucketCreated(LakeSoulBucket<IN, String> bucket) {
            LakesSoulAbstractStreamingWriter.this.partitionCreated(bucket.getBucketId());
          }

          @Override
          public void bucketInactive(LakeSoulBucket<IN, String> bucket) {
            LakesSoulAbstractStreamingWriter.this.partitionInactive(bucket.getBucketId());
          }
        });

    helper =
        new LakeSoulFileSinkHelper<>(
            buckets,
            context.isRestored(),
            context.getOperatorStateStore(),
            getRuntimeContext().getProcessingTimeService(),
            bucketCheckInterval);

    currentWatermark = Long.MIN_VALUE;
  }

  @Override
  public void processElement(StreamRecord<IN> element) throws Exception {
    helper.onElement(
        element.getValue(),
        getProcessingTimeService().getCurrentProcessingTime(),
        element.hasTimestamp() ? element.getTimestamp() : null,
        currentWatermark);
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);
    currentWatermark = mark.getTimestamp();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    helper.snapshotState(context.getCheckpointId());
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    commitUpToCheckpoint(checkpointId);
  }

  @Override
  public void endInput() throws Exception {
    buckets.onProcessingTime(Long.MAX_VALUE);
    helper.snapshotState(Long.MAX_VALUE);
    output.emitWatermark(new Watermark(Long.MAX_VALUE));
    commitUpToCheckpoint(Long.MAX_VALUE);
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (helper != null) {
      helper.close();
    }
  }

  @Override
  public void finish() {
  }
}
