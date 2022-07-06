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

package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulBuckets;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulBucketsBuilder;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulFileSinkHelper;
import org.apache.flink.lakesoul.sink.fileSystem.bulkFormat.DefaultBulkFormatBuilder;
import org.apache.flink.lakesoul.sink.fileSystem.bulkFormat.DefaultRowFormatBuilder;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;

public class LakeSoulFileSink<IN> extends RichSinkFunction<IN>
    implements CheckpointedFunction, CheckpointListener {

  private static final long serialVersionUID = 1L;
  private final long bucketCheckInterval;
  private LakeSoulBucketsBuilder<IN, ?, ? extends LakeSoulBucketsBuilder<IN, ?, ?>> bucketsBuilder;
  private transient LakeSoulFileSinkHelper<IN> helper;

  public LakeSoulFileSink(
      LakeSoulBucketsBuilder<IN, ?, ? extends LakeSoulBucketsBuilder<IN, ?, ?>> bucketsBuilder,
      long bucketCheckInterval) {
    Preconditions.checkArgument(bucketCheckInterval > 0L);
    this.bucketsBuilder = Preconditions.checkNotNull(bucketsBuilder);
    this.bucketCheckInterval = bucketCheckInterval;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    LakeSoulBuckets<IN, ?> buckets = bucketsBuilder.createBuckets(getRuntimeContext().getIndexOfThisSubtask());
    this.helper =
        new LakeSoulFileSinkHelper<>(
            buckets,
            context.isRestored(),
            context.getOperatorStateStore(),
            ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService(),
            bucketCheckInterval);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    this.helper.commitUpToCheckpoint(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) {
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    Preconditions.checkState(helper != null, "sink has not been initialized");
    this.helper.snapshotState(context.getCheckpointId());
  }

  @Override
  public void invoke(IN value, SinkFunction.Context context) throws Exception {
    this.helper.onElement(
        value,
        context.currentProcessingTime(),
        context.timestamp(),
        context.currentWatermark());
  }

  @Override
  public void close() throws Exception {
    if (this.helper != null) {
      this.helper.close();
    }
  }

  public static <IN> DefaultRowFormatBuilder<IN> forRowFormat(
      final Path basePath, final Encoder<IN> encoder) {
    return new DefaultRowFormatBuilder<>(basePath, encoder, new DateTimeBucketAssigner<>());
  }

  public static <IN> DefaultBulkFormatBuilder<IN> forBulkFormat(
      final Path basePath, final BulkWriter.Factory<IN> writerFactory) {
    return new DefaultBulkFormatBuilder<>(
        basePath, writerFactory, new DateTimeBucketAssigner<>());
  }
}

