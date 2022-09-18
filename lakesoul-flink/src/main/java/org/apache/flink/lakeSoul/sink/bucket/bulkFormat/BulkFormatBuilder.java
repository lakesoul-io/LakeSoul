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

package org.apache.flink.lakeSoul.sink.bucket.bulkFormat;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakeSoul.sink.bucket.*;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.BulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class BulkFormatBuilder<IN, BucketID, T extends LakeSoulBucketsBuilder<IN, BucketID, T>>
    extends LakeSoulBucketsBuilder<IN, BucketID, T> {

  private static final long serialVersionUID = 1L;

  private long bucketCheckInterval;

  private final Path basePath;

  private final BulkWriter.Factory<IN> writerFactory;

  private BucketAssigner<IN, BucketID> bucketAssigner;

  private LakeSoulRollingPolicyImpl rollingPolicy;

  private LakeSoulBucketFactory<IN, BucketID> bucketFactory;

  private OutputFileConfig outputFileConfig;

  protected BulkFormatBuilder(
      Path basePath,
      BulkWriter.Factory<IN> writerFactory,
      BucketAssigner<IN, BucketID> assigner) {
    this(
        basePath,
        writerFactory,
        assigner,
        new LakeSoulRollingPolicyImpl(false),
        DEFAULT_BUCKET_CHECK_INTERVAL,
        new LakeSoulBucketFactoryImpl<>(),
        OutputFileConfig.builder().build());
  }

  public BulkFormatBuilder(
      Path basePath,
      BulkWriter.Factory<IN> writerFactory,
      BucketAssigner<IN, BucketID> assigner,
      LakeSoulRollingPolicyImpl policy,
      long bucketCheckInterval,
      LakeSoulBucketFactory<IN, BucketID> bucketFactory,
      OutputFileConfig outputFileConfig) {
    this.basePath = Preconditions.checkNotNull(basePath);
    this.writerFactory = writerFactory;
    this.bucketAssigner = Preconditions.checkNotNull(assigner);
    this.rollingPolicy = Preconditions.checkNotNull(policy);
    this.bucketCheckInterval = bucketCheckInterval;
    this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
    this.outputFileConfig = Preconditions.checkNotNull(outputFileConfig);
  }

  public long getBucketCheckInterval() {
    return bucketCheckInterval;
  }

  public T withBucketCheckInterval(long interval) {
    this.bucketCheckInterval = interval;
    return self();
  }

  public T withBucketAssigner(BucketAssigner<IN, BucketID> assigner) {
    this.bucketAssigner = Preconditions.checkNotNull(assigner);
    return self();
  }

  public T withRollingPolicy(LakeSoulRollingPolicyImpl rollingPolicy) {
    this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
    return self();
  }

  @VisibleForTesting
  T withBucketFactory(final LakeSoulBucketFactory<IN, BucketID> factory) {
    this.bucketFactory = Preconditions.checkNotNull(factory);
    return self();
  }

  public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
    this.outputFileConfig = outputFileConfig;
    return self();
  }

  @Internal
  @Override
  public BucketWriter<IN, BucketID> createBucketWriter() throws IOException {
    return new BulkBucketWriter<>(
        FileSystem.get(basePath.toUri()).createRecoverableWriter(), writerFactory);
  }

  @Internal
  @Override
  public LakeSoulBuckets<IN, BucketID> createBuckets(int subtaskIndex) throws IOException {
    return new LakeSoulBuckets<>(
        basePath,
        bucketAssigner,
        bucketFactory,
        createBucketWriter(),
        rollingPolicy,
        subtaskIndex,
        outputFileConfig);
  }
}
