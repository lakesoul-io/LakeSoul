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

package org.apache.flink.lakeSoul.sink.fileSystem.bulkFormat;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakeSoul.sink.fileSystem.LakeSoulRollingPolicyImpl;
import org.apache.flink.lakeSoul.sink.LakeSoulFileSink;
import org.apache.flink.lakeSoul.sink.fileSystem.LakeSoulBucketFactory;
import org.apache.flink.lakeSoul.sink.fileSystem.LakeSoulBucketFactoryImpl;
import org.apache.flink.lakeSoul.sink.fileSystem.LakeSoulBuckets;
import org.apache.flink.lakeSoul.sink.fileSystem.LakeSoulBucketsBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class RowFormatBuilder<IN, BucketID, T extends RowFormatBuilder<IN, BucketID, T>>
    extends LakeSoulBucketsBuilder<IN, BucketID, T> {

  private static final long serialVersionUID = 1L;

  private long bucketCheckInterval;

  private final Path basePath;

  private Encoder<IN> encoder;

  private BucketAssigner<IN, BucketID> bucketAssigner;

  private LakeSoulRollingPolicyImpl rollingPolicy;

  private LakeSoulBucketFactory<IN, BucketID> bucketFactory;

  private OutputFileConfig outputFileConfig;

  protected RowFormatBuilder(
      Path basePath, Encoder<IN> encoder, BucketAssigner<IN, BucketID> bucketAssigner) {
    this(
        basePath,
        encoder,
        bucketAssigner,
        new LakeSoulRollingPolicyImpl(false),
        DEFAULT_BUCKET_CHECK_INTERVAL,
        new LakeSoulBucketFactoryImpl<>(),
        OutputFileConfig.builder().build());
  }

  protected RowFormatBuilder(
      Path basePath,
      Encoder<IN> encoder,
      BucketAssigner<IN, BucketID> assigner,
      LakeSoulRollingPolicyImpl policy,
      long bucketCheckInterval,
      LakeSoulBucketFactory<IN, BucketID> bucketFactory,
      OutputFileConfig outputFileConfig) {
    this.basePath = Preconditions.checkNotNull(basePath);
    this.encoder = Preconditions.checkNotNull(encoder);
    this.bucketAssigner = Preconditions.checkNotNull(assigner);
    this.rollingPolicy = Preconditions.checkNotNull(policy);
    this.bucketCheckInterval = bucketCheckInterval;
    this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
    this.outputFileConfig = Preconditions.checkNotNull(outputFileConfig);
  }

  public long getBucketCheckInterval() {
    return bucketCheckInterval;
  }

  public T withBucketCheckInterval(final long interval) {
    this.bucketCheckInterval = interval;
    return self();
  }

  public T withBucketAssigner(final BucketAssigner<IN, BucketID> assigner) {
    this.bucketAssigner = Preconditions.checkNotNull(assigner);
    return self();
  }

  public T withRollingPolicy(final LakeSoulRollingPolicyImpl policy) {
    this.rollingPolicy = Preconditions.checkNotNull(policy);
    return self();
  }

  public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
    this.outputFileConfig = outputFileConfig;
    return self();
  }

  public <ID> RowFormatBuilder<IN, ID, ? extends RowFormatBuilder<IN, ID, ?>> withNewBucketAssignerAndPolicy(
      final BucketAssigner<IN, ID> assigner,
      final LakeSoulRollingPolicyImpl policy) {
    return new RowFormatBuilder(
        basePath,
        encoder,
        Preconditions.checkNotNull(assigner),
        Preconditions.checkNotNull(policy),
        bucketCheckInterval,
        new LakeSoulBucketFactoryImpl<>(),
        outputFileConfig);
  }

  /**
   * Creates the actual sink.
   */
  public LakeSoulFileSink<IN> build() {
    return new LakeSoulFileSink<>(this, bucketCheckInterval);
  }

  T withBucketFactory(final LakeSoulBucketFactory<IN, BucketID> factory) {
    this.bucketFactory = Preconditions.checkNotNull(factory);
    return self();
  }

  @Internal
  @Override
  public BucketWriter<IN, BucketID> createBucketWriter() throws IOException {
    return new RowWiseBucketWriter<>(
        FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder);
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
