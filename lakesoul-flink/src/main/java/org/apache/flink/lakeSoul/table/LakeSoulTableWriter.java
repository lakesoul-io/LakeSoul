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

package org.apache.flink.lakeSoul.table;

import io.debezium.relational.TableId;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakeSoul.metaData.DataFileMetaData;
import org.apache.flink.lakeSoul.sink.bucket.LakeSoulBucketsBuilder;
import org.apache.flink.lakeSoul.sink.writer.LakesSoulOneTableWriter;
import org.apache.flink.lakeSoul.tool.LakeSoulKeyGen;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.filesystem.stream.PartitionCommitPredicate;
import org.apache.flink.table.runtime.generated.RecordComparator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class LakeSoulTableWriter<IN> extends AbstractStreamOperator<DataFileMetaData>
    implements OneInputStreamOperator<IN, DataFileMetaData>, BoundedOneInput {

  private static final long serialVersionUID = 2L;
  private final long bucketCheckInterval;
  private final LakeSoulBucketsBuilder<IN, String, ? extends LakeSoulBucketsBuilder<IN, ?, ?>> bucketsBuilder;
  private final List<String> partitionKeyList;
  private final Configuration flinkConf;
  private final OutputFileConfig outputFileConfig;
  private final LakeSoulKeyGen keyGen;

  private String tableName;

  private LakesSoulOneTableWriter<IN> writer;

  private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
          new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);
  private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC =
          new ListStateDescriptor<>("max-part-counter", LongSerializer.INSTANCE);

  public LakeSoulTableWriter(long bucketCheckInterval,
                             LakeSoulKeyGen keyGen,
                             LakeSoulBucketsBuilder<IN, String, ? extends LakeSoulBucketsBuilder<IN, ?, ?>> bucketsBuilder,
                             String tableName,
                             List<String> partitionKeyList, Configuration conf,
                             OutputFileConfig outputFileConf) {
    this.bucketCheckInterval = bucketCheckInterval;
    this.bucketsBuilder = bucketsBuilder;
    this.partitionKeyList = partitionKeyList;
    this.flinkConf = conf;
    this.outputFileConfig = outputFileConf;
    this.keyGen = keyGen;
    this.tableName = tableName;
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  private Tuple2<List<byte[]>, List<Long>> recoverStates(StateInitializationContext context) throws Exception {
    if (context.isRestored()) {
      ListState<byte[]> bucketStates = getRuntimeContext().getListState(BUCKET_STATE_DESC);
      ListState<Long> maxParCounterStates = getRuntimeContext().getListState(MAX_PART_COUNTER_STATE_DESC);
      return Tuple2.of(StreamSupport.stream(bucketStates.get().spliterator(), false).collect(Collectors.toList()),
              StreamSupport.stream(maxParCounterStates.get().spliterator(), false).collect(Collectors.toList()));
    } else {
      return Tuple2.of(null, null);
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.writer = new LakesSoulOneTableWriter<>(bucketCheckInterval, bucketsBuilder,
            PartitionCommitPredicate.create(flinkConf, getUserCodeClassloader(), partitionKeyList),
            getRuntimeContext());

    Tuple2<List<byte[]>, List<Long>> states = recoverStates(context);
    this.writer.initializeState(context.isRestored(), states.f0, states.f1);

    ClassLoader userCodeClassLoader = getContainingTask().getUserCodeClassLoader();
    RecordComparator recordComparator = this.keyGen.getComparator().newInstance(userCodeClassLoader);
    this.keyGen.setCompareFunction(recordComparator);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    this.writer.snapshotState(context);
  }

  /**
   * Close in-progress part file when partition is committable.
   */
  protected void commitUpToCheckpoint(long checkpointId) throws Exception {
    writer.commitUpToCheckpoint(checkpointId);
    NavigableMap<Long, Set<String>> headBuckets = writer.getNewBuckets().headMap(checkpointId, true);
    Set<String> partitions = new HashSet<>(writer.getCommittableBuckets());
    writer.getCommittableBuckets().clear();
    if (partitions.isEmpty()) {
      return;
    }
    headBuckets.values().forEach(partitions::addAll);
    headBuckets.clear();
    String pathPre = outputFileConfig.getPartPrefix() + "-";
    DataFileMetaData nowFileMeta = new DataFileMetaData(checkpointId,
        getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks(),
        new ArrayList<>(partitions), pathPre, tableName);
    output.collect(
        new StreamRecord<>(nowFileMeta)
    );

  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    commitUpToCheckpoint(checkpointId);
  }

  @Override
  public void endInput() throws Exception {
    writer.endInput(output);
  }

  @Override
  public void processElement(StreamRecord<IN> element) throws Exception {
    writer.processElement(element);
  }

  @Override
  public void processWatermark(Watermark mark) {
    writer.processWatermark(mark);
  }

  @Override
  public void finish() {
    writer.finish();
  }

  @Override
  public void close() throws Exception {
    writer.close();
    super.close();
  }

  @Override
  public void setKeyContextElement(StreamRecord<IN> record) throws Exception {
    OneInputStreamOperator.super.setKeyContextElement(record);
  }
}

