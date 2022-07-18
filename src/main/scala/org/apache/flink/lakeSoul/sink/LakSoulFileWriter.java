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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakeSoul.metaData.DataFileMetaData;
import org.apache.flink.lakeSoul.sink.bucket.LakeSoulBucketsBuilder;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.filesystem.stream.PartitionCommitPredicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.flink.lakeSoul.tools.LakeSoulSinkOptions.TABLE_NAME;

public class LakSoulFileWriter<IN> extends LakesSoulAbstractStreamingWriter<IN, DataFileMetaData> {

  private static final long serialVersionUID = 2L;
  private final List<String> partitionKeyList;
  private final Configuration flinkConf;
  private final OutputFileConfig outputFileConfig;
  private MapState<Long, DataFileMetaData> completeCkpState;
  private transient Set<String> currentNewBuckets;
  private transient TreeMap<Long, Set<String>> newBuckets;
  private transient Set<String> committableBuckets;
  private transient Map<String, Long> inProgressBuckets;
  private transient PartitionCommitPredicate partitionCommitPredicate;

  public LakSoulFileWriter(long bucketCheckInterval, LakeSoulBucketsBuilder<IN, String, ? extends LakeSoulBucketsBuilder<IN, ?, ?>> bucketsBuilder, List<String> partitionKeyList, Configuration conf,
                           OutputFileConfig outputFileConf) {
    super(bucketCheckInterval, bucketsBuilder);
    this.partitionKeyList = partitionKeyList;
    this.flinkConf = conf;
    this.outputFileConfig = outputFileConf;
  }

//  @Override
//  public void open() throws Exception {
//    super.open();
//    MapStateDescriptor<Long, DataFileMetaData> mapDescriptor =
//        new MapStateDescriptor<>("completeCkpStateDescriptor", Long.class, DataFileMetaData.class);
//    this.completeCkpState= getRuntimeContext().getMapState(mapDescriptor);
//  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.partitionCommitPredicate =
        PartitionCommitPredicate.create(flinkConf, getUserCodeClassloader(), partitionKeyList);
    this.currentNewBuckets = new HashSet<>();
    this.newBuckets = new TreeMap<>();
    this.committableBuckets = new HashSet<>();
    this.inProgressBuckets = new HashMap<>();
  }

  @Override
  protected void partitionCreated(String partition) {
    this.currentNewBuckets.add(partition);
    this.inProgressBuckets.putIfAbsent(partition, getProcessingTimeService().getCurrentProcessingTime());
  }

  @Override
  protected void partitionInactive(String partition) {
    this.committableBuckets.add(partition);
    this.inProgressBuckets.remove(partition);
  }

  @Override
  protected void onPartFileOpened(String s, Path newPath) {
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    closePartFileForPartitions();
    super.snapshotState(context);
    this.newBuckets.put(context.getCheckpointId(), new HashSet<>(currentNewBuckets));
    this.currentNewBuckets.clear();
  }

  /**
   * Close in-progress part file when partition is committable.
   */
  private void closePartFileForPartitions() {
    if (partitionCommitPredicate != null) {
      inProgressBuckets.forEach((BucketID, creationTime) -> {
        PartitionCommitPredicate.PredicateContext predicateContexts = PartitionCommitPredicate.createPredicateContext(
            BucketID, creationTime, processingTimeService.getCurrentProcessingTime(), currentWatermark);
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

  @Override
  protected void commitUpToCheckpoint(long checkpointId) throws Exception {
    super.commitUpToCheckpoint(checkpointId);
    NavigableMap<Long, Set<String>> headBuckets = this.newBuckets.headMap(checkpointId, true);
    Set<String> partitions = new HashSet<>(committableBuckets);
    committableBuckets.clear();
    headBuckets.values().forEach(partitions::addAll);
    headBuckets.clear();
    String pathPre = outputFileConfig.getPartPrefix() + "-";
    String tableName = flinkConf.getString(TABLE_NAME);
    DataFileMetaData nowFileMeta = new DataFileMetaData(checkpointId,
        getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks(),
        new ArrayList<>(partitions), pathPre, tableName);
    output.collect(
        new StreamRecord<>(nowFileMeta)
    );
//
//    if (!completeCkpState.isEmpty()){
//      completeCkpState.iterator().forEachRemaining(v->{
//        output.collect(new StreamRecord<>(v.getValue()));
//      });
//      completeCkpState.clear();
//    }
//
//    completeCkpState.put(checkpointId,nowFileMeta);

  }
}

