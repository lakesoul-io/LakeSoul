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

package org.apache.flink.lakesoul.sink;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.DataCommitInfo;
import com.dmetasoul.lakesoul.meta.entity.DataFileOp;
import com.dmetasoul.lakesoul.meta.entity.MetaInfo;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.lakesoul.metaData.DataFileMetaData;
import org.apache.flink.lakesoul.sink.fileSystem.TaskTracker;
import org.apache.flink.lakesoul.sink.partition.PartitionTrigger;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.APPEND_COMMIT_TYPE;
import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.FILE_EXIST_COLUMN;
import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.FILE_IN_PROGRESS_PART_PREFIX;
import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.FILE_OPTION_ADD;

public class MetaDataCommit extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<DataFileMetaData, Void> {

  private static final long serialVersionUID = 1L;

  private final Path locationPath;

  private transient PartitionTrigger trigger;

  private transient TaskTracker taskTracker;

  private transient long currentWatermark;

  private final String fileExistFiles;

  private DBManager dbManager;

  private final Configuration flinkConf;

  public MetaDataCommit(Path locationPath, Configuration conf) {
    this.locationPath = locationPath;
    this.fileExistFiles = conf.getString(FILE_EXIST_COLUMN);
    this.flinkConf = conf;
  }

  @Override
  public void processElement(StreamRecord<DataFileMetaData> element) throws Exception {
    DataFileMetaData metadata = element.getValue();
    for (String partition : metadata.getPartitions()) {
      trigger.addPartition(partition);
    }
    if (taskTracker == null) {
      taskTracker = new TaskTracker(metadata.getNumberOfTasks());
    }
    boolean readyCommit = taskTracker.addCompleteTask(metadata.getCheckpointId(), metadata.getTaskId());
    if (readyCommit) {
      commitPartitions(metadata);
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);
    this.currentWatermark = mark.getTimestamp();
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.currentWatermark = Long.MIN_VALUE;
    this.dbManager = new DBManager();
    this.trigger = PartitionTrigger.create(context.isRestored(), context.getOperatorStateStore());
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    trigger.snapshotState(context.getCheckpointId(), currentWatermark);
  }

  private void commitPartitions(DataFileMetaData element) throws Exception {
    TableInfo tableInfo = dbManager.getTableInfoByName(element.getTableName());
    MetaInfo metaInfo = new MetaInfo();
    metaInfo.setTableInfo(tableInfo);
    ArrayList<PartitionInfo> partitionLists = new ArrayList<>();
    ArrayList<DataCommitInfo> commitInfoList = new ArrayList<>();
    long checkpointId = element.getCheckpointId();
    List<String> partitionList;
    if (checkpointId == Long.MAX_VALUE){
      partitionList =trigger.endInput();
    }else {
      partitionList =trigger.committablePartitions(checkpointId);
    }
    if (partitionList.isEmpty()) {
      return;
    }

    String fileNamePrefix = element.getTaskDataPath();

    for (String partition : partitionList) {
      Path resultPath = new Path(locationPath, partition);
      org.apache.flink.core.fs.FileStatus[] files = resultPath.getFileSystem().listStatus(resultPath);
      DataCommitInfo dataCommitInfo =
          partitionMetaSet(tableInfo.getTableId(), partition, partitionLists);
      for (FileStatus fileStatus : files) {
        if (!fileStatus.isDir()) {
          String onePath = fileStatus.getPath().toString();
          if (onePath.contains(fileNamePrefix) && !onePath.contains(FILE_IN_PROGRESS_PART_PREFIX)) {
            DataFileOp dataFileOp = new DataFileOp();
            dataFileOp.setPath(onePath);
            dataFileOp.setSize(fileStatus.getLen());
            dataFileOp.setFileOp(FILE_OPTION_ADD);
            dataFileOp.setFileExistCols(this.fileExistFiles);
            dataCommitInfo.setOrAddFileOps(dataFileOp);
          }
        }
      }
      commitInfoList.add(dataCommitInfo);
    }
    metaInfo.setListPartition(partitionLists);
    boolean appendCommit = dbManager.commitData(metaInfo, true, APPEND_COMMIT_TYPE);

    if (appendCommit) {
      dbManager.batchCommitDataCommitInfo(commitInfoList);
    }
  }

  private DataCommitInfo partitionMetaSet(String tableId, String partition,
                                          ArrayList<PartitionInfo> partitionLists) {
    PartitionInfo partitionInfo = new PartitionInfo();
    partitionInfo.setCommitOp(APPEND_COMMIT_TYPE);
    partitionInfo.setTableId(tableId);
    UUID uuid = UUID.randomUUID();
    partitionInfo.setOrAddSnapshot(uuid);
    partitionInfo.setPartitionDesc(partition);
    partitionLists.add(partitionInfo);
    DataCommitInfo dataCommitInfo = new DataCommitInfo();
    dataCommitInfo.setCommitId(uuid);
    dataCommitInfo.setPartitionDesc(partition);
    dataCommitInfo.setCommitOp(APPEND_COMMIT_TYPE);
    dataCommitInfo.setTableId(tableId);
    dataCommitInfo.setTimestamp(System.currentTimeMillis());
    return dataCommitInfo;
  }

}