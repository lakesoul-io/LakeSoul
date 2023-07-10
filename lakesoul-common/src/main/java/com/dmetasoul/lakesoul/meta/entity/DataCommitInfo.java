/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.meta.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Data Files Commit information for specific table range partitions
 */
public class DataCommitInfo {

  /**
   * TableId of DataCommit
   */
  private String tableId;

  /**
   * Range partition description, which defines a specific range partition of the table, in the formatted of comma-separated range_colum=range_value
   * Especially, a table without range partitions use LAKESOUL_NON_PARTITION_TABLE_PART_DESC as partitionDesc
   */
  private String partitionDesc;

  /**
   * Global unique identifier of DataCommit
   */
  private UUID commitId;

  /**
   * Collection of DataFileOps included with DataCommit
   */
  private List<DataFileOp> fileOps;

  /**
   * Set of {AppendCommit, CompactionCommit, UpdateCommit, MergeCommit}, which define the specific operation of this DataCommit
   * AppendCommit: A commit type indicates that this DataCommit is to append files to a specific table range partition without hash partition
   * CompactionCommit: A commit type indicates that this DataCommit is to compact files in a specific table range partition
   * UpdateCommit: A commit type indicates that this DataCommit is to update data(add new files and delete old invalid files) in a specific table range partition
   * MergeCommit: A commit type indicates that this DataCommit is to append files to a specific table range partition with hash partition
   * Especially, the first commit to a specific table range partition will be set to 'AppendCommit', whether it has hash partition or not
   */
  private String commitOp;

  /**
   * Creation timestamp of the files of the DataCommit
   */
  private long timestamp;

  /**
   * A mark define if this DataCommit has already committed as PartitionInfo of table
   */
  private boolean committed;

  public String getTableId() {
    return tableId;
  }

  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  public String getPartitionDesc() {
    return partitionDesc;
  }

  public void setPartitionDesc(String partitionDesc) {
    this.partitionDesc = partitionDesc;
  }

  public UUID getCommitId() {
    return commitId;
  }

  public void setCommitId(UUID commitId) {
    this.commitId = commitId;
  }

  public List<DataFileOp> getFileOps() {
    return fileOps;
  }

  public void setFileOps(List<DataFileOp> fileOps) {
    this.fileOps = fileOps;
  }

  public void setOrAddFileOps(DataFileOp fileOp) {
    if (this.fileOps != null) {
      fileOps.add(fileOp);
    } else {
      this.fileOps = new ArrayList<>();
      fileOps.add(fileOp);
    }
  }

  public String getCommitOp() {
    return commitOp;
  }

  public void setCommitOp(String commitOp) {
    this.commitOp = commitOp == null ? null : commitOp.trim();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public boolean isCommitted() {
    return committed;
  }

  public void setCommitted(boolean committed) {
    this.committed = committed;
  }

  @Override
  public String toString() {
    return "DataCommitInfo{" +
            "tableId='" + tableId + '\'' +
            ", partitionDesc='" + partitionDesc + '\'' +
            ", commitId=" + commitId +
            ", fileOps=" + fileOps +
            ", commitOp='" + commitOp + '\'' +
            ", timestamp=" + timestamp +
            ", committed=" + committed +
            '}';
  }
}