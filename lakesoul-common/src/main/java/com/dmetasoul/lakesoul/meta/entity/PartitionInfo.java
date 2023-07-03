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
 * Version information for specific table range partitions
 */
public class PartitionInfo {
  /**
   * TableId of PartitionInfo
   */
  private String tableId;

  /**
   * Range partition description, which defines a specific range partition of the table, in the formatted of comma-separated range_colum=range_value
   * Especially, a table without range partitions use LAKESOUL_NON_PARTITION_TABLE_PART_DESC as partitionDesc
   */
  private String partitionDesc;

  /**
   * A version is defined as a number monotonically increasing by 1
   */
  private int version;

  /**
   * Set of {AppendCommit, CompactionCommit, UpdateCommit, MergeCommit}, which define the specific operation of the version information
   * AppendCommit: A commit type indicates that this DataCommit is to append files to a specific table range partition without hash partition
   * CompactionCommit: A commit type indicates that this DataCommit is to compact files in a specific table range partition
   * UpdateCommit: A commit type indicates that this DataCommit is to update data(add new files and delete old invalid files) in a specific table range partition
   * MergeCommit: A commit type indicates that this DataCommit is to append files to a specific table range partition with hash partition
   * Especially, the first commit to a specific table range partition will be set to 'AppendCommit', whether it has hash partition or not
   */
  private String commitOp;

  /**
   * Timestamp of the PartitionInfo successfully committed
   */
  private long timestamp;

  /**
   * Collection of commitId of DataCommitInfo included with PartitionInfo
   */
  private List<UUID> snapshot;

  /**
   * TODO: Expression used to calculate or filter data, will be launched in the future. Now it's just a meaningless empty string.
   */
  private String expression;

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

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public String getCommitOp() {
    return commitOp;
  }

  public void setCommitOp(String commitOp) {
    this.commitOp = commitOp;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public List<UUID> getSnapshot() {
    return snapshot;
  }

  public void setSnapshot(List<UUID> snapshot) {
    this.snapshot = snapshot;
  }

  public void setOrAddSnapshot(UUID uuid) {
    if (this.snapshot == null) {
      ArrayList<UUID> uuids = new ArrayList<>();
      uuids.add(uuid);
      this.snapshot = uuids;
    } else {
      this.snapshot.add(uuid);
    }
  }

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }
}