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

public class DataCommitInfo {
  private String tableId;

  private String partitionDesc;

  private UUID commitId;

  private List<DataFileOp> fileOps;

  private String commitOp;

  private long timestamp;

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
}