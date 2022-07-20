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

public class PartitionInfo {
  private String tableId;

  private String partitionDesc;

  private int version;

  private String commitOp;

  private List<UUID> snapshot;

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