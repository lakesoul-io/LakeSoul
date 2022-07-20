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

package org.apache.flink.lakeSoul.metaData;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class DataFileMetaData implements Serializable {

  private static final long serialVersionUID = 1L;

  private long checkpointId;
  private int taskId;
  private int numberOfTasks;
  private List<String> partitions;
  private String path;
  private String tableName;

  public DataFileMetaData() {
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public DataFileMetaData(
      long checkpointId, int taskId, int numberOfTasks, List<String> partitions, String path, String tableName) {
    this.checkpointId = checkpointId;
    this.taskId = taskId;
    this.numberOfTasks = numberOfTasks;
    this.partitions = partitions;
    this.path = path;
    this.tableName = tableName;
  }

  public long getCheckpointId() {
    return checkpointId;
  }

  public void setCheckpointId(long checkpointId) {
    this.checkpointId = checkpointId;
  }

  public int getTaskId() {
    return taskId;
  }

  public void setTaskDataPath(String path) {
    this.path = path;
  }

  public String getTaskDataPath() {
    return this.path;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  public int getNumberOfTasks() {
    return numberOfTasks;
  }

  public void setNumberOfTasks(int numberOfTasks) {
    this.numberOfTasks = numberOfTasks;
  }

  public List<String> getPartitions() {
    return partitions;
  }

  public void setPartitions(List<String> partitions) {
    this.partitions = partitions;
  }

  @Override
  public String toString() {
    return "DataFileMetaData{" +
        "checkpointId=" + checkpointId +
        ", taskId=" + taskId +
        ", numberOfTasks=" + numberOfTasks +
        ", partitions=" + partitions +
        ", path='" + path + '\'' +
        ", tableName='" + tableName + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataFileMetaData that = (DataFileMetaData) o;
    return checkpointId == that.checkpointId && taskId == that.taskId && numberOfTasks == that.numberOfTasks && Objects.equals(partitions, that.partitions) &&
        Objects.equals(path, that.path) && Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(checkpointId, taskId, numberOfTasks, partitions, path, tableName);
  }
}

