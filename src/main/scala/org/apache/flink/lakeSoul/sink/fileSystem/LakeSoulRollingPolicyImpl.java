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

package org.apache.flink.lakeSoul.sink.fileSystem;

import org.apache.flink.lakeSoul.tools.LakeSoulKeyGen;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.table.data.RowData;

import static org.apache.flink.lakeSoul.tools.LakeSoulSinkOptions.DEFAULT_BUCKET_ROLLING_SIZE;
import static org.apache.flink.lakeSoul.tools.LakeSoulSinkOptions.DEFAULT_BUCKET_ROLLING_TIME;

public class LakeSoulRollingPolicyImpl implements LakeSoulRollingPolicy<RowData, String> {

  private boolean rollOnCheckpoint;

  private LakeSoulKeyGen keyGen;

  private long rollingSize;

  private long rollingTime;

  public LakeSoulKeyGen getKeyGen() {
    return keyGen;
  }

  public void setKeyGen(LakeSoulKeyGen keyGen) {
    this.keyGen = keyGen;
  }

  public LakeSoulRollingPolicyImpl(long rollingSize, long rollingTime) {
    this.rollOnCheckpoint = true;
    this.rollingSize = rollingSize;
    this.rollingTime = rollingTime;
  }

  public LakeSoulRollingPolicyImpl(long rollingSize, long rollingTime, LakeSoulKeyGen keyGen) {
    this.keyGen = keyGen;
    this.rollOnCheckpoint = true;
    this.rollingSize = rollingSize;
    this.rollingTime = rollingTime;
  }

  public LakeSoulRollingPolicyImpl(boolean rollOnCheckpoint) {
    this.rollingSize = DEFAULT_BUCKET_ROLLING_SIZE;
    this.rollingTime = DEFAULT_BUCKET_ROLLING_TIME;
    this.rollOnCheckpoint = rollOnCheckpoint;
  }

  @Override
  public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
    return this.rollOnCheckpoint;
  }

  @Override
  public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element) {
    return false;
  }

  @Override
  public boolean shouldRollOnProcessingTime(
      PartFileInfo<String> partFileState, long currentTime) {
    return currentTime - partFileState.getLastUpdateTime() > rollingTime;
  }

  public boolean shouldRollOnMaxSize(long size) {
    return size > rollingSize;
  }

  @Override
  public boolean shouldRoll(PartFileInfo<String> partFileState, long currentTime) {
    return false;
  }

  public long getRollingSize() {
    return rollingSize;
  }

  public void setRollingSize(long rollingSize) {
    this.rollingSize = rollingSize;
  }

  public long getRollingTime() {
    return rollingTime;
  }

  public void setRollingTime(long rollingTime) {
    this.rollingTime = rollingTime;
  }

  public boolean isRollOnCheckpoint() {
    return rollOnCheckpoint;
  }

  public void setRollOnCheckpoint(boolean rollOnCheckpoint) {
    this.rollOnCheckpoint = rollOnCheckpoint;
  }
}
