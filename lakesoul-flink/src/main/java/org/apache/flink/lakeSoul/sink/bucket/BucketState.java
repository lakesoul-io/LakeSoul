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

package org.apache.flink.lakeSoul.sink.bucket;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BucketState<BucketID> {
  private final BucketID bucketId;
  private final Path bucketPath;
  private final long inProgressFileCreationTime;
  @Nullable
  private final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable;
  private final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverablesPerCheckpoint;

  BucketState(BucketID bucketId, Path bucketPath, long inProgressFileCreationTime, @Nullable InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable,
              Map<Long, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverablesPerCheckpoint) {
    this.bucketId = Preconditions.checkNotNull(bucketId);
    this.bucketPath = Preconditions.checkNotNull(bucketPath);
    this.inProgressFileCreationTime = inProgressFileCreationTime;
    this.inProgressFileRecoverable = inProgressFileRecoverable;
    this.pendingFileRecoverablesPerCheckpoint = Preconditions.checkNotNull(pendingFileRecoverablesPerCheckpoint);
  }

  BucketID getBucketId() {
    return this.bucketId;
  }

  Path getBucketPath() {
    return this.bucketPath;
  }

  long getInProgressFileCreationTime() {
    return this.inProgressFileCreationTime;
  }

  boolean hasInProgressFileRecoverable() {
    return this.inProgressFileRecoverable != null;
  }

  @Nullable
  InProgressFileWriter.InProgressFileRecoverable getInProgressFileRecoverable() {
    return this.inProgressFileRecoverable;
  }

  Map<Long, List<InProgressFileWriter.PendingFileRecoverable>> getPendingFileRecoverablesPerCheckpoint() {
    return this.pendingFileRecoverablesPerCheckpoint;
  }

  @Override
  public String toString() {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("BucketState for bucketId=").append(this.bucketId).append(" and bucketPath=").append(this.bucketPath);
    if (this.hasInProgressFileRecoverable()) {
      strBuilder.append(", has open part file created @ ").append(this.inProgressFileCreationTime);
    }

    if (!this.pendingFileRecoverablesPerCheckpoint.isEmpty()) {
      strBuilder.append(", has pending files for checkpoints: {");
      Iterator<Long> var2 = this.pendingFileRecoverablesPerCheckpoint.keySet().iterator();

      while (var2.hasNext()) {
        long checkpointId = var2.next();
        strBuilder.append(checkpointId).append(' ');
      }

      strBuilder.append('}');
    }

    return strBuilder.toString();
  }
}
