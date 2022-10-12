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

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Wrapper class for both type of committables in {@link LakeSoulMultiTablesSink}. One committable might be either
 * one or more pending files to commit, or one in-progress file to clean up.
 */
public class LakeSoulMultiTableSinkCommittable implements Serializable, Comparable<LakeSoulMultiTableSinkCommittable> {

    static final long serialVersionUID = 42L;

    private final long creationTime;

    private final String bucketId;

    private final TableSchemaIdentity identity;

    @Nullable private final List<InProgressFileWriter.PendingFileRecoverable> pendingFiles;

    @Nullable private final List<String> filePaths;

    @Nullable private final String commitId;

    @Nullable private final InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup;

    /**
     * Constructor for {@link org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucket} to prepare commit
     * with pending files
     */
    public LakeSoulMultiTableSinkCommittable(
            String bucketId,
            List<InProgressFileWriter.PendingFileRecoverable> pendingFiles,
            List<String> filePaths,
            long creationTime,
            TableSchemaIdentity identity) {
        this.bucketId = bucketId;
        this.pendingFiles = checkNotNull(pendingFiles);
        this.filePaths = checkNotNull(filePaths);
        this.creationTime = creationTime;
        this.identity = identity;
        this.commitId = UUID.randomUUID().toString();
        this.inProgressFileToCleanup = null;
    }

    /**
     * Constructor for {@link org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucket} to prepare commit
     * with in progress file to clean up
     */
    public LakeSoulMultiTableSinkCommittable(
            String bucketId,
            TableSchemaIdentity identity,
            InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup) {
        this.bucketId = bucketId;
        this.identity = identity;
        this.creationTime = Long.MAX_VALUE;
        this.filePaths = null;
        this.pendingFiles = null;
        this.commitId = null;
        this.inProgressFileToCleanup = checkNotNull(inProgressFileToCleanup);
    }

    /**
     * Constructor for {@link org.apache.flink.lakesoul.sink.state.LakeSoulSinkCommittableSerializer} to
     * restore commitable states
     */
    LakeSoulMultiTableSinkCommittable(
            String bucketId,
            TableSchemaIdentity identity,
            @Nullable List<InProgressFileWriter.PendingFileRecoverable> pendingFiles,
            @Nullable List<String> filePaths,
            long time,
            @Nullable String commitId,
            @Nullable InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup) {
        this.bucketId = bucketId;
        this.identity = identity;
        this.pendingFiles = pendingFiles;
        this.filePaths = filePaths;
        this.creationTime = time;
        this.commitId = commitId;
        this.inProgressFileToCleanup = inProgressFileToCleanup;
    }

    public boolean hasPendingFile() {
        return pendingFiles != null;
    }

    @Nullable
    public List<InProgressFileWriter.PendingFileRecoverable> getPendingFiles() {
        return pendingFiles;
    }

    public boolean hasInProgressFileToCleanup() {
        return inProgressFileToCleanup != null;
    }

    @Nullable
    public InProgressFileWriter.InProgressFileRecoverable getInProgressFileToCleanup() {
        return inProgressFileToCleanup;
    }

    @Override
    public int compareTo(LakeSoulMultiTableSinkCommittable o) {
        return Long.compare(creationTime, o.creationTime);
    }

    public long getCreationTime() {
        return creationTime;
    }

    public TableSchemaIdentity getIdentity() {
        return identity;
    }

    public String getBucketId() {
        return bucketId;
    }

    @Nullable
    public List<String> getFilePaths() {
        return filePaths;
    }

    @Override
    public String toString() {
        return "LakeSoulMultiTableSinkCommittable{" +
               "creationTime=" + creationTime +
               ", bucketId='" + bucketId + '\'' +
               ", identity=" + identity +
               ", filePaths=" + filePaths +
               ", commitId='" + commitId + '\'' +
               '}';
    }

    @Nullable
    public String getCommitId() {
        return commitId;
    }
}
