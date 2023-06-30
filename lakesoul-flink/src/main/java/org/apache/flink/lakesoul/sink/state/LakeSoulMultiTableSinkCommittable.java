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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Wrapper class for both type of committables in {@link LakeSoulMultiTablesSink}. One committable might be either
 * one or more pending files to commit, or one in-progress file to clean up.
 */
public class LakeSoulMultiTableSinkCommittable implements Serializable, Comparable<LakeSoulMultiTableSinkCommittable> {

    static final long serialVersionUID = 42L;

    private final long creationTime;

    private final String bucketId;

    private final TableSchemaIdentity identity;

    @Nullable
    private List<InProgressFileWriter.PendingFileRecoverable> pendingFiles;

    @Nullable
    private final String commitId;

    /**
     * Constructor for {@link org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucket} to prepare commit
     * with pending files
     */
    public LakeSoulMultiTableSinkCommittable(
            String bucketId,
            List<InProgressFileWriter.PendingFileRecoverable> pendingFiles,
            long creationTime,
            TableSchemaIdentity identity) {
        this(bucketId, identity, pendingFiles, creationTime,
//                UUID.randomUUID().toString()
                new UUID(Objects.hash(identity.tableId, bucketId), creationTime).toString()
        );
    }

    /**
     * Constructor for {@link org.apache.flink.lakesoul.sink.state.LakeSoulSinkCommittableSerializer} to
     * restore commitable states
     */
    LakeSoulMultiTableSinkCommittable(
            String bucketId,
            TableSchemaIdentity identity,
            @Nullable List<InProgressFileWriter.PendingFileRecoverable> pendingFiles,
            long time,
            @Nullable String commitId) {
        this.bucketId = bucketId;
        this.identity = identity;
        this.pendingFiles = pendingFiles;
        this.creationTime = time;
        this.commitId = commitId;
    }

    public boolean hasPendingFile() {
        return pendingFiles != null;
    }

    @Nullable
    public List<InProgressFileWriter.PendingFileRecoverable> getPendingFiles() {
        return pendingFiles;
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

    @Override
    public String toString() {
        return "LakeSoulMultiTableSinkCommittable{" +
                "creationTime=" + creationTime +
                ", bucketId='" + bucketId + '\'' +
                ", identity=" + identity +
                ", commitId='" + commitId + '\'' +
                '}';
    }

    @Nullable
    public String getCommitId() {
        return commitId;
    }

    public void merge(LakeSoulMultiTableSinkCommittable committable) {
        Preconditions.checkState(identity.equals(committable.getIdentity()));
        Preconditions.checkState(bucketId.equals(committable.getBucketId()));
        Preconditions.checkState(creationTime == committable.getCreationTime());
        Preconditions.checkState(commitId.equals(committable.getCommitId()));
        if (hasPendingFile()) {
            if (committable.hasPendingFile()) pendingFiles.addAll(committable.getPendingFiles());
        } else {
            if (committable.hasPendingFile()) pendingFiles = committable.getPendingFiles();
        }
    }
}
