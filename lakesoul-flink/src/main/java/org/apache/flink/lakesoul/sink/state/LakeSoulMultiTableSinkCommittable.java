// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
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

    private final long tsMs;

    private final String dmlType;


    /**
     * Constructor for {@link org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucket} to prepare commit
     * with pending files
     */
    public LakeSoulMultiTableSinkCommittable(
            String bucketId,
            List<InProgressFileWriter.PendingFileRecoverable> pendingFiles,
            long creationTime,
            TableSchemaIdentity identity, long tsMs, String dmlType) {
        this(bucketId, identity, pendingFiles, creationTime,
                UUID.randomUUID().toString(), tsMs,dmlType
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
            @Nullable String commitId,
            long tsMs, String dmlType) {
        this.bucketId = bucketId;
        this.identity = identity;
        this.pendingFiles = pendingFiles;
        this.creationTime = time;
        this.commitId = commitId;
        this.tsMs = tsMs;
        this.dmlType = dmlType;
    }

    public long getTsMs() {
        return tsMs;
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
        if (hasPendingFile()) {
            if (committable.hasPendingFile()) pendingFiles.addAll(committable.getPendingFiles());
        } else {
            if (committable.hasPendingFile()) pendingFiles = committable.getPendingFiles();
        }
    }

    public String getDmlType() {
        return dmlType;
    }
}
