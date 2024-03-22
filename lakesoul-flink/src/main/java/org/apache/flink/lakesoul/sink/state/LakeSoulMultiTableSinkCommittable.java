// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.state;

import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.google.protobuf.InvalidProtocolBufferException;
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
    private String sourcePartitionInfo;

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
            TableSchemaIdentity identity,
            long tsMs,
            String dmlType,
            String sourcePartitionInfo
    ) {
        this(bucketId,
                identity,
                pendingFiles,
                creationTime,
                UUID.randomUUID().toString(),
                tsMs,
                dmlType,
                sourcePartitionInfo
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
            long tsMs,
            String dmlType,
            String sourcePartitionInfo
    ) {
        this.bucketId = bucketId;
        this.identity = identity;
        this.pendingFiles = pendingFiles;
        this.creationTime = time;
        this.commitId = commitId;
        this.tsMs = tsMs;
        this.dmlType = dmlType;
        this.sourcePartitionInfo = sourcePartitionInfo;
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
                ", pendingFiles='" + pendingFiles + '\'' +
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
        mergeSourcePartitionInfo(committable);
    }

    private void mergeSourcePartitionInfo(LakeSoulMultiTableSinkCommittable committable) {
        if (sourcePartitionInfo == null) {
            sourcePartitionInfo = committable.getSourcePartitionInfo();
        } else {
            try {
                JniWrapper jniWrapper = JniWrapper
                        .parseFrom(sourcePartitionInfo.getBytes())
                        .toBuilder()
                        .addAllPartitionInfo(
                                JniWrapper
                                        .parseFrom(committable.getSourcePartitionInfo().getBytes())
                                        .getPartitionInfoList()
                        )
                        .build();
                sourcePartitionInfo = new String(jniWrapper.toByteArray());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String getDmlType() {
        return dmlType;
    }

    public String getSourcePartitionInfo() {
        return sourcePartitionInfo;
    }
}
