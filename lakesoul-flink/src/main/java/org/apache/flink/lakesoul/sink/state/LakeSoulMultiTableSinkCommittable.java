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
import java.util.*;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.DYNAMIC_BUCKET;

/**
 * Wrapper class for both type of committables in {@link LakeSoulMultiTablesSink}. One committable might be either
 * one or more pending files to commit, or one in-progress file to clean up.
 */
public class LakeSoulMultiTableSinkCommittable implements Serializable, Comparable<LakeSoulMultiTableSinkCommittable> {

    static final long serialVersionUID = 42L;

    private final long creationTime;

    private final String bucketId;

    private final boolean dynamicBucketing;

    private final TableSchemaIdentity identity;
    private String sourcePartitionInfo;

    private final Map<String, List<InProgressFileWriter.PendingFileRecoverable>> pendingFilesMap;

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
        this.dynamicBucketing = false;
        this.bucketId = bucketId;
        this.identity = identity;
        this.pendingFilesMap = new HashMap<>();
        this.pendingFilesMap.put(bucketId, pendingFiles);
        this.creationTime = time;
        this.commitId = commitId;
        this.tsMs = tsMs;
        this.dmlType = dmlType;
        this.sourcePartitionInfo = sourcePartitionInfo;
    }

    /**
     * Constructor for {@link org.apache.flink.lakesoul.sink.state.LakeSoulSinkCommittableSerializer} to
     * restore commitable states
     */
    public LakeSoulMultiTableSinkCommittable(
            TableSchemaIdentity identity,
            Map<String, List<InProgressFileWriter.PendingFileRecoverable>> pendingFilesMap,
            long time,
            @Nullable String commitId,
            long tsMs,
            String dmlType,
            String sourcePartitionInfo
    ) {
        Preconditions.checkNotNull(pendingFilesMap);
        this.dynamicBucketing = pendingFilesMap.keySet().size() != 1;
        this.bucketId = this.dynamicBucketing ? DYNAMIC_BUCKET : pendingFilesMap.keySet().stream().findFirst().get();
        this.identity = identity;
        this.pendingFilesMap = pendingFilesMap;
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
        if (dynamicBucketing) {
            return !pendingFilesMap.isEmpty();
        } else {
            return hasPendingFile(bucketId);
        }
    }

    public boolean hasPendingFile(String bucketId) {
        return pendingFilesMap.containsKey(bucketId);
    }

    @Nullable
    public List<InProgressFileWriter.PendingFileRecoverable> getPendingFiles() {
        if (dynamicBucketing) {
            List<InProgressFileWriter.PendingFileRecoverable> summary = new ArrayList<>();
            for (List<InProgressFileWriter.PendingFileRecoverable> list : pendingFilesMap.values()) {
                summary.addAll(list);
            }
            return summary;
        } else {
            return getPendingFiles(bucketId);
        }
    }

    @Nullable
    public List<InProgressFileWriter.PendingFileRecoverable> getPendingFiles(String bucketId) {
        return pendingFilesMap.get(bucketId);
    }

    public Map<String, List<InProgressFileWriter.PendingFileRecoverable>> getPendingFilesMap() {
        return pendingFilesMap;
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
                ", dynamicBucketing=" + dynamicBucketing +
                ", identity=" + identity +
                ", pendingFilesMap=" + pendingFilesMap +
                ", commitId='" + commitId + '\'' +
                ", tsMs=" + tsMs +
                ", dmlType='" + dmlType + '\'' +
                '}';
    }

    @Nullable
    public String getCommitId() {
        return commitId;
    }

    public void merge(LakeSoulMultiTableSinkCommittable committable) {
        Preconditions.checkState(identity.equals(committable.getIdentity()));
//        Preconditions.checkState(creationTime == committable.getCreationTime());

        for (Map.Entry<String, List<InProgressFileWriter.PendingFileRecoverable>> entry : committable.getPendingFilesMap().entrySet()) {
            String bucketId = entry.getKey();
            if (hasPendingFile(bucketId)) {
                if (!entry.getValue().isEmpty()) pendingFilesMap.get(bucketId).addAll(entry.getValue());
            } else {
                if (!entry.getValue().isEmpty()) pendingFilesMap.put(bucketId, entry.getValue());
            }
        }
        mergeSourcePartitionInfo(committable);
    }

    private void mergeSourcePartitionInfo(LakeSoulMultiTableSinkCommittable committable) {
        if (sourcePartitionInfo == null) {
            sourcePartitionInfo = committable.getSourcePartitionInfo();
        } else {
            if (committable.getSourcePartitionInfo() == null || committable.getSourcePartitionInfo().isEmpty()) return;
            try {
                JniWrapper jniWrapper = JniWrapper
                        .parseFrom(Base64.getDecoder().decode(committable.getSourcePartitionInfo()))
                        .toBuilder()
                        .addAllPartitionInfo(
                                JniWrapper
                                        .parseFrom(Base64.getDecoder().decode(committable.getSourcePartitionInfo()))
                                        .getPartitionInfoList()
                        )
                        .build();
                sourcePartitionInfo = Base64.getEncoder().encodeToString(jniWrapper.toByteArray());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
        Preconditions.checkState(bucketId.equals(committable.getBucketId()));

    }

    public String getDmlType() {
        return dmlType;
    }

    public String getSourcePartitionInfo() {
        return sourcePartitionInfo;
    }
}
