// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucket;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.DYNAMIC_BUCKET;

/**
 * States for {@link LakeSoulWriterBucket}.
 */
public class LakeSoulWriterBucketState {

    private final TableSchemaIdentity identity;

    private final String bucketId;

    /**
     * The directory where all the part files of the bucket are stored.
     */
    private final Path bucketPath;

    private final Map<String, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverableMap;

    private final int restartTimes;

    public LakeSoulWriterBucketState(
            TableSchemaIdentity identity,
            String bucketId,
            Path bucketPath,
            List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverableList
    ) {
        this.identity = identity;
        this.bucketId = bucketId;
        this.bucketPath = bucketPath;
        this.pendingFileRecoverableMap = new HashMap<>();
        this.pendingFileRecoverableMap.put(bucketId, pendingFileRecoverableList);
        restartTimes = 0;
    }

    public LakeSoulWriterBucketState(
            TableSchemaIdentity identity,
            Path bucketPath,
            HashMap<String, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverableMap
    ) {
        this(identity, bucketPath, pendingFileRecoverableMap, 0);
    }

    public LakeSoulWriterBucketState(
            TableSchemaIdentity identity,
            Path bucketPath,
            HashMap<String, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverableMap,
            int restartTimes
    ) {
        this.identity = identity;
        Optional<Map.Entry<String, List<InProgressFileWriter.PendingFileRecoverable>>> first = pendingFileRecoverableMap.entrySet().stream().findFirst();
        if (first.isPresent()) {
            this.bucketId = first.get().getKey();
        } else {
            this.bucketId = DYNAMIC_BUCKET;
        }
        this.bucketPath = bucketPath;

        this.pendingFileRecoverableMap = pendingFileRecoverableMap;
        this.restartTimes = restartTimes;
    }

    public String getBucketId() {
        return bucketId;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    public int getRestartTimes() {
        return restartTimes;
    }

    @Override
    public String toString() {

        return "BucketState for bucketId=" +
                bucketId +
                " and bucketPath=" +
                bucketPath +
                " and identity=" +
                identity +
                " and pendingFilesMap=" +
                pendingFileRecoverableMap.entrySet().stream().map(Object::toString).collect(Collectors.joining("; "))
                ;
    }

    public TableSchemaIdentity getIdentity() {
        return identity;
    }

    public List<InProgressFileWriter.PendingFileRecoverable> getPendingFileRecoverableList() {
        return pendingFileRecoverableMap.values().stream().findFirst().get();
    }

    public Map<String, List<InProgressFileWriter.PendingFileRecoverable>> getPendingFileRecoverableMap() {
        return pendingFileRecoverableMap;
    }
}
