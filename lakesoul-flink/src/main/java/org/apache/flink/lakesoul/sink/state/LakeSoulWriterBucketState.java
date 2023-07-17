// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.writer.LakeSoulWriterBucket;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import java.util.List;
import java.util.stream.Collectors;

/** States for {@link LakeSoulWriterBucket}. */
public class LakeSoulWriterBucketState {

    private final TableSchemaIdentity identity;

    private final String bucketId;

    /** The directory where all the part files of the bucket are stored. */
    private final Path bucketPath;

    private final List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverableList;

    public LakeSoulWriterBucketState(
            TableSchemaIdentity identity,
            String bucketId,
            Path bucketPath,
            List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverableList
            ) {
        this.identity = identity;
        this.bucketId = bucketId;
        this.bucketPath = bucketPath;
        this.pendingFileRecoverableList = pendingFileRecoverableList;
    }

    public String getBucketId() {
        return bucketId;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    @Override
    public String toString() {

        return "BucketState for bucketId=" +
                bucketId +
                " and bucketPath=" +
                bucketPath +
                " and identity=" +
                identity +
                " and pendingFiles=" +
                pendingFileRecoverableList.stream().map(Object::toString).collect(Collectors.joining("; "))
                ;
    }

    public TableSchemaIdentity getIdentity() {
        return identity;
    }

    public List<InProgressFileWriter.PendingFileRecoverable> getPendingFileRecoverableList() {
        return pendingFileRecoverableList;
    }
}
