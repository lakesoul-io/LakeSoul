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
