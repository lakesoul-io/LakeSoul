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

import javax.annotation.Nullable;

import static org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.InProgressFileRecoverable;

/** States for {@link LakeSoulWriterBucket}. */
public class LakeSoulWriterBucketState {

    private final TableSchemaIdentity identity;

    private final String bucketId;

    /** The directory where all the part files of the bucket are stored. */
    private final Path bucketPath;

    /**
     * The creation time of the currently open part file, or {@code Long.MAX_VALUE} if there is no
     * open part file.
     */
    private final long inProgressFileCreationTime;

    /**
     * A {@link InProgressFileRecoverable} for the currently open part file, or null if there is no
     * currently open part file.
     */
    @Nullable private final InProgressFileRecoverable inProgressFileRecoverable;

    private final String inProgressPath;

    public LakeSoulWriterBucketState(
            TableSchemaIdentity identity,
            String bucketId,
            Path bucketPath,
            long inProgressFileCreationTime,
            @Nullable InProgressFileRecoverable inProgressFileRecoverable, String inProgressPath) {
        this.identity = identity;
        this.bucketId = bucketId;
        this.bucketPath = bucketPath;
        this.inProgressFileCreationTime = inProgressFileCreationTime;
        this.inProgressFileRecoverable = inProgressFileRecoverable;
        this.inProgressPath = inProgressPath;
    }

    public String getBucketId() {
        return bucketId;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    public long getInProgressFileCreationTime() {
        return inProgressFileCreationTime;
    }

    @Nullable
    public InProgressFileRecoverable getInProgressFileRecoverable() {
        return inProgressFileRecoverable;
    }

    public boolean hasInProgressFileRecoverable() {
        return inProgressFileRecoverable != null;
    }

    @Override
    public String toString() {
        final StringBuilder strBuilder = new StringBuilder();

        strBuilder
                .append("BucketState for bucketId=")
                .append(bucketId)
                .append(" and bucketPath=")
                .append(bucketPath)
                .append(" and identity=")
                .append(identity);

        if (hasInProgressFileRecoverable()) {
            strBuilder.append(", has open part file created @ ").append(inProgressFileCreationTime);
        }

        return strBuilder.toString();
    }

    public TableSchemaIdentity getIdentity() {
        return identity;
    }

    public String getInProgressPath() {
        return inProgressPath;
    }
}
