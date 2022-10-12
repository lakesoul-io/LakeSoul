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

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.types.LakeSoulCDCComparator;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/** A factory returning {@link AbstractLakeSoulMultiTableSinkWriter writer}. */
@Internal
public class DefaultLakeSoulWriterBucketFactory implements LakeSoulWriterBucketFactory {

    @Override
    public LakeSoulWriterBucket getNewBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            String bucketId,
            Path bucketPath,
            BucketWriter<RowData, String> bucketWriter,
            RollingPolicy<RowData, String> rollingPolicy,
            OutputFileConfig outputFileConfig,
            LakeSoulCDCComparator comparator) {
        return LakeSoulWriterBucket.getNew(
                subTaskId, tableId,
                bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig, comparator);
    }

    @Override
    public LakeSoulWriterBucket restoreBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            BucketWriter<RowData, String> bucketWriter,
            RollingPolicy<RowData, String> rollingPolicy,
            LakeSoulWriterBucketState bucketState,
            OutputFileConfig outputFileConfig,
            LakeSoulCDCComparator comparator)
            throws IOException {
        return LakeSoulWriterBucket.restore(subTaskId, tableId, bucketWriter,
                                            rollingPolicy, bucketState, outputFileConfig,
                                            comparator);
    }
}
