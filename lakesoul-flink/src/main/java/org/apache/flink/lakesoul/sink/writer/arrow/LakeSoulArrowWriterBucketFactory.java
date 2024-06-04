// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;

import java.io.IOException;
import java.io.Serializable;

/**
 * A factory returning {@link AbstractLakeSoulMultiTableSinkWriter writer}.
 */
@Internal
public class LakeSoulArrowWriterBucketFactory implements Serializable {

    public LakeSoulArrowWriterBucket getNewBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            String bucketId,
            Path bucketPath,
            BucketWriter<LakeSoulArrowWrapper, String> bucketWriter,
            RollingPolicy<LakeSoulArrowWrapper, String> rollingPolicy,
            OutputFileConfig outputFileConfig) {
        return LakeSoulArrowWriterBucket.getNew(
                subTaskId, tableId,
                bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig);
    }

    public LakeSoulArrowWriterBucket restoreBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            BucketWriter<LakeSoulArrowWrapper, String> bucketWriter,
            RollingPolicy<LakeSoulArrowWrapper, String> rollingPolicy,
            LakeSoulWriterBucketState bucketState,
            OutputFileConfig outputFileConfig)
            throws IOException {
        return LakeSoulArrowWriterBucket.restore(subTaskId, tableId, bucketWriter, rollingPolicy, bucketState, outputFileConfig);
    }
}
