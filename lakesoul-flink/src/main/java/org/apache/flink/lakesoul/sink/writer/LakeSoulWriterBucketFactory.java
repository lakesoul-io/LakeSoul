// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;

/** A factory able to create {@link LakeSoulWriterBucket} for the {@link LakeSoulMultiTablesSink}. */
public interface LakeSoulWriterBucketFactory extends Serializable {

    LakeSoulWriterBucket getNewBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            String bucketId,
            Path bucketPath,
            BucketWriter<RowData, String> bucketWriter,
            RollingPolicy<RowData, String> rollingPolicy,
            OutputFileConfig outputFileConfig) throws IOException;

    LakeSoulWriterBucket restoreBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            BucketWriter<RowData, String> bucketWriter,
            RollingPolicy<RowData, String> rollingPolicy,
            LakeSoulWriterBucketState bucketState,
            OutputFileConfig outputFileConfig) throws IOException;
}
