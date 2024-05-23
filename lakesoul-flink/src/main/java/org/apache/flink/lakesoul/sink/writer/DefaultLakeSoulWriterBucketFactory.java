// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * A factory returning {@link AbstractLakeSoulMultiTableSinkWriter writer}.
 */
@Internal
public class DefaultLakeSoulWriterBucketFactory implements LakeSoulWriterBucketFactory {

    private final Configuration conf;

    public DefaultLakeSoulWriterBucketFactory(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public LakeSoulWriterBucket getNewBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            String bucketId,
            Path bucketPath,
            BucketWriter<RowData, String> bucketWriter,
            RollingPolicy<RowData, String> rollingPolicy,
            OutputFileConfig outputFileConfig) {
        return LakeSoulWriterBucket.getNew(
                subTaskId, tableId,
                bucketId, bucketPath, conf, bucketWriter, rollingPolicy, outputFileConfig);
    }

    @Override
    public LakeSoulWriterBucket restoreBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            BucketWriter<RowData, String> bucketWriter,
            RollingPolicy<RowData, String> rollingPolicy,
            LakeSoulWriterBucketState bucketState,
            OutputFileConfig outputFileConfig)
            throws IOException {
        return LakeSoulWriterBucket.restore(subTaskId, tableId, bucketWriter, conf,
                rollingPolicy, bucketState, outputFileConfig);
    }
}
