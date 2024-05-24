// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer.arrow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.sink.writer.NativeParquetWriter;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.WriterProperties;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.DYNAMIC_BUCKET;

public class NativeArrowBucketWriter implements BucketWriter<LakeSoulArrowWrapper, String> {

    private final RowType rowType;

    private final List<String> primaryKeys;

    private final Configuration conf;
    private final List<String> partitionKeys;

    public NativeArrowBucketWriter(RowType rowType, List<String> primaryKeys, List<String> partitionKeys, Configuration conf) {
        this.rowType = rowType;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.conf = conf;
    }

    @Override
    public InProgressFileWriter<LakeSoulArrowWrapper, String> openNewInProgressFile(String bucketId, Path path, long creationTime) throws IOException {
        if (DYNAMIC_BUCKET.equals(bucketId)) {
            return new NativeLakeSoulArrowWrapperWriter(rowType, primaryKeys, partitionKeys, path, creationTime, conf);
        }
        throw new RuntimeException("Static Bucketing not supported");
    }

    @Override
    public InProgressFileWriter<LakeSoulArrowWrapper, String> resumeInProgressFileFrom(
            String s,
            InProgressFileWriter.InProgressFileRecoverable inProgressFileSnapshot,
            long creationTime) throws IOException {
        throw new UnsupportedOperationException("NativeBucketWriter does not support resume");
    }

    @Override
    public WriterProperties getProperties() {
        return new WriterProperties(
                UnsupportedInProgressFileRecoverableSerializable.INSTANCE,
                NativeParquetWriter.NativePendingFileRecoverableSerializer.INSTANCE,
                false
        );
    }

    @Override
    public PendingFile recoverPendingFile(InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable) throws IOException {
        return null;
    }

    @Override
    public boolean cleanupInProgressFileRecoverable(InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable) throws IOException {
        return false;
    }

    // Copied from apache flink
    public static class UnsupportedInProgressFileRecoverableSerializable
            implements SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable> {

        static final UnsupportedInProgressFileRecoverableSerializable INSTANCE =
                new UnsupportedInProgressFileRecoverableSerializable();

        @Override
        public int getVersion() {
            throw new UnsupportedOperationException(
                    "Persists the path-based part file write is not supported");
        }

        @Override
        public byte[] serialize(InProgressFileWriter.InProgressFileRecoverable obj) {
            throw new UnsupportedOperationException(
                    "Persists the path-based part file write is not supported");
        }

        @Override
        public InProgressFileWriter.InProgressFileRecoverable deserialize(int version, byte[] serialized) {
            throw new UnsupportedOperationException(
                    "Persists the path-based part file write is not supported");
        }
    }


}
