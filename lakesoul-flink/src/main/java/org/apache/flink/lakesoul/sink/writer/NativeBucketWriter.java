// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.WriterProperties;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;

public class NativeBucketWriter implements BucketWriter<RowData, String> {

    private final RowType rowType;

    private final List<String> primaryKeys;

    private final Configuration conf;
    private final List<String> partitionKeys;
    private final int subTaskId;

    public NativeBucketWriter(RowType rowType, List<String> primaryKeys, List<String> partitionKeys, Configuration conf, int subTaskId) {
        this.rowType = rowType;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.conf = conf;
        this.subTaskId = subTaskId;
    }

    @Override
    public InProgressFileWriter<RowData, String> openNewInProgressFile(String bucketId, Path path, long creationTime) throws IOException {
        return new NativeParquetWriter(rowType, primaryKeys, partitionKeys, bucketId, path, creationTime, conf, subTaskId);
    }

    @Override
    public InProgressFileWriter<RowData, String> resumeInProgressFileFrom(
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
