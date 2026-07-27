// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @deprecated Use {@link NativeLakeSoulWriter} instead.
 */
@Deprecated
public class NativeParquetWriter extends NativeLakeSoulWriter {

    public NativeParquetWriter(RowType rowType,
                               List<String> primaryKeys,
                               List<String> rangeColumns,
                               String bucketID,
                               Path path,
                               long creationTime,
                               Configuration conf,
                               int subTaskId) throws IOException {
        super(rowType, primaryKeys, rangeColumns, bucketID, path, creationTime, conf, subTaskId);
    }

    @Override
    public InProgressFileWriter.PendingFileRecoverable closeForCommit() throws IOException {
        NativeLakeSoulWriter.NativeWriterPendingFileRecoverable recoverable =
                (NativeLakeSoulWriter.NativeWriterPendingFileRecoverable) super.closeForCommit();
        return new NativeParquetWriter.NativeWriterPendingFileRecoverable(
                recoverable.path, recoverable.creationTime);
    }

    @Override
    public Map<String, List<InProgressFileWriter.PendingFileRecoverable>> closeForCommitWithRecoverableMap()
            throws IOException {
        Map<String, List<InProgressFileWriter.PendingFileRecoverable>> recoverableMap =
                super.closeForCommitWithRecoverableMap();
        Map<String, List<InProgressFileWriter.PendingFileRecoverable>> parquetRecoverableMap =
                new HashMap<>();
        for (Map.Entry<String, List<InProgressFileWriter.PendingFileRecoverable>> entry : recoverableMap.entrySet()) {
            List<InProgressFileWriter.PendingFileRecoverable> parquetRecoverables = new ArrayList<>();
            for (InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable : entry.getValue()) {
                if (pendingFileRecoverable instanceof NativeLakeSoulWriter.NativeWriterPendingFileRecoverable) {
                    NativeLakeSoulWriter.NativeWriterPendingFileRecoverable recoverable =
                            (NativeLakeSoulWriter.NativeWriterPendingFileRecoverable) pendingFileRecoverable;
                    parquetRecoverables.add(new NativeParquetWriter.NativeWriterPendingFileRecoverable(
                            recoverable.path, recoverable.creationTime));
                } else {
                    parquetRecoverables.add(pendingFileRecoverable);
                }
            }
            parquetRecoverableMap.put(entry.getKey(), parquetRecoverables);
        }
        return parquetRecoverableMap;
    }

    /**
     * @deprecated Use {@link NativeLakeSoulWriter.NativePendingFileRecoverableSerializer} instead.
     */
    @Deprecated
    public static class NativePendingFileRecoverableSerializer
            implements SimpleVersionedSerializer<PendingFileRecoverable> {

        public static final NativePendingFileRecoverableSerializer INSTANCE =
                new NativePendingFileRecoverableSerializer();

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(InProgressFileWriter.PendingFileRecoverable obj) throws IOException {
            if (!(obj instanceof NativeLakeSoulWriter.NativeWriterPendingFileRecoverable)) {
                throw new UnsupportedOperationException(
                        "Only NativeLakeSoulWriter.NativeWriterPendingFileRecoverable is supported.");
            }
            DataOutputSerializer out = new DataOutputSerializer(256);
            NativeLakeSoulWriter.NativeWriterPendingFileRecoverable recoverable =
                    (NativeLakeSoulWriter.NativeWriterPendingFileRecoverable) obj;
            out.writeUTF(recoverable.path);
            out.writeLong(recoverable.creationTime);
            return out.getCopyOfBuffer();
        }

        @Override
        public InProgressFileWriter.PendingFileRecoverable deserialize(int version, byte[] serialized)
                throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            String path = in.readUTF();
            long time = in.readLong();
            return new NativeParquetWriter.NativeWriterPendingFileRecoverable(path, time);
        }
    }

    /**
     * @deprecated Use {@link NativeLakeSoulWriter.NativeWriterPendingFileRecoverable} instead.
     */
    @Deprecated
    static public class NativeWriterPendingFileRecoverable
            extends NativeLakeSoulWriter.NativeWriterPendingFileRecoverable {

        public NativeWriterPendingFileRecoverable(String path, long creationTime) {
            super(path, creationTime);
        }
    }
}
