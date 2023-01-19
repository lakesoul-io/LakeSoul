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
import java.nio.charset.StandardCharsets;

public class NativeBucketWriter implements BucketWriter<RowData, String> {

    private final RowType rowType;

    private final Configuration conf;

    public NativeBucketWriter(RowType rowType, Configuration conf) {
        this.rowType = rowType;
        this.conf = conf;
    }

    @Override
    public InProgressFileWriter<RowData, String> openNewInProgressFile(String s, Path path, long creationTime) throws IOException {
        return new NativeParquetWriter(rowType, 8192, s, path, creationTime, conf);
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
                NativePendingFileRecoverableSerializer.INSTANCE,
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
    private static class UnsupportedInProgressFileRecoverableSerializable
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

    private static class NativePendingFileRecoverableSerializer
        implements SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable> {

        static final NativePendingFileRecoverableSerializer INSTANCE =
                new NativePendingFileRecoverableSerializer();

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(InProgressFileWriter.PendingFileRecoverable obj) throws IOException {
            if (!(obj instanceof NativeParquetWriter.NativeWriterPendingFileRecoverable)) {
                throw new UnsupportedOperationException(
                        "Only NativeParquetWriter.NativeWriterPendingFileRecoverable is supported.");
            }
            NativeParquetWriter.NativeWriterPendingFileRecoverable recoverable =
                    (NativeParquetWriter.NativeWriterPendingFileRecoverable) obj;
            return recoverable.path.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public InProgressFileWriter.PendingFileRecoverable deserialize(int version, byte[] serialized) throws IOException {
            return new NativeParquetWriter.NativeWriterPendingFileRecoverable(new String(serialized, StandardCharsets. UTF_8));
        }
    }
}
