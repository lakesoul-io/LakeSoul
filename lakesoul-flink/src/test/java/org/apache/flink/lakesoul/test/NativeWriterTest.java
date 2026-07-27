package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.lakesoul.sink.writer.NativeLakeSoulWriter;
import org.apache.flink.lakesoul.sink.writer.NativeParquetWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class NativeWriterTest {

    @Test
    public void check_writer_test() {
        Schema testSchema = new Schema(Collections.emptyList());

        NativeIOWriter writer = new NativeIOWriter(testSchema);

        // this should raise an error on the native side
        writer.withPrefix("error://xxxxx");

        try {
            writer.initializeWriter();
            throw new AssertionError("Expected IOException to be thrown");
        } catch (IOException e) {
        }
    }

    @Test
    public void native_writer_serializer_accepts_deprecated_recoverable() throws IOException {
        NativeParquetWriter.NativeWriterPendingFileRecoverable recoverable =
                new NativeParquetWriter.NativeWriterPendingFileRecoverable("file:///tmp/part-0001.vortex", 123L);

        byte[] bytes = NativeLakeSoulWriter.NativePendingFileRecoverableSerializer.INSTANCE.serialize(recoverable);
        InProgressFileWriter.PendingFileRecoverable restored =
                NativeLakeSoulWriter.NativePendingFileRecoverableSerializer.INSTANCE.deserialize(0, bytes);

        assertTrue(restored instanceof NativeLakeSoulWriter.NativeWriterPendingFileRecoverable);
        NativeLakeSoulWriter.NativeWriterPendingFileRecoverable nativeRecoverable =
                (NativeLakeSoulWriter.NativeWriterPendingFileRecoverable) restored;
        assertEquals(recoverable.path, nativeRecoverable.path);
        assertEquals(recoverable.creationTime, nativeRecoverable.creationTime);
    }

    @Test
    public void deprecated_serializer_accepts_native_recoverable() throws IOException {
        NativeLakeSoulWriter.NativeWriterPendingFileRecoverable recoverable =
                new NativeLakeSoulWriter.NativeWriterPendingFileRecoverable("file:///tmp/part-0002.vortex", 456L);

        byte[] bytes = NativeParquetWriter.NativePendingFileRecoverableSerializer.INSTANCE.serialize(recoverable);
        InProgressFileWriter.PendingFileRecoverable restored =
                NativeParquetWriter.NativePendingFileRecoverableSerializer.INSTANCE.deserialize(0, bytes);

        assertTrue(restored instanceof NativeParquetWriter.NativeWriterPendingFileRecoverable);
        NativeParquetWriter.NativeWriterPendingFileRecoverable parquetRecoverable =
                (NativeParquetWriter.NativeWriterPendingFileRecoverable) restored;
        assertEquals(recoverable.path, parquetRecoverable.path);
        assertEquals(recoverable.creationTime, parquetRecoverable.creationTime);
    }
}
