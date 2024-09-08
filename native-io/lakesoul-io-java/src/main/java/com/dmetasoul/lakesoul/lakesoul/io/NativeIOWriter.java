// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.io;

import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import com.dmetasoul.lakesoul.lakesoul.io.jnr.LibLakeSoulIO;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class NativeIOWriter extends NativeIOBase implements AutoCloseable {

    private Pointer writer = null;

    public NativeIOWriter(Schema schema) {
        super("NativeWriter");
        setSchema(schema);
    }


    public void setAuxSortColumns(Iterable<String> auxSortColumns) {
        for (String col : auxSortColumns) {
            ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_aux_sort_column(ioConfigBuilder, col);
        }
    }

    public void setHashBucketNum(Integer hashBucketNum) {
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_hash_bucket_num(ioConfigBuilder, hashBucketNum);
    }


    public void setRowGroupRowNumber(int rowNum) {
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_max_row_group_size(ioConfigBuilder, rowNum);
    }

    public void setRowGroupValueNumber(int valueNum) {
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_max_row_group_num_values(ioConfigBuilder, valueNum);
    }

    public void initializeWriter() throws IOException {
        assert tokioRuntimeBuilder != null;
        assert ioConfigBuilder != null;

        tokioRuntime = libLakeSoulIO.create_tokio_runtime_from_builder(tokioRuntimeBuilder);
        config = libLakeSoulIO.create_lakesoul_io_config_from_builder(ioConfigBuilder);
        writer = libLakeSoulIO.create_lakesoul_writer_from_config(config, tokioRuntime);
        // tokioRuntime will be moved to writer, we don't need to free it
        tokioRuntime = null;
        Pointer p = libLakeSoulIO.check_writer_created(writer);
        if (p != null) {
            writer = null;
            throw new IOException("Init native writer failed with error: " + p.getString(0));
        }
    }

    public int writeIpc(byte[] encodedBatch) throws IOException {
//        Pointer ipc = getRuntime().getMemoryManager().allocateDirect(encodedBatch.length + 1, true);
//        ipc.put(0, encodedBatch, 0, encodedBatch.length);
//        ipc.putByte(encodedBatch.length, (byte) 0);
//        String msg = libLakeSoulIO.write_record_batch_ipc_blocked(writer, ipc.address(), ipc.size());
//        if (!msg.startsWith("Ok: ")) {
//            throw new IOException("Native writer write batch failed with error: " + msg);
//        }
//
//        return Integer.parseInt(msg.substring(4));

        int batchSize = 0;
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(encodedBatch), allocator)) {
            if (reader.loadNextBatch()) {
                ArrowArray array = ArrowArray.allocateNew(allocator);
                ArrowSchema schema = ArrowSchema.allocateNew(allocator);
                VectorSchemaRoot batch = reader.getVectorSchemaRoot();
                batchSize = batch.getRowCount();
                Data.exportVectorSchemaRoot(allocator, batch, provider, array, schema);
                String errMsg = libLakeSoulIO.write_record_batch_blocked(writer, schema.memoryAddress(), array.memoryAddress());
                array.close();
                schema.close();
                if (errMsg != null && !errMsg.isEmpty()) {
                    throw new IOException("Native writer write batch failed with error: " + errMsg);
                }
            }
        }
        return batchSize;
    }

    public void write(VectorSchemaRoot batch) throws IOException {
        System.out.println("writing batch: " + batch.getRowCount());
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, batch, provider, array, schema);
        String errMsg = libLakeSoulIO.write_record_batch_blocked(writer, schema.memoryAddress(), array.memoryAddress());
        array.close();
        schema.close();
        if (errMsg != null && !errMsg.isEmpty()) {
            throw new IOException("Native writer write batch failed with error: " + errMsg);
        }
    }

    public HashMap<String, List<String>> flush() throws IOException {
        AtomicReference<String> errMsg = new AtomicReference<>();
        AtomicReference<Integer> lenResult = new AtomicReference<>();
        IntegerCallback nativeIntegerCallback = new IntegerCallback((len, err) -> {
            if (len < 0 && err != null) {
                errMsg.set(err);
            }
            lenResult.set(len);

        }, intReferenceManager);
        nativeIntegerCallback.registerReferenceKey();
        Pointer ptrResult = libLakeSoulIO.flush_and_close_writer(writer, nativeIntegerCallback);
        writer = null;
        if (errMsg.get() != null && !errMsg.get().isEmpty()) {
            throw new IOException("Native writer flush failed with error: " + errMsg.get());
        }

        Integer len = lenResult.get();
        if (len != null && len > 0) {
            int lenWithTail = len + 1;
            Pointer buffer = fixedBuffer;
            if (lenWithTail > fixedBuffer.size()) {
                if (lenWithTail > mutableBuffer.size()) {
                    mutableBuffer = Runtime.getRuntime(libLakeSoulIO).getMemoryManager().allocateDirect(lenWithTail);
                }
                buffer = mutableBuffer;
            }
            AtomicReference<Boolean> exported = new AtomicReference<>();
            BooleanCallback nativeBooleanCallback = new BooleanCallback((status, err) -> {
                if (!status && err != null) {
                    errMsg.set(err);
                }
                exported.set(status);
            }, boolReferenceManager);
            nativeBooleanCallback.registerReferenceKey();
            libLakeSoulIO.export_bytes_result(nativeBooleanCallback, ptrResult, len, buffer.address());

            if (exported.get() != null && exported.get()) {
                byte[] bytes = new byte[len];
                buffer.get(0, bytes, 0, len);
                String decodedResult = new String(bytes);
                String[] splits = decodedResult.split("\u0001");
                int partitionNum = Integer.parseInt(splits[0]);
                if (partitionNum != splits.length - 1) {
                    throw new IOException("Dynamic Partitions Result [" + decodedResult + "] encode error: partition number mismatch " + partitionNum + "!=" + (splits.length - 1));
                }
                HashMap<String, List<String>> partitionDescAndFilesMap = new HashMap<>();
                for (int i = 1; i < splits.length; i++) {
                    String[] partitionDescAndFiles = splits[i].split("\u0002");
                    List<String> list = new ArrayList<>(Arrays.asList(partitionDescAndFiles).subList(1, partitionDescAndFiles.length));
                    partitionDescAndFilesMap.put(partitionDescAndFiles[0], list);

                }
                return partitionDescAndFilesMap;
            }
        }
        return null;
    }

    public void abort() throws IOException {
        AtomicReference<String> errMsg = new AtomicReference<>();
        BooleanCallback nativeBooleanCallback = new BooleanCallback((status, err) -> {
            if (!status && err != null) {
                errMsg.set(err);
            }
        }, boolReferenceManager);
        nativeBooleanCallback.registerReferenceKey();
        libLakeSoulIO.abort_and_close_writer(writer, nativeBooleanCallback);
        writer = null;
        if (errMsg.get() != null && !errMsg.get().isEmpty()) {
            throw new IOException("Native writer abort failed with error: " + errMsg.get());
        }
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            abort();
        }
        super.close();
    }
}
