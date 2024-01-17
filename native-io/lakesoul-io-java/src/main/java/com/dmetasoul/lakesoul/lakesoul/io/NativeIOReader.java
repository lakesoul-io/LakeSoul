// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.io;

import com.dmetasoul.lakesoul.lakesoul.io.jnr.LibLakeSoulIO;
import jnr.ffi.Pointer;
import jnr.ffi.byref.IntByReference;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class NativeIOReader extends NativeIOBase implements AutoCloseable {
    private Pointer reader = null;

    private Schema readerSchema = null;

    public NativeIOReader() {
        super("NativeReader");
    }

    public void addFile(String file) {
        super.addFile(file);
    }

    public void addFilter(String filter) {
        assert ioConfigBuilder != null;
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_filter(ioConfigBuilder, filter);
    }

    public void addMergeOps(Map<String, String> mergeOps) {
        for (Map.Entry<String, String> entry : mergeOps.entrySet()) {
            ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_merge_op(ioConfigBuilder, entry.getKey(), entry.getValue());
        }
    }

    public void setDefaultColumnValue(String column, String value) {
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_default_column_value(ioConfigBuilder, column, value);
    }

    public void initializeReader() throws IOException {
        assert tokioRuntimeBuilder != null;
        assert ioConfigBuilder != null;

        tokioRuntime = libLakeSoulIO.create_tokio_runtime_from_builder(tokioRuntimeBuilder);
        config = libLakeSoulIO.create_lakesoul_io_config_from_builder(ioConfigBuilder);
        ioConfigBuilder = null;
        // tokioRuntime will be moved to reader
        reader = libLakeSoulIO.create_lakesoul_reader_from_config(config, tokioRuntime);
        tokioRuntime = null;
        Pointer p = libLakeSoulIO.check_reader_created(reader);
        if (p != null) {
            throw new IOException(p.getString(0));
        }
        AtomicReference<String> errMsg = new AtomicReference<>();
        // startReader in C is a blocking call
        startReader((status, err) -> {
            if (!status) {
                errMsg.set("Init native reader failed with error: " + (err != null ? err : "unknown error"));
            }
        });
        if (errMsg.get() != null) {
            throw new IOException(errMsg.get());
        }
        if (readerSchema == null) {
            throw new IOException("Init native reader failed: Cannot retrieve native reader's schema");
        }
    }

    public Schema getSchema() {
        return readerSchema;
    }

    private Schema getReaderSchema() {
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
        libLakeSoulIO.lakesoul_reader_get_schema(reader, ffiSchema.memoryAddress());
        Schema schema = Data.importSchema(allocator, ffiSchema, provider);
        ffiSchema.close();
        return schema;
    }

    @Override
    public void close() throws Exception {
        if (reader != null) {
            libLakeSoulIO.free_lakesoul_reader(reader);
            reader = null;
        }
        super.close();
    }

    private void startReader(BiConsumer<Boolean, String> callback) {
        assert reader != null;
        BiConsumer<Boolean, String> wrapCallback = (status, err) -> {
            if (status) {
                this.readerSchema = getReaderSchema();
            }
            if (err != null) {
                System.err.println("[ERROR][com.dmetasoul.lakesoul.io.lakesoul.NativeIOReader.startReader]err=" + err);
            }
            callback.accept(status, err);
        };
        BooleanCallback nativeBooleanCallback = new BooleanCallback(wrapCallback, boolReferenceManager);
        nativeBooleanCallback.registerReferenceKey();
        libLakeSoulIO.start_reader(reader, nativeBooleanCallback);
    }

    public void nextBatch(BiConsumer<Integer, String> callback, long schemaAddr, long arrayAddr) {
        IntegerCallback nativeIntegerCallback = new IntegerCallback(callback, intReferenceManager);
        nativeIntegerCallback.registerReferenceKey();
        assert reader != null;
        Pointer p = libLakeSoulIO.check_reader_created(reader);
        if (p != null) {
            throw new RuntimeException(p.getString(0));
        }
        libLakeSoulIO.next_record_batch(reader, schemaAddr, arrayAddr, nativeIntegerCallback);
    }

    public int nextBatchBlocked(long arrayAddr) throws IOException {
        IntByReference count = new IntByReference();
        String err = libLakeSoulIO.next_record_batch_blocked(reader, arrayAddr, count);
        if (err != null) {
            throw new IOException(err);
        }
        return count.getValue();
    }
}
