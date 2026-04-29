// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.io;

import com.dmetasoul.lakesoul.lakesoul.io.jnr.LibLakeSoulIO;
import io.substrait.proto.Plan;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.byref.IntByReference;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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

    /**
     * usually use only once
     *
     * @param plan Filter{}
     */
    public void addFilterProto(Plan plan) {
        byte[] bytes = plan.toByteArray();
        Pointer buf = Runtime.getRuntime(libLakeSoulIO).getMemoryManager().allocateDirect(bytes.length);
        buf.put(0, bytes, 0, bytes.length);
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_filter_proto(ioConfigBuilder, buf.address(), bytes.length);
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
        tokioRuntimeBuilder = null;
        config = libLakeSoulIO.create_lakesoul_io_config_from_builder(ioConfigBuilder);
        ioConfigBuilder = null;
        // tokioRuntime will be moved to reader
        reader = libLakeSoulIO.create_lakesoul_reader_from_config(config, tokioRuntime);
        config = null;
        tokioRuntime = null;
        Pointer p = libLakeSoulIO.check_reader_created(reader);
        if (p != null) {
            String err = p.getString(0);
            libLakeSoulIO.free_lakesoul_reader(reader);
            reader = null;
            throw new IOException(err);
        }
        // startReader in C is a blocking call
        LibLakeSoulIO.CStatus status = startReader();
        try {
            if (status.status.get() != 0) {
                throw new IOException("Init native reader failed with error: " +
                        (status.err.get() != null ? status.err.get() : "unknown error"));
            }
            this.readerSchema = getReaderSchema();
        } finally {
            libLakeSoulIO.free_c_status(status);
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

    private LibLakeSoulIO.CStatus startReader() {
        assert reader != null;
        return libLakeSoulIO.start_reader(reader);
    }

    public int nextBatchBlocked(long arrayAddr) throws IOException {
        LibLakeSoulIO.CStatus status = libLakeSoulIO.next_record_batch_blocked(reader, arrayAddr);
        try {
            if (status.status.get() < 0) {
                throw new IOException("Init native reader failed with error: " +
                        (status.err.get() != null ? status.err.get() : "unknown error"));
            }
            return status.status.get();
        } finally {
            libLakeSoulIO.free_c_status(status);
        }
    }
}
