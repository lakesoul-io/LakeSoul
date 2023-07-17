// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.io;

import jnr.ffi.Pointer;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import com.dmetasoul.lakesoul.lakesoul.io.jnr.LibLakeSoulIO;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class NativeIOWriter extends NativeIOBase implements AutoCloseable {

    private Pointer writer = null;

    public NativeIOWriter(Schema schema) {
        super("NativeWriter");
        setSchema(schema);
    }


    public void setAuxSortColumns(Iterable<String> auxSortColumns) {
        for (String col : auxSortColumns) {
            Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, col);
            ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_aux_sort_column(ioConfigBuilder, ptr);
        }
    }

    public void setRowGroupRowNumber(int rowNum) {
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_max_row_group_size(ioConfigBuilder, rowNum);
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

    public void write(VectorSchemaRoot batch) throws IOException {
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, batch, provider, array, schema);
        AtomicReference<String> errMsg = new AtomicReference<>();
        BooleanCallback nativeBooleanCallback = new BooleanCallback((status, err) -> {
            array.close();
            schema.close();
            if (!status && err != null) {
                errMsg.set(err);
            }
        }, boolReferenceManager);
        nativeBooleanCallback.registerReferenceKey();
        libLakeSoulIO.write_record_batch(writer, schema.memoryAddress(), array.memoryAddress(), nativeBooleanCallback);
        if (errMsg.get() != null && !errMsg.get().isEmpty()) {
            throw new IOException("Native writer write batch failed with error: " + errMsg.get());
        }
    }

    public void flush() throws IOException {
        AtomicReference<String> errMsg = new AtomicReference<>();
        BooleanCallback nativeBooleanCallback = new BooleanCallback((status, err) -> {
            if (!status && err != null) {
                errMsg.set(err);
            }
        }, boolReferenceManager);
        nativeBooleanCallback.registerReferenceKey();
        libLakeSoulIO.flush_and_close_writer(writer, nativeBooleanCallback);
        writer = null;
        if (errMsg.get() != null && !errMsg.get().isEmpty()) {
            throw new IOException("Native writer flush failed with error: " + errMsg.get());
        }
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
