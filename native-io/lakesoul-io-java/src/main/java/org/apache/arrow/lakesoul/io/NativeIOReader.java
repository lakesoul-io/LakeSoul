/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.lakesoul.io;

import jnr.ffi.Pointer;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.lakesoul.io.jnr.LibLakeSoulIO;
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
        Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, filter);
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_filter(ioConfigBuilder, ptr);
    }

    public void addMergeOps(Map<String, String> mergeOps) {
        for (Map.Entry<String, String> entry:mergeOps.entrySet()) {
            Pointer fieldPtr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, entry.getKey());
            Pointer mergeOpPtr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, entry.getValue());
            ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_merge_op(ioConfigBuilder, fieldPtr, mergeOpPtr);
        }
    }

    public void initializeReader() throws IOException {
        assert tokioRuntimeBuilder != null;
        assert ioConfigBuilder != null;

        tokioRuntime = libLakeSoulIO.create_tokio_runtime_from_builder(tokioRuntimeBuilder);
        config = libLakeSoulIO.create_lakesoul_io_config_from_builder(ioConfigBuilder);
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
        if (readerSchema == null || readerSchema.getFields().isEmpty()) {
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
        if (tokioRuntime != null) {
            libLakeSoulIO.free_tokio_runtime(tokioRuntime);
            tokioRuntime = null;
        }
        super.close();
    }

    private void startReader(BiConsumer<Boolean, String> callback) {
        assert reader != null;
        BiConsumer<Boolean, String> wrapCallback = (status, err) -> {
            if (status) {
                this.readerSchema = getReaderSchema();
            }
            if (err!=null) {
                System.err.println("[ERROR][org.apache.arrow.lakesoul.io.NativeIOReader.startReader]err=" + err);
            }
            callback.accept(status, err);
        };
        Callback nativeCallback = new Callback(wrapCallback, referenceManager);
        nativeCallback.registerReferenceKey();
        libLakeSoulIO.start_reader(reader, nativeCallback);
    }

    public void nextBatch(BiConsumer<Boolean, String> callback, long schemaAddr, long arrayAddr) {
        Callback nativeCallback = new Callback(callback, referenceManager);
        nativeCallback.registerReferenceKey();
        assert reader != null;
        Pointer p = libLakeSoulIO.check_reader_created(reader);
        if (p != null) {
            throw new RuntimeException(p.getString(0));
        }
        libLakeSoulIO.next_record_batch(reader, schemaAddr, arrayAddr, nativeCallback);
    }
}
