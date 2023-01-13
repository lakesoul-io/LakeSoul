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
import org.apache.arrow.lakesoul.io.jnr.LibLakeSoulIO;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class NativeIOReader extends NativeIOBase implements AutoCloseable {
    private Pointer reader = null;
    private final boolean useJavaReader;

    private ArrowJavaReader.ArrowJavaReaderBuilder arrowJavaReaderBuilder;
    private ArrowJavaReader arrowJavaReader;

    private String readerSchema = null;

    public NativeIOReader() {
        this(false);
    }

    public NativeIOReader(boolean useJavaReader) {
        super();
        this.useJavaReader = useJavaReader;
        if (useJavaReader) {
            arrowJavaReaderBuilder = new ArrowJavaReader.ArrowJavaReaderBuilder();
        }
    }

    public void addFile(String file) {
        if (!useJavaReader) {
            super.addFile(file);
        } else {
            arrowJavaReaderBuilder.setUri(file);
        }
    }

    public void addFilter(String filter) {
        assert ioConfigBuilder != null;
        Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, filter);
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_filter(ioConfigBuilder, ptr);
    }

    public void initializeReader() throws IOException {
        if (!useJavaReader) {
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
            if (readerSchema.isEmpty()) {
                throw new IOException("Init native reader failed: Cannot retrieve native reader's schema");
            }
        } else {
            arrowJavaReader = arrowJavaReaderBuilder.build();
        }
    }

    public String getSchema() {
        return readerSchema;
    }

    private String getReaderSchemaJson() {
        if (!useJavaReader) {
            Pointer p = libLakeSoulIO.lakesoul_reader_get_schema(reader);
            String s = p.getString(0);
            libLakeSoulIO.lakesoul_schema_free(p);
            return s;
        } else {
            return arrowJavaReader.getSchema().toJson();
        }
    }

    @Override
    public void close() throws Exception {
        if (!useJavaReader) {
            if (reader != null) {
                libLakeSoulIO.free_lakesoul_reader(reader);
            }
            if (tokioRuntime != null) {
                libLakeSoulIO.free_tokio_runtime(tokioRuntime);
            }
        } else if (arrowJavaReader != null) {
            arrowJavaReader.close();
        }
    }

    private void startReader(BiConsumer<Boolean, String> callback) {
        if (!useJavaReader) {
            assert reader != null;
            libLakeSoulIO.start_reader(reader, (status, err) -> {
                this.readerSchema = getReaderSchemaJson();
                callback.accept(status, err);
            });
        }
    }

    public void nextBatch(BiConsumer<Boolean, String> callback, long schemaAddr, long arrayAddr) {
        Callback nativeCallback = new Callback(callback, referenceManager);
        nativeCallback.registerReferenceKey();
        if (!useJavaReader) {
            assert reader != null;
            libLakeSoulIO.next_record_batch(reader, schemaAddr, arrayAddr, nativeCallback);
        } else {
            // disable native for testing
            arrowJavaReader.nextRecordBatch(schemaAddr, arrayAddr, nativeCallback);
        }
    }
}
