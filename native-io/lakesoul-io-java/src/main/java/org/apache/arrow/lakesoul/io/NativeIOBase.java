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

import jnr.ffi.Runtime;
import jnr.ffi.*;
import org.apache.arrow.lakesoul.io.jnr.LibLakeSoulIO;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class NativeIOBase {
    protected Pointer ioConfigBuilder;
    protected Pointer config = null;
    protected Pointer tokioRuntimeBuilder;
    protected Pointer tokioRuntime = null;
    protected final LibLakeSoulIO libLakeSoulIO;
    protected final ObjectReferenceManager<NativeIOReader.Callback> referenceManager;

    private static boolean isMac() {
        String OS = System.getProperty("os.name").toLowerCase();
        return (OS.contains("mac"));
    }

    public static String getNativeIOLibPath() {
        String ext = ".dylib";
        if (!isMac()) {
            ext = ".so";
        }

        return Paths.get(System.getenv("LakeSoulLib"), "liblakesoul_io_c" + ext).toString();
    }

    public static boolean isNativeIOLibExist() {
        return new File(getNativeIOLibPath()).exists();
    }

    public NativeIOBase() {
        Map<LibraryOption, Object> libraryOptions = new HashMap<>();
        libraryOptions.put(LibraryOption.LoadNow, true);
        libraryOptions.put(LibraryOption.IgnoreError, true);

        String libName = getNativeIOLibPath(); // platform specific name for liblakesoul_io_c

        libLakeSoulIO = LibraryLoader.loadLibrary(
                LibLakeSoulIO.class,
                libraryOptions,
                libName
        );
        referenceManager = Runtime.getRuntime(libLakeSoulIO).newObjectReferenceManager();
        ioConfigBuilder = libLakeSoulIO.new_lakesoul_io_config_builder();
        tokioRuntimeBuilder = libLakeSoulIO.new_tokio_runtime_builder();
        setBatchSize(8192);
        setThreadNum(2);
    }

    public void addFile(String file) {
        Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, file);
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_file(ioConfigBuilder, ptr);
    }

    public void addColumn(String column) {
        assert ioConfigBuilder != null;
        Pointer columnPtr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, column);
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_column(ioConfigBuilder, columnPtr);
    }

    public void setSchema(String schemaJson) {
        assert ioConfigBuilder != null;
        Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, schemaJson);
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_schema(ioConfigBuilder, ptr);
    }

    public void setThreadNum(int threadNum) {
        assert ioConfigBuilder != null;
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_thread_num(ioConfigBuilder, threadNum);
    }

    public void setBatchSize(int batchSize) {
        assert ioConfigBuilder != null;
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_batch_size(ioConfigBuilder, batchSize);
    }

    public void setBufferSize(int bufferSize) {
        assert ioConfigBuilder != null;
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_buffer_size(ioConfigBuilder, bufferSize);
    }

    public void setObjectStoreOptions(String accessKey, String accessSecret, String region, String bucketName, String endpoint) {
        setObjectStoreOption("fs.s3a.access.key", accessKey);
        setObjectStoreOption("fs.s3a.access.secret", accessSecret);
        setObjectStoreOption("fs.s3a.endpoint.region", region);
        setObjectStoreOption("fs.s3a.bucket", bucketName);
        setObjectStoreOption("fs.s3a.endpoint", endpoint);
    }

    public void setObjectStoreOption(String key, String value) {
        assert ioConfigBuilder != null;
        Pointer ptrKey = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, key);
        Pointer ptrValue = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, value);
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_object_store_option(ioConfigBuilder, ptrKey, ptrValue);
    }

    public static final class Callback implements LibLakeSoulIO.JavaCallback {

        public BiConsumer<Boolean, String> callback;
        public long array_ptr;
        private Pointer key;
        private ObjectReferenceManager<Callback> referenceManager;

        public Callback(BiConsumer<Boolean, String> callback) {
            this(callback, 0L);
        }

        public Callback(BiConsumer<Boolean, String> callback, long array_ptr) {
            this.callback = callback;
            this.array_ptr = array_ptr;
        }

        public Callback(BiConsumer<Boolean, String> callback, ObjectReferenceManager<Callback> referenceManager) {
            this.callback = callback;
            this.referenceManager = referenceManager;
            key = null;
        }

        public void registerReferenceKey() {
            key = referenceManager.add(this);
        }

        public void removerReferenceKey() {
            if (key != null) {
                referenceManager.remove(key);
            }
        }

        @Override
        public void invoke(boolean status, String err) {
            callback.accept(status, err);
            removerReferenceKey();
        }
    }
}
