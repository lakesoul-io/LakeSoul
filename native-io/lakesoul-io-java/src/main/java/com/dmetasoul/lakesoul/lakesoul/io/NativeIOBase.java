// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.io;

import com.dmetasoul.lakesoul.lakesoul.io.jnr.JnrLoader;
import com.dmetasoul.lakesoul.lakesoul.io.jnr.LibLakeSoulIO;
import com.dmetasoul.lakesoul.lakesoul.memory.ArrowMemoryUtils;
import jnr.ffi.ObjectReferenceManager;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.function.BiConsumer;

public class NativeIOBase implements AutoCloseable {

    protected Pointer ioConfigBuilder;

    protected Pointer config = null;

    protected Pointer tokioRuntimeBuilder;

    protected Pointer tokioRuntime = null;

    protected final LibLakeSoulIO libLakeSoulIO;

    protected final ObjectReferenceManager<BooleanCallback> boolReferenceManager;

    protected final ObjectReferenceManager<IntegerCallback> intReferenceManager;

    protected BufferAllocator allocator;

    protected CDataDictionaryProvider provider;

    protected Pointer fixedBuffer = null;

    protected Pointer mutableBuffer = null;

    public static boolean isNativeIOLibExist() {
        return JnrLoader.get() != null;
    }

    public NativeIOBase(String allocatorName) {
        this.allocator = ArrowMemoryUtils.rootAllocator.newChildAllocator(allocatorName, 32 * 1024 * 1024, Long.MAX_VALUE);
        this.provider = new CDataDictionaryProvider();

        libLakeSoulIO = JnrLoader.get();

        boolReferenceManager = Runtime.getRuntime(libLakeSoulIO).newObjectReferenceManager();
        intReferenceManager = Runtime.getRuntime(libLakeSoulIO).newObjectReferenceManager();
        ioConfigBuilder = libLakeSoulIO.new_lakesoul_io_config_builder();
        tokioRuntimeBuilder = libLakeSoulIO.new_tokio_runtime_builder();

        fixedBuffer = Runtime.getRuntime(libLakeSoulIO).getMemoryManager().allocateDirect(5000L);
        mutableBuffer = Runtime.getRuntime(libLakeSoulIO).getMemoryManager().allocateDirect(1 << 12);

        setBatchSize(10240);
        setThreadNum(2);
        libLakeSoulIO.rust_logger_init();
    }

    public ObjectReferenceManager<IntegerCallback> getIntReferenceManager() {
        return intReferenceManager;
    }

    public ObjectReferenceManager<BooleanCallback> getBoolReferenceManager() {
        return boolReferenceManager;
    }

    public void setExternalAllocator(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    public void addFile(String file) {
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_file(ioConfigBuilder, file);
    }

    public void withPrefix(String prefix) {
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_with_prefix(ioConfigBuilder, prefix);
    }

    public void addColumn(String column) {
        assert ioConfigBuilder != null;
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_column(ioConfigBuilder, column);
    }

    public void setPrimaryKeys(Iterable<String> primaryKeys) {
        for (String pk : primaryKeys) {
            ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_primary_key(ioConfigBuilder, pk);
        }
    }

    public void setRangePartitions(Iterable<String> rangePartitions) {
        for (String col : rangePartitions) {
            ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_range_partition(ioConfigBuilder, col);
        }
    }

    public void setSchema(Schema schema) {
        assert ioConfigBuilder != null;
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
        CDataDictionaryProvider tmpProvider = new CDataDictionaryProvider();
        Data.exportSchema(allocator, schema, tmpProvider, ffiSchema);
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_schema(ioConfigBuilder, ffiSchema.memoryAddress());
        tmpProvider.close();
        ffiSchema.close();
    }

    public void setPartitionSchema(Schema schema) {
        assert ioConfigBuilder != null;
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
        CDataDictionaryProvider tmpProvider = new CDataDictionaryProvider();
        Data.exportSchema(allocator, schema, tmpProvider, ffiSchema);
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_partition_schema(ioConfigBuilder, ffiSchema.memoryAddress());
        tmpProvider.close();
        ffiSchema.close();
    }

    public void setThreadNum(int threadNum) {
        assert ioConfigBuilder != null;
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_thread_num(ioConfigBuilder, threadNum);
    }

    public void useDynamicPartition(boolean enable) {
        assert ioConfigBuilder != null;
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_dynamic_partition(ioConfigBuilder, enable);
    }

    public void setInferringSchema(boolean enable) {
        assert ioConfigBuilder != null;
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_inferring_schema(ioConfigBuilder, enable);
    }

    public void setBatchSize(int batchSize) {
        assert ioConfigBuilder != null;
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_batch_size(ioConfigBuilder, batchSize);
    }

    public void setBufferSize(int bufferSize) {
        assert ioConfigBuilder != null;
        ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_buffer_size(ioConfigBuilder, bufferSize);
    }

    public void setObjectStoreOptions(String accessKey, String accessSecret,
                                      String region, String bucketName, String endpoint,
                                      String user, String defaultFS,
                                      boolean virtual_path_style) {
        setObjectStoreOption("fs.s3a.access.key", accessKey);
        setObjectStoreOption("fs.s3a.secret.key", accessSecret);
        setObjectStoreOption("fs.s3a.endpoint.region", region);
        setObjectStoreOption("fs.s3a.bucket", bucketName);
        setObjectStoreOption("fs.s3a.endpoint", endpoint);
        setObjectStoreOption("fs.defaultFS", defaultFS);
        setObjectStoreOption("fs.hdfs.user", user);
        setObjectStoreOption("fs.s3a.path.style.access", String.valueOf(virtual_path_style));
    }

    public void setObjectStoreOption(String key, String value) {
        assert ioConfigBuilder != null;
        if (key != null && value != null) {
            ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_object_store_option(ioConfigBuilder, key, value);
        }
    }

    @Override
    public void close() throws Exception {
        if (tokioRuntime != null) {
            libLakeSoulIO.free_tokio_runtime(tokioRuntime);
            tokioRuntime = null;
        }
        if (provider != null) {
            provider.close();
            provider = null;
        }
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    public static final class BooleanCallback implements LibLakeSoulIO.BooleanCallback {

        public BiConsumer<Boolean, String> callback;
        private Pointer key;
        private final ObjectReferenceManager<BooleanCallback> referenceManager;

        public BooleanCallback(BiConsumer<Boolean, String> callback, ObjectReferenceManager<BooleanCallback> referenceManager) {
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
        public void invoke(Boolean status, String err) {
            if (err != null) {
                System.err.println("[ERROR][com.dmetasoul.lakesoul.io.lakesoul.NativeIOBase.BooleanCallback.invoke]" + err);
            }
            callback.accept(status, err);
            removerReferenceKey();
        }
    }

    public static final class IntegerCallback implements LibLakeSoulIO.IntegerCallback {

        public BiConsumer<Integer, String> callback;
        private Pointer key;
        private final ObjectReferenceManager<IntegerCallback> referenceManager;

        public IntegerCallback(BiConsumer<Integer, String> callback, ObjectReferenceManager<IntegerCallback> referenceManager) {
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
        public void invoke(Integer status, String err) {
            if (err != null) {
                System.err.println("[ERROR][com.dmetasoul.lakesoul.io.lakesoul.NativeIOBase.IntegerCallback.invoke]" + err);
            }
            callback.accept(status, err);
            removerReferenceKey();
        }
    }


    public BufferAllocator getAllocator() {
        return allocator;
    }

    public CDataDictionaryProvider getProvider() {
        return provider;
    }
}
