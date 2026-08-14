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

import java.io.IOException;
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
        this.allocator =
                ArrowMemoryUtils.rootAllocator.newChildAllocator(
                        allocatorName, 32 * 1024 * 1024, Long.MAX_VALUE);
        this.provider = new CDataDictionaryProvider();

        libLakeSoulIO = JnrLoader.get();

        boolReferenceManager = Runtime.getRuntime(libLakeSoulIO).newObjectReferenceManager();
        intReferenceManager = Runtime.getRuntime(libLakeSoulIO).newObjectReferenceManager();
        ioConfigBuilder = libLakeSoulIO.new_lakesoul_io_config_builder();
        tokioRuntimeBuilder = libLakeSoulIO.new_tokio_runtime_builder();

        if (ioConfigBuilder == null) {
            throw new RuntimeException("Failed to create native IO config builder");
        }
        if (tokioRuntimeBuilder == null) {
            throw new RuntimeException("Failed to create native tokio runtime builder");
        }

        fixedBuffer = getRuntime().getMemoryManager().allocateDirect(5000L);
        mutableBuffer = getRuntime().getMemoryManager().allocateDirect(1 << 12);

        try {
            setBatchSize(10240);
            setThreadNum(2);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize native IO builder defaults", e);
        }
        libLakeSoulIO.rust_logger_init();
    }

    protected Runtime getRuntime() {
        return Runtime.getRuntime(libLakeSoulIO);
    }

    public ObjectReferenceManager<IntegerCallback> getIntReferenceManager() {
        return intReferenceManager;
    }

    public ObjectReferenceManager<BooleanCallback> getBoolReferenceManager() {
        return boolReferenceManager;
    }

    public void setExternalAllocator(BufferAllocator allocator) {
        if (this.allocator != null && this.allocator != allocator) {
            this.allocator.close();
        }
        this.allocator = allocator;
    }

    /**
     * Check that a builder pointer is non-null after a native call. On null, the builder is in an
     * undefined state (the native operation panicked and the original allocation may have leaked).
     * We null our reference to avoid use-after-free and throw.
     */
    protected Pointer requireBuilderNonNull(Pointer result, String operation) throws IOException {
        if (result == null) {
            ioConfigBuilder = null;
            throw new IOException(
                    "Native IO builder operation returned null (native panic): " + operation);
        }
        return result;
    }

    /** Check that a pointer returned from a native call is non-null. */
    protected static Pointer requireNonNull(Pointer ptr, String operation) throws IOException {
        if (ptr == null) {
            throw new IOException("Native IO operation returned null: " + operation);
        }
        return ptr;
    }

    public void addFile(String file) throws IOException {
        ioConfigBuilder =
                requireBuilderNonNull(
                        libLakeSoulIO.lakesoul_config_builder_add_single_file(
                                ioConfigBuilder, file),
                        "addFile");
    }

    public void withPrefix(String prefix) throws IOException {
        ioConfigBuilder =
                requireBuilderNonNull(
                        libLakeSoulIO.lakesoul_config_builder_with_prefix(ioConfigBuilder, prefix),
                        "withPrefix");
    }

    public void addColumn(String column) throws IOException {
        assert ioConfigBuilder != null;
        ioConfigBuilder =
                requireBuilderNonNull(
                        libLakeSoulIO.lakesoul_config_builder_add_single_column(
                                ioConfigBuilder, column),
                        "addColumn");
    }

    public void setPrimaryKeys(Iterable<String> primaryKeys) throws IOException {
        for (String pk : primaryKeys) {
            ioConfigBuilder =
                    requireBuilderNonNull(
                            libLakeSoulIO.lakesoul_config_builder_add_single_primary_key(
                                    ioConfigBuilder, pk),
                            "addPrimaryKey");
        }
    }

    public void setRangePartitions(Iterable<String> rangePartitions) throws IOException {
        for (String col : rangePartitions) {
            ioConfigBuilder =
                    requireBuilderNonNull(
                            libLakeSoulIO.lakesoul_config_builder_add_single_range_partition(
                                    ioConfigBuilder, col),
                            "addRangePartition");
        }
    }

    public void setSchema(Schema schema) throws IOException {
        assert ioConfigBuilder != null;
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
        CDataDictionaryProvider tmpProvider = new CDataDictionaryProvider();
        Data.exportSchema(allocator, schema, tmpProvider, ffiSchema);
        Pointer result =
                libLakeSoulIO.lakesoul_config_builder_set_schema(
                        ioConfigBuilder, ffiSchema.memoryAddress());
        tmpProvider.close();
        ffiSchema.close();
        ioConfigBuilder = requireBuilderNonNull(result, "setSchema");
    }

    public void setPartitionSchema(Schema schema) throws IOException {
        assert ioConfigBuilder != null;
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
        CDataDictionaryProvider tmpProvider = new CDataDictionaryProvider();
        Data.exportSchema(allocator, schema, tmpProvider, ffiSchema);
        Pointer result =
                libLakeSoulIO.lakesoul_config_builder_set_partition_schema(
                        ioConfigBuilder, ffiSchema.memoryAddress());
        tmpProvider.close();
        ffiSchema.close();
        ioConfigBuilder = requireBuilderNonNull(result, "setPartitionSchema");
    }

    public void setThreadNum(int threadNum) throws IOException {
        assert ioConfigBuilder != null;
        ioConfigBuilder =
                requireBuilderNonNull(
                        libLakeSoulIO.lakesoul_config_builder_set_thread_num(
                                ioConfigBuilder, threadNum),
                        "setThreadNum");
    }

    public void useDynamicPartition(boolean enable) throws IOException {
        assert ioConfigBuilder != null;
        ioConfigBuilder =
                requireBuilderNonNull(
                        libLakeSoulIO.lakesoul_config_builder_set_dynamic_partition(
                                ioConfigBuilder, enable),
                        "useDynamicPartition");
    }

    public void setInferringSchema(boolean enable) throws IOException {
        assert ioConfigBuilder != null;
        ioConfigBuilder =
                requireBuilderNonNull(
                        libLakeSoulIO.lakesoul_config_builder_set_inferring_schema(
                                ioConfigBuilder, enable),
                        "setInferringSchema");
    }

    public void setBatchSize(int batchSize) throws IOException {
        assert ioConfigBuilder != null;
        ioConfigBuilder =
                requireBuilderNonNull(
                        libLakeSoulIO.lakesoul_config_builder_set_batch_size(
                                ioConfigBuilder, batchSize),
                        "setBatchSize");
    }

    public void setBufferSize(int bufferSize) throws IOException {
        assert ioConfigBuilder != null;
        ioConfigBuilder =
                requireBuilderNonNull(
                        libLakeSoulIO.lakesoul_config_builder_set_buffer_size(
                                ioConfigBuilder, bufferSize),
                        "setBufferSize");
    }

    public void setObjectStoreOptions(
            String accessKey,
            String accessSecret,
            String region,
            String bucketName,
            String endpoint,
            String signer,
            String user,
            String defaultFS,
            boolean virtual_path_style)
            throws IOException {
        setObjectStoreOption("fs.s3a.access.key", accessKey);
        setObjectStoreOption("fs.s3a.secret.key", accessSecret);
        setObjectStoreOption("fs.s3a.endpoint.region", region);
        setObjectStoreOption("fs.s3a.bucket", bucketName);
        setObjectStoreOption("fs.s3a.endpoint", endpoint);
        setObjectStoreOption("fs.defaultFS", defaultFS);
        setObjectStoreOption("fs.hdfs.user", user);
        setObjectStoreOption("fs.s3a.path.style.access", String.valueOf(virtual_path_style));
        setObjectStoreOption("fs.s3a.s3.signing-algorithm", signer);
    }

    public void setObjectStoreOption(String key, String value) throws IOException {
        assert ioConfigBuilder != null;
        if (key != null && value != null) {
            ioConfigBuilder =
                    requireBuilderNonNull(
                            libLakeSoulIO.lakesoul_config_builder_set_object_store_option(
                                    ioConfigBuilder, key, value),
                            "setObjectStoreOption");
        }
    }

    public void setOption(String key, String value) throws IOException {
        assert ioConfigBuilder != null;
        if (key != null && value != null) {
            ioConfigBuilder =
                    requireBuilderNonNull(
                            libLakeSoulIO.lakesoul_config_builder_set_option(
                                    ioConfigBuilder, key, value),
                            "setOption");
        }
    }

    @Override
    public void close() throws Exception {
        if (ioConfigBuilder != null) {
            libLakeSoulIO.free_lakesoul_io_config_builder(ioConfigBuilder);
            ioConfigBuilder = null;
        }
        if (tokioRuntimeBuilder != null) {
            libLakeSoulIO.free_tokio_runtime_builder(tokioRuntimeBuilder);
            tokioRuntimeBuilder = null;
        }
        if (config != null) {
            libLakeSoulIO.free_lakesoul_io_config(config);
            config = null;
        }
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

        public BooleanCallback(
                BiConsumer<Boolean, String> callback,
                ObjectReferenceManager<BooleanCallback> referenceManager) {
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
                System.err.println(
                        "[ERROR][com.dmetasoul.lakesoul.io.lakesoul.NativeIOBase.BooleanCallback.invoke]"
                                + err);
            }
            callback.accept(status, err);
            removerReferenceKey();
        }
    }

    public static final class IntegerCallback implements LibLakeSoulIO.IntegerCallback {

        public BiConsumer<Integer, String> callback;
        private Pointer key;
        private final ObjectReferenceManager<IntegerCallback> referenceManager;

        public IntegerCallback(
                BiConsumer<Integer, String> callback,
                ObjectReferenceManager<IntegerCallback> referenceManager) {
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
                System.err.println(
                        "[ERROR][com.dmetasoul.lakesoul.io.lakesoul.NativeIOBase.IntegerCallback.invoke]"
                                + err);
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
