// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.jnr;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.dao.NamespaceDao;
import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.google.protobuf.InvalidProtocolBufferException;
import jnr.ffi.ObjectReferenceManager;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.provider.MemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

public class NativeMetadataJavaClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeMetadataJavaClient.class);
    private static final String TEXT_SPLITTER = "____";

    private final long timeout;

    private Pointer nativeClient = null;

    protected final LibLakeSoulMetaData libLakeSoulMetaData;

    protected final ObjectReferenceManager<LibLakeSoulMetaData.BooleanCallback> booleanCallbackObjectReferenceManager;

    protected final ObjectReferenceManager<LibLakeSoulMetaData.BytesCallback> bytesCallbackObjectReferenceManager;

    protected final ObjectReferenceManager<LibLakeSoulMetaData.IntegerCallback> integerCallbackObjectReferenceManager;

    private static NativeMetadataJavaClient instance = null;

    public NativeMetadataJavaClient() {
        this(5000L);
    }

    public NativeMetadataJavaClient(long timeout) {
        this.timeout = timeout;
        libLakeSoulMetaData = JnrLoader.get();
        booleanCallbackObjectReferenceManager = Runtime.getRuntime(libLakeSoulMetaData).newObjectReferenceManager();
        bytesCallbackObjectReferenceManager = Runtime.getRuntime(libLakeSoulMetaData).newObjectReferenceManager();
        integerCallbackObjectReferenceManager = Runtime.getRuntime(libLakeSoulMetaData).newObjectReferenceManager();

    }

    public static NativeMetadataJavaClient getInstance() {
        if (instance == null) {
            instance = new NativeMetadataJavaClient();
            instance.initialize();
        }
        return instance;
    }

    public Pointer getNativeClient() {
        return nativeClient;
    }

    public ObjectReferenceManager<LibLakeSoulMetaData.BooleanCallback> getbooleanCallbackObjectReferenceManager() {
        return booleanCallbackObjectReferenceManager;
    }

    public ObjectReferenceManager<LibLakeSoulMetaData.BytesCallback> getBytesCallbackObjectReferenceManager() {
        return bytesCallbackObjectReferenceManager;
    }

    public ObjectReferenceManager<LibLakeSoulMetaData.IntegerCallback> getIntegerCallbackObjectReferenceManager() {
        return integerCallbackObjectReferenceManager;
    }

    public Runtime getRuntime() {
        return Runtime.getRuntime(libLakeSoulMetaData);
    }

    public LibLakeSoulMetaData getLibLakeSoulMetaData() {
        return libLakeSoulMetaData;
    }

    static class ReferencedBooleanCallback implements LibLakeSoulMetaData.BooleanCallback {
        public final BiConsumer<Boolean, String> callback;
        private final Pointer key;
        private final ObjectReferenceManager<LibLakeSoulMetaData.BooleanCallback> referenceManager;

        public ReferencedBooleanCallback(BiConsumer<Boolean, String> callback) {
            this.callback = callback;
            this.referenceManager = NativeMetadataJavaClient.getInstance().getbooleanCallbackObjectReferenceManager();
            key = referenceManager.add(this);
        }

        @Override
        public void invoke(Boolean result, String msg) {
            callback.accept(result, msg);
            close();
        }

        public void close() {
            if (key != null) {
                referenceManager.remove(key);
            }
        }
    }

    static class ReferencedIntegerCallback implements LibLakeSoulMetaData.IntegerCallback {
        public final BiConsumer<Integer, String> callback;
        private final Pointer key;
        private final ObjectReferenceManager<LibLakeSoulMetaData.IntegerCallback> referenceManager;

        public ReferencedIntegerCallback(BiConsumer<Integer, String> callback) {
            this.callback = callback;
            this.referenceManager = NativeMetadataJavaClient.getInstance().getIntegerCallbackObjectReferenceManager();
            key = referenceManager.add(this);
        }

        @Override
        public void invoke(Integer result, String msg) {
            callback.accept(result, msg);
            close();
        }

        public void close() {
            if (key != null) {
                referenceManager.remove(key);
            }
        }
    }

    static class ReferencedBytesCallback implements LibLakeSoulMetaData.BytesCallback, AutoCloseable {
        public final BiConsumer<byte[], String> callback;
        private final Pointer key;
        private final ObjectReferenceManager<LibLakeSoulMetaData.BytesCallback> referenceManager;

        public ReferencedBytesCallback(BiConsumer<byte[], String> callback) {
            this.callback = callback;
            this.referenceManager = NativeMetadataJavaClient.getInstance().getBytesCallbackObjectReferenceManager();
            key = referenceManager.add(this);
        }

        @Override
        public void invoke(byte[] result, String msg) {
            callback.accept(result, msg);
            close();
        }

        @Override
        public void close() {
            if (key != null) {
                referenceManager.remove(key);
            }
        }
    }


    private void initialize() {
        DataBaseProperty dataBaseProperty = DBUtil.getDBInfo();
        String config = "host=127.0.0.1 port=5433 dbname=test_lakesoul_meta user=yugabyte password=yugabyte";
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        nativeClient = libLakeSoulMetaData.create_native_client(
                new ReferencedBooleanCallback((bool, msg) -> {
                    if (msg.isEmpty()) {
                        future.complete(bool);
                    } else {
                        future.completeExceptionally(new SQLException(msg));
                    }
                }),

                config
        );
        try {
            future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("Configuring postgres with {} timeout", dataBaseProperty);
            throw new RuntimeException(e);
        }
    }

    public static CompletableFuture<String> asyncRequest(String url) {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        CompletableFuture<String> stringCompletableFuture = CompletableFuture.supplyAsync(() -> {
            return "a";
        });
        return completableFuture;
    }

    private void withConfig(DataBaseProperty dataBaseProperty) {

    }


    public static JniWrapper executeQuery(String queryType, List<String> textList) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();

        Pointer addr = getInstance().getRuntime().getMemoryManager().allocateDirect(128);
        getInstance().getLibLakeSoulMetaData().execute_query(
                new ReferencedIntegerCallback((result, msg) -> {
                    if (msg.isEmpty()) {
                        future.complete(result);
                    } else {
                        if (msg.equals("entity not found")) {
                            future.complete(-1);
                        }
                        future.completeExceptionally(new SQLException(msg));
                    }
                }),
                instance.getNativeClient(),
                queryType,
                String.join(TEXT_SPLITTER, textList),
                TEXT_SPLITTER,
                addr.address()
        );
        try {
            Integer len = future.get(instance.timeout, TimeUnit.MILLISECONDS);
            if (len < 0) return null;
            byte[] bytes = new byte[len];
            addr.get(0, bytes, 0, len);
            try {
                return JniWrapper.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("Execute Query {} with {} timeout", queryType, textList);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
    }
    
}
