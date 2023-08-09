// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.jnr;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.google.protobuf.InvalidProtocolBufferException;
import jnr.ffi.ObjectReferenceManager;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.dmetasoul.lakesoul.meta.jnr.NativeUtils.*;

public class NativeMetadataJavaClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeMetadataJavaClient.class);

    private final long timeout;
    private long bufferSize;

    private long largeBufferSize;

    private Pointer tokioPostgresClient = null;

    private Pointer sharedBuffer = null;

    private Pointer largeSharedBuffer = null;

    private Pointer tokioRuntime = null;

    private Pointer preparedStatement = null;


    protected final LibLakeSoulMetaData libLakeSoulMetaData;

    protected final ObjectReferenceManager<LibLakeSoulMetaData.BooleanCallback> booleanCallbackObjectReferenceManager;

    protected final ObjectReferenceManager<LibLakeSoulMetaData.StringCallback> stringCallbackObjectReferenceManager;

    protected final ObjectReferenceManager<LibLakeSoulMetaData.IntegerCallback> integerCallbackObjectReferenceManager;

    private static NativeMetadataJavaClient instance = null;

    private final ReentrantReadWriteLock lock;

    private static DataBaseProperty dataBaseProperty = null;

    public static void setDataBaseProperty(DataBaseProperty dataBaseProperty) {
        NativeMetadataJavaClient.dataBaseProperty = dataBaseProperty;
    }

    public NativeMetadataJavaClient() {
        this(5000L, 1 << 12, 1 << 16);
    }

    public NativeMetadataJavaClient(long timeout, int bufferSize, int largeBufferSize) {
        this.timeout = timeout;
        libLakeSoulMetaData = JnrLoader.get();
        booleanCallbackObjectReferenceManager = Runtime.getRuntime(libLakeSoulMetaData).newObjectReferenceManager();
        stringCallbackObjectReferenceManager = Runtime.getRuntime(libLakeSoulMetaData).newObjectReferenceManager();
        integerCallbackObjectReferenceManager = Runtime.getRuntime(libLakeSoulMetaData).newObjectReferenceManager();
        this.bufferSize = bufferSize;
        this.largeBufferSize = largeBufferSize;
        sharedBuffer = Runtime.getRuntime(libLakeSoulMetaData).getMemoryManager().allocateDirect(bufferSize);
        largeSharedBuffer = Runtime.getRuntime(libLakeSoulMetaData).getMemoryManager().allocateDirect(largeBufferSize);
        lock = new ReentrantReadWriteLock();
        initialize();
    }

    public static NativeMetadataJavaClient getInstance() {
        if (instance == null) {
            instance = new NativeMetadataJavaClient();
        }
        return instance;
    }


    public Pointer getTokioPostgresClient() {
        return tokioPostgresClient;
    }

    public ObjectReferenceManager<LibLakeSoulMetaData.BooleanCallback> getbooleanCallbackObjectReferenceManager() {
        return booleanCallbackObjectReferenceManager;
    }

    public ObjectReferenceManager<LibLakeSoulMetaData.StringCallback> getStringCallbackObjectReferenceManager() {
        return stringCallbackObjectReferenceManager;
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

        public ReferencedBooleanCallback(BiConsumer<Boolean, String> callback, ObjectReferenceManager<LibLakeSoulMetaData.BooleanCallback> referenceManager) {
            this.callback = callback;
            this.referenceManager = referenceManager;
            key = this.referenceManager.add(this);
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

        public ReferencedIntegerCallback(BiConsumer<Integer, String> callback, ObjectReferenceManager<LibLakeSoulMetaData.IntegerCallback> referenceManager) {
            this.callback = callback;
            this.referenceManager = referenceManager;
            key = this.referenceManager.add(this);
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

    static class ReferencedStringCallback implements LibLakeSoulMetaData.StringCallback, AutoCloseable {
        public final BiConsumer<String, String> callback;
        private final Pointer key;
        private final ObjectReferenceManager<LibLakeSoulMetaData.StringCallback> referenceManager;

        public ReferencedStringCallback(BiConsumer<String, String> callback, ObjectReferenceManager<LibLakeSoulMetaData.StringCallback> referenceManager) {
            this.callback = callback;
            this.referenceManager = referenceManager;
            key = this.referenceManager.add(this);
        }

        @Override
        public void invoke(String result, String msg) {
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
        DataBaseProperty dataBaseProperty = NativeMetadataJavaClient.dataBaseProperty;
        if (dataBaseProperty == null) {
            dataBaseProperty = DBUtil.getDBInfo();
        }
        tokioRuntime = libLakeSoulMetaData.create_tokio_runtime();

        String config = String.format(
                "host=%s port=%s dbname=%s user=%s password=%s",
                dataBaseProperty.getHost(),
                dataBaseProperty.getPort(),
                dataBaseProperty.getDbName(),
                dataBaseProperty.getUsername(),
                dataBaseProperty.getPassword());
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        tokioPostgresClient = libLakeSoulMetaData.create_tokio_postgres_client(
                new ReferencedBooleanCallback((bool, msg) -> {
                    if (msg.isEmpty()) {
                        future.complete(bool);
                    } else {
                        System.err.println(msg);
                        future.completeExceptionally(new IOException(msg));
                    }
                }, getbooleanCallbackObjectReferenceManager()),
                config,
                tokioRuntime
        );
        preparedStatement = libLakeSoulMetaData.create_prepared_statement();
        try {
            future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("Configuring postgres with {} timeout", dataBaseProperty);
            throw new RuntimeException(e);
        }
    }


    public JniWrapper executeQuery(Integer queryType, List<String> params) {
        getReadLock();
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            getLibLakeSoulMetaData().execute_query(
                    new ReferencedIntegerCallback((result, msg) -> {
                        if (msg.isEmpty()) {
                            future.complete(result);
                        } else {
                            future.completeExceptionally(new SQLException(msg));
                        }
                    }, getIntegerCallbackObjectReferenceManager()),
                    tokioRuntime,
                    tokioPostgresClient,
                    preparedStatement,
                    queryType,
                    String.join(PARAM_DELIM, params),
                    queryType < DAO_TYPE_QUERY_LIST_OFFSET ? sharedBuffer.address() : largeSharedBuffer.address()
            );
            Integer len = future.get(timeout, TimeUnit.MILLISECONDS);
            if (len < 0) return null;
            byte[] bytes = new byte[len];
            if (queryType < DAO_TYPE_QUERY_LIST_OFFSET)
                sharedBuffer.get(0, bytes, 0, len);
            else
                largeSharedBuffer.get(0, bytes, 0, len);
            try {
                return JniWrapper.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("Execute Query {} with {} timeout", queryType, params);
            throw new RuntimeException(e);
        } finally {
            unlockReadLock();
        }
    }

    private void getReadLock() {
        lock.readLock().lock();
    }

    private void unlockReadLock() {
        lock.readLock().unlock();
    }

    private void getWriteLock() {
        lock.writeLock().lock();
    }

    private void unlockWriteLock() {
        lock.writeLock().unlock();
    }


    public Integer executeInsert(Integer insertType, JniWrapper jniWrapper) {
        getWriteLock();
        try {
            final CompletableFuture<Integer> future = new CompletableFuture<>();

            byte[] bytes = jniWrapper.toByteArray();
            if (insertType < DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET)
                sharedBuffer.put(0, bytes, 0, bytes.length);
            else
                largeSharedBuffer.put(0, bytes, 0, bytes.length);

            getLibLakeSoulMetaData().execute_insert(
                    new ReferencedIntegerCallback((result, msg) -> {
                        if (msg.isEmpty()) {
                            future.complete(result);
                        } else {
                            future.completeExceptionally(new SQLException(msg));
                        }
                    }, getIntegerCallbackObjectReferenceManager()),
                    tokioRuntime,
                    tokioPostgresClient,
                    preparedStatement,
                    insertType,
                    insertType < DAO_TYPE_TRANSACTION_INSERT_LIST_OFFSET ? sharedBuffer.address() : largeSharedBuffer.address(),
                    bytes.length
            );
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("Execute Insert {} with {} timeout", insertType, jniWrapper);
            throw new RuntimeException(e);
        } finally {
            unlockWriteLock();
        }
    }

    public Integer executeUpdate(Integer updateType, List<String> params) {
        getWriteLock();
        try {
            final CompletableFuture<Integer> future = new CompletableFuture<>();

            getLibLakeSoulMetaData().execute_update(
                    new ReferencedIntegerCallback((result, msg) -> {
                        if (msg.isEmpty()) {
                            future.complete(result);
                        } else {
                            future.completeExceptionally(new SQLException(msg));
                        }
                    }, getIntegerCallbackObjectReferenceManager()),
                    tokioRuntime,
                    tokioPostgresClient,
                    preparedStatement,
                    updateType,
                    String.join(PARAM_DELIM, params)
            );
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("Execute Update {} with {} timeout", updateType, params);
            throw new RuntimeException(e);
        } finally {
            unlockWriteLock();
        }
    }

    public List<String> executeQueryScalar(Integer updateType, List<String> params) {
        getReadLock();
        try {
            final CompletableFuture<String> future = new CompletableFuture<>();

            getLibLakeSoulMetaData().execute_query_scalar(
                    new ReferencedStringCallback((result, msg) -> {
                        if (msg.isEmpty()) {
                            future.complete(result);
                        } else {
                            future.completeExceptionally(new SQLException(msg));
                        }
                    }, getStringCallbackObjectReferenceManager()),
                    tokioRuntime,
                    tokioPostgresClient,
                    preparedStatement,
                    updateType,
                    String.join(PARAM_DELIM, params)
            );
            String result = future.get(timeout, TimeUnit.MILLISECONDS);
            if (result.isEmpty()) return Collections.emptyList();
            return Arrays.stream(result.split(PARAM_DELIM)).collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("Execute Update {} with {} timeout", updateType, params);
            throw new RuntimeException(e);
        } finally {
            unlockReadLock();
        }
    }

    public static Integer insert(NativeUtils.CodedDaoType insertType, JniWrapper jniWrapper) {
        return getInstance().executeInsert(insertType.getCode(), jniWrapper);
    }

    public static JniWrapper query(NativeUtils.CodedDaoType queryType, List<String> params) {
        if (params.size() != queryType.getParamsNum()) {
            throw new RuntimeException("Params Num mismatch for " + queryType.name() + ", params=" + params + " paramsNum=" + params.size());
        }
        return getInstance().executeQuery(queryType.getCode(), params);
    }

    public static Integer update(NativeUtils.CodedDaoType updateType, List<String> params) {
        if (params.size() != updateType.getParamsNum()) {
            throw new RuntimeException("Params Num mismatch for " + updateType.name() + ", params=" + params + " paramsNum=" + params.size());
        }
        return getInstance().executeUpdate(updateType.getCode(), params);
    }

    public static List<String> queryScalar(NativeUtils.CodedDaoType queryScalarType, List<String> params) {
        if (params.size() != queryScalarType.getParamsNum()) {
            throw new RuntimeException("Params Num mismatch for " + queryScalarType.name() + ", params=" + params + " paramsNum=" + params.size());
        }
        return getInstance().executeQueryScalar(queryScalarType.getCode(), params);
    }

    public static int cleanMeta() {
        final CompletableFuture<Integer> future = new CompletableFuture<>();

        NativeMetadataJavaClient instance = getInstance();
        instance.getWriteLock();
        try {
            instance.getLibLakeSoulMetaData().clean_meta_for_test(
                    new ReferencedIntegerCallback((result, msg) -> {
                        if (msg.isEmpty()) {
                            future.complete(result);
                        } else {
                            future.completeExceptionally(new SQLException(msg));
                        }
                    }, instance.getIntegerCallbackObjectReferenceManager()),
                    instance.tokioRuntime,
                    instance.tokioPostgresClient
            );
            return future.get(instance.timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("Clean Meta timeout");
            throw new RuntimeException(e);
        } finally {
            instance.unlockWriteLock();
        }
    }

    @Override
    public void close() {
        if (tokioRuntime != null) {
            libLakeSoulMetaData.free_tokio_runtime(tokioRuntime);
            tokioRuntime = null;
        }
        if (tokioPostgresClient != null) {
            libLakeSoulMetaData.free_tokio_postgres_client(tokioPostgresClient);
            tokioPostgresClient = null;
        }
        if (preparedStatement != null) {
            libLakeSoulMetaData.free_prepared_statement(preparedStatement);
            preparedStatement = null;
        }
    }

    public static void closeAll() {
        if (instance != null) {
            instance.close();
            instance = null;
        }
    }
}
