package com.dmetasoul.lakesoul.lakesoul.io.substrait;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase;
import com.dmetasoul.lakesoul.lakesoul.io.jnr.JnrLoader;
import com.dmetasoul.lakesoul.lakesoul.io.jnr.LibLakeSoulIO;
import com.dmetasoul.lakesoul.lakesoul.memory.ArrowMemoryUtils;
import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;

import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.NamedScan;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubstraitUtil {
    public static final SimpleExtension.ExtensionCollection EXTENSIONS;
    public static final SubstraitBuilder BUILDER;

    public static final String CompNamespace = "/functions_comparison.yaml";
    public static final String BooleanNamespace = "/functions_boolean.yaml";

    private static final LibLakeSoulIO LIB;

    private static final Pointer BUFFER1;
    private static final Pointer BUFFER2;

    private static final NativeIOBase NATIVE_IO_BASE;

    private static final long TIMEOUT = 2000;
    private static final ReentrantReadWriteLock LOCK;

    static {
        try {
            EXTENSIONS = SimpleExtension.loadDefaults();
            BUILDER = new SubstraitBuilder(EXTENSIONS);
            LIB = JnrLoader.get();
            BUFFER1 = Runtime.getRuntime(LIB).getMemoryManager().allocateDirect(4096);
            BUFFER2 = Runtime.getRuntime(LIB).getMemoryManager().allocateDirect(4096);
            LOCK = new ReentrantReadWriteLock();
            NATIVE_IO_BASE = new NativeIOBase("Substrait");
        } catch (IOException e) {
            throw new RuntimeException("load simple extension failed");
        }
    }

    public static Expression and(Expression left, Expression right) {
        SimpleExtension.FunctionAnchor fa = SimpleExtension.FunctionAnchor.of(BooleanNamespace, "and:bool");
        return ExpressionCreator.scalarFunction(EXTENSIONS.getScalarFunction(fa), TypeCreator.NULLABLE.BOOLEAN, left, right);
    }

    public static Expression not(Expression expression) {
        SimpleExtension.FunctionAnchor fa = SimpleExtension.FunctionAnchor.of(BooleanNamespace, "not:bool");
        return ExpressionCreator.scalarFunction(EXTENSIONS.getScalarFunction(fa), TypeCreator.NULLABLE.BOOLEAN, expression);
    }

    public static io.substrait.proto.Plan substraitExprToProto(Expression e, String tableName) {
        return planToProto(exprToFilter(e, tableName));
    }

    public static Plan exprToFilter(Expression e, String tableName) {
        if (e == null) {
            return null;
        }
        List<String> tableNames = Stream.of(tableName).collect(Collectors.toList());
        List<String> columnNames = new ArrayList<>();
        List<Type> columnTypes = new ArrayList<>();
        NamedScan namedScan = BUILDER.namedScan(tableNames, columnNames, columnTypes);
        namedScan =
                NamedScan.builder()
                        .from(namedScan)
                        .filter(e)
                        .build();


        Plan.Root root = BUILDER.root(namedScan);
        return BUILDER.plan(root);
    }


    public static io.substrait.proto.Expression exprToProto(Expression expr) {
        ExpressionProtoConverter converter = new ExpressionProtoConverter(null, null);
        return expr.accept(converter);
    }


    public static io.substrait.proto.Plan planToProto(Plan plan) {
        if (plan == null) {
            return null;
        }
        return new PlanProtoConverter().toProto(plan);
    }

    public static List<PartitionInfo> applyPartitionFilters(List<PartitionInfo> allPartitionInfo, Schema schema, io.substrait.proto.Plan partitionFilter) {
        if (allPartitionInfo.isEmpty()) {
            return Collections.emptyList();
        }
        if (partitionFilter == null) {
            return allPartitionInfo;
        }
        List<PartitionInfo> resultPartitionInfo = allPartitionInfo;
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(ArrowMemoryUtils.rootAllocator);
        CDataDictionaryProvider tmpProvider = new CDataDictionaryProvider();
        Data.exportSchema(ArrowMemoryUtils.rootAllocator, schema, tmpProvider, ffiSchema);


        JniWrapper jniWrapper = JniWrapper.newBuilder().addAllPartitionInfo(allPartitionInfo).build();

        byte[] jniBytes = jniWrapper.toByteArray();
        BUFFER1.put(0, jniBytes, 0, jniBytes.length);
        BUFFER1.putByte(jniBytes.length, (byte) 0);

        byte[] filterBytes = partitionFilter.toByteArray();
        BUFFER2.put(0, filterBytes, 0, filterBytes.length);
        BUFFER2.putByte(filterBytes.length, (byte) 0);

        try {
            final CompletableFuture<Integer> filterFuture = new CompletableFuture<>();
            Pointer filterResult = LIB.apply_partition_filter(
                    new NativeIOBase.IntegerCallback((resultLen, msg) -> {
                        if (msg == null || msg.isEmpty()) {
                            filterFuture.complete(resultLen);
                        } else {
                            filterFuture.completeExceptionally(new SQLException(msg));
                        }
                    }, NATIVE_IO_BASE.getIntReferenceManager()),
                    jniBytes.length, BUFFER1.address(),
                    ffiSchema.memoryAddress(),
                    filterBytes.length,
                    BUFFER2.address()
            );
            Integer len = null;
            len = filterFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
            if (len < 0) return null;
            Integer lenWithTail = len + 1;

            final CompletableFuture<Boolean> importFuture = new CompletableFuture<>();
            LIB.export_bytes_result(
                    new NativeIOBase.BooleanCallback((result, msg) -> {
                        if (msg == null || msg.isEmpty()) {
                            importFuture.complete(result);
                        } else {
                            importFuture.completeExceptionally(new SQLException(msg));
                        }
                    }, NATIVE_IO_BASE.getBoolReferenceManager()),
                    filterResult,
                    len,
                    BUFFER1.address()
            );
            Boolean b = importFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
            if (!b) return null;

            byte[] bytes = new byte[len];
            BUFFER1.get(0, bytes, 0, len);
            resultPartitionInfo = JniWrapper.parseFrom(bytes).getPartitionInfoList();
            LIB.free_bytes_result(filterResult);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        } finally {
            tmpProvider.close();
            ffiSchema.close();
        }

        return resultPartitionInfo;
    }
}

