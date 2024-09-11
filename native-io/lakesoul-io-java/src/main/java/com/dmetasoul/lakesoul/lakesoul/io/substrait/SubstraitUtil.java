package com.dmetasoul.lakesoul.lakesoul.io.substrait;

import com.dmetasoul.lakesoul.lakesoul.io.DateTimeUtils;
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
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableMapKey;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.DefaultExtensionCatalog;
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
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.substrait.extension.DefaultExtensionCatalog.*;

public class SubstraitUtil {
    public static final SimpleExtension.ExtensionCollection EXTENSIONS;
    public static final SubstraitBuilder BUILDER;

    public static final String CompNamespace = "/functions_comparison.yaml";
    public static final String BooleanNamespace = "/functions_boolean.yaml";

    public static final Expression CONST_TRUE = ExpressionCreator.bool(false, true);

    public static final Expression CONST_FALSE = ExpressionCreator.bool(false, false);

    public static final Expression CONST_ZERO = ExpressionCreator.i64(false, 0);

    private static final LibLakeSoulIO LIB;

//    private static Pointer BUFFER1;
//    private static Pointer BUFFER2;

    private static final NativeIOBase NATIVE_IO_BASE;

    private static final long TIMEOUT = 2000;
    private static final ReentrantReadWriteLock LOCK;

    static {
        try {
            EXTENSIONS = SimpleExtension.loadDefaults();
            BUILDER = new SubstraitBuilder(EXTENSIONS);
            LIB = JnrLoader.get();
//            BUFFER1 = Runtime.getRuntime(LIB).getMemoryManager().allocateDirect(4096);
//            BUFFER2 = Runtime.getRuntime(LIB).getMemoryManager().allocateDirect(4096);
            LOCK = new ReentrantReadWriteLock();
            NATIVE_IO_BASE = new NativeIOBase("Substrait");
        } catch (IOException e) {
            throw new RuntimeException("load simple extension failed");
        }
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                NATIVE_IO_BASE.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public static Expression and(Expression left, Expression right) {
        return makeBinary(left, right, FUNCTIONS_BOOLEAN, "and:bool", TypeCreator.NULLABLE.BOOLEAN);
    }

    public static Expression or(Expression left, Expression right) {
        return makeBinary(left, right, FUNCTIONS_BOOLEAN, "or:bool", TypeCreator.NULLABLE.BOOLEAN);
    }

    public static Expression not(Expression expression) {
        return makeUnary(expression, FUNCTIONS_BOOLEAN, "not:bool", TypeCreator.NULLABLE.BOOLEAN);
    }

    public static Expression in(Expression expr, List<Expression.Literal> set) {
        List<Expression> eqList = set.stream().map(
                lit -> makeBinary(expr, lit, FUNCTIONS_COMPARISON, "equal:any_any", TypeCreator.NULLABLE.BOOLEAN)
        ).collect(Collectors.toList());
        Expression ret = null;
        for (int i = 0; i < eqList.size(); i++) {
            if (i == 0) {
                ret = eqList.get(i);
            } else {
                ret = or(ret, eqList.get(i));
            }
        }
        return ret;
    }

    public static Expression notIn(Expression expr, List<Expression.Literal> set) {
        List<Expression> notEqList = set.stream().map(
                lit -> makeBinary(expr, lit, FUNCTIONS_COMPARISON, "not_equal:any_any", TypeCreator.NULLABLE.BOOLEAN)
        ).collect(Collectors.toList());
        Expression ret = null;
        for (int i = 0; i < notEqList.size(); i++) {
            if (i == 0) {
                ret = notEqList.get(i);
            } else {
                ret = and(ret, notEqList.get(i));
            }
        }
        return ret;
    }

    public static Expression makeBinary(Expression left, Expression right, String namespace, String funcKey, Type type) {
        SimpleExtension.FunctionAnchor fa = SimpleExtension.FunctionAnchor.of(namespace, funcKey);
        return ExpressionCreator.scalarFunction(EXTENSIONS.getScalarFunction(fa), type, left, right);
    }

    public static Expression makeUnary(Expression expr, String namespace, String funcKey, Type type) {
        SimpleExtension.FunctionAnchor fa = SimpleExtension.FunctionAnchor.of(namespace, funcKey);
        return ExpressionCreator.scalarFunction(EXTENSIONS.getScalarFunction(fa), type, expr);
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

    public static String encodeBase64String(io.substrait.proto.Plan plan) {
        return Base64.getEncoder().encodeToString(plan.toByteArray());
    }

    public static io.substrait.proto.Plan decodeBase64String(String base64) throws InvalidProtocolBufferException {
        return io.substrait.proto.Plan.parseFrom(Base64.getDecoder().decode(base64));
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
        Pointer jniBuffer = Runtime.getRuntime(LIB).getMemoryManager().allocateDirect(jniBytes.length + 1, true);
        jniBuffer.put(0, jniBytes, 0, jniBytes.length);
        jniBuffer.putByte(jniBytes.length, (byte) 0);

        byte[] filterBytes = partitionFilter.toByteArray();
        Pointer filterBuffer = Runtime.getRuntime(LIB).getMemoryManager().allocateDirect(filterBytes.length + 1, true);
        filterBuffer.put(0, filterBytes, 0, filterBytes.length);
        filterBuffer.putByte(filterBytes.length, (byte) 0);
//        tryPutBuffer1(jniBytes);
//        tryPutBuffer2(filterBytes);

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
                    jniBytes.length, jniBuffer.address(),
                    ffiSchema.memoryAddress(),
                    filterBytes.length,
                    filterBuffer.address()
            );
            Integer len = null;
            len = filterFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
            if (len < 0) return null;
            Integer lenWithTail = len + 1;
            Pointer exportBuffer = Runtime.getRuntime(LIB).getMemoryManager().allocateDirect(lenWithTail, true);

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
                    exportBuffer.address()
            );
            Boolean b = importFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
            if (!b) return null;

            byte[] bytes = new byte[len];
            exportBuffer.get(0, bytes, 0, len);
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

    public static FieldReference arrowFieldToSubstraitField(Field field) {
        return FieldReference
                .builder()
                .type(arrowFieldToSubstraitType(field)).addSegments(
                        ImmutableMapKey.of(ExpressionCreator.string(true, field.getName()))
                )
                .build();
    }

    public static Type arrowFieldToSubstraitType(Field field) {
        Type type = null;
        if (field.getType() instanceof ArrowType.Struct) {
            type = TypeCreator
                    .of(field.isNullable())
                    .struct(field
                            .getChildren()
                            .stream()
                            .map(SubstraitUtil::arrowFieldToSubstraitType)
                            .collect(Collectors.toList())
                    );
        } else if (field.getType() instanceof ArrowType.List
                || field.getType() instanceof ArrowType.LargeList
                || field.getType() instanceof ArrowType.FixedSizeList
        ) {
            type = TypeCreator
                    .of(field.isNullable())
                    .list(arrowFieldToSubstraitType(field.getChildren().get(0)));
        } else if (field.getType() instanceof ArrowType.Map) {
            type = TypeCreator
                    .of(field.isNullable())
                    .map(
                            arrowFieldToSubstraitType(field.getChildren().get(0)),
                            arrowFieldToSubstraitType(field.getChildren().get(1))
                    );
        } else {
            type = field.getType().accept(ArrowTypeToSubstraitTypeConverter.of(field.isNullable()));
        }
        return type;
    }

    public static class ArrowTypeToSubstraitTypeConverter
            implements ArrowType.ArrowTypeVisitor<Type> {

        public final TypeCreator typeCreator;

        public static final ArrowTypeToSubstraitTypeConverter NULLABLE = new ArrowTypeToSubstraitTypeConverter(true);
        public static final ArrowTypeToSubstraitTypeConverter REQUIRED = new ArrowTypeToSubstraitTypeConverter(false);

        public static ArrowTypeToSubstraitTypeConverter of(boolean nullability) {
            return nullability ? NULLABLE : REQUIRED;
        }

        public ArrowTypeToSubstraitTypeConverter(boolean nullable) {
            typeCreator = TypeCreator.of(nullable);
        }

        @Override
        public Type visit(ArrowType.Null aNull) {
            return null;
        }

        @Override
        public Type visit(ArrowType.Struct struct) {
            return null;
        }

        @Override
        public Type visit(ArrowType.List list) {
            return null;
        }

        @Override
        public Type visit(ArrowType.LargeList largeList) {
            return null;
        }

        @Override
        public Type visit(ArrowType.FixedSizeList fixedSizeList) {
            return null;
        }

        @Override
        public Type visit(ArrowType.Union union) {
            return null;
        }

        @Override
        public Type visit(ArrowType.Map map) {
            return null;
        }

        @Override
        public Type visit(ArrowType.Int anInt) {
            return typeCreator.I32;
        }

        @Override
        public Type visit(ArrowType.FloatingPoint floatingPoint) {
            if (floatingPoint.getPrecision() == FloatingPointPrecision.SINGLE) return typeCreator.FP32;
            if (floatingPoint.getPrecision() == FloatingPointPrecision.DOUBLE) return typeCreator.FP64;
            return null;
        }

        @Override
        public Type visit(ArrowType.Utf8 utf8) {
            return typeCreator.STRING;
        }

        @Override
        public Type visit(ArrowType.LargeUtf8 largeUtf8) {
            return typeCreator.STRING;
        }

        @Override
        public Type visit(ArrowType.Binary binary) {
            return typeCreator.BINARY;
        }

        @Override
        public Type visit(ArrowType.LargeBinary largeBinary) {
            return typeCreator.BINARY;
        }

        @Override
        public Type visit(ArrowType.FixedSizeBinary fixedSizeBinary) {
            return typeCreator.BINARY;
        }

        @Override
        public Type visit(ArrowType.Bool bool) {
            return typeCreator.BOOLEAN;
        }

        @Override
        public Type visit(ArrowType.Decimal decimal) {
            return typeCreator.decimal(decimal.getPrecision(), decimal.getScale());
        }

        @Override
        public Type visit(ArrowType.Date date) {
            return typeCreator.DATE;
        }

        @Override
        public Type visit(ArrowType.Time time) {
            return typeCreator.TIME;
        }

        @Override
        public Type visit(ArrowType.Timestamp timestamp) {
            return timestamp.getTimezone() != null ? typeCreator.TIMESTAMP_TZ : typeCreator.TIMESTAMP;
        }

        @Override
        public Type visit(ArrowType.Interval interval) {
            return interval.getUnit() == IntervalUnit.DAY_TIME ? typeCreator.INTERVAL_DAY : typeCreator.INTERVAL_YEAR;
        }

        @Override
        public Type visit(ArrowType.Duration duration) {
            return null;
        }
    }

    public static Expression.Literal anyToSubstraitLiteral(Type type, Object any) throws IOException {
        if (type instanceof Type.Date) {
            if (any instanceof Integer) {
                return ExpressionCreator.date(false, (Integer) any);
            } else if (any instanceof Date || any instanceof LocalDate) {
                return ExpressionCreator.date(false, DateTimeUtils$.MODULE$.anyToDays(any));
            }
        }
        if (type instanceof Type.Timestamp) {
            if (any instanceof Long) {
                return ExpressionCreator.timestamp(false, (Long) any);
            } else if (any instanceof LocalDateTime || any instanceof Timestamp || any instanceof Instant) {
                return ExpressionCreator.timestamp(false, DateTimeUtils.toMicros(any));
            }
        }
        if (type instanceof Type.TimestampTZ) {
            if (any instanceof Long) {
                return ExpressionCreator.timestampTZ(false, (Long) any);
            } else if (any instanceof LocalDateTime || any instanceof Timestamp || any instanceof Instant) {
                return ExpressionCreator.timestampTZ(false, DateTimeUtils.toMicros(any));
            }
        }

        if (any instanceof String) {
            return ExpressionCreator.string(false, (String) any);
        }
        if (any instanceof Boolean) {
            return ExpressionCreator.bool(false, (Boolean) any);
        }
        if (any instanceof byte[]) {
            return ExpressionCreator.binary(false, (byte[]) any);
        }

        if (any instanceof Byte) {
            return ExpressionCreator.i8(false, (Byte) any);
        }
        if (any instanceof Short) {
            return ExpressionCreator.i16(false, (Short) any);
        }
        if (any instanceof Integer) {
            return ExpressionCreator.i32(false, (Integer) any);
        }
        if (any instanceof Long) {
            return ExpressionCreator.i64(false, (Long) any);
        }
        if (any instanceof Float) {
            return ExpressionCreator.fp32(false, (Float) any);
        }
        if (any instanceof Double) {
            return ExpressionCreator.fp64(false, (Double) any);
        }
        if (type instanceof Type.Decimal || any instanceof BigDecimal) {
            int precision = 10;
            int scale = 0;
            if (type instanceof Type.Decimal) {
                precision = ((Type.Decimal) type).precision();
                scale = ((Type.Decimal) type).scale();
            }
            return ExpressionCreator.decimal(false, (BigDecimal) any, precision, scale);
        }


        throw new IOException("Fail convert to SubstraitLiteral for " + any.toString());
    }

    public static Expression cdcColumnMergeOnReadFilter(Field field) {
        Preconditions.checkArgument(field.getType() instanceof ArrowType.Utf8);
        FieldReference fieldReference = arrowFieldToSubstraitField(field);
        Expression literal = ExpressionCreator.string(false, "delete");
        return makeBinary(fieldReference, literal, DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "not_equal:any_any", TypeCreator.REQUIRED.STRING);
    }
}

