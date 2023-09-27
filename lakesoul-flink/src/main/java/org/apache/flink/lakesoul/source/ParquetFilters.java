// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator5.com.google.common.collect.ImmutableMap;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.function.TriFunction;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;

public class ParquetFilters {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetFilters.class);

    public static Tuple2<SupportsFilterPushDown.Result, FilterPredicate> toParquetFilter(
            List<ResolvedExpression> exprs,
            List<ResolvedExpression> remainingFilters
    ) {
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        FilterPredicate last = null;
        for (ResolvedExpression expr : exprs) {
            FilterPredicate p = toParquetFilter(expr);
            if (p == null) {
                remainingFilters.add(expr);
            } else {
                acceptedFilters.add(expr);
                // AND all the filters to single one
                if (last != null) {
                    last = FilterApi.and(last, p);
                } else {
                    last = p;
                }
            }
        }
        return Tuple2.of(SupportsFilterPushDown.Result.of(acceptedFilters, remainingFilters), last);
    }

    private static FilterPredicate toParquetFilter(Expression expression) {
        if (expression instanceof CallExpression) {
            CallExpression callExp = (CallExpression) expression;
            if (FILTERS.get(callExp.getFunctionDefinition()) == null) {
                // unsupported predicate
                LOG.info(
                        "Unsupported predicate [{}] cannot be pushed into native io.",
                        expression);
                return null;
            }
            return FILTERS.get(callExp.getFunctionDefinition()).apply(callExp);
        } else {
            // unsupported predicate
            LOG.info(
                    "Unsupported predicate [{}] cannot be pushed into native io.",
                    expression);
            return null;
        }
    }

    public static boolean filterContainsPartitionColumn(ResolvedExpression expression, Set<String> partitionCols) {
        if (expression instanceof FieldReferenceExpression) {
            return partitionCols.contains(((FieldReferenceExpression) expression).getName());
        } else if (expression instanceof CallExpression) {
            for (ResolvedExpression child : expression.getResolvedChildren()) {
                if (filterContainsPartitionColumn(child, partitionCols)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static final ImmutableMap<FunctionDefinition, Function<CallExpression, FilterPredicate>>
            FILTERS =
            new ImmutableMap.Builder<
                    FunctionDefinition, Function<CallExpression, FilterPredicate>>()
                    .put(BuiltInFunctionDefinitions.IS_NULL, ParquetFilters::convertIsNull)
                    .put(
                            BuiltInFunctionDefinitions.IS_NOT_NULL,
                            ParquetFilters::convertIsNotNull)
                    .put(BuiltInFunctionDefinitions.NOT, ParquetFilters::convertNot)
                    .put(BuiltInFunctionDefinitions.OR, ParquetFilters::convertOr)
                    .put(BuiltInFunctionDefinitions.AND, ParquetFilters::convertAnd)
                    .put(
                            BuiltInFunctionDefinitions.EQUALS,
                            call ->
                                    convertBinary(
                                            call,
                                            ParquetFilters::makeEquals,
                                            ParquetFilters::makeEquals))
                    .put(
                            BuiltInFunctionDefinitions.NOT_EQUALS,
                            call ->
                                    convertBinary(
                                            call,
                                            ParquetFilters::makeNotEquals,
                                            ParquetFilters::makeNotEquals))
                    .put(
                            BuiltInFunctionDefinitions.GREATER_THAN,
                            call ->
                                    convertBinary(
                                            call,
                                            ParquetFilters::makeGreaterThan,
                                            ParquetFilters::makeLessThanEq))
                    .put(
                            BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                            call ->
                                    convertBinary(
                                            call,
                                            ParquetFilters::makeGreaterThanEq,
                                            ParquetFilters::makeLessThan))
                    .put(
                            BuiltInFunctionDefinitions.LESS_THAN,
                            call ->
                                    convertBinary(
                                            call,
                                            ParquetFilters::makeLessThan,
                                            ParquetFilters::makeGreaterThanEq))
                    .put(
                            BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                            call ->
                                    convertBinary(
                                            call,
                                            ParquetFilters::makeLessThanEq,
                                            ParquetFilters::makeGreaterThan))
                    .build();

    private static FilterPredicate convertIsNull(CallExpression callExp) {
        if (!isUnaryValid(callExp)) {
            // not a valid predicate
            LOG.info(
                    "Unsupported predicate [{}] cannot be pushed into native io.",
                    callExp);
            return null;
        }

        String colName = ((FieldReferenceExpression) callExp.getChildren().get(0)).getName();
        DataType colType = ((FieldReferenceExpression) callExp.getChildren().get(0)).getOutputDataType();
        if (colType == null) {
            // unsupported type
            LOG.info(
                    "Unsupported predicate [{}] cannot be pushed into native io.",
                    callExp);
            return null;
        }

        return convertBinaryLeafOp(colName, new ValueLiteralExpression(null, colType), ParquetFilters::makeEquals);
    }

    private static FilterPredicate convertIsNotNull(CallExpression callExp) {
        if (!isUnaryValid(callExp)) {
            // not a valid predicate
            LOG.info(
                    "Unsupported predicate [{}] cannot be pushed into native io.",
                    callExp);
            return null;
        }

        String colName = ((FieldReferenceExpression) callExp.getChildren().get(0)).getName();
        DataType colType = ((FieldReferenceExpression) callExp.getChildren().get(0)).getOutputDataType();
        if (colType == null) {
            // unsupported type
            LOG.info(
                    "Unsupported predicate [{}] cannot be pushed into native io.",
                    callExp);
            return null;
        }

        return convertBinaryLeafOp(colName, new ValueLiteralExpression(null, colType), ParquetFilters::makeNotEquals);
    }

    private static FilterPredicate convertNot(CallExpression callExp) {
        if (callExp.getChildren().size() != 1) {
            // not a valid predicate
            LOG.info(
                    "Unsupported predicate [{}] cannot be pushed into native io.",
                    callExp);
            return null;
        }

        FilterPredicate child = toParquetFilter(callExp.getChildren().get(0));

        return child == null ? null : FilterApi.not(child);
    }

    private static FilterPredicate convertAnd(CallExpression callExp) {
        if (callExp.getChildren().size() < 2) {
            return null;
        }
        Expression left = callExp.getChildren().get(0);
        Expression right = callExp.getChildren().get(1);

        FilterPredicate c1 = toParquetFilter(left);
        FilterPredicate c2 = toParquetFilter(right);
        if (c1 == null || c2 == null) {
            return null;
        } else {
            return FilterApi.and(c1, c2);
        }
    }

    private static FilterPredicate convertOr(CallExpression callExp) {
        if (callExp.getChildren().size() < 2) {
            return null;
        }
        Expression left = callExp.getChildren().get(0);
        Expression right = callExp.getChildren().get(1);

        FilterPredicate c1 = toParquetFilter(left);
        FilterPredicate c2 = toParquetFilter(right);
        if (c1 == null || c2 == null) {
            return null;
        } else {
            return FilterApi.or(c1, c2);
        }
    }

    private static FilterPredicate convertBinary(CallExpression callExp,
                                                 TriFunction<String, Object, LogicalTypeRoot, FilterPredicate> func,
                                                 TriFunction<String, Object, LogicalTypeRoot, FilterPredicate> reverseFunc
    ) {
        if (!isBinaryValid(callExp)) {
            // not a valid predicate
            LOG.info(
                    "Unsupported binary predicate [{}] cannot be pushed into native io.",
                    callExp);
            return null;
        }

        boolean literalOnRight = literalOnRight(callExp);
        Tuple2<String, ValueLiteralExpression> pair = getColumnNameAndValueLiteral(callExp, literalOnRight);
        if (literalOnRight) {
            return convertBinaryLeafOp(pair.f0, pair.f1, func);
        } else {
            return convertBinaryLeafOp(pair.f0, pair.f1, reverseFunc);
        }
    }

    // since parquet.FilterApi.eq etc. have multiple type bound in generics,
    // we cannot express them in a generic function, hence we have to wrap
    // each of them
    private static FilterPredicate makeEquals(String colName, Object value, LogicalTypeRoot type) {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER: {
                return FilterApi.eq(FilterApi.intColumn(colName), (Integer) value);
            }
            case BIGINT: {
                return FilterApi.eq(FilterApi.longColumn(colName), (Long) value);
            }
            case FLOAT: {
                return FilterApi.eq(FilterApi.floatColumn(colName), (Float) value);
            }
            case DOUBLE: {
                return FilterApi.eq(FilterApi.doubleColumn(colName), (Double) value);
            }
            case BOOLEAN: {
                return FilterApi.eq(FilterApi.booleanColumn(colName), (Boolean) value);
            }
            case CHAR:
            case VARCHAR: {
                return FilterApi.eq(FilterApi.binaryColumn(colName),
                        value == null ? null :
                                Binary.fromString((String) value));
            }
            case BINARY:
            case VARBINARY: {
                return FilterApi.eq(FilterApi.binaryColumn(colName),
                        value == null ? null :
                                Binary.fromReusedByteArray((byte[]) value));
            }
            case DATE: {
                if (value == null) {
                    return FilterApi.eq(FilterApi.intColumn(colName), null);
                }
                if (value instanceof Date || value instanceof LocalDate) {
                    int days = DateTimeUtils$.MODULE$.anyToDays(value);
                    return FilterApi.eq(FilterApi.intColumn(colName), days);
                } else {
                    LOG.info("Date eq filter pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                if (value == null) {
                    return FilterApi.eq(FilterApi.longColumn(colName), null);
                }
                if (value instanceof Timestamp || value instanceof Instant) {
                    long micros = DateTimeUtils$.MODULE$.anyToMicros(value);
                    return FilterApi.eq(FilterApi.longColumn(colName), micros);
                } else {
                    LOG.info("Timestamp eq filter pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            default:
                // currently we do not support other types for filter pushdown
                LOG.info("Eq Filter pushdown not supported: {}, {}, {}", colName, value, type);
                return null;
        }
    }

    private static FilterPredicate makeNotEquals(String colName, Object value, LogicalTypeRoot type) {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER: {
                return FilterApi.notEq(FilterApi.intColumn(colName), (Integer) value);
            }
            case BIGINT: {
                return FilterApi.notEq(FilterApi.longColumn(colName), (Long) value);
            }
            case FLOAT: {
                return FilterApi.notEq(FilterApi.floatColumn(colName), (Float) value);
            }
            case DOUBLE: {
                return FilterApi.notEq(FilterApi.doubleColumn(colName), (Double) value);
            }
            case BOOLEAN: {
                return FilterApi.notEq(FilterApi.booleanColumn(colName), (Boolean) value);
            }
            case CHAR:
            case VARCHAR: {
                return FilterApi.notEq(FilterApi.binaryColumn(colName),
                        value == null ? null :
                                Binary.fromString((String) value));
            }
            case BINARY:
            case VARBINARY: {
                return FilterApi.notEq(FilterApi.binaryColumn(colName),
                        value == null ? null :
                                Binary.fromReusedByteArray((byte[]) value));
            }
            case DATE: {
                if (value == null) {
                    return FilterApi.notEq(FilterApi.intColumn(colName), null);
                }
                if (value instanceof Date || value instanceof LocalDate) {
                    int days = DateTimeUtils$.MODULE$.anyToDays(value);
                    return FilterApi.notEq(FilterApi.intColumn(colName), days);
                } else {
                    LOG.info("Date filter notEq pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                if (value == null) {
                    return FilterApi.notEq(FilterApi.longColumn(colName), null);
                }
                if (value instanceof Timestamp || value instanceof Instant) {
                    long micros = DateTimeUtils$.MODULE$.anyToMicros(value);
                    return FilterApi.notEq(FilterApi.longColumn(colName), micros);
                } else {
                    LOG.info("Timestamp filter notEq pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            default:
                // currently we do not support other types for filter pushdown
                LOG.info("NotEq Filter pushdown not supported: {}, {}, {}", colName, value, type);
                return null;
        }
    }

    private static FilterPredicate makeLessThan(String colName, Object value, LogicalTypeRoot type) {
        // less than does not support comparing null
        if (value == null) {
            LOG.info("LessThan Filter pushdown not supported for null value: {}, {}", colName, type);
            return null;
        }
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER: {
                return FilterApi.lt(FilterApi.intColumn(colName), (Integer) value);
            }
            case BIGINT: {
                return FilterApi.lt(FilterApi.longColumn(colName), (Long) value);
            }
            case FLOAT: {
                return FilterApi.lt(FilterApi.floatColumn(colName), (Float) value);
            }
            case DOUBLE: {
                return FilterApi.lt(FilterApi.doubleColumn(colName), (Double) value);
            }
            case CHAR:
            case VARCHAR: {
                return FilterApi.lt(FilterApi.binaryColumn(colName),
                        Binary.fromString((String) value));
            }
            case BINARY:
            case VARBINARY: {
                return FilterApi.lt(FilterApi.binaryColumn(colName),
                        Binary.fromReusedByteArray((byte[]) value));
            }
            case DATE: {
                if (value instanceof Date || value instanceof LocalDate) {
                    int days = DateTimeUtils$.MODULE$.anyToDays(value);
                    return FilterApi.lt(FilterApi.intColumn(colName), days);
                } else {
                    LOG.info("Date filter lt pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                if (value instanceof Timestamp || value instanceof Instant) {
                    long micros = DateTimeUtils$.MODULE$.anyToMicros(value);
                    return FilterApi.lt(FilterApi.longColumn(colName), micros);
                } else {
                    LOG.info("Timestamp filter lt pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            default:
                // currently we do not support other types for filter pushdown
                LOG.info("LT Filter pushdown not supported: {}, {}, {}", colName, value, type);
                return null;
        }
    }

    private static FilterPredicate makeLessThanEq(String colName, Object value, LogicalTypeRoot type) {
        // less than does not support comparing null
        if (value == null) {
            LOG.info("LessThanOrEqualTo Filter pushdown not supported for null value: {}, {}", colName, type);
            return null;
        }
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER: {
                return FilterApi.ltEq(FilterApi.intColumn(colName), (Integer) value);
            }
            case BIGINT: {
                return FilterApi.ltEq(FilterApi.longColumn(colName), (Long) value);
            }
            case FLOAT: {
                return FilterApi.ltEq(FilterApi.floatColumn(colName), (Float) value);
            }
            case DOUBLE: {
                return FilterApi.ltEq(FilterApi.doubleColumn(colName), (Double) value);
            }
            case CHAR:
            case VARCHAR: {
                return FilterApi.ltEq(FilterApi.binaryColumn(colName),
                        Binary.fromString((String) value));
            }
            case BINARY:
            case VARBINARY: {
                return FilterApi.ltEq(FilterApi.binaryColumn(colName),
                        Binary.fromReusedByteArray((byte[]) value));
            }
            case DATE: {
                if (value instanceof Date || value instanceof LocalDate) {
                    int days = DateTimeUtils$.MODULE$.anyToDays(value);
                    return FilterApi.ltEq(FilterApi.intColumn(colName), days);
                } else {
                    LOG.info("Date filter lteq pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                if (value instanceof Timestamp || value instanceof Instant) {
                    long micros = DateTimeUtils$.MODULE$.anyToMicros(value);
                    return FilterApi.ltEq(FilterApi.longColumn(colName), micros);
                } else {
                    LOG.info("Timestamp filter lteq pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            default:
                // currently we do not support other types for filter pushdown
                LOG.info("LTEQ Filter pushdown not supported: {}, {}, {}", colName, value, type);
                return null;
        }
    }

    private static FilterPredicate makeGreaterThan(String colName, Object value, LogicalTypeRoot type) {
        // less than does not support comparing null
        if (value == null) {
            LOG.info("GreaterThan Filter pushdown not supported for null value: {}, {}", colName, type);
            return null;
        }
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER: {
                return FilterApi.gt(FilterApi.intColumn(colName), (Integer) value);
            }
            case BIGINT: {
                return FilterApi.gt(FilterApi.longColumn(colName), (Long) value);
            }
            case FLOAT: {
                return FilterApi.gt(FilterApi.floatColumn(colName), (Float) value);
            }
            case DOUBLE: {
                return FilterApi.gt(FilterApi.doubleColumn(colName), (Double) value);
            }
            case CHAR:
            case VARCHAR: {
                return FilterApi.gt(FilterApi.binaryColumn(colName),
                        Binary.fromString((String) value));
            }
            case BINARY:
            case VARBINARY: {
                return FilterApi.gt(FilterApi.binaryColumn(colName),
                        Binary.fromReusedByteArray((byte[]) value));
            }
            case DATE: {
                if (value instanceof Date || value instanceof LocalDate) {
                    int days = DateTimeUtils$.MODULE$.anyToDays(value);
                    return FilterApi.gt(FilterApi.intColumn(colName), days);
                } else {
                    LOG.info("Date filter gt pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                if (value instanceof Timestamp || value instanceof Instant) {
                    long micros = DateTimeUtils$.MODULE$.anyToMicros(value);
                    return FilterApi.gt(FilterApi.longColumn(colName), micros);
                } else {
                    LOG.info("Timestamp filter gt pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            default:
                // currently we do not support other types for filter pushdown
                LOG.info("GT Filter pushdown not supported: {}, {}, {}", colName, value, type);
                return null;
        }
    }

    private static FilterPredicate makeGreaterThanEq(String colName, Object value, LogicalTypeRoot type) {
        // less than does not support comparing null
        if (value == null) {
            LOG.info("GreaterThanEquals Filter pushdown not supported for null value: {}, {}", colName, type);
            return null;
        }
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER: {
                return FilterApi.gtEq(FilterApi.intColumn(colName), (Integer) value);
            }
            case BIGINT: {
                return FilterApi.gtEq(FilterApi.longColumn(colName), (Long) value);
            }
            case FLOAT: {
                return FilterApi.gtEq(FilterApi.floatColumn(colName), (Float) value);
            }
            case DOUBLE: {
                return FilterApi.gtEq(FilterApi.doubleColumn(colName), (Double) value);
            }
            case CHAR:
            case VARCHAR: {
                return FilterApi.gtEq(FilterApi.binaryColumn(colName),
                        Binary.fromString((String) value));
            }
            case BINARY:
            case VARBINARY: {
                return FilterApi.gtEq(FilterApi.binaryColumn(colName),
                        Binary.fromReusedByteArray((byte[]) value));
            }
            case DATE: {
                if (value instanceof Date || value instanceof LocalDate) {
                    int days = DateTimeUtils$.MODULE$.anyToDays(value);
                    return FilterApi.gtEq(FilterApi.intColumn(colName), days);
                } else {
                    LOG.info("Date filter gtEq pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                if (value instanceof Timestamp || value instanceof Instant) {
                    long micros = DateTimeUtils$.MODULE$.anyToMicros(value);
                    return FilterApi.gtEq(FilterApi.longColumn(colName), micros);
                } else {
                    LOG.info("Timestamp filter gtEq pushdown not supported: {}, {}", colName, value);
                    return null;
                }
            }
            default:
                // currently we do not support other types for filter pushdown
                LOG.info("gteq Filter pushdown not supported: {}, {}, {}", colName, value, type);
                return null;
        }
    }

    private static FilterPredicate convertBinaryLeafOp(
            String colName, ValueLiteralExpression literal,
            TriFunction<String, Object, LogicalTypeRoot, FilterPredicate> op) {
        Object value = getLiteralValue(literal);
        return op.apply(colName, value, getLiteralType(literal).getLogicalType().getTypeRoot());
    }

    private static Tuple2<String, ValueLiteralExpression> getColumnNameAndValueLiteral(CallExpression comp,
                                                                                       boolean literalOnRight) {
        if (literalOnRight) {
            return Tuple2.of(((FieldReferenceExpression) comp.getChildren().get(0)).getName(),
                    (ValueLiteralExpression) comp.getChildren().get(1));
        } else {
            return Tuple2.of(((FieldReferenceExpression) comp.getChildren().get(1)).getName(),
                    (ValueLiteralExpression) comp.getChildren().get(0));
        }
    }

    private static boolean literalOnRight(CallExpression comp) {
        if (comp.getChildren().size() == 1
                && comp.getChildren().get(0) instanceof FieldReferenceExpression) {
            return true;
        } else if (isLit(comp.getChildren().get(0)) && isRef(comp.getChildren().get(1))) {
            return false;
        } else if (isRef(comp.getChildren().get(0)) && isLit(comp.getChildren().get(1))) {
            return true;
        } else {
            throw new RuntimeException("Invalid binary comparison.");
        }
    }

    private static boolean isRef(Expression expression) {
        return expression instanceof FieldReferenceExpression;
    }

    private static boolean isLit(Expression expression) {
        return expression instanceof ValueLiteralExpression;
    }

    private static DataType getLiteralType(ValueLiteralExpression expression) {
        return expression.getOutputDataType();
    }

    @Nullable
    private static Object getLiteralValue(ValueLiteralExpression valueLiteralExpression) {
        Optional<?> value = valueLiteralExpression.getValueAs(
                valueLiteralExpression.getOutputDataType().getConversionClass());
        return value.orElse(null);
    }

    private static boolean isUnaryValid(CallExpression callExpression) {
        return callExpression.getChildren().size() == 1
                && isRef(callExpression.getChildren().get(0));
    }

    private static boolean isBinaryValid(CallExpression callExpression) {
        return callExpression.getChildren().size() == 2
                && (isRef(callExpression.getChildren().get(0))
                && isLit(callExpression.getChildren().get(1))
                || isLit(callExpression.getChildren().get(0))
                && isRef(callExpression.getChildren().get(1)));
    }
}
