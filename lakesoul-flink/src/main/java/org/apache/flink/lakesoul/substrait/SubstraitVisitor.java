package org.apache.flink.lakesoul.substrait;

import com.dmetasoul.lakesoul.lakesoul.io.DateTimeUtils;
import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import com.google.common.collect.ImmutableMap;
import io.substrait.expression.*;
import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;


/**
 * return null means cannot convert
 */
public class SubstraitVisitor implements ExpressionVisitor<Expression> {
    private static final Logger LOG = LoggerFactory.getLogger(SubstraitVisitor.class);

    @Override
    public Expression visit(CallExpression call) {
        CallExprVisitor callVisitor = new CallExprVisitor();
        return callVisitor.visit(call);
    }

    @Override
    public Expression visit(ValueLiteralExpression valueLiteral) {
        return new LiteralVisitor().visit(valueLiteral);
    }

    @Override
    public Expression visit(FieldReferenceExpression fieldReference) {
        return new FieldRefVisitor().visit(fieldReference);
    }

    @Override
    public Expression visit(TypeLiteralExpression typeLiteral) {
        LOG.error("not supported");
        return null;
    }

    @Override
    public Expression visit(org.apache.flink.table.expressions.Expression other) {
        if (other instanceof CallExpression) {
            return this.visit((CallExpression) other);
        } else if (other instanceof ValueLiteralExpression) {
            return this.visit((ValueLiteralExpression) other);
        } else if (other instanceof FieldReferenceExpression) {
            return this.visit((FieldReferenceExpression) other);
        } else if (other instanceof TypeLiteralExpression) {
            return this.visit((TypeLiteralExpression) other);
        } else {
            LOG.info("not supported");
            return null;
        }
    }
}


class LiteralVisitor extends ExpressionDefaultVisitor<Expression.Literal> {
    private static final Logger LOG = LoggerFactory.getLogger(LiteralVisitor.class);

    @Override
    public Expression.Literal visit(ValueLiteralExpression valueLiteral) {
        DataType dataType = valueLiteral.getOutputDataType();
        LogicalType logicalType = dataType.getLogicalType();
        Optional<?> valueAs = valueLiteral.getValueAs(dataType.getConversionClass());
        Object value = null;
        if (valueAs.isPresent()) {
            value = valueAs.get();
        }
        boolean nullable = logicalType.isNullable();
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        switch (typeRoot) {
            case CHAR:
            case VARCHAR: {
                String s = "";
                if (value != null) {
                    s = (String) value;
                }
                return ExpressionCreator.string(nullable, s);
            }
            case BOOLEAN: {
                boolean b = false;
                if (value != null) {
                    b = (Boolean) value;
                }
                return ExpressionCreator.bool(nullable, b);
            }
            case BINARY:
            case VARBINARY: {
                byte[] b = new byte[]{};
                if (value != null) {
                    b = (byte[]) value;
                }
                return ExpressionCreator.binary(nullable, b);
            }
            case TINYINT: {
                byte b = 0;
                if (value != null) {
                    b = (byte) value;
                }
                return ExpressionCreator.i8(nullable, b);
            }
            case SMALLINT: {
                short s = 0;
                if (value != null) {
                    s = (short) value;
                }
                return ExpressionCreator.i16(nullable, s);
            }
            case INTEGER: {
                int i = 0;
                if (value != null) {
                    i = (int) value;
                }
                return ExpressionCreator.i32(nullable, i);

            }
            case BIGINT: {
                long l = 0;
                if (value != null) {
                    l = (long) value;
                }
                return ExpressionCreator.i64(nullable, l);
            }
            case FLOAT: {
                float f = 0.0F;
                if (value != null) {
                    f = (float) value;
                }
                return ExpressionCreator.fp32(nullable, f);
            }
            case DOUBLE: {
                double d = 0.0;
                if (value != null) {
                    d = (double) value;
                }
                return ExpressionCreator.fp64(nullable, d);
            }
            case DECIMAL: {
                BigDecimal bigDecimal = new BigDecimal(0);
                DecimalType dt = (DecimalType) logicalType;
                if (value != null) {
                    bigDecimal = (BigDecimal) value;
                }
                return ExpressionCreator.decimal(nullable, bigDecimal, dt.getPrecision(), dt.getScale());
            }
            case DATE: {
                int days = 0;
                if (value != null) {
                    Object o = value;
                    if (o instanceof Date || o instanceof LocalDate) {
                        days = DateTimeUtils$.MODULE$.anyToDays(o);
                    } else {
                        LOG.info("Date filter push down not supported");
                        return null;
                    }
                }
                return ExpressionCreator.date(nullable, days);
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE: {
                long micros = 0;
                if (value != null) {
                    Long res = DateTimeUtils.toMicros(value);
                    if (res == null) {
                        LOG.warn("Timestamp filter push down not supported");
                        return null;
                    }
                    micros = res;
                }
                return ExpressionCreator.timestamp(nullable, micros);
            }
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                long micros = 0;
                if (value != null) {
                    Long res = DateTimeUtils.toMicros(value);
                    if (res == null) {
                        LOG.warn("Timestamp filter push down not supported");
                        return null;
                    }
                    micros = res;
                }
                return ExpressionCreator.timestampTZ(nullable, micros);
            }
            default:
                LOG.warn("Filter push down not supported");
                break;
        }
        return null;
    }

    @Override
    protected Expression.Literal defaultMethod(org.apache.flink.table.expressions.Expression expression) {
        return null;
    }

}

class FieldRefVisitor extends ExpressionDefaultVisitor<FieldReference> {
    private static final Logger LOG = LoggerFactory.getLogger(FieldRefVisitor.class);

    public FieldReference visit(FieldReferenceExpression fieldReference) {
        // only care about the last name
        // may fail?
        while (!fieldReference.getChildren().isEmpty()) {
            fieldReference = (FieldReferenceExpression) fieldReference.getChildren().get(0);
        }
        LogicalType logicalType = fieldReference.getOutputDataType().getLogicalType();
        Type type = logicalTypeToSubstraitType(logicalType);
        if (type == null) {
            // not supported
            return null;
        }
        String name = fieldReference.getName();
        return FieldReference.builder()
                .type(Objects.requireNonNull(type))
                .addSegments(
                        ImmutableMapKey.of(ExpressionCreator.string(true, name))
                )
                .build();
    }

    @Override
    protected FieldReference defaultMethod(org.apache.flink.table.expressions.Expression expression) {
        return null;
    }

    public static Type logicalTypeToSubstraitType(LogicalType logicalType) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        boolean nullable = logicalType.isNullable();
        TypeCreator R = TypeCreator.of(nullable);
        switch (typeRoot) {
            case CHAR:
            case VARCHAR: {
                // datafusion only support STRING
                return R.STRING;
            }
            case BOOLEAN: {
                return R.BOOLEAN;
            }
            case BINARY:
            case VARBINARY: {
                return R.BINARY;
            }
            case TINYINT:
                return R.I8;
            case SMALLINT:
                return R.I16;
            case INTEGER: {
                return R.I32;
            }
            case BIGINT: {
                return R.I64;
            }
            case FLOAT: {
                return R.FP32;
            }
            case DOUBLE: {
                return R.FP64;
            }
            case DECIMAL: {
                DecimalType dt = (DecimalType) logicalType;
                return R.decimal(dt.getPrecision(), dt.getScale());
            }
            case DATE: {
                return R.DATE;
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE: {
                return R.TIMESTAMP;
            }
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                return R.TIMESTAMP_TZ;
            }
            default:
                String msg = String.format("Unsupported LogicalType %s for LogicalTypeToSubstraitType", typeRoot);
                LOG.info(msg);
                throw new RuntimeException(msg);
        }
    }


}

class CallExprVisitor extends ExpressionDefaultVisitor<Expression> {
    private static final Logger LOG = LoggerFactory.getLogger(CallExprVisitor.class);
    private static final ImmutableMap<FunctionDefinition, Function<CallExpression, Expression>>
            FILTERS =
            new ImmutableMap.Builder<
                    FunctionDefinition, Function<CallExpression, Expression>>()
                    .put(BuiltInFunctionDefinitions.IS_NULL, call -> makeUnaryFunction(call, "is_null:any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.IS_NOT_NULL, call -> makeUnaryFunction(call, "is_not_null:any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.NOT, call -> makeUnaryFunction(call, "not:bool", SubstraitUtil.BooleanNamespace))
                    .put(BuiltInFunctionDefinitions.OR, call -> makeBinaryFunction(call, "or:bool", SubstraitUtil.BooleanNamespace))
                    .put(BuiltInFunctionDefinitions.AND, call -> makeBinaryFunction(call, "and:bool", SubstraitUtil.BooleanNamespace))
                    .put(BuiltInFunctionDefinitions.EQUALS, call -> makeBinaryFunction(call, "equal:any_any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.NOT_EQUALS, call -> makeBinaryFunction(call, "not_equal:any_any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.GREATER_THAN, call -> makeBinaryFunction(call, "gt:any_any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, call -> makeBinaryFunction(call, "gte:any_any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.LESS_THAN, call -> makeBinaryFunction(call, "lt:any_any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, call -> makeBinaryFunction(call, "lte:any_any", SubstraitUtil.CompNamespace))
                    .build();

    @Override
    public Expression visit(CallExpression call) {
        if (FILTERS.get(call.getFunctionDefinition()) == null) {
            // unsupported predicate
            LOG.info(
                    "Unsupported predicate [{}] cannot be pushed into native io.",
                    call);
            return null;
        }
        return FILTERS.get(call.getFunctionDefinition()).apply(call);
    }

    static Expression makeBinaryFunction(CallExpression call, String funcKey, String namespace) {
        List<org.apache.flink.table.expressions.Expression> children = call.getChildren();
        assert children.size() == 2;
        SubstraitVisitor visitor = new SubstraitVisitor();
        Expression left = children.get(0).accept(visitor);
        Expression right = children.get(1).accept(visitor);
        if (left == null || right == null) {
            return null;
        }
        SimpleExtension.ScalarFunctionVariant func = SubstraitUtil.EXTENSIONS.getScalarFunction(SimpleExtension.FunctionAnchor.of(namespace, funcKey));
        List<Expression> args = new ArrayList<>();
        args.add(left);
        args.add(right);
        return ExpressionCreator.scalarFunction(func, TypeCreator.NULLABLE.BOOLEAN, args);
    }

    static Expression makeUnaryFunction(CallExpression call, String funcKey, String namespace) {
        List<org.apache.flink.table.expressions.Expression> children = call.getChildren();
        assert children.size() == 1;
        SubstraitVisitor visitor = new SubstraitVisitor();
        Expression child = children.get(0).accept(visitor);
        if (child == null) {
            return null;
        }
        SimpleExtension.ScalarFunctionVariant func = SubstraitUtil.EXTENSIONS.getScalarFunction(SimpleExtension.FunctionAnchor.of(namespace, funcKey));
        List<Expression> args = new ArrayList<>();
        args.add(child);
        return ExpressionCreator.scalarFunction(func, TypeCreator.NULLABLE.BOOLEAN, args);
    }

    @Override
    protected Expression defaultMethod(org.apache.flink.table.expressions.Expression expression) {
        return null;
    }
}
