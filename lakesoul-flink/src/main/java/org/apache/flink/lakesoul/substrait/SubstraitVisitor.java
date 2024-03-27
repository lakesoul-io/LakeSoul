package org.apache.flink.lakesoul.substrait;

import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import com.google.common.collect.ImmutableMap;
import io.substrait.expression.*;
import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiFunction;


/**
 * return null means cannot convert
 */
public class SubstraitVisitor implements ExpressionVisitor<Expression> {

    public SubstraitVisitor(Schema arrowSchema) {
        this.arrowSchema = arrowSchema;
    }


    private static final Logger LOG = LoggerFactory.getLogger(SubstraitVisitor.class);

    private Schema arrowSchema;

    @Override
    public Expression visit(CallExpression call) {
        CallExprVisitor callVisitor = new CallExprVisitor(this.arrowSchema);
        return callVisitor.visit(call);
    }

    @Override
    public Expression visit(ValueLiteralExpression valueLiteral) {
        return new LiteralVisitor().visit(valueLiteral);
    }

    @Override
    public Expression visit(FieldReferenceExpression fieldReference) {
        return new FieldRefVisitor(this.arrowSchema).visit(fieldReference);
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
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                long micros = 0;
                if (value != null) {
                    if (value instanceof LocalDateTime) {
                        value = Timestamp.valueOf((LocalDateTime) value);
                    }
                    if (value instanceof Timestamp || value instanceof Instant) {
                        micros = DateTimeUtils$.MODULE$.anyToMicros(value);
                    } else {
                        LOG.warn("Timestamp filter push down not supported");
                        return null;
                    }
                }
                return ExpressionCreator.timestamp(nullable, micros);
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
    public FieldRefVisitor(Schema arrow_schema) {
        this.arrow_schema = arrow_schema;
    }

    private Schema arrow_schema;

    private static final Logger LOG = LoggerFactory.getLogger(FieldRefVisitor.class);

    public FieldReference visit(FieldReferenceExpression fieldReference) {
        // only care about the last name
        // may fail?
        while (!fieldReference.getChildren().isEmpty()) {
            fieldReference = (FieldReferenceExpression) fieldReference.getChildren().get(0);
        }
        LogicalType logicalType = fieldReference.getOutputDataType().getLogicalType();
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        Type type = mapType(typeRoot, logicalType.isNullable());
        if (type == null) {
            // not supported
            return null;
        }
        String name = fieldReference.getName();
        int idx = 0;
        List<Field> fields = arrow_schema.getFields();
        // find idx
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(name)) {
                idx = i;
                break;
            }
        }

        return FieldReference.builder()
                .type(Objects.requireNonNull(type))
                .addSegments(
                        ImmutableStructField.builder()
                                .offset(idx)
                                .build()
                )
                .build();
    }

    @Override
    protected FieldReference defaultMethod(org.apache.flink.table.expressions.Expression expression) {
        return null;
    }

    public static Type mapType(LogicalTypeRoot typeRoot, Boolean nullable) {
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
            case DATE: {
                return R.DATE;
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                return R.TIMESTAMP;
            }
            default:
                LOG.info("unsupported type");
        }
        return null;
    }


}

class CallExprVisitor extends ExpressionDefaultVisitor<Expression> {

    public CallExprVisitor(Schema arrowSchema) {
        this.arrowSchema = arrowSchema;
    }

    private Schema arrowSchema;
    private static final Logger LOG = LoggerFactory.getLogger(CallExprVisitor.class);
    private static final ImmutableMap<FunctionDefinition, BiFunction<CallExpression, Schema, Expression>>
            FILTERS =
            new ImmutableMap.Builder<
                    FunctionDefinition, BiFunction<CallExpression, Schema, Expression>>()
                    .put(BuiltInFunctionDefinitions.IS_NULL, (call, schema) -> makeUnaryFunction(call, schema, "is_null:any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.IS_NOT_NULL, (call, schema) -> makeUnaryFunction(call, schema, "is_not_null:any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.NOT, (call, schema) -> makeUnaryFunction(call, schema, "not:bool", SubstraitUtil.BooleanNamespace))
                    .put(BuiltInFunctionDefinitions.OR, (call, schema) -> makeBinaryFunction(call, schema, "or:bool", SubstraitUtil.BooleanNamespace))
                    .put(BuiltInFunctionDefinitions.AND, (call, schema) -> makeBinaryFunction(call, schema, "and:bool", SubstraitUtil.BooleanNamespace))
                    .put(BuiltInFunctionDefinitions.EQUALS, (call, schema) -> makeBinaryFunction(call, schema, "equal:any_any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.NOT_EQUALS, (call, schema) -> makeBinaryFunction(call, schema, "not_equal:any_any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.GREATER_THAN, (call, schema) -> makeBinaryFunction(call, schema, "gt:any_any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, (call, schema) -> makeBinaryFunction(call, schema, "gte:any_any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.LESS_THAN, (call, schema) -> makeBinaryFunction(call, schema, "lt:any_any", SubstraitUtil.CompNamespace))
                    .put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, (call, schema) -> makeBinaryFunction(call, schema, "lte:any_any", SubstraitUtil.CompNamespace))
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
        return FILTERS.get(call.getFunctionDefinition()).apply(call, this.arrowSchema);
    }

    static Expression makeBinaryFunction(CallExpression call, Schema arrow_schema, String funcKey, String namespace) {
        List<org.apache.flink.table.expressions.Expression> children = call.getChildren();
        assert children.size() == 2;
        SubstraitVisitor visitor = new SubstraitVisitor(arrow_schema);
        Expression left = children.get(0).accept(visitor);
        Expression right = children.get(1).accept(visitor);
        if (left == null || right == null) {
            return null;
        }
        SimpleExtension.ScalarFunctionVariant func = SubstraitUtil.Se.getScalarFunction(SimpleExtension.FunctionAnchor.of(namespace, funcKey));
        List<Expression> args = new ArrayList<>();
        args.add(left);
        args.add(right);
        return ExpressionCreator.scalarFunction(func, TypeCreator.NULLABLE.BOOLEAN, args);
    }

    static Expression makeUnaryFunction(CallExpression call, Schema arrow_schema, String funcKey, String namespace) {
        List<org.apache.flink.table.expressions.Expression> children = call.getChildren();
        assert children.size() == 1;
        SubstraitVisitor visitor = new SubstraitVisitor(arrow_schema);
        Expression child = children.get(0).accept(visitor);
        if (child == null) {
            return null;
        }
        SimpleExtension.ScalarFunctionVariant func = SubstraitUtil.Se.getScalarFunction(SimpleExtension.FunctionAnchor.of(namespace, funcKey));
        List<Expression> args = new ArrayList<>();
        args.add(child);
        return ExpressionCreator.scalarFunction(func, TypeCreator.NULLABLE.BOOLEAN, args);
    }

    @Override
    protected Expression defaultMethod(org.apache.flink.table.expressions.Expression expression) {
        return null;
    }
}
