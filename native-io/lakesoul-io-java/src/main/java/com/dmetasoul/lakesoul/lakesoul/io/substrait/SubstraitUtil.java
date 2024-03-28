package com.dmetasoul.lakesoul.lakesoul.io.substrait;

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
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubstraitUtil {
    public static final SimpleExtension.ExtensionCollection Se;
    public static final SubstraitBuilder Builder;

    public static final String CompNamespace = "/functions_comparison.yaml";
    public static final String BooleanNamespace = "/functions_boolean.yaml";

    static {
        try {
            Se = SimpleExtension.loadDefaults();
            Builder = new SubstraitBuilder(Se);
        } catch (IOException e) {
            throw new RuntimeException("load simple extension failed");
        }
    }



    public static Plan exprToFilter(Expression e, String tableName, Schema arrowSchema) {
        if (e == null) {
            return null;
        }
        List<String> tableNames = Stream.of(tableName).collect(Collectors.toList());
        List<String> columnNames = new ArrayList<>();
        List<Field> fields = arrowSchema.getFields();
        List<Type> columnTypes = new ArrayList<>();
        for (Field field : fields) {
            Type type = fromArrowType(field.getType(), field.isNullable());
            if (type == null) {
                return null;
            }
            columnTypes.add(type);
            String name = field.getName();
            columnNames.add(name);
        }
        NamedScan namedScan = Builder.namedScan(tableNames, columnNames, columnTypes);
        namedScan =
                NamedScan.builder()
                        .from(namedScan)
                        .filter(e)
                        .build();


        Plan.Root root = Builder.root(namedScan);
        return Builder.plan(root);
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



    public static Type fromArrowType(ArrowType arrowType, boolean nullable) {
        TypeCreator R = TypeCreator.of(nullable);
        switch (arrowType.getTypeID()) {
            case Null:
                break;
            case Struct:
                break;
            case List:
                break;
            case LargeList:
                break;
            case FixedSizeList:
                break;
            case Union:
                break;
            case Map:
                break;
            case Int: {
                ArrowType.Int intType = (ArrowType.Int) arrowType;
                if (intType.getIsSigned()) {
                    if (intType.getBitWidth() == 8) {
                        return R.I8;
                    } else if (intType.getBitWidth() == 16) {
                        return R.I16;
                    } else if (intType.getBitWidth() == 32) {
                        return R.I32;
                    } else if (intType.getBitWidth() == 64) {
                        return R.I64;
                    }
                }
                break;
            }
            case FloatingPoint: {
                ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
                if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
                    return R.FP32;
                } else if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
                    return R.FP64;
                }
                break;
            }
            case Utf8: {
                return R.STRING;
            }
            case LargeUtf8:
                break;
            case Binary: {
                return R.BINARY;
            }
            case LargeBinary:
                break;
            case FixedSizeBinary:
                break;
            case Bool: {
                return R.BOOLEAN;
            }
            case Decimal: {
                ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
                return R.decimal(decimalType.getPrecision(), decimalType.getScale());
            }
            case Date: {
                ArrowType.Date dateType = (ArrowType.Date) arrowType;
                if (dateType.getUnit() == DateUnit.DAY) {
                    return R.DATE;
                }
                break;
            }
            case Time:
                break;
            case Timestamp: {
                ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
                if (tsType.getUnit() == TimeUnit.MICROSECOND) {
                    if (tsType.getTimezone() != null) {
                        return R.TIMESTAMP_TZ;
                    }
                    return R.TIMESTAMP;
                }
                break;
            }
            case Interval: {
                ArrowType.Interval intervalType = (ArrowType.Interval) arrowType;
                if (intervalType.getUnit() == IntervalUnit.YEAR_MONTH) {
                    return R.INTERVAL_YEAR;
                }
                break;
            }
            case Duration: {
                ArrowType.Duration durationType = (ArrowType.Duration) arrowType;
                if (durationType.getUnit() == TimeUnit.MICROSECOND) {
                    return R.INTERVAL_DAY;
                }
                break;
            }
            case NONE:
                break;
        }
        // not supported
        return null;
    }
}

