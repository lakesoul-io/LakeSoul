// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.util;

import com.facebook.presto.common.type.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

// modified from prestodb's code presto-base-arrow-flight/src/main/java/com/facebook/plugin/arrow/ArrowBlockBuilder.java
public class TypeConverter {

    private final TypeManager typeManager;

    public TypeConverter(TypeManager typeManager) {
        this.typeManager = typeManager;
    }

    public Type getPrestoTypeFromArrowField(Field field) {
        switch (field.getType().getTypeID()) {
            case Int:
                ArrowType.Int intType = (ArrowType.Int) field.getType();
                return getPrestoTypeForArrowIntType(intType);
            case Binary:
            case LargeBinary:
            case FixedSizeBinary:
                return VarbinaryType.VARBINARY;
            case Date:
                return DateType.DATE;
            case Timestamp:
                return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
            case Utf8:
            case LargeUtf8:
                return VarcharType.VARCHAR;
            case FloatingPoint:
                ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) field.getType();
                return getPrestoTypeForArrowFloatingPointType(floatingPoint);
            case Decimal:
                ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
                return DecimalType.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
            case Bool:
                return BooleanType.BOOLEAN;
            case Time:
                return TimeType.TIME;
            case List: {
                List<Field> children = field.getChildren();
                checkArgument(children.size() == 1,
                        "Arrow List expected to have 1 child Field, got: " + children.size());
                return new ArrayType(getPrestoTypeFromArrowField(field.getChildren().get(0)));
            }
            case Map: {
                List<Field> children = field.getChildren();
                checkArgument(children.size() == 1,
                        "Arrow Map expected to have 1 child Field for entries, got: " + children.size());
                Field entryField = children.get(0);
                checkArgument(entryField.getChildren().size() == 2,
                        "Arrow Map entries expected to have 2 child Fields, got: " + children.size());
                Type keyType = getPrestoTypeFromArrowField(entryField.getChildren().get(0));
                Type valueType = getPrestoTypeFromArrowField(entryField.getChildren().get(1));
                return typeManager.getType(parseTypeSignature(
                        format("map(%s,%s)", keyType.getTypeSignature(), valueType.getTypeSignature())));
            }
            case Struct: {
                List<RowType.Field>
                        children =
                        field.getChildren().stream().map(child -> new RowType.Field(Optional.of(child.getName()),
                                getPrestoTypeFromArrowField(child))).collect(toImmutableList());
                return RowType.from(children);
            }
            default:
                throw new UnsupportedOperationException(
                        "The data type " + field.getType().getTypeID() + " is not supported.");
        }
    }

    private Type getPrestoTypeForArrowFloatingPointType(ArrowType.FloatingPoint floatingPoint) {
        switch (floatingPoint.getPrecision()) {
            case SINGLE:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            default:
                throw new IllegalArgumentException(
                        "Unexpected floating point precision: " + floatingPoint.getPrecision());
        }
    }

    private Type getPrestoTypeForArrowIntType(ArrowType.Int intType) {
        switch (intType.getBitWidth()) {
            case 64:
                return BigintType.BIGINT;
            case 32:
                return IntegerType.INTEGER;
            case 16:
                return SmallintType.SMALLINT;
            case 8:
                return TinyintType.TINYINT;
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + intType.getBitWidth());
        }
    }
}
