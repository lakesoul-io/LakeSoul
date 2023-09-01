// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.util;

import com.facebook.presto.common.type.*;
import com.facebook.presto.jdbc.internal.joda.time.DateTimeField;
import com.facebook.presto.jdbc.internal.joda.time.DateTimeFieldType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.types.BinaryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

/**
 * Utilities for Arrow.
 */

public final class ArrowUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowUtil.class);

    private static RootAllocator rootAllocator;

    private static ZoneId LocalTimeZone = ZoneId.systemDefault();

    public static synchronized RootAllocator getRootAllocator() {
        if (rootAllocator == null) {
            rootAllocator = new RootAllocator(Long.MAX_VALUE);
        }
        return rootAllocator;
    }

    public static void checkArrowUsable() {
        // Arrow requires the property io.netty.tryReflectionSetAccessible to
        // be set to true for JDK >= 9. Please refer to ARROW-5412 for more details.
        if (System.getProperty("io.netty.tryReflectionSetAccessible") == null) {
            System.setProperty("io.netty.tryReflectionSetAccessible", "true");
        } else if (!io.netty.util.internal.PlatformDependent
                .hasDirectBufferNoCleanerConstructor()) {
            throw new RuntimeException(
                    "Arrow depends on "
                            + "DirectByteBuffer.<init>(long, int) which is not available. Please set the "
                            + "system property 'io.netty.tryReflectionSetAccessible' to 'true'.");
        }
    }

    public static void setLocalTimeZone(ZoneId localTimeZone) {
        LocalTimeZone = localTimeZone;
    }


    public static ArrowType convertToArrowType(Type type) {
        if (type instanceof BigintType) {
            return new ArrowType.Int(64, true);
        } else if (type instanceof IntegerType) {
            return new ArrowType.Int(32, true);
        } else if (type instanceof SmallintType) {
            return new ArrowType.Int(16, true);
        } else if (type instanceof TinyintType) {
            return new ArrowType.Int(8, true); // eqauls to byte
        } else if (type instanceof RealType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (type instanceof DoubleType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (type instanceof BooleanType) {
            return new ArrowType.Bool();
        } else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return new ArrowType.Decimal(
                    decimalType.getPrecision(),
                    decimalType.getScale());
        } else if (type instanceof VarcharType) {
            return new ArrowType.Utf8();
        } else if (type instanceof BinaryType) {
            return new ArrowType.Binary();
        } else if (type instanceof VarbinaryType) {
            return new ArrowType.Binary();
        } else if (type instanceof DateType) {
            return new ArrowType.Date(DateUnit.DAY);
        } else if (type instanceof TimeType) {
            return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
        } else if (type instanceof TimestampType) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, ZoneId.of("UTC").toString());
        } else if (type instanceof TimestampWithTimeZoneType) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, ZoneId.of("UTC").toString());
        } else if (type instanceof DecimalType) {
            DecimalType dt = (DecimalType) type;
            return new ArrowType.Decimal(dt.getPrecision(), dt.getScale());
        } else {
            return new ArrowType.Null();
        }
    }

}
