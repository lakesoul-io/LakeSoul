/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.lakesoul.util;

import com.facebook.presto.common.type.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.types.BinaryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

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
        if(type instanceof  IntegerType) {
            return new ArrowType.Int(32, true);
        } else if (type instanceof DoubleType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (type instanceof  VarcharType){
            return new ArrowType.Utf8();
        } else if (type instanceof BinaryType){
            return new ArrowType.Binary();
        } else if(type instanceof BooleanType){
            return new ArrowType.Bool();
        } else if(type instanceof DateType){
            return new ArrowType.Date(DateUnit.DAY);
        } else if(type instanceof TimeType){
            return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
        } else if(type instanceof TimestampType){
            return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "utc");
        } else {
            return new ArrowType.Null();
        }
    }

}
