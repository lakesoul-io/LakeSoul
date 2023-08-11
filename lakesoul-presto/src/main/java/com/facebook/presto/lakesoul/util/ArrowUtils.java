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

import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.arrow.vector.types.TimeUnit.*;

/**
 * Utilities for Arrow.
 */

public final class ArrowUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowUtils.class);

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

    public static ArrowType convertToArrayType(String type) {
        if(type.equals("integer")){
            return new ArrowType.Int(32, true) ;
        }else if (type.equals("string")){
            return new ArrowType.Utf8() ;
        }else{
            return new ArrowType.Utf8() ;
        }
    }

    public static ArrowType convertToArrayType(Type type) {
        if(type.equals(IntegerType.INTEGER)){
            return new ArrowType.Int(32, true) ;
        }else if (type.equals(VarcharType.VARCHAR)){
            return new ArrowType.Utf8() ;
        }else{
            return new ArrowType.Utf8() ;
        }
    }
}
