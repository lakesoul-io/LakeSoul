/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow.vectors;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/** Arrow column vector for VarBinary. */
@Internal
public final class ArrowVarBinaryColumnVector implements BytesColumnVector {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowVarBinaryColumnVector.class);
    private static final AtomicLong LOG_COUNT = new AtomicLong();
    private static final long MAX_LOG = 20;

    /** Container which is used to store the sequence of varbinary values of a column to read. */
    private final VarBinaryVector varBinaryVector;

    public ArrowVarBinaryColumnVector(VarBinaryVector varBinaryVector) {
        this.varBinaryVector = Preconditions.checkNotNull(varBinaryVector);
    }

    private static boolean shouldLog() {
        return LOG_COUNT.incrementAndGet() <= MAX_LOG;
    }

    @Override
    public Bytes getBytes(int i) {
        byte[] bytes;
        try {
            bytes = varBinaryVector.get(i);
        } catch (Throwable t) {
            if (shouldLog()) {
                LOG.warn(
                        "ArrowVarBinary.getBytes error i={} isNull={} valueCount={} nullCount={}"
                            + " field={} class={} dataBuffer={} offsetBuffer={} validityBuffer={}",
                        i,
                        varBinaryVector.isNull(i),
                        varBinaryVector.getValueCount(),
                        varBinaryVector.getNullCount(),
                        varBinaryVector.getField(),
                        varBinaryVector.getClass().getName(),
                        varBinaryVector.getDataBuffer(),
                        varBinaryVector.getOffsetBuffer(),
                        varBinaryVector.getValidityBuffer(),
                        t);
            }
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            throw new RuntimeException(t);
        }
        if (bytes == null) {
            LOG.warn(
                    "ArrowVarBinary.getBytes null i={} isNull={} valueCount={} nullCount={}"
                            + " field={} class={} dataBuffer={} offsetBuffer={} validityBuffer={}",
                    i,
                    varBinaryVector.isNull(i),
                    varBinaryVector.getValueCount(),
                    varBinaryVector.getNullCount(),
                    varBinaryVector.getField(),
                    varBinaryVector.getClass().getName(),
                    varBinaryVector.getDataBuffer(),
                    varBinaryVector.getOffsetBuffer(),
                    varBinaryVector.getValidityBuffer());
            // Keep the original behaviour: fail on the null dereference.
            return new Bytes(bytes, 0, bytes.length);
        }
        return new Bytes(bytes, 0, bytes.length);
    }

    @Override
    public boolean isNullAt(int i) {
        // Diagnostic only: keep the original null semantics.
        boolean isNull = varBinaryVector.isNull(i);
        if (!isNull) {
            byte[] bytes;
            try {
                bytes = varBinaryVector.get(i);
            } catch (Throwable t) {
                LOG.warn(
                        "ArrowVarBinary.get error i={} isNull={} valueCount={} nullCount={}"
                            + " field={} class={} dataBuffer={} offsetBuffer={} validityBuffer={}",
                        i,
                        isNull,
                        varBinaryVector.getValueCount(),
                        varBinaryVector.getNullCount(),
                        varBinaryVector.getField(),
                        varBinaryVector.getClass().getName(),
                        varBinaryVector.getDataBuffer(),
                        varBinaryVector.getOffsetBuffer(),
                        varBinaryVector.getValidityBuffer(),
                        t);
                return isNull;
            }
            if (bytes == null) {
                LOG.warn(
                        "ArrowVarBinary null mismatch i={} isNull={} valueCount={} nullCount={}"
                            + " field={} class={} dataBuffer={} offsetBuffer={} validityBuffer={}",
                        i,
                        isNull,
                        varBinaryVector.getValueCount(),
                        varBinaryVector.getNullCount(),
                        varBinaryVector.getField(),
                        varBinaryVector.getClass().getName(),
                        varBinaryVector.getDataBuffer(),
                        varBinaryVector.getOffsetBuffer(),
                        varBinaryVector.getValidityBuffer());
            }
        }
        return isNull;
    }
}
