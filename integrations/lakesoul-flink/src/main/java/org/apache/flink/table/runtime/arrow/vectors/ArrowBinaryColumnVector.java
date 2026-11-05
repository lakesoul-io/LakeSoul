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

import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Arrow column vector for Binary. */
@Internal
public final class ArrowBinaryColumnVector implements BytesColumnVector {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowBinaryColumnVector.class);

    /** Container which is used to store the sequence of varbinary values of a column to read. */
    private final FixedSizeBinaryVector fixedSizeBinaryVector;

    public ArrowBinaryColumnVector(FixedSizeBinaryVector fixedSizeBinaryVector) {
        this.fixedSizeBinaryVector = Preconditions.checkNotNull(fixedSizeBinaryVector);
    }

    @Override
    public Bytes getBytes(int i) {
        byte[] bytes;
        try {
            bytes = fixedSizeBinaryVector.get(i);
        } catch (Throwable t) {
            LOG.warn(
                    "ArrowBinary.getBytes error i={} isNull={} valueCount={} nullCount={} field={}"
                            + " class={} dataBuffer={} validityBuffer={}",
                    i,
                    fixedSizeBinaryVector.isNull(i),
                    fixedSizeBinaryVector.getValueCount(),
                    fixedSizeBinaryVector.getNullCount(),
                    fixedSizeBinaryVector.getField(),
                    fixedSizeBinaryVector.getClass().getName(),
                    fixedSizeBinaryVector.getDataBuffer(),
                    fixedSizeBinaryVector.getValidityBuffer(),
                    t);
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
                    "ArrowBinary.getBytes null i={} isNull={} valueCount={} nullCount={} field={}"
                            + " class={} dataBuffer={} validityBuffer={}",
                    i,
                    fixedSizeBinaryVector.isNull(i),
                    fixedSizeBinaryVector.getValueCount(),
                    fixedSizeBinaryVector.getNullCount(),
                    fixedSizeBinaryVector.getField(),
                    fixedSizeBinaryVector.getClass().getName(),
                    fixedSizeBinaryVector.getDataBuffer(),
                    fixedSizeBinaryVector.getValidityBuffer());
            // Keep the original behaviour: fail on the null dereference.
            return new Bytes(bytes, 0, bytes.length);
        }
        return new Bytes(bytes, 0, bytes.length);
    }

    @Override
    public boolean isNullAt(int i) {
        // Diagnostic only: keep the original null semantics.
        boolean isNull = fixedSizeBinaryVector.isNull(i);
        if (!isNull) {
            byte[] bytes;
            try {
                bytes = fixedSizeBinaryVector.get(i);
            } catch (Throwable t) {
                LOG.warn(
                        "ArrowBinary.get error i={} isNull={} valueCount={} nullCount={} field={}"
                                + " class={} dataBuffer={} validityBuffer={}",
                        i,
                        isNull,
                        fixedSizeBinaryVector.getValueCount(),
                        fixedSizeBinaryVector.getNullCount(),
                        fixedSizeBinaryVector.getField(),
                        fixedSizeBinaryVector.getClass().getName(),
                        fixedSizeBinaryVector.getDataBuffer(),
                        fixedSizeBinaryVector.getValidityBuffer(),
                        t);
                return isNull;
            }
            if (bytes == null) {
                LOG.warn(
                        "ArrowBinary null mismatch i={} isNull={} valueCount={} nullCount={}"
                                + " field={} class={} dataBuffer={} validityBuffer={}",
                        i,
                        isNull,
                        fixedSizeBinaryVector.getValueCount(),
                        fixedSizeBinaryVector.getNullCount(),
                        fixedSizeBinaryVector.getField(),
                        fixedSizeBinaryVector.getClass().getName(),
                        fixedSizeBinaryVector.getDataBuffer(),
                        fixedSizeBinaryVector.getValidityBuffer());
            }
        }
        return isNull;
    }
}
