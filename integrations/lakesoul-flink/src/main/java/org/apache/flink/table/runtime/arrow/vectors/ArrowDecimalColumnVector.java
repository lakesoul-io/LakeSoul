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

import org.apache.arrow.vector.DecimalVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.columnar.vector.DecimalColumnVector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/** Arrow column vector for DecimalData. */
@Internal
public final class ArrowDecimalColumnVector implements DecimalColumnVector {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowDecimalColumnVector.class);

    /** Container which is used to store the sequence of DecimalData values of a column to read. */
    private final DecimalVector decimalVector;

    public ArrowDecimalColumnVector(DecimalVector decimalVector) {
        this.decimalVector = Preconditions.checkNotNull(decimalVector);
    }

    @Override
    public DecimalData getDecimal(int i, int precision, int scale) {
        BigDecimal value = decimalVector.getObject(i);
        if (value == null) {
            LOG.warn(
                    "ArrowDecimal.getDecimal null i={} isNull={} valueCount={} nullCount={}"
                            + " field={} class={}",
                    i,
                    decimalVector.isNull(i),
                    decimalVector.getValueCount(),
                    decimalVector.getNullCount(),
                    decimalVector.getField(),
                    decimalVector.getClass().getName());
            return null;
        }
        return DecimalData.fromBigDecimal(value, precision, scale);
    }

    @Override
    public boolean isNullAt(int i) {
        // Guard both the Arrow validity bit and the actual value; native-reader
        // batches can present null values inconsistently.
        boolean isNull = decimalVector.isNull(i);
        if (!isNull && decimalVector.getObject(i) == null) {
            LOG.warn(
                    "ArrowDecimal null mismatch i={} isNull={} valueCount={} nullCount={} field={}"
                            + " class={}",
                    i,
                    isNull,
                    decimalVector.getValueCount(),
                    decimalVector.getNullCount(),
                    decimalVector.getField(),
                    decimalVector.getClass().getName());
            return true;
        }
        return isNull;
    }
}
