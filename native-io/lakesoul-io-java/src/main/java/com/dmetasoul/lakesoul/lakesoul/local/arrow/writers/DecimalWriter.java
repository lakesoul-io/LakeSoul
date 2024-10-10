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

package com.dmetasoul.lakesoul.lakesoul.local.arrow.writers;

import org.apache.arrow.vector.DecimalVector;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * {@link ArrowFieldWriter} for Decimal.
 */
public abstract class DecimalWriter<T> extends ArrowFieldWriter<T> {

    public static DecimalWriter<Object[]> forObject(
            DecimalVector decimalVector, int precision, int scale) {
        return new DecimalWriterForObject(decimalVector, precision, scale);
    }

    // ------------------------------------------------------------------------------------------

    protected final int precision;
    protected final int scale;

    private DecimalWriter(DecimalVector decimalVector, int precision, int scale) {
        super(decimalVector);
        this.precision = precision;
        this.scale = scale;
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract BigDecimal readDecimal(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((DecimalVector) getValueVector()).setNull(getCount());
        } else {
            BigDecimal bigDecimal = readDecimal(in, ordinal);
            bigDecimal = fromBigDecimal(bigDecimal, precision, scale);
            if (bigDecimal == null) {
                ((DecimalVector) getValueVector()).setNull(getCount());
            } else {
                ((DecimalVector) getValueVector()).setSafe(getCount(), bigDecimal);
            }
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link DecimalWriter} for {@link Object[]} input.
     */
    public static final class DecimalWriterForObject extends DecimalWriter<Object[]> {

        private DecimalWriterForObject(DecimalVector decimalVector, int precision, int scale) {
            super(decimalVector, precision, scale);
        }

        @Override
        boolean isNullAt(Object[] in, int ordinal) {
            return in[ordinal] == null;
        }

        @Override
        BigDecimal readDecimal(Object[] in, int ordinal) {
            return (BigDecimal) in[ordinal];
        }
    }

    /**
     * Convert the specified bigDecimal according to the specified precision and scale. The
     * specified bigDecimal may be rounded to have the specified scale and then the specified
     * precision is checked. If precision overflow, it will return `null`.
     */
    public static BigDecimal fromBigDecimal(BigDecimal bigDecimal, int precision, int scale) {
        if (bigDecimal.scale() != scale || bigDecimal.precision() > precision) {
            // need adjust the precision and scale
            bigDecimal = bigDecimal.setScale(scale, RoundingMode.HALF_UP);
            if (bigDecimal.precision() > precision) {
                return null;
            }
        }
        return bigDecimal;
    }

    public static int getPrecision(DecimalVector decimalVector) {
        int precision = -1;
        try {
            java.lang.reflect.Field precisionField =
                    decimalVector.getClass().getDeclaredField("precision");
            precisionField.setAccessible(true);
            precision = (int) precisionField.get(decimalVector);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // should not happen, ignore
        }
        return precision;
    }

}
