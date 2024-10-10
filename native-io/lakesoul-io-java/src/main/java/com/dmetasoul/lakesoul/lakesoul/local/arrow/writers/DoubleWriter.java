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

import org.apache.arrow.vector.Float8Vector;

/**
 * {@link ArrowFieldWriter} for Double.
 */
public abstract class DoubleWriter<T> extends ArrowFieldWriter<T> {

    public static DoubleWriter<Object[]> forObject(Float8Vector doubleVector) {
        return new DoubleWriterForObject(doubleVector);
    }

    // ------------------------------------------------------------------------------------------

    private DoubleWriter(Float8Vector doubleVector) {
        super(doubleVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract double readDouble(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((Float8Vector) getValueVector()).setNull(getCount());
        } else {
            ((Float8Vector) getValueVector()).setSafe(getCount(), readDouble(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link DoubleWriter} for {@link Object[]} input.
     */
    public static final class DoubleWriterForObject extends DoubleWriter<Object[]> {

        private DoubleWriterForObject(Float8Vector doubleVector) {
            super(doubleVector);
        }

        @Override
        boolean isNullAt(Object[] in, int ordinal) {
            return in[ordinal] == null;
        }

        @Override
        double readDouble(Object[] in, int ordinal) {
            return (double) in[ordinal];
        }
    }

}
