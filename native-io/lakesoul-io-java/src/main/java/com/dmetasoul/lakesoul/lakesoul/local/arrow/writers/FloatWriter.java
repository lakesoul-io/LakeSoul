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

import org.apache.arrow.vector.Float4Vector;

/**
 * {@link ArrowFieldWriter} for Float.
 */
public abstract class FloatWriter<T> extends ArrowFieldWriter<T> {

    public static FloatWriter<Object[]> forObject(Float4Vector floatVector) {
        return new FloatWriterForObject(floatVector);
    }


    // ------------------------------------------------------------------------------------------

    private FloatWriter(Float4Vector floatVector) {
        super(floatVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract float readFloat(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((Float4Vector) getValueVector()).setNull(getCount());
        } else {
            ((Float4Vector) getValueVector()).setSafe(getCount(), readFloat(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link FloatWriter} for {@link Object[]} input.
     */
    public static final class FloatWriterForObject extends FloatWriter<Object[]> {

        private FloatWriterForObject(Float4Vector floatVector) {
            super(floatVector);
        }

        @Override
        boolean isNullAt(Object[] in, int ordinal) {
            return in[ordinal] == null;
        }

        @Override
        float readFloat(Object[] in, int ordinal) {
            return (float) in[ordinal];
        }
    }

}
