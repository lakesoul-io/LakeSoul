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

import org.apache.arrow.vector.IntVector;

/**
 * {@link ArrowFieldWriter} for Int.
 */
public abstract class IntWriter<T> extends ArrowFieldWriter<T> {

    public static IntWriter<Object[]> forObject(IntVector intVector) {
        return new IntWriterforObject(intVector);
    }


    // ------------------------------------------------------------------------------------------

    private IntWriter(IntVector intVector) {
        super(intVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract int readInt(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((IntVector) getValueVector()).setNull(getCount());
        } else {
            ((IntVector) getValueVector()).setSafe(getCount(), readInt(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link IntWriter} for {@link Object[]} input.
     */
    public static final class IntWriterforObject extends IntWriter<Object[]> {

        private IntWriterforObject(IntVector intVector) {
            super(intVector);
        }

        @Override
        boolean isNullAt(Object[] in, int ordinal) {
            return in[ordinal] == null;
        }

        @Override
        int readInt(Object[] in, int ordinal) {
            return (int) in[ordinal];
        }
    }


}
