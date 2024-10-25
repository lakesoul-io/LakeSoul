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

import org.apache.arrow.vector.BitVector;

/**
 * {@link ArrowFieldWriter} for Boolean.
 */
public abstract class BooleanWriter<T> extends ArrowFieldWriter<T> {

    public static BooleanWriter<Object[]> forObject(BitVector bitVector) {
        return new BooleanWriterForObject(bitVector);
    }


    // ------------------------------------------------------------------------------------------

    private BooleanWriter(BitVector bitVector) {
        super(bitVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract boolean readBoolean(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((BitVector) getValueVector()).setNull(getCount());
        } else if (readBoolean(in, ordinal)) {
            ((BitVector) getValueVector()).setSafe(getCount(), 1);
        } else {
            ((BitVector) getValueVector()).setSafe(getCount(), 0);
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link BooleanWriter} for {@link Object[]} input.
     */
    public static final class BooleanWriterForObject extends BooleanWriter<Object[]> {

        private BooleanWriterForObject(BitVector bitVector) {
            super(bitVector);
        }

        @Override
        boolean isNullAt(Object[] in, int ordinal) {
            return in[ordinal] == null;
        }

        @Override
        boolean readBoolean(Object[] in, int ordinal) {
            return (boolean) in[ordinal];
        }
    }

}
