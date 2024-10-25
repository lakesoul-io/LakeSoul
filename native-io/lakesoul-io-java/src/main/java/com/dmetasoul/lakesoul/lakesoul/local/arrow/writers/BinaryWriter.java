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

import org.apache.arrow.vector.FixedSizeBinaryVector;

/**
 * {@link ArrowFieldWriter} for Binary.
 */
public abstract class BinaryWriter<T> extends ArrowFieldWriter<T> {

    public static BinaryWriter<Object[]> forObject(FixedSizeBinaryVector fixedSizeBinaryVector) {
        return new BinaryWriterForObject(fixedSizeBinaryVector);
    }


    // ------------------------------------------------------------------------------------------

    private BinaryWriter(FixedSizeBinaryVector fixedSizeBinaryVector) {
        super(fixedSizeBinaryVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract byte[] readBinary(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((FixedSizeBinaryVector) getValueVector()).setNull(getCount());
        } else {
            ((FixedSizeBinaryVector) getValueVector()).setSafe(getCount(), readBinary(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link BinaryWriter} for {@link Object[]} input.
     */
    public static final class BinaryWriterForObject extends BinaryWriter<Object[]> {

        private BinaryWriterForObject(FixedSizeBinaryVector fixedSizeBinaryVector) {
            super(fixedSizeBinaryVector);
        }

        @Override
        boolean isNullAt(Object[] in, int ordinal) {
            return in[ordinal] == null;
        }

        @Override
        byte[] readBinary(Object[] in, int ordinal) {
            return (byte[]) in[ordinal];
        }
    }

}
