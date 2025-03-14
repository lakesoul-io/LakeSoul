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

import org.apache.arrow.vector.VarBinaryVector;

/**
 * {@link ArrowFieldWriter} for VarBinary.
 */
public abstract class VarBinaryWriter<T> extends ArrowFieldWriter<T> {

    public static VarBinaryWriter<Object[]> forObject(VarBinaryVector varBinaryVector) {
        return new VarBinaryWriterForObject(varBinaryVector);
    }


    // ------------------------------------------------------------------------------------------

    private VarBinaryWriter(VarBinaryVector varBinaryVector) {
        super(varBinaryVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract byte[] readBinary(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((VarBinaryVector) getValueVector()).setNull(getCount());
        } else {
            ((VarBinaryVector) getValueVector()).setSafe(getCount(), readBinary(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link VarBinaryWriter} for {@link Object[]} input.
     */
    public static final class VarBinaryWriterForObject extends VarBinaryWriter<Object[]> {

        private VarBinaryWriterForObject(VarBinaryVector varBinaryVector) {
            super(varBinaryVector);
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
