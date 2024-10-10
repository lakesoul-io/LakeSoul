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

import org.apache.arrow.vector.VarCharVector;

/**
 * {@link ArrowFieldWriter} for VarChar.
 */
public abstract class VarCharWriter<T> extends ArrowFieldWriter<T> {

    public static VarCharWriter<Object[]> forObject(VarCharVector varCharVector) {
        return new VarCharWriterForObject(varCharVector);
    }


    // ------------------------------------------------------------------------------------------

    private VarCharWriter(VarCharVector varCharVector) {
        super(varCharVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract String readString(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((VarCharVector) getValueVector()).setNull(getCount());
        } else {
            ((VarCharVector) getValueVector())
                    .setSafe(getCount(), readString(in, ordinal).getBytes());
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link VarCharWriter} for {@link Object[]} input.
     */
    public static final class VarCharWriterForObject extends VarCharWriter<Object[]> {

        private VarCharWriterForObject(VarCharVector varCharVector) {
            super(varCharVector);
        }

        @Override
        boolean isNullAt(Object[] in, int ordinal) {
            return in[ordinal] == null;
        }

        @Override
        String readString(Object[] in, int ordinal) {
            return (String) in[ordinal];
        }
    }

}
