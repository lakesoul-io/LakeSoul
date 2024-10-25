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

import org.apache.arrow.vector.TinyIntVector;


/**
 * {@link ArrowFieldWriter} for TinyInt.
 */
public abstract class TinyIntWriter<T> extends ArrowFieldWriter<T> {

    public static TinyIntWriter<Object[]> forObject(TinyIntVector tinyIntVector) {
        return new TinyIntWriterForObject(tinyIntVector);
    }

    // ------------------------------------------------------------------------------------------

    private TinyIntWriter(TinyIntVector tinyIntVector) {
        super(tinyIntVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract byte readByte(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((TinyIntVector) getValueVector()).setNull(getCount());
        } else {
            ((TinyIntVector) getValueVector()).setSafe(getCount(), readByte(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link TinyIntWriter} for {@link Object} input.
     */
    public static final class TinyIntWriterForObject extends TinyIntWriter<Object[]> {

        private TinyIntWriterForObject(TinyIntVector tinyIntVector) {
            super(tinyIntVector);
        }

        @Override
        boolean isNullAt(Object[] in, int ordinal) {
            return in[ordinal] == null;
        }

        @Override
        byte readByte(Object[] in, int ordinal) {
            return (byte) in[ordinal];
        }
    }

}
