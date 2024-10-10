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

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;

/**
 * {@link ArrowFieldWriter} for Time.
 */
public abstract class TimeWriter<T> extends ArrowFieldWriter<T> {

    public static TimeWriter<Object[]> forObject(ValueVector valueVector) {
        return new TimeWriterForObject(valueVector);
    }


    // ------------------------------------------------------------------------------------------

    private TimeWriter(ValueVector valueVector) {
        super(valueVector);
        Preconditions.checkState(
                valueVector instanceof TimeSecVector
                        || valueVector instanceof TimeMilliVector
                        || valueVector instanceof TimeMicroVector
                        || valueVector instanceof TimeNanoVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract int readTime(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        ValueVector valueVector = getValueVector();
        if (isNullAt(in, ordinal)) {
            ((BaseFixedWidthVector) valueVector).setNull(getCount());
        } else if (valueVector instanceof TimeSecVector) {
            ((TimeSecVector) valueVector).setSafe(getCount(), readTime(in, ordinal) / 1000);
        } else if (valueVector instanceof TimeMilliVector) {
            ((TimeMilliVector) valueVector).setSafe(getCount(), readTime(in, ordinal));
        } else if (valueVector instanceof TimeMicroVector) {
            ((TimeMicroVector) valueVector).setSafe(getCount(), readTime(in, ordinal) * 1000L);
        } else {
            ((TimeNanoVector) valueVector).setSafe(getCount(), readTime(in, ordinal) * 1000000L);
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link TimeWriter} for {@link Object[]} input.
     */
    public static final class TimeWriterForObject extends TimeWriter<Object[]> {

        private TimeWriterForObject(ValueVector valueVector) {
            super(valueVector);
        }

        @Override
        boolean isNullAt(Object[] in, int ordinal) {
            return in[ordinal] == null;
        }

        @Override
        int readTime(Object[] in, int ordinal) {
            return (int) in[ordinal];
        }
    }
    
}
