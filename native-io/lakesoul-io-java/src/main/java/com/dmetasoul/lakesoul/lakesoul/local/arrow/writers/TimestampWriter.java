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

import java.sql.Timestamp;

/**
 * {@link ArrowFieldWriter} for Timestamp.
 */
public abstract class TimestampWriter<T> extends ArrowFieldWriter<T> {

    public static TimestampWriter<Object[]> forObject(ValueVector valueVector, int precision) {
        return new TimestampWriterforObject(valueVector, precision);
    }


    // ------------------------------------------------------------------------------------------

    protected final int precision;

    private TimestampWriter(ValueVector valueVector, int precision) {
        super(valueVector);
        Preconditions.checkState(
                valueVector instanceof TimeStampVector
        );
        this.precision = precision;
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract Timestamp readTimestamp(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        ValueVector valueVector = getValueVector();
        if (isNullAt(in, ordinal)) {
            ((TimeStampVector) valueVector).setNull(getCount());
        } else {
            Timestamp timestamp = readTimestamp(in, ordinal);

            if (valueVector instanceof TimeStampSecTZVector) {
                ((TimeStampSecTZVector) valueVector)
                        .setSafe(getCount(), timestamp.getTime() / 1000);
            } else if (valueVector instanceof TimeStampSecVector) {
                ((TimeStampSecVector) valueVector)
                        .setSafe(getCount(), timestamp.getTime() / 1000);
            } else if (valueVector instanceof TimeStampMilliTZVector) {
                ((TimeStampMilliTZVector) valueVector)
                        .setSafe(getCount(), timestamp.getTime());
            } else if (valueVector instanceof TimeStampMilliVector) {
                ((TimeStampMilliVector) valueVector)
                        .setSafe(getCount(), timestamp.getTime());
            } else if (valueVector instanceof TimeStampMicroTZVector) {
                ((TimeStampMicroTZVector) valueVector)
                        .setSafe(
                                getCount(),
                                timestamp.getTime() * 1000
                                        + timestamp.getNanos() / 1000);
            } else if (valueVector instanceof TimeStampMicroVector) {
                ((TimeStampMicroVector) valueVector)
                        .setSafe(
                                getCount(),
                                timestamp.getTime() * 1000
                                        + timestamp.getNanos() / 1000);
            } else if (valueVector instanceof TimeStampNanoTZVector) {
                ((TimeStampNanoTZVector) valueVector)
                        .setSafe(
                                getCount(),
                                timestamp.getTime() * 1_000_000
                                        + timestamp.getNanos());
            } else {
                ((TimeStampNanoVector) valueVector)
                        .setSafe(
                                getCount(),
                                timestamp.getTime() * 1_000_000
                                        + timestamp.getNanos());
            }
        }
    }

    // ------------------------------------------------------------------------------------------

    /**
     * {@link TimestampWriter} for {@link Object[]} input.
     */
    public static final class TimestampWriterforObject extends TimestampWriter<Object[]> {

        private TimestampWriterforObject(ValueVector valueVector, int precision) {
            super(valueVector, precision);
        }

        @Override
        boolean isNullAt(Object[] in, int ordinal) {
            return in[ordinal] == null;
        }

        @Override
        Timestamp readTimestamp(Object[] in, int ordinal) {
            return (Timestamp) in[ordinal];
        }
    }

}
