/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow.vectors;

import org.apache.arrow.vector.*;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.TimestampColumnVector;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.time.*;
import java.time.zone.ZoneRules;

/**
 * Arrow column vector for Timestamp.
 */
@Internal
public final class ArrowTimestampColumnVector implements TimestampColumnVector {

    /**
     * Container which is used to store the sequence of timestamp values of a column to read.
     */
    private final ValueVector valueVector;

    public ArrowTimestampColumnVector(ValueVector valueVector) {
        this.valueVector = Preconditions.checkNotNull(valueVector);
        Preconditions.checkState(
                valueVector instanceof TimeStampVector);
    }

    @Override
    public TimestampData getTimestamp(int i, int precision) {
        if (valueVector instanceof TimeStampSecVector) {
            return TimestampData.fromEpochMillis(((TimeStampSecVector) valueVector).get(i) * 1000);
        } else if (valueVector instanceof TimeStampSecTZVector) {
            return TimestampData.fromEpochMillis(((TimeStampSecTZVector) valueVector).get(i) * 1000);
        } else if (valueVector instanceof TimeStampMilliVector) {
            return TimestampData.fromEpochMillis(((TimeStampMilliVector) valueVector).get(i));
        } else if (valueVector instanceof TimeStampMilliTZVector) {
            return TimestampData.fromEpochMillis(((TimeStampMilliTZVector) valueVector).get(i));
        } else if (valueVector instanceof TimeStampMicroVector) {
            long micros = ((TimeStampMicroVector) valueVector).get(i);
            return TimestampData.fromEpochMillis(micros / 1000, (int) (micros % 1000) * 1000);
        } else if (valueVector instanceof TimeStampMicroTZVector) {
            long micros = ((TimeStampMicroTZVector) valueVector).get(i);
            return TimestampData.fromEpochMillis(micros / 1000, (int) (micros % 1000) * 1000);
        } else if (valueVector instanceof TimeStampNanoVector) {
            long nanos = ((TimeStampNanoVector) valueVector).get(i);
            return TimestampData.fromEpochMillis(nanos / 1_000_000, (int) (nanos % 1_000_000));
        } else {
            long nanos = ((TimeStampNanoTZVector) valueVector).get(i);
            return TimestampData.fromEpochMillis(nanos / 1_000_000, (int) (nanos % 1_000_000));
        }
    }

    @Override
    public boolean isNullAt(int i) {
        return valueVector.isNull(i);
    }
}
