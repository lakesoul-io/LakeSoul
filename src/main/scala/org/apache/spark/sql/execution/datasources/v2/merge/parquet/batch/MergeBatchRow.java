/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;

abstract class MergeBatchRow extends InternalRow {

    void setRowData(int i, DataType dt, GenericInternalRow row) {
        if (dt instanceof BooleanType) {
            row.setBoolean(i, getBoolean(i));
        } else if (dt instanceof ByteType) {
            row.setByte(i, getByte(i));
        } else if (dt instanceof ShortType) {
            row.setShort(i, getShort(i));
        } else if (dt instanceof IntegerType) {
            row.setInt(i, getInt(i));
        } else if (dt instanceof LongType) {
            row.setLong(i, getLong(i));
        } else if (dt instanceof FloatType) {
            row.setFloat(i, getFloat(i));
        } else if (dt instanceof DoubleType) {
            row.setDouble(i, getDouble(i));
        } else if (dt instanceof StringType) {
            row.update(i, getUTF8String(i).copy());
        } else if (dt instanceof BinaryType) {
            row.update(i, getBinary(i));
        } else if (dt instanceof DecimalType) {
            DecimalType t = (DecimalType) dt;
            row.setDecimal(i, getDecimal(i, t.precision(), t.scale()), t.precision());
        } else if (dt instanceof DateType) {
            row.setInt(i, getInt(i));
        } else if (dt instanceof TimestampType) {
            row.setLong(i, getLong(i));
        } else {
            throw new RuntimeException("Not implemented. " + dt);
        }
    }


    @Override
    public Object get(int ordinal, DataType dataType) {
        if (dataType instanceof BooleanType) {
            return getBoolean(ordinal);
        } else if (dataType instanceof ByteType) {
            return getByte(ordinal);
        } else if (dataType instanceof ShortType) {
            return getShort(ordinal);
        } else if (dataType instanceof IntegerType) {
            return getInt(ordinal);
        } else if (dataType instanceof LongType) {
            return getLong(ordinal);
        } else if (dataType instanceof FloatType) {
            return getFloat(ordinal);
        } else if (dataType instanceof DoubleType) {
            return getDouble(ordinal);
        } else if (dataType instanceof StringType) {
            return getUTF8String(ordinal);
        } else if (dataType instanceof BinaryType) {
            return getBinary(ordinal);
        } else if (dataType instanceof DecimalType) {
            DecimalType t = (DecimalType) dataType;
            return getDecimal(ordinal, t.precision(), t.scale());
        } else if (dataType instanceof DateType) {
            return getInt(ordinal);
        } else if (dataType instanceof TimestampType) {
            return getLong(ordinal);
        } else if (dataType instanceof ArrayType) {
            return getArray(ordinal);
        } else if (dataType instanceof StructType) {
            return getStruct(ordinal, ((StructType) dataType).fields().length);
        } else if (dataType instanceof MapType) {
            return getMap(ordinal);
        } else {
            throw new UnsupportedOperationException("Datatype not supported " + dataType);
        }
    }

}
