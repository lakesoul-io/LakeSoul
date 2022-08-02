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
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.sql.vectorized.ColumnarRow;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

class SingletonBatchRow extends MergeBatchRow {

    int rowId;
    private final ColumnVector[] columns;

    SingletonBatchRow(ColumnVector[] columns) {
        this.columns = columns;
    }

    public void setRowId(Integer id) {
        this.rowId = id;
    }

    @Override
    public int numFields() {
        return columns.length;
    }

    @Override
    public InternalRow copy() {
        GenericInternalRow row = new GenericInternalRow(numFields());
        for (int i = 0; i < numFields(); i++) {
            if (isNullAt(i)) {
                row.setNullAt(i);
            } else {
                DataType dt = columns[i].dataType();
                setRowData(i, dt, row);
            }
        }
        return row;
    }

    @Override
    public boolean anyNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNullAt(int ordinal) {
        if (columns[ordinal] == null) return true;
        return columns[ordinal].isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int ordinal) {
        return columns[ordinal].getBoolean(rowId);
    }

    @Override
    public byte getByte(int ordinal) {
        return columns[ordinal].getByte(rowId);
    }

    @Override
    public short getShort(int ordinal) {
        return columns[ordinal].getShort(rowId);
    }

    @Override
    public int getInt(int ordinal) {
        return columns[ordinal].getInt(rowId);
    }

    @Override
    public long getLong(int ordinal) {
        return columns[ordinal].getLong(rowId);
    }

    @Override
    public float getFloat(int ordinal) {
        return columns[ordinal].getFloat(rowId);
    }

    @Override
    public double getDouble(int ordinal) {
        return columns[ordinal].getDouble(rowId);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
        return columns[ordinal].getDecimal(rowId, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
        return columns[ordinal].getUTF8String(rowId);
    }

    @Override
    public byte[] getBinary(int ordinal) {
        return columns[ordinal].getBinary(rowId);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
        return columns[ordinal].getInterval(rowId);
    }

    @Override
    public ColumnarRow getStruct(int ordinal, int numFields) {
        return columns[ordinal].getStruct(rowId);
    }

    @Override
    public ColumnarArray getArray(int ordinal) {
        return columns[ordinal].getArray(rowId);
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        return columns[ordinal].getMap(rowId);
    }

    @Override
    public void update(int ordinal, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNullAt(int ordinal) {
        throw new UnsupportedOperationException();
    }

}
