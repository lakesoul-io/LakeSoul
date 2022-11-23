package org.apache.spark.sql.execution.vectorized;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public final class WritableArrowColumnVector extends WritableColumnVector {

    /**
     * Returns the dictionary Id for rowId.
     * <p>
     * This should only be called when this `WritableColumnVector` represents dictionaryIds.
     * We have this separate method for dictionaryIds as per SPARK-16928.
     *
     * @param rowId
     */
    @Override
    public int getDictId(int rowId) {
        return 0;
    }

    /**
     * Ensures that there is enough storage to store capacity elements. That is, the put() APIs
     * must work for all rowIds < capacity.
     *
     * @param capacity
     */
    @Override
    protected void reserveInternal(int capacity) {

    }

    /**
     * Sets null/not null to the value at rowId.
     *
     * @param rowId
     */
    @Override
    public void putNotNull(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putNull(int rowId) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets null/not null to the values at [rowId, rowId + count).
     *
     * @param rowId
     * @param count
     */
    @Override
    public void putNulls(int rowId, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putNotNulls(int rowId, int count) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets `value` to the value at rowId.
     *
     * @param rowId
     * @param value
     */
    @Override
    public void putBoolean(int rowId, boolean value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets value to [rowId, rowId + count).
     *
     * @param rowId
     * @param count
     * @param value
     */
    @Override
    public void putBooleans(int rowId, int count, boolean value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets `value` to the value at rowId.
     *
     * @param rowId
     * @param value
     */
    @Override
    public void putByte(int rowId, byte value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets value to [rowId, rowId + count).
     *
     * @param rowId
     * @param count
     * @param value
     */
    @Override
    public void putBytes(int rowId, int count, byte value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets `value` to the value at rowId.
     *
     * @param rowId
     * @param value
     */
    @Override
    public void putShort(int rowId, short value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets value to [rowId, rowId + count).
     *
     * @param rowId
     * @param count
     * @param value
     */
    @Override
    public void putShorts(int rowId, int count, short value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putShorts(int rowId, int count, short[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count * 2]) to [rowId, rowId + count)
     * The data in src must be 2-byte platform native endian shorts.
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets `value` to the value at rowId.
     *
     * @param rowId
     * @param value
     */
    @Override
    public void putInt(int rowId, int value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets value to [rowId, rowId + count).
     *
     * @param rowId
     * @param count
     * @param value
     */
    @Override
    public void putInts(int rowId, int count, int value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putInts(int rowId, int count, int[] src, int srcIndex) {

    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
     * The data in src must be 4-byte platform native endian ints.
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putInts(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
     * The data in src must be 4-byte little endian ints.
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets `value` to the value at rowId.
     *
     * @param rowId
     * @param value
     */
    @Override
    public void putLong(int rowId, long value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets value to [rowId, rowId + count).
     *
     * @param rowId
     * @param count
     * @param value
     */
    @Override
    public void putLongs(int rowId, int count, long value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putLongs(int rowId, int count, long[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
     * The data in src must be 8-byte platform native endian longs.
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src + srcIndex, src + srcIndex + count * 8) to [rowId, rowId + count)
     * The data in src must be 8-byte little endian longs.
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets `value` to the value at rowId.
     *
     * @param rowId
     * @param value
     */
    @Override
    public void putFloat(int rowId, float value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets value to [rowId, rowId + count).
     *
     * @param rowId
     * @param count
     * @param value
     */
    @Override
    public void putFloats(int rowId, int count, float value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putFloats(int rowId, int count, float[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
     * The data in src must be ieee formatted floats in platform native endian.
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
     * The data in src must be ieee formatted floats in little endian.
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets `value` to the value at rowId.
     *
     * @param rowId
     * @param value
     */
    @Override
    public void putDouble(int rowId, double value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets value to [rowId, rowId + count).
     *
     * @param rowId
     * @param count
     * @param value
     */
    @Override
    public void putDoubles(int rowId, int count, double value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
     * The data in src must be ieee formatted doubles in platform native endian.
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
     * The data in src must be ieee formatted doubles in little endian.
     *
     * @param rowId
     * @param count
     * @param src
     * @param srcIndex
     */
    @Override
    public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Puts a byte array that already exists in this column.
     *
     * @param rowId
     * @param offset
     * @param length
     */
    @Override
    public void putArray(int rowId, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets values from [value + offset, value + offset + count) to the values at rowId.
     *
     * @param rowId
     * @param value
     * @param offset
     * @param count
     */
    @Override
    public int putByteArray(int rowId, byte[] value, int offset, int count) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the values of bytes from [rowId, rowId + count), as a UTF8String.
     * This method is similar to {@link ColumnVector#getBytes(int, int)}, but can save data copy as
     * UTF8String is used as a pointer.
     *
     * @param rowId
     * @param count
     */
    @Override
    protected UTF8String getBytesAsUTF8String(int rowId, int count) {
        return null;
    }

    @Override
    public int getArrayLength(int rowId) {
        return 0;
    }

    @Override
    public int getArrayOffset(int rowId) {
        return 0;
    }

    /**
     * Reserve a new column.
     *
     * @param capacity
     * @param type
     */
    @Override
    protected WritableColumnVector reserveNewColumn(int capacity, DataType type) {
        return null;
    }


    private final ArrowVectorAccessor accessor;
    private WritableArrowColumnVector[] childColumns;

    @Override
    public boolean hasNull() {
        return accessor.getNullCount() > 0;
    }

    @Override
    public int numNulls() {
        return accessor.getNullCount();
    }

    @Override
    public void close() {
        if (childColumns != null) {
            for (int i = 0; i < childColumns.length; i++) {
                childColumns[i].close();
                childColumns[i] = null;
            }
            childColumns = null;
        }
        accessor.close();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return accessor.isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        return accessor.getBoolean(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return accessor.getByte(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return accessor.getShort(rowId);
    }

    @Override
    public int getInt(int rowId) {
        return accessor.getInt(rowId);
    }

    @Override
    public long getLong(int rowId) {
        return accessor.getLong(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return accessor.getFloat(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return accessor.getDouble(rowId);
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        if (isNullAt(rowId)) return null;
        return accessor.getDecimal(rowId, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        if (isNullAt(rowId)) return null;
        return accessor.getUTF8String(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
        if (isNullAt(rowId)) return null;
        return accessor.getBinary(rowId);
    }

//    @Override
//    public ColumnarArray getArray(int rowId) {
//        if (isNullAt(rowId)) return null;
//        return accessor.getArray(rowId);
//    }
//
//    @Override
//    public ColumnarMap getMap(int rowId) {
//        if (isNullAt(rowId)) return null;
//        return accessor.getMap(rowId);
//    }

    @Override
    public WritableArrowColumnVector getChild(int ordinal) { return childColumns[ordinal]; }

    public WritableArrowColumnVector(ValueVector vector) {
        super(vector.getValueCapacity(), ArrowUtils.fromArrowField(vector.getField()));

        if (vector instanceof BitVector) {
            accessor = new BooleanAccessor((BitVector) vector);
        } else if (vector instanceof TinyIntVector) {
            accessor = new ByteAccessor((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            accessor = new ShortAccessor((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            accessor = new IntAccessor((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            accessor = new LongAccessor((BigIntVector) vector);
        } else if (vector instanceof Float4Vector) {
            accessor = new FloatAccessor((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            accessor = new DoubleAccessor((Float8Vector) vector);
        } else if (vector instanceof DecimalVector) {
            accessor = new DecimalAccessor((DecimalVector) vector);
        } else if (vector instanceof VarCharVector) {
            accessor = new StringAccessor((VarCharVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            accessor = new BinaryAccessor((VarBinaryVector) vector);
        } else if (vector instanceof DateDayVector) {
            accessor = new DateAccessor((DateDayVector) vector);
        } else if (vector instanceof TimeStampMicroTZVector) {
            accessor = new TimestampAccessor((TimeStampMicroTZVector) vector);
        } else if (vector instanceof TimeStampMicroVector) {
            accessor = new TimestampMicroAccessor((TimeStampMicroVector) vector);
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector) vector;
            accessor = new MapAccessor(mapVector);
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            accessor = new ArrayAccessor(listVector);
        } else if (vector instanceof StructVector) {
            StructVector structVector = (StructVector) vector;
            accessor = new StructAccessor(structVector);

            childColumns = new WritableArrowColumnVector[structVector.size()];
            for (int i = 0; i < childColumns.length; ++i) {
                childColumns[i] = new WritableArrowColumnVector(structVector.getVectorById(i));
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private abstract static class ArrowVectorAccessor {

        private final ValueVector vector;

        ArrowVectorAccessor(ValueVector vector) {
            this.vector = vector;
        }

        // TODO: should be final after removing ArrayAccessor workaround
        boolean isNullAt(int rowId) {
            return vector.isNull(rowId);
        }

        final int getNullCount() {
            return vector.getNullCount();
        }

        final void close() {
            vector.close();
        }

        boolean getBoolean(int rowId) {
            throw new UnsupportedOperationException();
        }

        byte getByte(int rowId) {
            throw new UnsupportedOperationException();
        }

        short getShort(int rowId) {
            throw new UnsupportedOperationException();
        }

        int getInt(int rowId) {
            throw new UnsupportedOperationException();
        }

        long getLong(int rowId) {
            throw new UnsupportedOperationException();
        }

        float getFloat(int rowId) {
            throw new UnsupportedOperationException();
        }

        double getDouble(int rowId) {
            throw new UnsupportedOperationException();
        }

        Decimal getDecimal(int rowId, int precision, int scale) {
            throw new UnsupportedOperationException();
        }

        UTF8String getUTF8String(int rowId) {
            throw new UnsupportedOperationException();
        }

        byte[] getBinary(int rowId) {
            throw new UnsupportedOperationException();
        }

        ColumnarArray getArray(int rowId) {
            throw new UnsupportedOperationException();
        }

        ColumnarMap getMap(int rowId) {
            throw new UnsupportedOperationException();
        }
    }

    private static class BooleanAccessor extends ArrowVectorAccessor {

        private final BitVector accessor;

        BooleanAccessor(BitVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final boolean getBoolean(int rowId) {
            return accessor.get(rowId) == 1;
        }
    }

    private static class ByteAccessor extends ArrowVectorAccessor {

        private final TinyIntVector accessor;

        ByteAccessor(TinyIntVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final byte getByte(int rowId) {
            return accessor.get(rowId);
        }
    }

    private static class ShortAccessor extends ArrowVectorAccessor {

        private final SmallIntVector accessor;

        ShortAccessor(SmallIntVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final short getShort(int rowId) {
            return accessor.get(rowId);
        }
    }

    private static class IntAccessor extends ArrowVectorAccessor {

        private final IntVector accessor;

        IntAccessor(IntVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final int getInt(int rowId) {
            return accessor.get(rowId);
        }
    }

    private static class LongAccessor extends ArrowVectorAccessor {

        private final BigIntVector accessor;

        LongAccessor(BigIntVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final long getLong(int rowId) {
            return accessor.get(rowId);
        }
    }

    private static class FloatAccessor extends ArrowVectorAccessor {

        private final Float4Vector accessor;

        FloatAccessor(Float4Vector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final float getFloat(int rowId) {
            return accessor.get(rowId);
        }
    }

    private static class DoubleAccessor extends ArrowVectorAccessor {

        private final Float8Vector accessor;

        DoubleAccessor(Float8Vector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final double getDouble(int rowId) {
            return accessor.get(rowId);
        }
    }

    private static class DecimalAccessor extends ArrowVectorAccessor {

        private final DecimalVector accessor;

        DecimalAccessor(DecimalVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final Decimal getDecimal(int rowId, int precision, int scale) {
            if (isNullAt(rowId)) return null;
            return Decimal.apply(accessor.getObject(rowId), precision, scale);
        }
    }

    private static class StringAccessor extends ArrowVectorAccessor {

        private final VarCharVector accessor;
        private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

        StringAccessor(VarCharVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final UTF8String getUTF8String(int rowId) {
            accessor.get(rowId, stringResult);
            if (stringResult.isSet == 0) {
                return null;
            } else {
                return UTF8String.fromAddress(null,
                        stringResult.buffer.memoryAddress() + stringResult.start,
                        stringResult.end - stringResult.start);
            }
        }
    }

    private static class BinaryAccessor extends ArrowVectorAccessor {

        private final VarBinaryVector accessor;

        BinaryAccessor(VarBinaryVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final byte[] getBinary(int rowId) {
            return accessor.getObject(rowId);
        }
    }

    private static class DateAccessor extends ArrowVectorAccessor {

        private final DateDayVector accessor;

        DateAccessor(DateDayVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final int getInt(int rowId) {
            return accessor.get(rowId);
        }
    }

    private static class TimestampAccessor extends ArrowVectorAccessor {

        private final TimeStampMicroTZVector accessor;

        TimestampAccessor(TimeStampMicroTZVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final long getLong(int rowId) {
            return accessor.get(rowId);
        }
    }

    private static class TimestampMicroAccessor extends ArrowVectorAccessor {

        private final TimeStampMicroVector accessor;

        TimestampMicroAccessor(TimeStampMicroVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final long getLong(int rowId) {
            return accessor.get(rowId);
        }
    }


    private static class ArrayAccessor extends ArrowVectorAccessor {

        private final ListVector accessor;
        private final WritableArrowColumnVector arrayData;

        ArrayAccessor(ListVector vector) {
            super(vector);
            this.accessor = vector;
            this.arrayData = new WritableArrowColumnVector(vector.getDataVector());
        }

        @Override
        final boolean isNullAt(int rowId) {
            // TODO: Workaround if vector has all non-null values, see ARROW-1948
            if (accessor.getValueCount() > 0 && accessor.getValidityBuffer().capacity() == 0) {
                return false;
            } else {
                return super.isNullAt(rowId);
            }
        }

        @Override
        final ColumnarArray getArray(int rowId) {
            int start = accessor.getElementStartIndex(rowId);
            int end = accessor.getElementEndIndex(rowId);
            return new ColumnarArray(arrayData, start, end - start);
        }
    }

    /**
     * Any call to "get" method will throw UnsupportedOperationException.
     *
     * Access struct values in a WritableArrowColumnVector doesn't use this accessor. Instead, it uses
     * getStruct() method defined in the parent class. Any call to "get" method in this class is a
     * bug in the code.
     *
     */
    private static class StructAccessor extends ArrowVectorAccessor {

        StructAccessor(StructVector vector) {
            super(vector);
        }
    }

    private static class MapAccessor extends ArrowVectorAccessor {
        private final MapVector accessor;
        private final WritableArrowColumnVector keys;
        private final WritableArrowColumnVector values;

        MapAccessor(MapVector vector) {
            super(vector);
            this.accessor = vector;
            StructVector entries = (StructVector) vector.getDataVector();
            this.keys = new WritableArrowColumnVector(entries.getChild(MapVector.KEY_NAME));
            this.values = new WritableArrowColumnVector(entries.getChild(MapVector.VALUE_NAME));
        }

        @Override
        final ColumnarMap getMap(int rowId) {
            int index = rowId * MapVector.OFFSET_WIDTH;
            int offset = accessor.getOffsetBuffer().getInt(index);
            int length = accessor.getInnerValueCountAt(rowId);
            return new ColumnarMap(keys, values, offset, length);
        }
    }
}
