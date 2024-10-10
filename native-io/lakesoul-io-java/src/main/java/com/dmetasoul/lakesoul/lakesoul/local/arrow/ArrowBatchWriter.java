package com.dmetasoul.lakesoul.lakesoul.local.arrow;

import com.dmetasoul.lakesoul.lakesoul.local.arrow.writers.*;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

import static com.dmetasoul.lakesoul.lakesoul.local.arrow.writers.DecimalWriter.getPrecision;

public class ArrowBatchWriter<IN> {

    /**
     * Container that holds a set of vectors for the rows to be sent to the Python worker.
     */
    private final VectorSchemaRoot root;

    /**
     * An array of writers which are responsible for the serialization of each column of the rows.
     */
    private final ArrowFieldWriter<IN>[] fieldWriters;

    public ArrowBatchWriter(VectorSchemaRoot root, ArrowFieldWriter<IN>[] fieldWriters) {
        this.root = Preconditions.checkNotNull(root);
        this.fieldWriters = Preconditions.checkNotNull(fieldWriters);
    }

    /**
     * Gets the field writers.
     */
    public ArrowFieldWriter<IN>[] getFieldWriters() {
        return fieldWriters;
    }

    /**
     * Writes the specified row which is serialized into Arrow format.
     */
    public void write(IN row) {
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(row, i);
        }
    }

    /**
     * Finishes the writing of the current row batch.
     */
    public void finish() {
        root.setRowCount(fieldWriters[0].getCount());
        for (ArrowFieldWriter<IN> fieldWriter : fieldWriters) {
            fieldWriter.finish();
        }
    }

    /**
     * Resets the state of the writer to write the next batch of rows.
     */
    public void reset() {
        root.setRowCount(0);
        for (ArrowFieldWriter fieldWriter : fieldWriters) {
            fieldWriter.reset();
        }
    }

    public static ArrowBatchWriter<Object[]> createWriter(VectorSchemaRoot root) {
        ArrowFieldWriter<Object[]>[] fieldWriters =
                new ArrowFieldWriter[root.getFieldVectors().size()];
        List<FieldVector> vectors = root.getFieldVectors();
        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            vector.allocateNew();
            fieldWriters[i] = createArrowFieldWriterForObject(vector, vector.getField());
        }

        return new ArrowBatchWriter<>(root, fieldWriters);
    }

    private static ArrowFieldWriter<Object[]> createArrowFieldWriterForObject(
            ValueVector vector, Field field) {
        if (vector instanceof TinyIntVector) {
            return TinyIntWriter.forObject((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return SmallIntWriter.forObject((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return IntWriter.forObject((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return BigIntWriter.forObject((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return BooleanWriter.forObject((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return FloatWriter.forObject((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return DoubleWriter.forObject((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            return VarCharWriter.forObject((VarCharVector) vector);
        } else if (vector instanceof FixedSizeBinaryVector) {
            return BinaryWriter.forObject((FixedSizeBinaryVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return VarBinaryWriter.forObject((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            DecimalVector decimalVector = (DecimalVector) vector;
            return DecimalWriter.forObject(
                    decimalVector, getPrecision(decimalVector), decimalVector.getScale());
        } else if (vector instanceof DateDayVector) {
            return DateWriter.forObject((DateDayVector) vector);
        } else if (vector instanceof TimeSecVector
                || vector instanceof TimeMilliVector
                || vector instanceof TimeMicroVector
                || vector instanceof TimeNanoVector) {
            return TimeWriter.forObject(vector);
        } else if (vector instanceof TimeStampVector) {
            int precision = 0;
            switch (((ArrowType.Timestamp) field.getType()).getUnit()) {
                case MILLISECOND:
                    precision = 3;
                    break;
                case MICROSECOND:
                    precision = 6;
                    break;
                case NANOSECOND:
                    precision = 9;
                    break;
            }
            return TimestampWriter.forObject(vector, precision);
//        } else if (vector instanceof MapVector) {
//            MapVector mapVector = (MapVector) vector;
//            LogicalType keyType = ((MapType) field).getKeyType();
//            LogicalType valueType = ((MapType) field).getValueType();
//            StructVector structVector = (StructVector) mapVector.getDataVector();
//            return MapWriter.forObject(
//                    mapVector,
//                    createArrowFieldWriterForArray(
//                            structVector.getChild(MapVector.KEY_NAME), keyType),
//                    createArrowFieldWriterForArray(
//                            structVector.getChild(MapVector.VALUE_NAME), valueType));
//        } else if (vector instanceof ListVector) {
//            ListVector listVector = (ListVector) vector;
//            LogicalType elementType = ((ArrayType) field).getElementType();
//            return ArrayWriter.forObject(
//                    listVector,
//                    createArrowFieldWriterForArray(listVector.getDataVector(), elementType));
//        } else if (vector instanceof StructVector) {
//            RowType rowType = (RowType) field;
//            ArrowFieldWriter<RowData>[] fieldsWriters =
//                    new ArrowFieldWriter[rowType.getFieldCount()];
//            for (int i = 0; i < fieldsWriters.length; i++) {
//                fieldsWriters[i] =
//                        createArrowFieldWriterforObject(
//                                ((StructVector) vector).getVectorById(i), rowType.getTypeAt(i));
//            }
//            return RowWriter.forObject((StructVector) vector, fieldsWriters);
        } else if (vector instanceof NullVector) {
            return new NullWriter<>((NullVector) vector);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported type %s.", field));
        }
    }
}
