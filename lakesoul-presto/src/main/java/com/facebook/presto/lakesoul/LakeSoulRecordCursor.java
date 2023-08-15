package com.facebook.presto.lakesoul;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.lakesoul.pojo.Path;
import com.facebook.presto.lakesoul.util.ArrowUtil;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class LakeSoulRecordCursor implements RecordCursor {

    private final LakeSoulSplit split;
    private final LakeSoulArrowReader reader;
    private int curRecordIdx = 0;
    private VectorSchemaRoot currentVCR;
    private List<Type> desiredTypes;
    public LakeSoulRecordCursor(LakeSoulSplit split) throws IOException {
        this.split = split;
        NativeIOReader reader = new NativeIOReader();
        // set paths, schema, pks
        for (Path path : split.getPaths()) {
            reader.addFile(path.getFilename());
        }
        reader.setPrimaryKeys(split.getLayout().getPrimaryKeys());
        reader.setSchema(new Schema(
                split.getLayout().getDataColumns().get().stream().map(item -> {
                    LakeSoulTableColumnHandle columnHandle = (LakeSoulTableColumnHandle) item;
                    return Field.nullable(columnHandle.getColumnName(), ArrowUtil.convertToArrowType(columnHandle.getColumnType()));
                }).collect(Collectors.toList())
        ));
        desiredTypes = split.getLayout().getDataColumns().get().stream().map(item -> ((LakeSoulTableColumnHandle)item).getColumnType()).collect(Collectors.toList());
        // init reader
        reader.initializeReader();
        this.reader = new LakeSoulArrowReader(reader,
                10000);
        if (this.reader.hasNext()) {
            this.currentVCR = this.reader.nextResultVectorSchemaRoot();
            curRecordIdx = 0;
        } else {
            close();
            return;
        }
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        return desiredTypes.get(field);
    }

    @Override
    public boolean advanceNextPosition() {
        if (curRecordIdx >= currentVCR.getRowCount()) {
            if (this.reader.hasNext()) {
                this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                makeCurrentArrowReader();
                curRecordIdx = 0;
            } else {
                this.reader.close();
                return false;
            }
        }
        return true;
    }

    public void makeCurrentArrowReader() {

    }

    @Override
    public boolean getBoolean(int field) {
        return ((BitVector) this.currentVCR.getVector(field)).get(curRecordIdx) != 0;
    }

    @Override
    public long getLong(int field) {
        FieldVector fv = this.currentVCR.getVector(field);
        if(fv instanceof BigIntVector){
            return ((BigIntVector)fv).get(curRecordIdx);
        }
        if(fv instanceof IntVector){
            return ((IntVector)fv).get(curRecordIdx);
        }
        throw new IllegalArgumentException("Field " + field + " is not a number, but is a " + fv.getClass().getName());
    }

    @Override
    public double getDouble(int field) {
       return ((Float8Vector) this.currentVCR.getVector(field)).get(curRecordIdx);
    }

    @Override
    public Slice getSlice(int field) {
        Object value = getObject(field);
        requireNonNull(value, "value is null");
        if (value instanceof byte[]) {
            return Slices.wrappedBuffer((byte[]) value);
        }
        if (value instanceof String) {
            return Slices.utf8Slice((String) value);
        }
        if (value instanceof Slice) {
            return (Slice) value;
        }
        throw new IllegalArgumentException("Field " + field + " is not a String, but is a " + value.getClass().getName());
    }

    @Override
    public Object getObject(int field) {
        return this.currentVCR.getVector(field).getObject(curRecordIdx);
    }

    @Override
    public boolean isNull(int field) {
        return this.currentVCR.getVector(field) == null;
    }

    @Override
    public void close() {
        if (this.currentVCR != null) {
            this.currentVCR.close();
            this.currentVCR = null;
        }
    }
}
