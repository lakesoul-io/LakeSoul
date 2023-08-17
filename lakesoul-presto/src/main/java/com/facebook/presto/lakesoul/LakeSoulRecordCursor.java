package com.facebook.presto.lakesoul;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.lakesoul.pojo.Path;
import com.facebook.presto.lakesoul.util.ArrowUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class LakeSoulRecordCursor implements RecordCursor {


    private final LakeSoulRecordSet recordSet;
    private final LakeSoulArrowReader reader;
    private int curRecordIdx = -1;
    private VectorSchemaRoot currentVCR;
    private List<Type> desiredTypes;

    public LakeSoulRecordCursor(LakeSoulRecordSet recordSet) throws IOException {

        this.recordSet = recordSet;
        LakeSoulSplit split = this.recordSet.getSplit();
        NativeIOReader reader = new NativeIOReader();
        // set paths, schema, pks
        for (Path path : split.getPaths()) {
            reader.addFile(path.getFilename());
        }
        List<Field> fields = recordSet.getColumnHandles().stream().map(item -> {
            LakeSoulTableColumnHandle columnHandle = (LakeSoulTableColumnHandle) item;
            return Field.nullable(columnHandle.getColumnName(), ArrowUtil.convertToArrowType(columnHandle.getColumnType()));
        }).collect(Collectors.toList());
        HashMap<String, ColumnHandle> allcolumns = split.getLayout().getAllColumns();
        List<String> dataCols = recordSet.getColumnHandles().stream().map(item -> {
            LakeSoulTableColumnHandle columnHandle = (LakeSoulTableColumnHandle) item;
            return columnHandle.getColumnName();
        }).collect(Collectors.toList());
        List<String> prikeys = split.getLayout().getPrimaryKeys();
        for (String item : prikeys) {
            if (!dataCols.contains(item)) {
                LakeSoulTableColumnHandle columnHandle = (LakeSoulTableColumnHandle) allcolumns.get(item);
                fields.add(Field.nullable(columnHandle.getColumnName(), ArrowUtil.convertToArrowType(columnHandle.getColumnType())));
            }
        }
        reader.setPrimaryKeys(prikeys);
        reader.setSchema(new Schema(fields));
        desiredTypes = recordSet.getColumnHandles().stream().map(item -> ((LakeSoulTableColumnHandle) item).getColumnType()).collect(Collectors.toList());
        // 设置filter
        this.recordSet.getSplit().getLayout().getFilters().forEach((filter) -> reader.addFilter(filter.toString()));

        // init reader
        reader.initializeReader();
        this.reader = new LakeSoulArrowReader(reader,
                10000);
        if (this.reader.hasNext()) {
            this.currentVCR = this.reader.nextResultVectorSchemaRoot();
            curRecordIdx = -1;
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
        if (curRecordIdx >= currentVCR.getRowCount() - 1) {
            if (this.reader.hasNext()) {
                this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                makeCurrentArrowReader();
                curRecordIdx = 0;
                return true;
            } else {
                this.reader.close();
                return false;
            }
        }
        curRecordIdx++;
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
        if (fv instanceof BigIntVector) {
            return ((BigIntVector) fv).get(curRecordIdx);
        }
        if (fv instanceof IntVector) {
            return ((IntVector) fv).get(curRecordIdx);
        }
        if (fv instanceof DateDayVector) {
            return ((DateDayVector) fv).get(curRecordIdx);
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
        if (value instanceof Text) {
            return Slices.wrappedBuffer(((Text) value).getBytes());
        }
        throw new IllegalArgumentException("Field " + field + " is not a String, but is a " + value.getClass().getName());
    }

    @Override
    public Object getObject(int field) {
        return this.currentVCR.getVector(field).getObject(curRecordIdx);
    }

    @Override
    public boolean isNull(int field) {
        return false;
    }

    @Override
    public void close() {
        if (this.currentVCR != null) {
            this.currentVCR.close();
            this.currentVCR = null;
        }
    }
}
