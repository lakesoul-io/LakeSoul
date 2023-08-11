package com.facebook.presto.lakesoul;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.lakesoul.pojo.Path;
import com.facebook.presto.lakesoul.util.ArrowUtils;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.stream.Collectors;

public class LakeSoulRecordCursor implements RecordCursor {

    private final  LakeSoulSplit split;
    private final NativeIOReader reader;

    int count = 2;
    int cur = 0;

    public LakeSoulRecordCursor(LakeSoulSplit split) throws IOException {
        this.split = split;
        this.reader = new NativeIOReader();

        // set paths, schema, pks
        for(Path path : split.getPaths()){
            this.reader.addFile(path.getFilename());
        }
        this.reader.setPrimaryKeys(split.getLayout().getPrimaryKeys());
        this.reader.setSchema(new Schema(
                split.getLayout().getDataColumns().get().stream().map(item -> {
                        LakeSoulTableColumnHandle columnHandle = (LakeSoulTableColumnHandle) item;
                        return Field.nullable(columnHandle.getColumnName(), ArrowUtils.convertToArrayType(columnHandle.getColumnType()));
                }).collect(Collectors.toList())
        ));
        split.getLayout().getDataColumns().get().forEach(item -> {
            LakeSoulTableColumnHandle columnHandle = (LakeSoulTableColumnHandle) item;
            this.reader.addColumn(columnHandle.getColumnName());
        });
        // init reader
        this.reader.initializeReader();
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
        return null;
    }

    @Override
    public boolean advanceNextPosition() {
        return cur++ < count;
    }

    @Override
    public boolean getBoolean(int field) {
        return false;
    }

    @Override
    public long getLong(int field) {
        return 0;
    }

    @Override
    public double getDouble(int field) {
        return 0;
    }

    @Override
    public Slice getSlice(int field) {
        return Slices.utf8Slice("hello");
    }

    @Override
    public Object getObject(int field) {
        return null;
    }

    @Override
    public boolean isNull(int field) {
        return false;
    }

    @Override
    public void close() {

    }
}
