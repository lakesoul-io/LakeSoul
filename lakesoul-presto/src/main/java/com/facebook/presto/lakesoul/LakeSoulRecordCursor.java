package com.facebook.presto.lakesoul;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;

public class LakeSoulRecordCursor implements RecordCursor {

    int count = 2;
    int cur = 0;

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
