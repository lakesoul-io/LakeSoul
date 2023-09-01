// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.spi.*;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LakeSoulRecordSet implements RecordSet {
    private final LakeSoulSplit split;
    private final List<? extends ColumnHandle> columnHandles;

    public LakeSoulRecordSet(LakeSoulSplit split, List<? extends ColumnHandle> columnHandles){
        this.split = requireNonNull(split, "split should not be null");
        this.columnHandles =  requireNonNull(columnHandles, "columnHandles should not be null");
    }

    @Override
    public List<Type> getColumnTypes() {
        List<Type> types = new LinkedList<>();
        this.columnHandles.forEach(item -> {
            Type type = ((LakeSoulTableColumnHandle)item).getColumnType();
            types.add(type);
        });
        return types;
    }

    public LakeSoulSplit getSplit() {
        return split;
    }

    public List<? extends ColumnHandle> getColumnHandles() {
        return columnHandles;
    }

    @Override
    public RecordCursor cursor() {
        try {
            return new LakeSoulRecordCursor(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
