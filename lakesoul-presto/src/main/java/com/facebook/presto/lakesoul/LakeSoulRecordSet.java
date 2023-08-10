// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.*;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.LinkedList;
import java.util.List;

public class LakeSoulRecordSet implements RecordSet {

    private LakeSoulSplit split;
    private List<? extends ColumnHandle> columnHandles;

    public LakeSoulRecordSet(LakeSoulSplit split, List<? extends ColumnHandle> columnHandles){
        this.split = split;
        this.columnHandles = columnHandles;
    }

    @Override
    public List<Type> getColumnTypes() {
        List<Type> types = new LinkedList<>();
        types.add(IntegerType.INTEGER);
        types.add(VarcharType.VARCHAR);
        return types;
    }

    @Override
    public RecordCursor cursor() {
        return new LakeSoulRecordCursor();
    }

}





