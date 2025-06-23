// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.lakesoul.util.ArrowBlockBuilder;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

public class LakeSoulPagesSourceProvider implements ConnectorPageSourceProvider {
    private final TypeManager typeManager;

    public LakeSoulPagesSourceProvider(TypeManager typeManager) {
        this.typeManager = typeManager;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                                ConnectorSplit split, ConnectorTableLayoutHandle layout,
                                                List<ColumnHandle> columns, SplitContext splitContext,
                                                RuntimeStats runtimeStats) {
        ImmutableList.Builder<LakeSoulTableColumnHandle> columnHandles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            columnHandles.add((LakeSoulTableColumnHandle) handle);
        }
        LakeSoulSplit lakeSoulSplit = (LakeSoulSplit) split;
        ArrowBlockBuilder builder = new ArrowBlockBuilder(typeManager);
        try {
            return new LakeSoulPageSource(lakeSoulSplit, builder, columnHandles.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
