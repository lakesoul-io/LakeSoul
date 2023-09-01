// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTableHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTableLayoutHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTransactionHandle;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class LakeSoulHandleResolver implements ConnectorHandleResolver {
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return LakeSoulTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return LakeSoulTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return LakeSoulTableColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return LakeSoulSplit.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        return LakeSoulTransactionHandle.class;
    }
}
