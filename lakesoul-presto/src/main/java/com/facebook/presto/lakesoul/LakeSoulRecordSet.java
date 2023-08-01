/*
 * // SPDX-FileCopyrightText: 2023 LakeSoul Contributors
 * //
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.facebook.presto.lakesoul;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;

public class LakeSoulRecordSet implements ConnectorRecordSetProvider {
    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        return null;
    }
}
