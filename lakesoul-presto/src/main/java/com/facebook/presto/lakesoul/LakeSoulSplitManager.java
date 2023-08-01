/*
 * // SPDX-FileCopyrightText: 2023 LakeSoul Contributors
 * //
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.facebook.presto.lakesoul;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class LakeSoulSplitManager implements ConnectorSplitManager {
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext) {
        return null;
    }
}
