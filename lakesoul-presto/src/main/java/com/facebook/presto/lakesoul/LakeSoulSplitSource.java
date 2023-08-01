/*
 * // SPDX-FileCopyrightText: 2023 LakeSoul Contributors
 * //
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.facebook.presto.lakesoul;

import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;

import java.util.concurrent.CompletableFuture;

public class LakeSoulSplitSource implements ConnectorSplitSource {
    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize) {
        return null;
    }

    @Override
    public void rewind(ConnectorPartitionHandle partitionHandle) {
        ConnectorSplitSource.super.rewind(partitionHandle);
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isFinished() {
        return false;
    }
}
