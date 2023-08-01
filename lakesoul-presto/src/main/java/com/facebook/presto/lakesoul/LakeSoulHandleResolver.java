/*
 * // SPDX-FileCopyrightText: 2023 LakeSoul Contributors
 * //
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.facebook.presto.lakesoul;

import com.facebook.presto.spi.*;

public class LakeSoulHandleResolver implements ConnectorHandleResolver {
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return null;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return null;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return null;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return null;
    }

}
