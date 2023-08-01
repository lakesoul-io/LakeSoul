/*
 * // SPDX-FileCopyrightText: 2023 LakeSoul Contributors
 * //
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.facebook.presto.lakesoul;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

public class LakeSoulSplit implements ConnectorSplit {
    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy() {
        return null;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider) {
        return null;
    }

    @Override
    public Object getInfo() {
        return null;
    }

    @Override
    public Map<String, String> getInfoMap() {
        return ConnectorSplit.super.getInfoMap();
    }

    @Override
    public Object getSplitIdentifier() {
        return ConnectorSplit.super.getSplitIdentifier();
    }

    @Override
    public OptionalLong getSplitSizeInBytes() {
        return ConnectorSplit.super.getSplitSizeInBytes();
    }

    @Override
    public SplitWeight getSplitWeight() {
        return ConnectorSplit.super.getSplitWeight();
    }
}
