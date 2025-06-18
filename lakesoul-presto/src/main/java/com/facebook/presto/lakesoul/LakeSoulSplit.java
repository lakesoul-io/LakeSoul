// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.lakesoul.handle.LakeSoulTableLayoutHandle;
import com.facebook.presto.lakesoul.pojo.Path;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

import static java.util.Objects.requireNonNull;

public class LakeSoulSplit implements ConnectorSplit {
    private final LakeSoulTableLayoutHandle layout;
    private final List<Path> paths;

    @JsonCreator
    public LakeSoulSplit(
            @JsonProperty("layout") LakeSoulTableLayoutHandle layout,
            @JsonProperty("paths")  List<Path> paths
    ){
        this.layout = requireNonNull(layout, "layout is not null") ;
        this.paths = requireNonNull(paths, "paths is not null") ;
    }

    @JsonProperty
    public LakeSoulTableLayoutHandle getLayout() {
        return layout;
    }

    @JsonProperty
    public List<Path> getPaths() {
        return paths;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy() {
        return NodeSelectionStrategy.SOFT_AFFINITY;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider) {
        return nodeProvider.get(paths.get(0).toString());
    }

    @Override
    @JsonProperty
    public Object getInfo() {
        return getInfoMap();
    }

    @Override
    public Map<String, String> getInfoMap() {
        return Collections.emptyMap();
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

    @Override public String toString() {
        return "LakeSoulSplit{" +
                "layout=" + layout +
                ", paths=" + paths +
                '}';
    }
}
