/*
 * // SPDX-FileCopyrightText: 2023 LakeSoul Contributors
 * //
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.facebook.presto.lakesoul;

import com.facebook.presto.lakesoul.pojo.Path;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class LakeSoulSplit implements ConnectorSplit {

    private String hash;
    private List<Path> paths;
    private Integer key;

    @JsonCreator
    public LakeSoulSplit(
            @JsonProperty("hash") String hash,
            @JsonProperty("paths")  List<Path> paths,
            @JsonProperty("key") Integer key
    ){
        this.hash = requireNonNull(hash, "hash is not null") ;
        this.paths = requireNonNull(paths, "paths is not null") ;
        this.key = requireNonNull(key, "key is not null") ;
    }

    @JsonProperty
    public String getHash() {
        return hash;
    }

    @JsonProperty
    public List<Path> getPaths() {
        return paths;
    }

    @JsonProperty
    public Integer getKey() {
        return key;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy() {
        return NodeSelectionStrategy.HARD_AFFINITY;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider) {
        return nodeProvider.get("*", 1);
    }

    @Override
    @JsonProperty
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
