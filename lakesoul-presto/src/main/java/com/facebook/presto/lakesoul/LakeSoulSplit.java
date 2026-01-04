// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.lakesoul.handle.LakeSoulTableLayoutHandle;
import com.facebook.presto.lakesoul.pojo.Path;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.dmetasoul.lakesoul.meta.BucketingUtils;
import scala.Option;

import java.util.*;

import static java.util.Objects.requireNonNull;

public class LakeSoulSplit implements ConnectorSplit {
    private final LakeSoulTableLayoutHandle layout;
    private final List<Path> paths;

    private static final Logger log = Logger.get(LakeSoulSplit.class);

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
	   return nodeProvider.get(buildStableSplitIdentifier());
    }

    private String buildStableSplitIdentifier() {
	    
        String tableName = layout.getTableHandle().getNames().toString();
        String partitionKey = "";
        List<String> partitions = layout.getRangeKeys();
        if (partitions != null && !partitions.isEmpty()) {
            partitionKey = String.join(",", partitions);
        }
        Option<Object> hashBucketId = BucketingUtils.getBucketId(paths.get(0).toString());
        String fileIdentifier;
        if (!hashBucketId.isEmpty()) {
       	    fileIdentifier = "bucket=" + hashBucketId.get();
        }else {
            fileIdentifier = "bucket=0";
        }
        String result = String.format("LakeSoul:%s#%s#%s", tableName, partitionKey.isEmpty() ? "NO_PARTITION" : partitionKey, fileIdentifier);
        log.info("buildStableSplitIdentifier partitionKey %s, hashBucketId %s, result %s", partitionKey, fileIdentifier, result);
        return result;
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
