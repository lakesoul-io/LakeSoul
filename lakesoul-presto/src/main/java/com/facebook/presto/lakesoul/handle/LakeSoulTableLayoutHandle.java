// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.handle;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LakeSoulTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final LakeSoulTableHandle tableHandle;
    private final Optional<Set<ColumnHandle>> dataColumns;
    private final  List<String> primaryKeys;
    private final List<String> rangeKeys;
    private final JSONObject tableParameters;
    @JsonCreator
    public LakeSoulTableLayoutHandle(
            @JsonProperty("tableHandle") LakeSoulTableHandle tableHandle,
            @JsonProperty("dataColumns") Optional<Set<ColumnHandle>> dataColumns,
            @JsonProperty("primaryKeys") List<String> primaryKeys,
            @JsonProperty("rangeKeys") List<String> rangeKeys,
            @JsonProperty("tableParameters") JSONObject tableParameters
            ){
        this.tableHandle = requireNonNull(tableHandle, "tableHandle should not be null");
        this.dataColumns = requireNonNull(dataColumns, "dataColumns should not be null");
        this.primaryKeys = requireNonNull(primaryKeys, "primaryKeys should not be null");
        this.rangeKeys = requireNonNull(rangeKeys, "rangeKeys should not be null");
        this.tableParameters = requireNonNull(tableParameters, "tableParameters should not be null");
    }


    @JsonProperty
    public Optional<Set<ColumnHandle>> getDataColumns() {
        return dataColumns;
    }

    @JsonProperty
    public LakeSoulTableHandle getTableHandle() {
        return tableHandle;
    }
    @JsonProperty
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }
    @JsonProperty
    public List<String> getRangeKeys() {
        return rangeKeys;
    }

    @JsonProperty
    public JSONObject getTableParameters() {
        return tableParameters;
    }
}
