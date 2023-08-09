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

import java.util.Optional;
import java.util.Set;

public class LakeSoulTableLayoutHandle implements ConnectorTableLayoutHandle {
    private Long id;
    private LakeSoulTableHandle tableHandle;
    private Optional<Set<ColumnHandle>> dataColumns;
    private DBUtil.TablePartitionKeys partitionColumns;
    private JSONObject tableParameters;
    @JsonCreator
    public LakeSoulTableLayoutHandle(
            @JsonProperty("table") LakeSoulTableHandle tableHandle,
            @JsonProperty("dataColumns") Optional<Set<ColumnHandle>> dataColumns,
            @JsonProperty("partitionColumns") DBUtil.TablePartitionKeys partitionColumns,
            @JsonProperty("tableParameters") JSONObject tableParameters
            ){
        this.tableHandle = tableHandle;
        this.dataColumns = dataColumns;
        this.partitionColumns = partitionColumns;
        this.tableParameters = tableParameters;
    }

    @JsonProperty
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Optional<Set<ColumnHandle>> getDataColumns() {
        return dataColumns;
    }

    public LakeSoulTableHandle getTableHandle() {
        return tableHandle;
    }

    public DBUtil.TablePartitionKeys getPartitionColumns() {
        return partitionColumns;
    }

    public JSONObject getTableParameters() {
        return tableParameters;
    }
}
