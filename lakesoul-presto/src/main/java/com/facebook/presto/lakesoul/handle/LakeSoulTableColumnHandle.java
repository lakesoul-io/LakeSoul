// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.handle;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class LakeSoulTableColumnHandle implements ColumnHandle {
    private LakeSoulTableHandle tableHandle;
    private String columnName;
    private Type columnType;

    @JsonCreator
    public LakeSoulTableColumnHandle(
            @JsonProperty("tableHandle") LakeSoulTableHandle tableHandle,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType) {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle should not be null") ;
        this.columnName = requireNonNull(columnName, "columnName should not be null") ;
        this.columnType = requireNonNull(columnType, "columnType should not be null") ;
    }

    @JsonProperty
    public LakeSoulTableHandle getTableHandle() {
        return tableHandle;
    }

    public void setTableHandle(LakeSoulTableHandle tableHandle) {
        this.tableHandle = tableHandle;
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @JsonProperty
    public Type getColumnType() {
        return columnType;
    }

    public void setColumnType(Type columnType) {
        this.columnType = columnType;
    }
}

