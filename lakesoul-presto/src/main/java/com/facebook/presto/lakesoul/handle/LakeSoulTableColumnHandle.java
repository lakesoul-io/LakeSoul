// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.handle;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LakeSoulTableColumnHandle implements ColumnHandle {
    private LakeSoulTableHandle tableHandle;
    private String columnName;

    @JsonCreator
    public LakeSoulTableColumnHandle(
            @JsonProperty("tableHandle") LakeSoulTableHandle tableHandle,
            @JsonProperty("columnName") String columnName) {
        this.tableHandle = tableHandle;
        this.columnName = columnName;
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


}

