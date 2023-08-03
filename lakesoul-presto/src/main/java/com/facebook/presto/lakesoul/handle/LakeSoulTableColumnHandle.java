// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.handle;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;

public class LakeSoulTableColumnHandle implements ColumnHandle {
    private LakeSoulTableHandle tableHandle;
    private String columnName;

    private ColumnMetadata columnMetadata;

    public LakeSoulTableColumnHandle(LakeSoulTableHandle tableHandle, String columnName, ColumnMetadata columnMetadata) {
        this.tableHandle = tableHandle;
        this.columnName = columnName;
        this.columnMetadata = columnMetadata;
    }

    public LakeSoulTableHandle getTableHandle() {
        return tableHandle;
    }

    public void setTableHandle(LakeSoulTableHandle tableHandle) {
        this.tableHandle = tableHandle;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public ColumnMetadata getColumnMetadata() {
        return columnMetadata;
    }

    public void setColumnMetadata(ColumnMetadata columnMetadata) {
        this.columnMetadata = columnMetadata;
    }
}

