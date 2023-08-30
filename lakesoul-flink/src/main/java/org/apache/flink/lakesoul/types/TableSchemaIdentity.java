// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public final class TableSchemaIdentity implements Serializable {
    public final TableId tableId;

    public RowType rowType;

    public final String tableLocation;

    public final List<String> primaryKeys;

    public final List<String> partitionKeyList;

    public final boolean useCDC;

    public final String cdcColumn;

    public TableSchemaIdentity(TableId tableId, RowType rowType, String tableLocation, List<String> primaryKeys,
                               List<String> partitionKeyList, boolean useCDC, String cdcColumn) {
        this.tableId = tableId;
        this.rowType = rowType;
        this.tableLocation = tableLocation;
        this.primaryKeys = primaryKeys;
        this.partitionKeyList = partitionKeyList;
        this.useCDC = useCDC;
        this.cdcColumn = cdcColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableSchemaIdentity that = (TableSchemaIdentity) o;
        assert tableId != null;
        return tableId.equals(that.tableId) && rowType.equals(that.rowType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, rowType);
    }

    @Override public String toString() {
        return "TableSchemaIdentity{" +
                "tableId=" + tableId +
                ", rowType=" + rowType +
                ", tableLocation='" + tableLocation + '\'' +
                ", primaryKeys=" + primaryKeys +
                ", partitionKeyList=" + partitionKeyList +
                ", useCDC=" + useCDC +
                ", cdcColumn='" + cdcColumn + '\'' +
                '}';
    }
}
