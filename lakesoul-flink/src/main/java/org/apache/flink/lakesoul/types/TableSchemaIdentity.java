package org.apache.flink.lakesoul.types;

import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public final class TableSchemaIdentity implements Serializable {
    public final TableId tableId;

    public final RowType rowType;

    public final String tableLocation;
    public final List<String> primaryKeys;
    public final List<String> partitionKeyList;

    public TableSchemaIdentity(TableId tableId, RowType rowType, String tableLocation, List<String> primaryKeys, List<String> partitionKeyList) {
        this.tableId = tableId;
        this.rowType = rowType;
        this.tableLocation = tableLocation;
        this.primaryKeys = primaryKeys;
        this.partitionKeyList = partitionKeyList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableSchemaIdentity that = (TableSchemaIdentity) o;
        return tableId.equals(that.tableId) && rowType.equals(that.rowType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, rowType);
    }
}
