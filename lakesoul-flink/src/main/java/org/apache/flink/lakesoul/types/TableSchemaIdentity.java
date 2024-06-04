// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;

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

    public static TableSchemaIdentity fromTableInfo(TableInfo tableInfo) throws IOException {
        RowType rowType = ArrowUtils.fromArrowSchema(Schema.fromJSON(tableInfo.getTableSchema()));
        DBUtil.TablePartitionKeys tablePartitionKeys = DBUtil.parseTableInfoPartitions(tableInfo.getPartitions());
        JSONObject properties = JSON.parseObject(tableInfo.getProperties());
        String lakesoulCdcColumnName = properties.getOrDefault(CDC_CHANGE_COLUMN, CDC_CHANGE_COLUMN_DEFAULT).toString();
        boolean useCdc = properties.getOrDefault(USE_CDC.key(), "false").equals("true");
        return new TableSchemaIdentity(
                new TableId(LakeSoulCatalog.CATALOG_NAME, tableInfo.getTableNamespace(), tableInfo.getTableName()),
                rowType,
                tableInfo.getTablePath(),
                tablePartitionKeys.primaryKeys,
                tablePartitionKeys.rangeKeys,
                useCdc,
                lakesoulCdcColumnName
        );
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

    @Override
    public String toString() {
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
