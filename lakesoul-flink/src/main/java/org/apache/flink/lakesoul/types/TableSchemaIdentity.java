/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.types;

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

    public TableSchemaIdentity() {
        this.tableId = null;
        this.tableLocation = null;
        this.primaryKeys = null;
        this.partitionKeyList = null;
    }

    public TableSchemaIdentity(TableId tableId, RowType rowType, String tableLocation, List<String> primaryKeys,
                               List<String> partitionKeyList) {
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
               '}';
    }
}
