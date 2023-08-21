// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class LakeSoulRowDataWrapper {
    TableId tableId;
    String op;
    RowData before;
    RowData after;
    RowType beforeType;
    RowType afterType;

    JSONObject properties;

    public LakeSoulRowDataWrapper(TableId tableId, String op, RowData before, RowData after, RowType beforeType,
                                  RowType afterType, JSONObject properties) {
        this.tableId = tableId;
        this.op = op;
        this.before = before;
        this.after = after;
        this.beforeType = beforeType;
        this.afterType = afterType;
        this.properties = properties;
    }

    public TableId getTableId() {
        return tableId;
    }

    public RowData getAfter() {
        return after;
    }

    public RowData getBefore() {
        return before;
    }

    public RowType getAfterType() {
        return afterType;
    }

    public RowType getBeforeType() {
        return beforeType;
    }

    public String getOp() {
        return op;
    }

    public JSONObject getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "LakeSoulRowDataWrapper{" +
                "tableId=" + tableId +
                ", op='" + op + '\'' +
                ", before=" + before +
                ", after=" + after +
                ", beforeType=" + beforeType +
                ", afterType=" + afterType +
                ", properties=" + properties +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        TableId tableId;
        String op;
        RowData before;
        RowData after;
        RowType beforeType;
        RowType afterType;

        JSONObject properties;

        public Builder setTableId(TableId tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder setOperation(String op) {
            this.op = op;
            return this;
        }

        public Builder setBeforeRowData(RowData before) {
            this.before = before;
            return this;
        }

        public Builder setAfterRowData(RowData after) {
            this.after = after;
            return this;
        }

        public Builder setBeforeRowType(RowType before) {
            this.beforeType = before;
            return this;
        }

        public Builder setAfterType(RowType after) {
            this.afterType = after;
            return this;
        }

        public Builder setProperties(JSONObject properties) {
            this.properties = properties;
            return this;
        }

        public LakeSoulRowDataWrapper build() {
            return new LakeSoulRowDataWrapper(this.tableId, this.op, this.before, this.after, this.beforeType,
                    this.afterType, this.properties);
        }
    }
}
