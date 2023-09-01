// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class LakeSoulRowDataWrapper {
    private final TableId tableId;
    private final String op;
    private final RowData before;
    private final RowData after;
    private final RowType beforeType;
    private final RowType afterType;
    private final long tsMs;
    private final boolean useCDC;
    private final String cdcColumn;

    public LakeSoulRowDataWrapper(TableId tableId, String op, RowData before, RowData after, RowType beforeType,
                                  RowType afterType, long rsMs, boolean useCDC, String cdcColumn) {
        this.tableId = tableId;
        this.op = op;
        this.before = before;
        this.after = after;
        this.beforeType = beforeType;
        this.afterType = afterType;
        this.tsMs = rsMs;
        this.useCDC = useCDC;
        this.cdcColumn = cdcColumn;
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

    public long getTsMs() {
        return tsMs;
    }

    public boolean getUseCDC() {
        return useCDC;
    }

    public String getCdcColumn() {
        return cdcColumn;
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
                ", tsMs=" + tsMs +
                ", useCDC=" + useCDC +
                ", cdcColumn=" + cdcColumn +
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
        long tsMs;
        boolean useCDC;
        String cdcColumn;

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

        public Builder setTsMs(long tsMs) {
            this.tsMs = tsMs;
            return this;
        }

        public Builder setUseCDC(boolean useCDC) {
            this.useCDC = useCDC;
            return this;
        }

        public Builder setCDCColumn(String cdcColumn) {
            this.cdcColumn = cdcColumn;
            return this;
        }

        public LakeSoulRowDataWrapper build() {
            return new LakeSoulRowDataWrapper(this.tableId, this.op, this.before, this.after, this.beforeType,
                    this.afterType, this.tsMs, useCDC, cdcColumn);
        }
    }
}
