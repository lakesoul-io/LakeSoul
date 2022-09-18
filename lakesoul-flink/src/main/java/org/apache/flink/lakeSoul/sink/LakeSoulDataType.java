package org.apache.flink.lakeSoul.sink;

import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import io.debezium.relational.TableId;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class LakeSoulDataType {
    TableId tableId;
    String op;
    RowData before;
    RowData after;
    RowType beforeType;
    RowType afterType;

    public LakeSoulDataType(TableId tableId, String op, RowData before, RowData after, RowType beforeType, RowType afterType) {
        this.tableId = tableId;
        this.op = op;
        this.before = before;
        this.after = after;
        this.beforeType = beforeType;
        this.afterType = afterType;
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

    @Override
    public String toString() {
        return "LakeSoulDataType{" +
                "tableId=" + tableId +
                ", op='" + op + '\'' +
                ", before=" + before +
                ", after=" + after +
                ", beforeType=" + beforeType +
                ", afterType=" + afterType +
                '}';
    }

    public static Build newBuild() {
        return new Build();
    }

    public static class Build {
        TableId tableId;
        String op;
        RowData before;
        RowData after;
        RowType beforeType;
        RowType afterType;

        public Build setTableId(TableId tableId) {
            this.tableId = tableId;
            return this;
        }

        public Build setOperation(String op) {
            this.op = op;
            return this;
        }

        public Build setBeforeRowData(RowData before) {
            this.before = before;
            return this;
        }

        public Build setAfterRowData(RowData after) {
            this.after = after;
            return this;
        }

        public Build setBeforeRowType(RowType before) {
            this.beforeType = before;
            return this;
        }

        public Build setAfterType(RowType after) {
            this.afterType = after;
            return this;
        }
        public LakeSoulDataType build() {
            return new LakeSoulDataType(this.tableId, this.op, this.before, this.after, this.beforeType, this.afterType);
        }
    }
}
