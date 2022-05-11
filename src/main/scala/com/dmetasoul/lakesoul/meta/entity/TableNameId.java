package com.dmetasoul.lakesoul.meta.entity;

public class TableNameId {
    private String tableName;

    private String tableId;

    public TableNameId() {}

    public TableNameId(String tableName, String tableId) {
        this.tableName = tableName;
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName == null ? null : tableName.trim();
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId == null ? null : tableId.trim();
    }
}