package com.dmetasoul.lakesoul.meta.entity;

public class TablePathId {
    private String tablePath;

    private String tableId;

    public TablePathId() {}

    public TablePathId(String tablePath, String tableId) {
        this.tablePath = tablePath;
        this.tableId = tableId;
    }

    public String getTablePath() {
        return tablePath;
    }

    public void setTablePath(String tablePath) {
        this.tablePath = tablePath == null ? null : tablePath.trim();
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId == null ? null : tableId.trim();
    }
}