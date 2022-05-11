package com.dmetasoul.lakesoul.meta.entity;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class TableInfo {
    private String tableId;

    private String tableName;

    private String tablePath;

    private String tableSchema;

    private JSONObject properties;

    private JSONArray partitions;

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTablePath() {
        return tablePath;
    }

    public void setTablePath(String tablePath) {
        this.tablePath = tablePath;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public JSONObject getProperties() {
        return properties;
    }

    public void setProperties(JSONObject properties) {
        this.properties = properties;
    }

    public JSONArray getPartitions() {
        return partitions;
    }

    public void setPartitions(JSONArray partitions) {
        this.partitions = partitions;
    }
}