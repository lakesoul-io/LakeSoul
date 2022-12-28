package com.dmetasoul.lakesoul.meta.entity;

public class StreamingRecordInfo {

    private String table_id;

    private String query_id;

    private long batch_id;

    private long timestamp;

    public StreamingRecordInfo() {
    }

    public StreamingRecordInfo(String table_id, String query_id, long batch_id, long timestamp) {
        this.table_id = table_id;
        this.query_id = query_id;
        this.batch_id = batch_id;
        this.timestamp = timestamp;
    }

    public String getTableId() {
        return table_id;
    }

    public void setTableId(String table_id) {
        this.table_id = table_id;
    }

    public String getQueryId() {
        return query_id;
    }

    public void setQueryId(String query_id) {
        this.query_id = query_id;
    }

    public long getBatchId() {
        return batch_id;
    }

    public void setBatchId(long batch_id) {
        this.batch_id = batch_id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
