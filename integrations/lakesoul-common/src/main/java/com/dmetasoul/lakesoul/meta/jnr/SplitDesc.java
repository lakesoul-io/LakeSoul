package com.dmetasoul.lakesoul.meta.jnr;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.HashMap;
import java.util.List;

public class SplitDesc {
    @JsonProperty("file_paths")
    private List<String> filePaths;

    @JsonProperty("primary_keys")
    private List<String> primaryKeys;

    @JsonProperty("partition_desc")
    private HashMap<String, String> partitionDesc;

    @JsonProperty("table_schema")
    private String tableSchema;

    public List<String> getFilePaths() {
        return filePaths;
    }

    public void setFilePaths(List<String> filePaths) {
        this.filePaths = filePaths;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public HashMap<String, String> getPartitionDesc() {
        return partitionDesc;
    }

    public void setPartitionDesc(HashMap<String, String> partitionDesc) {
        this.partitionDesc = partitionDesc;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                .append("file_paths", filePaths)
                .append("primary_keys", primaryKeys)
                .append("partition_desc", partitionDesc)
                .append("table_schema", tableSchema)
                .toString();
    }
}
