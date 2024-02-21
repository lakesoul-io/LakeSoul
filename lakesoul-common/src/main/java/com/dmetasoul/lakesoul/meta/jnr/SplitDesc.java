package com.dmetasoul.lakesoul.meta.jnr;

import com.alibaba.fastjson.annotation.JSONField;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.util.hash.Hash;

import java.util.HashMap;
import java.util.List;

public class SplitDesc {
    @JSONField(name = "file_paths")
    private List<String> filePaths;
    @JSONField(name = "primary_keys")
    private List<String> primaryKeys;
    @JSONField(name = "partition_desc")
    private HashMap<String, String> partitionDesc;
    @JSONField(name = "table_schema")
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
        this.partitionDesc= partitionDesc;
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
