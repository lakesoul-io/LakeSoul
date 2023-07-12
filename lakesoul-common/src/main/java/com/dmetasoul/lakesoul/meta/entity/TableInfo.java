/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.meta.entity;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBUtil;

/**
 * Meta Information for LakeSoul Table
 */
public class TableInfo {

    /**
     * Global unique identifier of table
     */
    private String tableId;

    /**
     * Namespace of table. A string of 'tableNamespace.tablePath' or 'tableNamespace.tableName'  maps one unique table globally
     */
    private String tableNamespace;

    /**
     * Name of table, optional
     */
    private String tableName;

    /**
     * Physical qualified path of table
     */
    private String tablePath;

    /**
     * Spark-formatted schema of table
     */
    private String tableSchema;

    /**
     * Properties of table, used to tag table with information not tracked by SQL
     */
    private JSONObject properties = new JSONObject();

    /**
     * Partition columns of table. Format of partitions is 'comma_separated_range_column;hash_column'
     */
    private String partitions;

    /**
     * Domain this entry belongs to.
     * Only when rbac feature enabled will have contents different to 'public'
     */
    private String domain = DBUtil.getDomain();

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getTableNamespace() {
        return tableNamespace;
    }

    public void setTableNamespace(String tableNamespace) {
        this.tableNamespace = tableNamespace;
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
        if (!properties.containsKey("domain")) {
            properties.put("domain", domain);
        }
    }

    public String getPartitions() {
        return partitions;
    }

    public void setPartitions(String partitions) {
        this.partitions = partitions;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "tableId='" + tableId + '\'' +
                ", tableNamespace='" + tableNamespace + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tablePath='" + tablePath + '\'' +
                ", tableSchema='" + tableSchema + '\'' +
                ", properties=" + properties +
                ", partitions='" + partitions + '\'' +
                '}';
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }
}