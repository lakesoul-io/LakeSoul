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

public class TablePathId {
    private String tablePath;

    private String tableId;

    private String tableNamespace;

    public TablePathId() {}

    public TablePathId(String tablePath, String tableId) {
        this(tablePath, tableId, "default");
    }
    public TablePathId(String tablePath, String tableId, String tableNamespace) {
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.tableNamespace = tableNamespace;
    }

    public void setTableNamespace(String tableNamespace) {
        this.tableNamespace = tableNamespace;
    }

    public String getTableNamespace() {
        return tableNamespace;
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