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

import com.dmetasoul.lakesoul.meta.DBUtil;

/**
 * Relationship between 'TableNamespace.TablePath' and TableId
 */
public class TablePathId {

    /**
     * Physical qualified path of table
     */
    private String tablePath;

    /**
     * Global unique identifier of table
     */
    private String tableId;

    /**
     * Namespace of table
     */
    private String tableNamespace;

    /**
     * Domain this entry belongs to.
     * Only when rbac feature enabled will have contents different to 'public'
     */
    private String domain = DBUtil.getDomain();

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

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }
}