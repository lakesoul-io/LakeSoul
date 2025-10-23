// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.assets;

public class TableCountsWithTableInfo {
    String tableId;
    String tableName;
    String creator;
    String namespace;
    String domain;
    int partionCount;
    int fileBaseCount;
    int fileCount;
    long fileBaseSize;
    long fileSize;

    public TableCountsWithTableInfo(String tableId, String tableName, String creator, String namespace, String domain, int partionCount, int fileBaseCount, int fileCount, long fileBaseSize, long fileSize) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.creator = creator;
        this.namespace = namespace;
        this.domain = domain;
        this.partionCount = partionCount;
        this.fileBaseCount = fileBaseCount;
        this.fileCount = fileCount;
        this.fileBaseSize = fileBaseSize;
        this.fileSize = fileSize;
    }

    @Override
    public String toString() {
        return "TableCountsWithTableInfo{" +
                "tableId='" + tableId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", creator='" + creator + '\'' +
                ", namespace='" + namespace + '\'' +
                ", domain='" + domain + '\'' +
                ", partionCount=" + partionCount +
                ", fileBaseCount=" + fileBaseCount +
                ", fileCount=" + fileCount +
                ", fileBaseSize=" + fileBaseSize +
                ", fileSize=" + fileSize +
                '}';
    }
}
