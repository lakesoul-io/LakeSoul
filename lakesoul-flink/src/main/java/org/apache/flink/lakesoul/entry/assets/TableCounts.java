// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.assets;

public class TableCounts {
    String tableId;
    int partitionCounts;
    int baseFileCounts;
    int totalFileCounts;
    long baseFileSize;
    long totalFileSize;

    public TableCounts(String tableId, int partitionCounts, int baseFileCounts, int totalFileCounts, long baseFileSize, long totalFileSize) {
        this.tableId = tableId;
        this.partitionCounts = partitionCounts;
        this.baseFileCounts = baseFileCounts;
        this.totalFileCounts = totalFileCounts;
        this.baseFileSize = baseFileSize;
        this.totalFileSize = totalFileSize;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return "TableCounts{" +
                "tableId='" + tableId + '\'' +
                ", partitionCounts=" + partitionCounts +
                ", baseFileCounts=" + baseFileCounts +
                ", totalFileCounts=" + totalFileCounts +
                ", baseFileSize=" + baseFileSize +
                ", totalFileSize=" + totalFileSize +
                '}';
    }
}
