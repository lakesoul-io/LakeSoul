// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.assets;

public class NameSpaceCount {
    String nameSpace;
    int tableCounts;
    int fileCounts;
    int fileBaseCount;
    int partitionCounts;
    long fileTotalSize;
    long fileBaseSize;

    public NameSpaceCount(String nameSpace, int tableCounts, int fileCounts, int fileBaseCount, int partitionCounts, long fileTotalSize, long fileBaseSize) {
        this.nameSpace = nameSpace;
        this.tableCounts = tableCounts;
        this.fileCounts = fileCounts;
        this.fileBaseCount = fileBaseCount;
        this.partitionCounts = partitionCounts;
        this.fileTotalSize = fileTotalSize;
        this.fileBaseSize = fileBaseSize;
    }

    @Override
    public String toString() {
        return "NameSpaceCount{" +
                "nameSpace='" + nameSpace + '\'' +
                ", tableCounts=" + tableCounts +
                ", fileCounts=" + fileCounts +
                ", fileBaseCount=" + fileBaseCount +
                ", partitionCounts=" + partitionCounts +
                ", fileTotalSize=" + fileTotalSize +
                ", fileBaseSize=" + fileBaseSize +
                '}';
    }
}
