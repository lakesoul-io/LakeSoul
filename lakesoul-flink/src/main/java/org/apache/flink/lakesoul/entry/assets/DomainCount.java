// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.assets;

public class DomainCount {
    String domain;
    int tableCounts;
    int fileCounts;
    int fileBaseCount;
    int partitionCounts;
    long fileTotalSize;
    long fileBaseSize;

    public DomainCount(String domain, int tableCounts, int fileCounts, int fileBaseCount, int partitionCounts, long fileTotalSize, long fileBaseSize) {
        this.domain = domain;
        this.tableCounts = tableCounts;
        this.fileCounts = fileCounts;
        this.fileBaseCount = fileBaseCount;
        this.partitionCounts = partitionCounts;
        this.fileTotalSize = fileTotalSize;
        this.fileBaseSize = fileBaseSize;
    }

    @Override
    public String toString() {
        return "DomainCount{" +
                "domain='" + domain + '\'' +
                ", tableCounts=" + tableCounts +
                ", fileCounts=" + fileCounts +
                ", fileBaseCount=" + fileBaseCount +
                ", partitionCounts=" + partitionCounts +
                ", fileTotalSize=" + fileTotalSize +
                ", fileBaseSize=" + fileBaseSize +
                '}';
    }
}
