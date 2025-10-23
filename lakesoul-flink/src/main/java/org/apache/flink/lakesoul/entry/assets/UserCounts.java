// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.assets;

public class UserCounts {
    String creator;
    int tableCounts;
    int fileCounts;
    int fileBaseCount;
    int partitionCounts;
    long fileTotalSize;
    long fileBaseSize;

    public UserCounts(String creator, int tableCounts, int fileCounts, int fileBaseCount, int partitionCounts, long fileTotalSize, long fileBaseSize) {
        this.creator = creator;
        this.tableCounts = tableCounts;
        this.fileCounts = fileCounts;
        this.fileBaseCount = fileBaseCount;
        this.partitionCounts = partitionCounts;
        this.fileTotalSize = fileTotalSize;
        this.fileBaseSize = fileBaseSize;
    }

    @Override
    public String toString() {
        return "UserCounts{" +
                "creator='" + creator + '\'' +
                ", tableCounts=" + tableCounts +
                ", fileCounts=" + fileCounts +
                ", fileBaseCount=" + fileBaseCount +
                ", partitionCounts=" + partitionCounts +
                ", fileTotalSize=" + fileTotalSize +
                ", fileBaseSize=" + fileBaseSize +
                '}';
    }
}
