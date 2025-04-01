// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.arrow;

public class CompressDataFileInfo {
    private String filePath;
    private long fileSize;
    private String fileExistsCols;
    private long timestamp;

    public CompressDataFileInfo(String filePath, long fileSize, String fileExistCols, long timestamp) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.fileExistsCols = fileExistCols;
        this.timestamp = timestamp;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getFileExistCols() {
        return fileExistsCols;
    }

    public void setFileExistCols(String fileExistsCols) {
        this.fileExistsCols = fileExistsCols;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override public String toString() {
        return "CompressDataFileInfo{" +
                "filePath='" + filePath + '\'' +
                ", fileSize=" + fileSize +
                ", fileExistsCols='" + fileExistsCols + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
