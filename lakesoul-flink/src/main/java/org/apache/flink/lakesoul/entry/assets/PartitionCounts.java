package org.apache.flink.lakesoul.entry.assets;

public class PartitionCounts {
    String tableId;
    String partitionDesc;
    int allFileCounts;
    int baseFileCounts;
    long totalPartitionSize;
    long partitionSize;

    public PartitionCounts(String tableId, String partitionDesc, int allFileCounts, int fileCounts, long totalPartitionSize, long partitionSize) {
        this.tableId = tableId;
        this.partitionDesc = partitionDesc;
        this.allFileCounts = allFileCounts;
        this.baseFileCounts = fileCounts;
        this.totalPartitionSize = totalPartitionSize;
        this.partitionSize = partitionSize;
    }

    @Override
    public String toString() {
        return "PartitionCounts{" +
                "tableId='" + tableId + '\'' +
                ", partitionDesc='" + partitionDesc + '\'' +
                ", allFileCounts=" + allFileCounts +
                ", fileCounts=" + baseFileCounts +
                ", totalPartitionSize=" + totalPartitionSize +
                ", partitionSize=" + partitionSize +
                '}';
    }
}
