package org.apache.flink.lakesoul.entry.assets;

public class DataCommitInfo {
    String tableId;
    String partition_desc;
    String fileOps;
    String commitOp;
    Boolean committed;

    public DataCommitInfo(String tableId, String partition_desc, String fileOps, String commitOp, Boolean committed) {
        this.tableId = tableId;
        this.partition_desc = partition_desc;
        this.fileOps = fileOps;
        this.commitOp = commitOp;
        this.committed = committed;
    }

    @Override
    public String toString() {
        return "DataCommitInfo{" +
                "tableId='" + tableId + '\'' +
                ", partition_desc='" + partition_desc + '\'' +
                ", fileOps='" + fileOps + '\'' +
                ", commitOp='" + commitOp + '\'' +
                ", committed=" + committed +
                '}';
    }
}
