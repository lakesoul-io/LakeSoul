package org.apache.flink.lakesoul.table;

import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;

import java.util.Base64;
import java.util.List;
import java.util.Map;

public class LakeSoulRowLevelModificationScanContext implements RowLevelModificationScanContext {

    private final JniWrapper sourcePartitionInfo;
    private final SupportsRowLevelModificationScan.RowLevelModificationType type;

    private List<Map<String, String>> remainingPartitions;

    public LakeSoulRowLevelModificationScanContext(SupportsRowLevelModificationScan.RowLevelModificationType type, List<PartitionInfo> listPartitionInfo) {
        this.type = type;
        sourcePartitionInfo = JniWrapper.newBuilder().addAllPartitionInfo(listPartitionInfo).build();
        remainingPartitions = null;
    }

    public JniWrapper getSourcePartitionInfo() {
        return sourcePartitionInfo;
    }

    public String getBas64EncodedSourcePartitionInfo() {
        return Base64.getEncoder().encodeToString(getSourcePartitionInfo().toByteArray());
    }

    public SupportsRowLevelModificationScan.RowLevelModificationType getType() {
        return type;
    }

    public void setRemainingPartitions(List<Map<String, String>> remainingPartitions) {
        this.remainingPartitions = remainingPartitions;
    }

    public List<Map<String, String>> getRemainingPartitions() {
        return remainingPartitions;
    }
}
