package org.apache.flink.lakesoul.table;

import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import io.substrait.proto.Plan;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;

import java.util.Base64;
import java.util.List;
import java.util.Map;

public class LakeSoulRowLevelModificationScanContext implements RowLevelModificationScanContext {

    private JniWrapper sourcePartitionInfo;
    private final SupportsRowLevelModificationScan.RowLevelModificationType type;

    Plan partitionFilters;

    Plan nonPartitionFilters;


    public LakeSoulRowLevelModificationScanContext(SupportsRowLevelModificationScan.RowLevelModificationType type, List<PartitionInfo> listPartitionInfo) {
        this.type = type;
        sourcePartitionInfo = JniWrapper.newBuilder().addAllPartitionInfo(listPartitionInfo).build();
    }

    public void setSourcePartitionInfo(JniWrapper sourcePartitionInfo) {
        this.sourcePartitionInfo = sourcePartitionInfo;
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

    public void setNonPartitionFilters(Plan nonPartitionFilters) {
        this.nonPartitionFilters = nonPartitionFilters;
    }

    public void setPartitionFilters(Plan partitionFilters) {
        this.partitionFilters = partitionFilters;
    }

    public Plan getNonPartitionFilters() {
        return nonPartitionFilters;
    }

    public Plan getPartitionFilters() {
        return partitionFilters;
    }

    public boolean isDelete() {
        return type == SupportsRowLevelModificationScan.RowLevelModificationType.DELETE;
    }

    public boolean isUpdate() {
        return type == SupportsRowLevelModificationScan.RowLevelModificationType.UPDATE;
    }

    @Override
    public String toString() {
        return "LakeSoulRowLevelModificationScanContext{" +
                "sourcePartitionInfo=" + sourcePartitionInfo +
                ", type=" + type +
                ", partitionFilters=" + partitionFilters +
                ", nonPartitionFilters=" + nonPartitionFilters +
                '}';
    }
}
