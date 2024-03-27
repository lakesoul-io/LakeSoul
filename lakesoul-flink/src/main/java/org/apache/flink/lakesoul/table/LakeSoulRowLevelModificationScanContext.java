package org.apache.flink.lakesoul.table;

import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.table.connector.RowLevelModificationScanContext;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class LakeSoulRowLevelModificationScanContext implements RowLevelModificationScanContext {

    private final JniWrapper sourcePartitionInfo;

    public LakeSoulRowLevelModificationScanContext(List<PartitionInfo> listPartitionInfo) {
        sourcePartitionInfo = JniWrapper.newBuilder().addAllPartitionInfo(listPartitionInfo).build();
    }

    public JniWrapper getSourcePartitionInfo() {
        return sourcePartitionInfo;
    }

    public String getBas64EncodedSourcePartitionInfo() {
        return Base64.getEncoder().encodeToString(getSourcePartitionInfo().toByteArray());
    }
}
