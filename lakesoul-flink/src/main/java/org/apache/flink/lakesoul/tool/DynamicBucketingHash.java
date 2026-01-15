package org.apache.flink.lakesoul.tool;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.List;
import java.util.Random;

public class DynamicBucketingHash implements Serializable {
    private static final long serialVersionUID = -1022775401990432854L;

    static Random random = new Random(System.currentTimeMillis());

    public static long hash(String table, RowData rowData,
                            LakeSoulKeyGen pkKeyGen,
                            LakeSoulKeyGen partKeyGen) {
        if (pkKeyGen == null && partKeyGen == null) {
            return random.nextLong();
        }
        long pkHash = 0;
        if (pkKeyGen != null) {
            pkHash = pkKeyGen.getRePartitionHash(rowData);
        }
        long partHash = 0;
        if (partKeyGen != null) {
            partHash = partKeyGen.getRePartitionHash(rowData);
        }
        String key = String.format("%s-%d-%d", table, pkHash, partHash);
        return key.hashCode();
    }

    public static long hash(String table, RowType rowType, RowData rowData,
                            List<String> primaryKeyList,
                            List<String> partitionKeyList,
                            int hashBucketNum,
                            int parallelism) {
        if (primaryKeyList.isEmpty() && partitionKeyList.isEmpty()) {
            return random.nextLong();
        }
        long pkHash = 0;
        if (!primaryKeyList.isEmpty()) {
            pkHash = hash(rowType, rowData, primaryKeyList);
        }
        long partitionHash = 0;
        if  (!partitionKeyList.isEmpty()) {
            partitionHash = hash(rowType, rowData, partitionKeyList);
        }
        String key = String.format("%s-%d-%d", table, pkHash % hashBucketNum,
                partitionHash % parallelism);
        return key.hashCode();
    }

    private static long hash(RowType rowType, RowData rowData, List<String> keyList) {
        long hash = 42;
        for (String key : keyList) {
            int typeIndex = rowType.getFieldIndex(key);
            if (typeIndex == -1) {
                continue;
            }
            LogicalType type = rowType.getTypeAt(typeIndex);
            Object fieldOrNull = RowData.createFieldGetter(type, typeIndex).getFieldOrNull(rowData);
            hash = LakeSoulKeyGen.getHash(type, fieldOrNull, hash);
        }
        return hash;
    }
}
