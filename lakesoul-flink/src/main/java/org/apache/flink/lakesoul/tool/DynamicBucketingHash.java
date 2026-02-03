// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

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

    public static long hash(RowData rowData,
                            LakeSoulKeyGen pkKeyGen) {
        if (pkKeyGen == null) {
            return random.nextLong();
        }
        return pkKeyGen.getRePartitionHash(rowData);
    }

    public static long hash(RowType rowType, RowData rowData,
                            List<String> primaryKeyList) {
        if (primaryKeyList.isEmpty()) {
            return random.nextLong();
        }
        long hash = 42;
        for (String key : primaryKeyList) {
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
