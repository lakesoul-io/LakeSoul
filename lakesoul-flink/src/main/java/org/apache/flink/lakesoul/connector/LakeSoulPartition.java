// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.types.TableId;

import java.io.Serializable;
import java.util.List;

@PublicEvolving
public class LakeSoulPartition implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    private final List<Path> paths;

    private final List<String> partitionKeys;
    private final List<String> partitionValues;

    public LakeSoulPartition(TableId tableId, List<Path> paths,
                             List<String> partitionKeys, List<String> partitionValues) {
        this.tableId = tableId;
        this.paths = paths;
        this.partitionKeys = partitionKeys;
        this.partitionValues = partitionValues;
    }

    public List<Path> getPaths() {
        return paths;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }

    @Override public String toString() {
        return "LakeSoulPartition{" +
                "tableId=" + tableId +
                ", paths=" + paths +
                ", partitionKeys=" + partitionKeys +
                ", partitionValues=" + partitionValues +
                '}';
    }
}
