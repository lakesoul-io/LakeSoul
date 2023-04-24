package org.apache.flink.lakesoul.connector;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@PublicEvolving
public class LakeSoulPartition implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<Path> paths;

    private List<String> partitionKeys= new ArrayList<>(), getPartitionValues= new ArrayList<>();

    public LakeSoulPartition(List<Path> paths) {
        this.paths = paths;
    }

    public List<Path> getPaths() {
        return paths;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getGetPartitionValues() {
        return getPartitionValues;
    }
}
