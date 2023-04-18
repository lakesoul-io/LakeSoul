package org.apache.flink.lakesoul.connector;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;
import java.util.List;

@PublicEvolving
public class LakeSoulPartition implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<Path> paths;

    public LakeSoulPartition(List<Path> paths) {
        this.paths = paths;
    }

    public List<Path> getPaths() {
        return paths;
//        return dataFileInfo.path();
    }
}
