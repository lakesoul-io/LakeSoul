package org.apache.flink.lakesoul.source;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.fs.Path;

import java.util.List;

/**
 * Source split for LakeSoul's flink source
 */
public class LakeSoulSplit implements SourceSplit {

    private final String id;

    private final List<Path> files;

    public LakeSoulSplit(String id, List<Path> files) {
        this.id = id;
        this.files = files;
    }

    @Override
    public String splitId() {
        return id;
    }

    public List<Path> getFiles() {
        return files;
    }

    @Override
    public String toString() {
        return "LakeSoulSplit[" +
                files.toString() +
                "]";
    }
}
