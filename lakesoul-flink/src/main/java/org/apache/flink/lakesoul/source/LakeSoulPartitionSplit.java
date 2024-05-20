// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Source split for LakeSoul's flink source
 */
public class LakeSoulPartitionSplit implements SourceSplit, Serializable {

    private final String id;
    private long skipRecord = 0;

    private final List<Path> files;
    private int bucketId = -1;

    private final String partitionDesc;

    public LakeSoulPartitionSplit(String id, List<Path> files, long skipRecord, String partitionDesc) {
        assert id != null;
        this.id = id;
        this.files = files;
        this.skipRecord = skipRecord;
        this.partitionDesc = partitionDesc;
    }

    public LakeSoulPartitionSplit(String id, List<Path> files, long skipRecord, int bucketId, String partitionDesc) {
        assert id != null;
        this.id = id;
        this.files = files;
        this.skipRecord = skipRecord;
        this.bucketId = bucketId;
        this.partitionDesc = partitionDesc;
    }

    @Override
    public String splitId() {
        return id;
    }

    public List<Path> getFiles() {
        return files;
    }

    public String getPartitionDesc() {
        return partitionDesc;
    }

    public void incrementRecord() {
        this.skipRecord++;
    }

    @Override
    public String toString() {
        return "LakeSoulSplit:" + id +
                "[" +
                files.stream().map(Object::toString)
                        .collect(Collectors.joining(", ")) +
                "]";
    }

    public long getSkipRecord() {
        return skipRecord;
    }

    public int getBucketId() {
        return this.bucketId;
    }
}
