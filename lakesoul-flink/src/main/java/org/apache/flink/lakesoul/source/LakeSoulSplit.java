/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.source;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.bucket.BucketsBuilder;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Source split for LakeSoul's flink source
 */
public class LakeSoulSplit implements SourceSplit {

    private final String id;
    private long skipRecord = 0;

    private final List<Path> files;
    private int bucketId = -1;

    public LakeSoulSplit(String id, List<Path> files, long skipRecord) {
        this.id = id;
        this.files = files;
        this.skipRecord = skipRecord;
    }

    public LakeSoulSplit(String id, List<Path> files, long skipRecord, int bucketId) {
        this.id = id;
        this.files = files;
        this.skipRecord = skipRecord;
        this.bucketId = bucketId;
    }

    @Override
    public String splitId() {
        return id;
    }

    public List<Path> getFiles() {
        return files;
    }

    public void incrementRecord() {
        this.skipRecord++;
    }

    @Override
    public String toString() {
        return "LakeSoulSplit[" +
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
