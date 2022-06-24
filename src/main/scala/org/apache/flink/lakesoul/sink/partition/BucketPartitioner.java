package org.apache.flink.lakesoul.sink.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class BucketPartitioner <String> implements Partitioner<String> {


    @Override
    public int partition(String key, int numPartitions) {
        int globalHash = key.hashCode() & Integer.MAX_VALUE;

        return globalHash%numPartitions;
    }
}
