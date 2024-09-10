// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.common.functions.Partitioner;

public class HashPartitioner implements Partitioner<Long> {

  private final int hashBucketNum;

  public HashPartitioner(int hashBucketNum) {
    this.hashBucketNum = hashBucketNum;
  }

  @Override
  public int partition(Long key, int numPartitions) {
    long hash = key;
    if (numPartitions > hashBucketNum) {
      int ceiling = ((numPartitions - 1) / hashBucketNum + 1) * hashBucketNum;
      int part = (int) (hash % (long) ceiling);
      if (part < 0) {
        part = (part + ceiling) % ceiling;
      }
      return part >= numPartitions ? part % hashBucketNum : part;
    } else {
      int part = (int) (hash % (long) numPartitions);
      return part < 0? (part + numPartitions) % numPartitions : part;
    }
  }
}
