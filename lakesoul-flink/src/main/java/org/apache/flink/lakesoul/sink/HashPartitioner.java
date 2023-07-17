// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.common.functions.Partitioner;

public class HashPartitioner implements Partitioner<Long> {

  @Override
  public int partition(Long key, int numPartitions) {
    long hash = key;
    int part = (int) (hash % (long) numPartitions);
    return part < 0 ? (part + numPartitions) % numPartitions : part;
  }
}
