/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakeSoul.sink.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.spark.sql.catalyst.expressions.Murmur3HashFunction;
import org.apache.spark.unsafe.types.UTF8String;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class DataPartitioner<String> implements Partitioner<String> {

  @Override
  public int partition(String key, int numPartitions) {
    UTF8String utf8String = UTF8String.fromString(java.lang.String.valueOf(key));
    int hash = (int) Murmur3HashFunction.hash(utf8String, StringType, 42) % numPartitions;
    return hash < 0 ? (hash + numPartitions) % numPartitions : hash;
  }
}
