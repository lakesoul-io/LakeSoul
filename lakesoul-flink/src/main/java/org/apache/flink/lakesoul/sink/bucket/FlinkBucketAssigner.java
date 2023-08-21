// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.connector.file.table.PartitionComputer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.data.RowData;

public class FlinkBucketAssigner implements BucketAssigner<RowData, String> {

  private final PartitionComputer<RowData> computer;

  public FlinkBucketAssigner(PartitionComputer<RowData> computer) {
    this.computer = computer;
  }

  /*
   * RowData bucket logic
   */
  @Override
  public String getBucketId(RowData element, Context context) {
    try {
      return FlinkUtil.generatePartitionPath(
          computer.generatePartValues(element));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }
}