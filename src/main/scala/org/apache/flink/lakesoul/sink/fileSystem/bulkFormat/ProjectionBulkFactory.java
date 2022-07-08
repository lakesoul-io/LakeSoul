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

package org.apache.flink.lakesoul.sink.fileSystem.bulkFormat;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.lakesoul.sink.partition.LakeSoulCdcPartitionComputer;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class ProjectionBulkFactory implements BulkWriter.Factory<RowData> {

  private final BulkWriter.Factory<RowData> factory;
  private final LakeSoulCdcPartitionComputer computer;

  public ProjectionBulkFactory(
      BulkWriter.Factory<RowData> factory, LakeSoulCdcPartitionComputer computer) {
    this.factory = factory;
    this.computer = computer;
  }

  @Override
  public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
    BulkWriter<RowData> writer = factory.create(out);
    return new BulkWriter<RowData>() {

      @Override
      public void addElement(RowData element) throws IOException {
        writer.addElement(computer.projectColumnsToWrite(element));
      }

      @Override
      public void flush() throws IOException {
        writer.flush();
      }

      @Override
      public void finish() throws IOException {
        writer.finish();
      }
    };
  }
}
