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

package org.apache.flink.lakesoul.tools;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.lakesoul.sink.partition.LakesoulCdcPartitionComputer;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.OutputStream;

public class ProjectionEncoder implements Encoder<RowData> {

  private final Encoder<RowData> encoder;
  private final LakesoulCdcPartitionComputer computer;

  public ProjectionEncoder(Encoder<RowData> encoder, LakesoulCdcPartitionComputer computer) {
    this.encoder = encoder;
    this.computer = computer;
  }

  @Override
  public void encode(RowData element, OutputStream stream) throws IOException {
    encoder.encode(computer.projectColumnsToWrite(element), stream);
  }
}

