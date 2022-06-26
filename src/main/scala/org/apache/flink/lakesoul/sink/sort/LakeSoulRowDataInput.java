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

package org.apache.flink.lakesoul.sink.sort;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class LakeSoulRowDataInput implements StreamTaskInput<RowData> {
  private final Iterator<RowData> elementsIterator;
  private final int inputIdx;
  private boolean endOfInput = false;

  public LakeSoulRowDataInput(Iterator<RowData> elements) {
    this.elementsIterator = elements;
    this.inputIdx = 0;
  }

  LakeSoulRowDataInput(Collection<RowData> elements, int inputIdx) {
    this.elementsIterator = elements.iterator();
    this.inputIdx = inputIdx;
  }

  @Override
  public DataInputStatus emitNext(DataOutput<RowData> output) throws Exception {
    if (elementsIterator.hasNext()) {
      RowData row = elementsIterator.next();
      output.emitRecord(new StreamRecord<>(row));
    }
    if (elementsIterator.hasNext()) {
      return DataInputStatus.MORE_AVAILABLE;
    } else if (endOfInput) {
      return DataInputStatus.END_OF_INPUT;
    } else {
      endOfInput = true;
      return DataInputStatus.END_OF_DATA;
    }
  }

  @Override
  public CompletableFuture<?> getAvailableFuture() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public int getInputIndex() {
    return inputIdx;
  }

  @Override
  public CompletableFuture<Void> prepareSnapshot(
      ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
    return null;
  }

  @Override
  public void close() throws IOException {
  }
}
