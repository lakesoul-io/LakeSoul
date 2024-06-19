// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.RowData;

public class LakeSoulRecordEmitter<OUT> implements RecordEmitter<OUT, OUT, LakeSoulPartitionSplit> {
    @Override
    public void emitRecord(OUT element, SourceOutput<OUT> output, LakeSoulPartitionSplit splitState) throws Exception {
        output.collect(element);
        splitState.incrementRecord();
    }
}
