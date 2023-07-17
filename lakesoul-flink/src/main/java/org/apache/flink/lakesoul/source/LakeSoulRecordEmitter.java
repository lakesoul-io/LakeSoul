// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.RowData;

public class LakeSoulRecordEmitter implements RecordEmitter<RowData, RowData, LakeSoulSplit> {
    @Override
    public void emitRecord(RowData element, SourceOutput<RowData> output, LakeSoulSplit splitState) throws Exception {
        output.collect(element);
        splitState.incrementRecord();
    }
}
