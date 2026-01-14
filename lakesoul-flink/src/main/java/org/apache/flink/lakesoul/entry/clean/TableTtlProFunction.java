// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TableTtlProFunction extends KeyedProcessFunction<String,PartitionInfoRecordGets.PartitionInfo, TableTtlProFunction.PartitionINfoUpdateEvents> {

    @Override
    public void processElement(PartitionInfoRecordGets.PartitionInfo value, KeyedProcessFunction<String, PartitionInfoRecordGets.PartitionInfo, PartitionINfoUpdateEvents>.Context ctx, Collector<PartitionINfoUpdateEvents> out) throws Exception {
        String tableId = value.tableId;
        String partitionDesc = value.partitionDesc;
        long timestamp = value.timestamp;
        PartitionINfoUpdateEvents partitionINfoUpdateEvents = new PartitionINfoUpdateEvents(tableId, partitionDesc, timestamp);
        out.collect(partitionINfoUpdateEvents);
    }

    public static class PartitionINfoUpdateEvents {
        String tableId;
        String partitionDesc;
        Long timestamp;

        public PartitionINfoUpdateEvents(String tableId, String partitionDesc, Long timestamp) {
            this.tableId = tableId;
            this.timestamp = timestamp;
            this.partitionDesc = partitionDesc;
        }

        @Override
        public String toString() {
            return "PartitionINfoUpdateEvents{" +
                    "tableId='" + tableId + '\'' +
                    ", partitionDesc='" + partitionDesc + '\'' +
                    ", updateTimestamp=" + timestamp +
                    '}';
        }
    }
}
