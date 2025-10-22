// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.assets;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DomainLevelAssets extends KeyedProcessFunction<String, TableCountsWithTableInfo, DomainCount> {
    private MapState<String,TableCounts> tableCountsMapState;
    private ValueState<DomainCount> domainCountValueState;
    private MapState<String, Boolean> tableNumbersState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, TableCounts> tableStateDescriptor =
                new MapStateDescriptor<>("tableCountsMapState",
                        String.class,
                        TableCounts.class);

        ValueStateDescriptor<DomainCount> databaseStateDescriptor =
                new ValueStateDescriptor<>("nameSpaceCountValueState", DomainCount.class);

        MapStateDescriptor<String, Boolean> tableNumbers =
                new MapStateDescriptor<>("tableNumbersState",String.class,Boolean.class);

        tableCountsMapState = getRuntimeContext().getMapState(tableStateDescriptor);
        domainCountValueState = getRuntimeContext().getState(databaseStateDescriptor);
        tableNumbersState = getRuntimeContext().getMapState(tableNumbers);

    }
    @Override
    public void processElement(TableCountsWithTableInfo tableCountsWithTableInfo, KeyedProcessFunction<String, TableCountsWithTableInfo, DomainCount>.Context context, Collector<DomainCount> collector) throws Exception {

        String domain = tableCountsWithTableInfo.domain;
        int fileCount = tableCountsWithTableInfo.fileCount;
        int fileBaseCount = tableCountsWithTableInfo.fileBaseCount;
        int partitionCount = tableCountsWithTableInfo.partionCount;
        long fileBaseSize = tableCountsWithTableInfo.fileBaseSize;
        long fileSize = tableCountsWithTableInfo.fileSize;
        String tableId = tableCountsWithTableInfo.tableId;

        DomainCount domainCount = domainCountValueState.value();
        int currentPartitionCount = domainCount == null? 0 : domainCount.partitionCounts;
        int currentFileCount = domainCount == null ? 0: domainCount.fileCounts;
        int currentFileBaseCount = domainCount == null? 0 : domainCount.fileBaseCount;
        long currentFileSize = domainCount == null? 0 : domainCount.fileTotalSize;
        long currentFileBaseSize = domainCount == null? 0 : domainCount.fileBaseSize;


        int oldPartitionCounts = tableCountsMapState.get(tableId) == null ? 0 : tableCountsMapState.get(tableId).partitionCounts;
        long oldBaseFileSize = tableCountsMapState.get(tableId) == null ? 0 : tableCountsMapState.get(tableId).baseFileSize;
        long oldFileSize = tableCountsMapState.get(tableId) == null ? 0 : tableCountsMapState.get(tableId).totalFileSize;
        int oldBaseFileCounts = tableCountsMapState.get(tableId) == null ? 0 : tableCountsMapState.get(tableId).baseFileCounts;
        int oldFileCounts = tableCountsMapState.get(tableId) == null ? 0 : tableCountsMapState.get(tableId).totalFileCounts;

        if (!tableNumbersState.contains(tableId) && fileCount > 0 && fileSize > 0) {
            tableNumbersState.put(tableId,true);
        } else {
            if (fileCount == 0) {
                tableNumbersState.remove(tableId);
            }
        }

        int k = 0;
        for (String key : tableNumbersState.keys()) {
            k ++ ;
        }
        TableCounts newTableCounts = new TableCounts(tableId,partitionCount,fileBaseCount,fileCount,fileBaseSize,fileSize);

        if (!tableCountsMapState.contains(tableId)){
            DomainCount newDomainCount = new DomainCount(domain,k, currentFileCount + fileCount,
                    currentFileBaseCount + fileBaseCount,currentPartitionCount + partitionCount,currentFileSize + fileSize, currentFileBaseSize + fileBaseSize);
            domainCountValueState.update(newDomainCount);
            collector.collect(newDomainCount);
            tableCountsMapState.put(tableId,newTableCounts);
        } else {
            DomainCount newDomainCount = new DomainCount(domain,k,currentFileCount + fileCount - oldFileCounts,
                    currentFileBaseCount + fileBaseCount - oldBaseFileCounts,
                    currentPartitionCount + partitionCount - oldPartitionCounts,
                    currentFileSize + fileSize - oldFileSize, currentFileBaseSize + fileBaseSize - oldBaseFileSize);
            domainCountValueState.update(newDomainCount);
            collector.collect(newDomainCount);
            tableCountsMapState.put(tableId,newTableCounts);
        }
    }
}
