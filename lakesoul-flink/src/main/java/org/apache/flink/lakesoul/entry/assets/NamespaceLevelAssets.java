package org.apache.flink.lakesoul.entry.assets;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class NamespaceLevelAssets extends KeyedProcessFunction<String, TableCountsWithTableInfo, NameSpaceCount>{

    private MapState<String,TableCounts> tableCountsMapState;
    private ValueState<NameSpaceCount> nameSpaceCountValueState;
    private MapState<String, Boolean> tableNumbersState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, TableCounts> tableStateDescriptor =
                new MapStateDescriptor<>("tableCountsMapState",
                        String.class,
                        TableCounts.class);


        ValueStateDescriptor<NameSpaceCount> databaseStateDescriptor =
                new ValueStateDescriptor<>("nameSpaceCountValueState", NameSpaceCount.class);

        MapStateDescriptor<String, Boolean> tableNumbers =
                new MapStateDescriptor<>("tableNumbersState",String.class,Boolean.class);

        tableCountsMapState = getRuntimeContext().getMapState(tableStateDescriptor);
        nameSpaceCountValueState = getRuntimeContext().getState(databaseStateDescriptor);
        tableNumbersState = getRuntimeContext().getMapState(tableNumbers);

    }
    @Override
    public void processElement(TableCountsWithTableInfo tableCountsWithTableInfo, KeyedProcessFunction<String, TableCountsWithTableInfo, NameSpaceCount>.Context context, Collector<NameSpaceCount> collector) throws Exception {
        String namespace = tableCountsWithTableInfo.namespace;
        int fileCount = tableCountsWithTableInfo.fileCount;
        int fileBaseCount = tableCountsWithTableInfo.fileBaseCount;
        int partitionCount = tableCountsWithTableInfo.partionCount;
        long fileBaseSize = tableCountsWithTableInfo.fileBaseSize;
        long fileSize = tableCountsWithTableInfo.fileSize;
        String tableId = tableCountsWithTableInfo.tableId;

        //查询当前状态下的统计

        NameSpaceCount nameSpaceCount = nameSpaceCountValueState.value();
        int currentPartitionCount = nameSpaceCount == null? 0 : nameSpaceCount.partitionCounts;
        int currentFileCount = nameSpaceCount == null ? 0: nameSpaceCount.fileCounts;
        int currentFileBaseCount = nameSpaceCount == null? 0 : nameSpaceCount.fileBaseCount;
        int currentTableCount = nameSpaceCount == null? 0 : nameSpaceCount.tableCounts;
        long currentFileSize = nameSpaceCount == null? 0 : nameSpaceCount.fileTotalSize;
        long currentFileBaseSize = nameSpaceCount == null? 0 : nameSpaceCount.fileBaseSize;

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
            NameSpaceCount newNameSpaceCount = new NameSpaceCount(namespace,k, currentFileCount + fileCount,
                    currentFileBaseCount + fileBaseCount,currentPartitionCount + partitionCount,currentFileSize + fileSize, currentFileBaseSize + fileBaseSize);
            nameSpaceCountValueState.update(newNameSpaceCount);
            collector.collect(newNameSpaceCount);
            tableCountsMapState.put(tableId,newTableCounts);
        } else {
            NameSpaceCount newNameSpaceCount = new NameSpaceCount(namespace,k ,currentFileCount + fileCount - oldFileCounts,
                    currentFileBaseCount + fileBaseCount - oldBaseFileCounts,
                    currentPartitionCount + partitionCount - oldPartitionCounts,
                    currentFileSize + fileSize - oldFileSize, currentFileBaseSize + fileBaseSize - oldBaseFileSize);
            nameSpaceCountValueState.update(newNameSpaceCount);
            collector.collect(newNameSpaceCount);
            tableCountsMapState.put(tableId,newTableCounts);
        }
    }
}
