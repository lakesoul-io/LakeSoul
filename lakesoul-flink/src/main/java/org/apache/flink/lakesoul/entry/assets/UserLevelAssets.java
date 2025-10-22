package org.apache.flink.lakesoul.entry.assets;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UserLevelAssets extends KeyedProcessFunction<String, TableCountsWithTableInfo, UserCounts> {
    private MapState<String,TableCounts> tableCountsMapState;
    private ValueState<UserCounts> userCountValueState;

    private MapState<String, Boolean> tableNumbersState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, TableCounts> tableStateDescriptor =
                new MapStateDescriptor<>("tableCountsMapState",
                        String.class,
                        TableCounts.class);


        ValueStateDescriptor<UserCounts> databaseStateDescriptor =
                new ValueStateDescriptor<>("userCountValueState", UserCounts.class);

        MapStateDescriptor<String, Boolean> tableNumbers =
                new MapStateDescriptor<>("tableNumbersState",String.class,Boolean.class);

        tableCountsMapState = getRuntimeContext().getMapState(tableStateDescriptor);
        userCountValueState = getRuntimeContext().getState(databaseStateDescriptor);
        tableNumbersState = getRuntimeContext().getMapState(tableNumbers);

    }
    @Override
    public void processElement(TableCountsWithTableInfo tableCountsWithTableInfo, KeyedProcessFunction<String, TableCountsWithTableInfo, UserCounts>.Context context, Collector<UserCounts> collector) throws Exception {

        //System.out.println(tableCountsWithTableInfo);
        String user = tableCountsWithTableInfo.creator;
        int fileCount = tableCountsWithTableInfo.fileCount;
        int fileBaseCount = tableCountsWithTableInfo.fileBaseCount;
        int partitionCount = tableCountsWithTableInfo.partionCount;
        long fileBaseSize = tableCountsWithTableInfo.fileBaseSize;
        long fileSize = tableCountsWithTableInfo.fileSize;
        String tableId = tableCountsWithTableInfo.tableId;


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
        UserCounts userCount = userCountValueState.value();
        int currentPartitionCount = userCount == null? 0 : userCount.partitionCounts;
        int currentFileCount = userCount == null ? 0: userCount.fileCounts;
        int currentFileBaseCount = userCount == null? 0 : userCount.fileBaseCount;
        long currentFileSize = userCount == null? 0 : userCount.fileTotalSize;
        long currentFileBaseSize = userCount == null? 0 : userCount.fileBaseSize;

        int oldPartitionCounts = tableCountsMapState.get(tableId) == null ? 0 : tableCountsMapState.get(tableId).partitionCounts;
        long oldBaseFileSize = tableCountsMapState.get(tableId) == null ? 0 : tableCountsMapState.get(tableId).baseFileSize;
        long oldFileSize = tableCountsMapState.get(tableId) == null ? 0 : tableCountsMapState.get(tableId).totalFileSize;
        int oldBaseFileCounts = tableCountsMapState.get(tableId) == null ? 0 : tableCountsMapState.get(tableId).baseFileCounts;
        int oldFileCounts = tableCountsMapState.get(tableId) == null ? 0 : tableCountsMapState.get(tableId).totalFileCounts;

        TableCounts newTableCounts = new TableCounts(tableId,partitionCount,fileBaseCount,fileCount,fileBaseSize,fileSize);

        if (!tableCountsMapState.contains(tableId)){
            UserCounts newUserCounts = new UserCounts(user,k, currentFileCount + fileCount,
                    currentFileBaseCount + fileBaseCount,currentPartitionCount + partitionCount,currentFileSize + fileSize, currentFileBaseSize + fileBaseSize);
            userCountValueState.update(newUserCounts);
            collector.collect(newUserCounts);
            tableCountsMapState.put(tableId,newTableCounts);
        } else {
            UserCounts newUserCount = new UserCounts(user,k,currentFileCount + fileCount - oldFileCounts,
                    currentFileBaseCount + fileBaseCount - oldBaseFileCounts,
                    currentPartitionCount + partitionCount - oldPartitionCounts,
                    currentFileSize + fileSize - oldFileSize, currentFileBaseSize + fileBaseSize - oldBaseFileSize);
            userCountValueState.update(newUserCount);
            collector.collect(newUserCount);
            tableCountsMapState.put(tableId,newTableCounts);
        }
        //tableCountsMapState.put(tableId,newTableCounts);
    }
}
