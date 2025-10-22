package org.apache.flink.lakesoul.entry.assets;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class TableLevelAsstes extends KeyedProcessFunction<String, PartitionCounts, TableCounts> {
    private transient ValueState<Integer> tableBaseFileCountValue;
    private transient ValueState<Integer> tableTotalFileCountValue;
    private transient ValueState<Long> tableBaseSizeValue;
    private transient ValueState<Long> tableTotalSizeValue;
    private transient MapState<String, PartitionCounts> partitionCountsMapStateValue;
    private transient MapState<String, Boolean> partitionCountStateValue;
    private transient ValueState<HashSet<Integer>> partitionHashSetStateValue;


    @Override
    public void open(Configuration parameters) throws Exception {

        MapStateDescriptor<String, PartitionCounts> partitionCountsMapStateDescriptor = new MapStateDescriptor<>(
                "partitionCountsMapStateValue",
                String.class,
                PartitionCounts.class
        );
        partitionCountsMapStateValue = getRuntimeContext().getMapState(partitionCountsMapStateDescriptor);

        // 初始化累加状态
        ValueStateDescriptor<Integer> tableBaseFileCountDescriptor = new ValueStateDescriptor<>(
                "tableBaseFileCountValue", // 状态的名称
                Integer.class,      // 状态的类型
                0                   // 默认值为 0
        );
        tableBaseFileCountValue = getRuntimeContext().getState(tableBaseFileCountDescriptor);
        ValueStateDescriptor<Integer> tableTotalFileCountDescriptor = new ValueStateDescriptor<>(
                "tableTotalFileCountValue", // 状态的名称
                Integer.class,      // 状态的类型
                0                   // 默认值为 0
        );
        tableTotalFileCountValue = getRuntimeContext().getState(tableTotalFileCountDescriptor);

        ValueStateDescriptor<Long> tableBaseSizeDescriptor = new ValueStateDescriptor<>(
                "tableBaseSizeValue", // 状态的名称
                Long.class,          // 状态的类型
                0L                   // 默认值为 0L
        );
        tableBaseSizeValue = getRuntimeContext().getState(tableBaseSizeDescriptor);

        ValueStateDescriptor<Long> tableTotalSizeValueDescriptor = new ValueStateDescriptor<>(
                "tableTotalSizeValue", // 状态的名称
                Long.class,          // 状态的类型
                0L                   // 默认值为 0L
        );
        tableTotalSizeValue = getRuntimeContext().getState(tableTotalSizeValueDescriptor);

        MapStateDescriptor<String, Boolean> partitionCountValueDescriptor = new MapStateDescriptor<>(
                "partitionCountStateValue",
                String.class,
                Boolean.class
        );

        partitionCountStateValue = getRuntimeContext().getMapState(partitionCountValueDescriptor);

        ValueStateDescriptor<HashSet<Integer>> partitionHashSetCountValueDescriptor = new ValueStateDescriptor<HashSet<Integer>>(
                "partitionHashCountStateValue",
                TypeInformation.of(new TypeHint<HashSet<Integer>>() {}),
                null);

        partitionHashSetStateValue = getRuntimeContext().getState(partitionHashSetCountValueDescriptor);

    }

    @Override
    public void processElement(PartitionCounts partitionCounts, KeyedProcessFunction<String, PartitionCounts, TableCounts>.Context context, Collector<TableCounts> collector) throws Exception {

        String tableId = partitionCounts.tableId;
        String partitionDesc = partitionCounts.partitionDesc;

        int fileBaseCounts = partitionCounts.baseFileCounts;
        int fileCounts = partitionCounts.allFileCounts;
        long partitionBaseSize = partitionCounts.partitionSize;
        long partitionSize = partitionCounts.totalPartitionSize;

        int currentBaseFileCount = tableBaseFileCountValue.value();
        int currentFileCount = tableTotalFileCountValue.value();
        long currentBaseFileSize = tableBaseSizeValue.value();
        long currentFileSize = tableTotalSizeValue.value();

        int oldPartitionBaseFileCount = partitionCountsMapStateValue.get(partitionDesc) == null ? 0 : partitionCountsMapStateValue.get(partitionDesc).baseFileCounts;
        int oldPartitionFileCount = partitionCountsMapStateValue.get(partitionDesc) == null ? 0 : partitionCountsMapStateValue.get(partitionDesc).allFileCounts;
        long oldPartitionBaseFileSize = partitionCountsMapStateValue.get(partitionDesc) == null ? 0 : partitionCountsMapStateValue.get(partitionDesc).partitionSize;
        long oldPartitionFileSize = partitionCountsMapStateValue.get(partitionDesc) == null ? 0 : partitionCountsMapStateValue.get(partitionDesc).totalPartitionSize;

        // 防止文件数目或文件大小计算为负值
        int newBaseFileCounts = Math.max(0, currentBaseFileCount + fileBaseCounts - oldPartitionBaseFileCount);
        int newFileCounts = currentFileCount + fileCounts - oldPartitionFileCount;
        long newBaseFileSize = Math.max(0, currentBaseFileSize + partitionBaseSize - oldPartitionBaseFileSize);
        long newFileSize = currentFileSize + partitionSize - oldPartitionFileSize;

//        int partitionCount = 0;
//        partitionCountStateValue.put(partitionDesc, true);
        int partitionDescHashValue = partitionDesc.hashCode();
        HashSet<Integer> currentSet = partitionHashSetStateValue.value();
        if (currentSet == null) {
            currentSet = new HashSet<>();
        }

        currentSet.add(partitionDescHashValue);
        if (fileCounts == 0) {
            currentSet.remove(partitionDescHashValue);
        }

        partitionHashSetStateValue.update(currentSet);
//        if (fileCounts == 0 && partitionCountStateValue.contains(partitionDesc)){
//            partitionCountStateValue.remove(partitionDesc);
//        }
//        for (String key : partitionCountStateValue.keys()) {
//            partitionCount++;
//        }

        int partitionDescSize = partitionHashSetStateValue.value().size();

        TableCounts tableCounts = new TableCounts(tableId, partitionDescSize, newBaseFileCounts,
                newFileCounts,
                newBaseFileSize,
                newFileSize
        );
        partitionCountsMapStateValue.put(partitionDesc, partitionCounts);


        tableBaseFileCountValue.update(newBaseFileCounts);
        tableTotalFileCountValue.update(newFileCounts);
        tableBaseSizeValue.update(newBaseFileSize);
        tableTotalSizeValue.update(newFileSize);
        collector.collect(tableCounts);

    }

    public static class MergeFunction extends CoProcessFunction<Tuple3<String, String, String[]>, TableCounts, TableCountsWithTableInfo> {

        private ValueState<Tuple3<String, String, String[]>> latestTableInfo;
        private ValueState<TableCounts> latestTableCountsValue;

        @Override
        public void open(Configuration parameters) {
            // 为流A定义ValueStateDescriptor，存储最新的Tuple3数据
            ValueStateDescriptor<Tuple3<String, String, String[]>> tableInfoStateDesc =
                    new ValueStateDescriptor<>("latestTableInfo", TypeInformation.of(new TypeHint<Tuple3<String, String, String[]>>() {
                    }));

            // 为流B定义ValueStateDescriptor，存储最新的Tuple2数据
            ValueStateDescriptor<TableCounts> partitionInfoStateDesc =
                    new ValueStateDescriptor<>("latestTableCountsValue", TableCounts.class);

            // 获取状态
            latestTableInfo = getRuntimeContext().getState(tableInfoStateDesc);
            latestTableCountsValue = getRuntimeContext().getState(partitionInfoStateDesc);
        }


        @Override
        public void processElement1(Tuple3<String, String, String[]> tableInfoTuple, CoProcessFunction<Tuple3<String, String, String[]>, TableCounts, TableCountsWithTableInfo>.Context context, Collector<TableCountsWithTableInfo> collector) throws Exception {
            latestTableInfo.update(tableInfoTuple);

            TableCounts tableCounts = latestTableCountsValue.value();

            String tableName = tableInfoTuple.f2[1];
            String namespace = tableInfoTuple.f2[0];
            String domain = tableInfoTuple.f2[2];
            String creator = tableInfoTuple.f2[3];
            String eventType = tableInfoTuple.f2[4];

            if (tableCounts!=null && tableInfoTuple.f1.equals(tableCounts.tableId)){
                String tableId = tableCounts.tableId;
                int baseFileCounts = tableCounts.baseFileCounts;
                int totalFileCounts = tableCounts.totalFileCounts;
                int partitionCounts = tableCounts.partitionCounts;
                long baseFileSize = tableCounts.baseFileSize;
                long totalFileSize = tableCounts.totalFileSize;
                TableCountsWithTableInfo tableCountsWithTableInfo = new TableCountsWithTableInfo(tableId, tableName, creator, namespace, domain,partitionCounts,baseFileCounts,totalFileCounts,baseFileSize,totalFileSize);
                collector.collect(tableCountsWithTableInfo);
            }
        }

        @Override
        public void processElement2(TableCounts tableCounts, CoProcessFunction<Tuple3<String, String, String[]>, TableCounts, TableCountsWithTableInfo>.Context context, Collector<TableCountsWithTableInfo> collector) throws Exception {
            latestTableCountsValue.update(tableCounts);

            String tableId = tableCounts.tableId;
            int partitionCounts = tableCounts.partitionCounts;
            int baseFileCounts = tableCounts.baseFileCounts;
            int totalFileCounts = tableCounts.totalFileCounts;
            long baseFileSize = tableCounts.baseFileSize;
            long totalFileSize = tableCounts.totalFileSize;

            Tuple3<String, String, String[]> tableInfo = latestTableInfo.value();

            if (tableInfo!=null && tableCounts.tableId.equals(tableInfo.f1)) {
                String tableName = tableInfo.f2[1];
                String namespace = tableInfo.f2[0];
                String domain = tableInfo.f2[2];
                String creator = tableInfo.f2[3];
                String eventType = tableInfo.f2[4];

                TableCountsWithTableInfo tableCountsWithTableInfo = new TableCountsWithTableInfo(tableId,tableName,creator,namespace,domain,partitionCounts,baseFileCounts,totalFileCounts,baseFileSize,totalFileSize);
                collector.collect(tableCountsWithTableInfo);
            }
        }
    }
}