package org.apache.flink.lakesoul.entry.assets;


import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class JdbcTableLevelAssets extends ProcessFunction<TableCounts, TableCountsWithTableInfo> {
    String dbUrl = CountDataAssets.pgUrl;
    String dbUser = CountDataAssets.userName;
    String dbPassword = CountDataAssets.passWord;

    private transient MapState<String, TableInfo> tableInfoCount;
    @Override
    public void open(Configuration parameters) throws Exception {


        MapStateDescriptor<String, TableInfo> tableValueDescriptor = new MapStateDescriptor<>(
                "partitionCountStateValue",
                String.class,
                TableInfo.class
        );

        tableInfoCount = getRuntimeContext().getMapState(tableValueDescriptor);

    }
    @Override
    public void processElement(TableCounts tableCounts, ProcessFunction<TableCounts, TableCountsWithTableInfo>.Context context, Collector<TableCountsWithTableInfo> collector) throws Exception {


        String table_id = tableCounts.tableId;
        int partitionCounts = tableCounts.partitionCounts;
        int baseFileCounts = tableCounts.baseFileCounts;
        long baseFileSize = tableCounts.baseFileSize;
        long totalFileSize = tableCounts.totalFileSize;
        int totalFileCounts = tableCounts.totalFileCounts;
        String tableName = null;
        String tableNameSpace = null;
        String domain = null;
        String creator = null;
        if (tableInfoCount.contains(table_id)) {
            TableInfo tableInfo = tableInfoCount.get(table_id);
            tableName = tableInfo.tableName;
            tableNameSpace = tableInfo.nameSpace;
            domain = tableInfo.domain;
            creator = tableInfo.creator;

        } else {
            PostgresTableReader postgresTableReader = new PostgresTableReader(dbUrl,dbUser,dbPassword);
            Map<String, String> tableInfoById = postgresTableReader.getTableInfoById(table_id);
            tableName = tableInfoById.get("table_name") == null ? null:tableInfoById.get("table_name");
            tableNameSpace = tableInfoById.get("table_namespace");
            domain = tableInfoById.get("domain");
            creator = tableInfoById.get("creator");

            tableInfoCount.put(table_id,new TableInfo(table_id,tableName,domain,creator,tableNameSpace));
        }
        try {
            collector.collect(new TableCountsWithTableInfo(table_id,tableName,creator,tableNameSpace,domain,partitionCounts,baseFileCounts,totalFileCounts,baseFileSize,totalFileSize));
        } catch (Exception e) {
            System.out.println(table_id+"  "+ tableNameSpace+"===================");
        }

    }
}
