// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.lakesoul.entry.SourceOptions;
import org.apache.flink.lakesoul.entry.PgDeserialization;
import org.apache.flink.lakesoul.entry.clean.PartitionInfoRecordGets.PartitionInfo;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.lakesoul.entry.SourceOptions;
import org.apache.flink.lakesoul.entry.clean.PartitionInfoRecordGets.PartitionInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

public class NewCleanJob {
    private static final Logger log = LoggerFactory.getLogger(NewCleanJob.class);
    public static int expiredTime;
    public static String host;
    private static String dbName;
    public static String userName;
    public static String passWord;
    public static int port;
    private static int splitSize;
    private static String slotName;
    private static String pluginName;
    private static String schemaList;
    public static String pgUrl;
    private static int sourceParallelism;
    private static String targetTables;
    private static String startMode;

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        PgCleanDeserialization deserialization = new PgCleanDeserialization();
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("include.unknown.datatypes", "true");
        String[] tableList = new String[]{"public.partition_info", "public.discard_compressed_file_info", "public.table_info"};
        userName = parameter.get(SourceOptions.SOURCE_DB_USER.key());
        dbName = parameter.get(SourceOptions.SOURCE_DB_DB_NAME.key());
        passWord = parameter.get(SourceOptions.SOURCE_DB_PASSWORD.key());
        host = parameter.get(SourceOptions.SOURCE_DB_HOST.key());
        port = parameter.getInt(SourceOptions.SOURCE_DB_PORT.key(), SourceOptions.SOURCE_DB_PORT.defaultValue());
        slotName = parameter.get(SourceOptions.SLOT_NAME.key());
        pluginName = parameter.get(SourceOptions.PLUG_NAME.key());
        splitSize = parameter.getInt(SourceOptions.SPLIT_SIZE.key(), SourceOptions.SPLIT_SIZE.defaultValue());
        schemaList = parameter.get(SourceOptions.SCHEMA_LIST.key());
        startMode = parameter.get(SourceOptions.STARTUP_OPTIONS_CONFIG_OPTION.key(), (String)SourceOptions.STARTUP_OPTIONS_CONFIG_OPTION.defaultValue());
        pgUrl = parameter.get(SourceOptions.PG_URL.key());
        sourceParallelism = parameter.getInt(SourceOptions.SOURCE_PARALLELISM.key(), SourceOptions.SOURCE_PARALLELISM.defaultValue());
        targetTables = parameter.get(SourceOptions.TARGET_TABLES.key(),null);
        StartupOptions startupOptions = StartupOptions.initial();
        if (startMode.equals("latest")) {
            startupOptions = StartupOptions.latest();
        } else if (startMode.equals("earliest")) {
            startupOptions = StartupOptions.earliest();
        }

        //int ontimerInterval = 60000;
        int ontimerInterval = parameter.getInt(SourceOptions.ONTIMER_INTERVAL.key(), 10) * 60000 * 60;
        //expiredTime = 60000;
        expiredTime = parameter.getInt(SourceOptions.DATA_EXPIRED_TIME.key(),120000) ;
        if (expiredTime < 10){
            expiredTime = expiredTime * 86400000;
        }

        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(host)
                        .port(port)
                        .database(dbName)
                        .schemaList(schemaList)
                        .tableList(tableList)
                        .startupOptions(startupOptions)
                        .username(userName)
                        .password(passWord)
                        .slotName(slotName)
                        .decodingPluginName(pluginName) // use pgoutput for PostgreSQL 10+
                        .deserializer(deserialization)
                        .splitSize(splitSize) // the split size of each snapshot split
                        .debeziumProperties(debeziumProperties)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> postgresParallelSource = env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(sourceParallelism);
        final OutputTag<String> partitionInfoTag = new OutputTag<String>("partition_info") {};
        final OutputTag<String> discardFileInfoTag = new OutputTag<String>("discard_compressed_file_info") {};
        final OutputTag<String> tableInfoTag = new OutputTag<String>("table_info") {};
        SingleOutputStreamOperator<String> mainStream = postgresParallelSource.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            String tableName = json.getString("tableName");
                            if ("partition_info".equals(tableName)) {
                                ctx.output(partitionInfoTag, value);
                            } else if ("discard_compressed_file_info".equals(tableName)) {
                                ctx.output(discardFileInfoTag, value);
                            } else if ("table_info".equals(tableName)) {
                                ctx.output(tableInfoTag, value);
                            }
                        } catch (Exception e) {
                            System.err.println("JSON parse error: " + e.getMessage());
                        }
                    }
                }
        );
        SideOutputDataStream<String> partitionInfoStream = mainStream.getSideOutput(partitionInfoTag);
        SideOutputDataStream<String> discardFileInfoStream = mainStream.getSideOutput(discardFileInfoTag);
        SideOutputDataStream<String> tableInfoStream = mainStream.getSideOutput(tableInfoTag);
        CleanUtils utils = new CleanUtils();
        final OutputTag<PartitionInfo> compactionCommitTag =
                new OutputTag<PartitionInfo>(
                        "compactionCommit",
                        TypeInformation.of(PartitionInfo.class)
                ) {};
        List<String> tableIdList = null;

        if (targetTables != null) {
            try (Connection connection = DriverManager.getConnection(pgUrl, userName, passWord)) {
                tableIdList = utils.getTableIdByTableName(targetTables, connection);
            }
        }

        SingleOutputStreamOperator<PartitionInfo> mainStreaming = partitionInfoStream.map(new PartitionInfoRecordGets.metaMapper(tableIdList))
                .process(new ProcessFunction<PartitionInfo, PartitionInfo>() {
                    @Override
                    public void processElement(PartitionInfo value,
                                               ProcessFunction<PartitionInfo,
                                                       PartitionInfo>.Context ctx,
                                               Collector<PartitionInfo> out) throws Exception {
                        if (value.commitOp != null){
                            if (value.commitOp.equals("CompactionCommit") || value.commitOp.equals("UpdateCommit")){
                                ctx.output(compactionCommitTag,value);
                            }
                        }
                        out.collect(value);

                    }
                });

        SideOutputDataStream<PartitionInfo> compactStreaming = mainStreaming.getSideOutput(compactionCommitTag);
        KeyedStream<PartitionInfo, String> partitionInfoStringKeyedStream = mainStreaming.keyBy(value -> value.tableId + "/" + value.partitionDesc + "/" + value.version);
        SingleOutputStreamOperator<CompactProcessFunction.CompactionOut> compactiomStreaming = compactStreaming
                .keyBy(value -> value.tableId + "/" + value.partitionDesc)
                .process(new CompactProcessFunction(pgUrl, userName, passWord));

        MapStateDescriptor<String, CompactProcessFunction.CompactionOut> broadcastStateDesc =
                new MapStateDescriptor<>(
                        "compactionBroadcastState",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CompactProcessFunction.CompactionOut>() {}));
        MapStateDescriptor<String, Integer> TABLE_TTL_DESC =
                new MapStateDescriptor<>(
                        "table-ttl-broadcast-state",
                        String.class,
                        Integer.class
                );
        BroadcastStream<TableInfoRecordGets.TableInfo> tableInfoBroadcastStreming = tableInfoStream
                .map(new TableInfoRecordGets.tableInfoMapper())
                .broadcast(TABLE_TTL_DESC);
        BroadcastStream<CompactProcessFunction.CompactionOut> broadcastStream =
                compactiomStreaming.broadcast(broadcastStateDesc);

        BroadcastConnectedStream<PartitionInfo, CompactProcessFunction.CompactionOut> connectedStream =
                partitionInfoStringKeyedStream.connect(broadcastStream);

        connectedStream.process(new CompactionBroadcastProcessFunction(broadcastStateDesc, pgUrl, userName, passWord, expiredTime, ontimerInterval));

        BroadcastConnectedStream<TableTtlProFunction.PartitionINfoUpdateEvents, TableInfoRecordGets.TableInfo> connect = mainStreaming
                .keyBy(value -> value.tableId + "/" + value.partitionDesc)
                .process(new TableTtlProFunction())
                .keyBy(value -> value.tableId + "/" + value.partitionDesc)
                .connect(tableInfoBroadcastStreming);
        connect.process(new TtlBroadcastProcessFunction(TABLE_TTL_DESC,1)).name("清理过期分区数据");

        discardFileInfoStream
                .map(new DiscardPathMapFunction())
                .filter(value -> !value.f0.equals("delete"))
                .keyBy(value -> value.f0)
                .process(new DiscardFilePathProcessFunction(expiredTime))
                .name("处理新版过期数据")
                .process(new DiscardFileDeleteFunction(pgUrl, userName, passWord))
                .name("批量异步删除数据");

        env.execute("清理服务");

    }
}
