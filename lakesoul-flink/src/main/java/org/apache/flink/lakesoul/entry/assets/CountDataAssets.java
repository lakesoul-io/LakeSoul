// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.assets;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.lakesoul.entry.PgDeserialization;
import org.apache.flink.lakesoul.entry.SourceOptions;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Properties;


public class CountDataAssets {
    private static String host;
    private static String dbName;
    public static String userName;
    public static String passWord;
    private static int port;
    private static int splitSize;
    private static String slotName;
    private static String pluginName;
    private static String schemaList;
    public static String pgUrl;
    public static int sourceParallelism;
    private static int fetchSize;
    private static String startMode;

    private static int jdbcBatchSize;
    private static int sinkInterval;
    public static boolean dealingDataSkew;

    public CountDataAssets() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        userName = parameter.get(SourceOptions.SOURCE_DB_USER.key());
        dbName = parameter.get(SourceOptions.SOURCE_DB_DB_NAME.key());
        passWord = parameter.get(SourceOptions.SOURCE_DB_PASSWORD.key());
        host = parameter.get(SourceOptions.SOURCE_DB_HOST.key());
        port = parameter.getInt(SourceOptions.SOURCE_DB_PORT.key(), (Integer)SourceOptions.SOURCE_DB_PORT.defaultValue());
        slotName = parameter.get(SourceOptions.SLOT_NAME.key());
        pluginName = parameter.get(SourceOptions.PLUG_NAME.key());
        splitSize = parameter.getInt(SourceOptions.SPLIT_SIZE.key(), (Integer)SourceOptions.SPLIT_SIZE.defaultValue());
        schemaList = parameter.get(SourceOptions.SCHEMA_LIST.key());
        pgUrl = parameter.get(SourceOptions.PG_URL.key());
        sourceParallelism = parameter.getInt(SourceOptions.SOURCE_PARALLELISM.key(), (Integer)SourceOptions.SOURCE_PARALLELISM.defaultValue());
        fetchSize = parameter.getInt(SourceOptions.FETCH_SIZE.key(), (Integer)SourceOptions.FETCH_SIZE.defaultValue());
        startMode = parameter.get(SourceOptions.STARTUP_OPTIONS_CONFIG_OPTION.key(), (String)SourceOptions.STARTUP_OPTIONS_CONFIG_OPTION.defaultValue());
        jdbcBatchSize = parameter.getInt(SourceOptions.JDBC_SINK_BATCH_SIZE.key(),SourceOptions.JDBC_SINK_BATCH_SIZE.defaultValue());
        sinkInterval = parameter.getInt(SourceOptions.SINK_INTERVAL.key(),SourceOptions.SINK_INTERVAL.defaultValue());
        dealingDataSkew = parameter.getBoolean(SourceOptions.DEALING_WITH_DATA_SKEW.key(),SourceOptions.DEALING_WITH_DATA_SKEW.defaultValue());
        StartupOptions startupOptions = StartupOptions.initial();
        if (startMode.equals("latest")) {
            startupOptions = StartupOptions.latest();
        } else if (startMode.equals("earliest")) {
            startupOptions = StartupOptions.earliest();
        }
        PgDeserialization deserialization = new PgDeserialization();
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("include.unknown.datatypes", "true");
        debeziumProperties.setProperty("event.deserialization.failure.handling.mode", "warn");
        debeziumProperties.setProperty("schema.history.internal.store.only.captured.tables.ddl", "true");
        String[] tableList = new String[]{"public.data_commit_info"};
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(host)
                        .port(port)
                        .database(dbName)
                        .schemaList(schemaList)
                        .tableList(tableList)
                        .username(userName)
                        .password(passWord)
                        .slotName(slotName)
                        .decodingPluginName(pluginName) // use pgoutput for PostgreSQL 10+
                        .deserializer(deserialization)
                        .includeSchemaChanges(true)
                        .startupOptions(startupOptions)
                        .fetchSize(fetchSize)
                        .splitSize(splitSize) // the split size of each snapshot split
                        .debeziumProperties(debeziumProperties)
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> postgresParallelSource = env.fromSource(postgresIncrementalSource, WatermarkStrategy.noWatermarks(), "PostgresParallelSource").setParallelism(sourceParallelism);
        SingleOutputStreamOperator<Tuple3<String, String, String[]>> mainProcess = postgresParallelSource.map(new PartitionLevelAssets.metaMapper());

        DataStream<TableCounts> datacommitInfoStream = mainProcess.keyBy((value) -> {
            return (String)value.f1;
        }).process(new PartitionLevelAssets.PartitionLevelProcessFunction()).keyBy((value) -> {
            return value.tableId;
        }).process(new TableLevelAsstes());

        SingleOutputStreamOperator<TableCounts> tableCountsLatestStreaming = datacommitInfoStream
                .keyBy(value -> value.tableId)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(sinkInterval)))
                .process(new LatestTableCountsProcessFunction());

        SingleOutputStreamOperator<TableCountsWithTableInfo> tableCountsStreaming;
        if (dealingDataSkew) {
            tableCountsStreaming = tableCountsLatestStreaming
                    .process(new Desalting())
                    .keyBy(value -> value.tableId).process(new JdbcTableLevelAssets());
        } else {
            tableCountsStreaming = tableCountsLatestStreaming
                    .keyBy(value -> value.tableId).process(new JdbcTableLevelAssets());
        }


        JdbcConnectionOptions build = (new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()).withUrl(pgUrl).withDriverName("org.postgresql.Driver").withUsername(userName).withPassword(passWord).build();
        String tableLevelAssetsSql = "INSERT INTO table_level_assets (" +
                "table_id, " +
                "table_name, " +
                "domain, " +
                "creator, " +
                "namespace, " +
                "partition_counts, " +
                "file_counts, " +
                "file_total_size, " +
                "file_base_count, " +
                "file_base_size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (table_id) " +
                "DO UPDATE SET " +
                "partition_counts = EXCLUDED.partition_counts, " +
                "file_counts = EXCLUDED.file_counts, " +
                "file_total_size = EXCLUDED.file_total_size, " +
                "file_base_count = EXCLUDED.file_base_count, " +
                "file_base_size=EXCLUDED.file_base_size";
        SinkFunction<TableCountsWithTableInfo> sink = JdbcSink.sink(tableLevelAssetsSql, (ps, t) -> {
            ps.setString(1, t.tableId);
            ps.setString(2, t.tableName);
            ps.setString(3, t.domain);
            ps.setString(4, t.creator);
            ps.setString(5, t.namespace);
            ps.setInt(6, t.partionCount);
            ps.setInt(7, t.fileCount);
            ps.setLong(8, t.fileSize);
            ps.setInt(9, t.fileBaseCount);
            ps.setLong(10, t.fileBaseSize);
        }, JdbcExecutionOptions.builder().withBatchIntervalMs(200L).withBatchSize(jdbcBatchSize).withMaxRetries(3).build(), build);
        tableCountsStreaming.addSink(sink).name("tableAssetsSink");
        String dataBaseLevelAssetsSql = "INSERT INTO namespace_level_assets (namespace, " +
                "table_counts, " +
                "partition_counts, " +
                "file_counts, " +
                "file_total_size, " +
                "file_base_counts, " +
                "file_base_size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (namespace) DO UPDATE SET " +
                "table_counts = EXCLUDED.table_counts," +
                "partition_counts = EXCLUDED.partition_counts, " +
                "file_counts = EXCLUDED.file_counts, " +
                "file_total_size = EXCLUDED.file_total_size, " +
                "file_base_counts = EXCLUDED.file_base_counts, " +
                "file_base_size = EXCLUDED.file_base_size";
        SinkFunction<NameSpaceCount> namespaceAssetsSink = JdbcSink.sink(dataBaseLevelAssetsSql, (ps, t) -> {
            ps.setString(1, t.nameSpace);
            ps.setInt(2, t.tableCounts);
            ps.setInt(3, t.partitionCounts);
            ps.setInt(4, t.fileCounts);
            ps.setLong(5, t.fileTotalSize);
            ps.setInt(6, t.fileBaseCount);
            ps.setLong(7, t.fileBaseSize);
        }, JdbcExecutionOptions.builder().withBatchIntervalMs(200L).withBatchSize(jdbcBatchSize).withMaxRetries(3).build(), build);
        tableCountsStreaming.keyBy((value) -> {
            return value.namespace;
        }).process(new NamespaceLevelAssets()).addSink(namespaceAssetsSink).name("namespaceAssetsSink");
        String domainLevelAssetsSql = "INSERT INTO domain_level_assets (domain, " +
                "table_counts, " +
                "partition_counts, " +
                "file_counts, " +
                "file_total_size, " +
                "file_base_counts, " +
                "file_base_size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (domain) DO UPDATE SET " +
                "table_counts = EXCLUDED.table_counts," +
                "partition_counts = EXCLUDED.partition_counts, " +
                "file_counts = EXCLUDED.file_counts, " +
                "file_total_size = EXCLUDED.file_total_size, " +
                "file_base_counts = EXCLUDED.file_base_counts , " +
                "file_base_size = EXCLUDED.file_base_size";
        SinkFunction<DomainCount> domainAssetsSink = JdbcSink.sink(domainLevelAssetsSql, (ps, t) -> {
            ps.setString(1, t.domain);
            ps.setInt(2, t.tableCounts);
            ps.setInt(3, t.partitionCounts);
            ps.setInt(4, t.fileCounts);
            ps.setLong(5, t.fileTotalSize);
            ps.setInt(6, t.fileBaseCount);
            ps.setLong(7, t.fileBaseSize);
        }, JdbcExecutionOptions.builder().withBatchIntervalMs(200L).withBatchSize(jdbcBatchSize).withMaxRetries(3).build(), build);
        tableCountsStreaming.keyBy((value) -> {
            return value.domain;
        }).process(new DomainLevelAssets()).addSink(domainAssetsSink).name("domainAssetsSink");
        String userLevelAssetsSql = "INSERT INTO user_level_assets (creator, table_counts, partition_counts, file_counts, file_total_size, file_base_counts, file_base_size) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (creator) DO UPDATE SET table_counts = EXCLUDED.table_counts,partition_counts = EXCLUDED.partition_counts, file_counts = EXCLUDED.file_counts, file_total_size = EXCLUDED.file_total_size, file_base_counts = EXCLUDED.file_base_counts , file_base_size = EXCLUDED.file_base_size";
        SinkFunction<UserCounts> userLevelAssetsSink = JdbcSink.sink(userLevelAssetsSql, (ps, t) -> {
            ps.setString(1, t.creator);
            ps.setInt(2, t.tableCounts);
            ps.setInt(3, t.partitionCounts);
            ps.setInt(4, t.fileCounts);
            ps.setLong(5, t.fileTotalSize);
            ps.setInt(6, t.fileBaseCount);
            ps.setLong(7, t.fileBaseSize);
        }, JdbcExecutionOptions.builder().withBatchIntervalMs(200L).withBatchSize(jdbcBatchSize).withMaxRetries(3).build(), build);
        tableCountsStreaming.keyBy((value) -> {
            return value.creator;
        }).process(new UserLevelAssets()).addSink(userLevelAssetsSink).name("userAssetsSink");
        env.execute();
    }
}
