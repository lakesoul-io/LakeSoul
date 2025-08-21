// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import org.apache.flink.lakesoul.entry.SourceOptions;
import org.apache.flink.lakesoul.entry.PgDeserialization;
import org.apache.flink.lakesoul.entry.clean.PartitionInfoRecordGets.PartitionInfo;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

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
    private static CleanUtils cleanUtils;
    private static String targetTables;


    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        PgDeserialization deserialization = new PgDeserialization();
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("include.unknown.datatypes", "true");
        String[] tableList = new String[]{"public.partition_info"};

        userName = parameter.get(SourceOptions.SOURCE_DB_USER.key());
        dbName = parameter.get(SourceOptions.SOURCE_DB_DB_NAME.key());
        passWord = parameter.get(SourceOptions.SOURCE_DB_PASSWORD.key());
        host = parameter.get(SourceOptions.SOURCE_DB_HOST.key());
        port = parameter.getInt(SourceOptions.SOURCE_DB_PORT.key(), SourceOptions.SOURCE_DB_PORT.defaultValue());
        slotName = parameter.get(SourceOptions.SLOT_NAME.key());
        pluginName = parameter.get(SourceOptions.PLUG_NAME.key());
        splitSize = parameter.getInt(SourceOptions.SPLIT_SIZE.key(), SourceOptions.SPLIT_SIZE.defaultValue());
        schemaList = parameter.get(SourceOptions.SCHEMA_LIST.key());
        pgUrl = parameter.get(SourceOptions.PG_URL.key());
        sourceParallelism = parameter.getInt(SourceOptions.SOURCE_PARALLELISM.key(), SourceOptions.SOURCE_PARALLELISM.defaultValue());
        targetTables = parameter.get(SourceOptions.TARGET_TABLES.key(),null);

        //int ontimerInterval = 60000;
        int ontimerInterval = parameter.getInt(SourceOptions.ONTIMER_INTERVAL.key(), 5) * 60000;
        //expiredTime = 60000;
        expiredTime = parameter.getInt(SourceOptions.DATA_EXPIRED_TIME.key(), 3) ;
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
                        .username(userName)
                        .password(passWord)
                        .slotName(slotName)
                        .decodingPluginName(pluginName) // use pgoutput for PostgreSQL 10+
                        .deserializer(deserialization)
                        .splitSize(splitSize) // the split size of each snapshot split
                        .debeziumProperties(debeziumProperties)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> tickStream = env
                .addSource(new TickSource(ontimerInterval));


        DataStreamSource<String> postgresParallelSource = env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(sourceParallelism);

        CleanUtils utils = new CleanUtils();
        Connection connection = DriverManager.getConnection(pgUrl, userName, passWord);
        List<String> tableIdList = utils.getTableIdByTableName(targetTables, connection);

        SingleOutputStreamOperator<PartitionInfo> filter = postgresParallelSource.map(new PartitionInfoRecordGets.metaMapper(tableIdList))
                .filter(Objects::nonNull);

        SingleOutputStreamOperator<PartitionInfo> streamOperator =
                tickStream.connect(filter)
                        .flatMap(new TickTriggeringCleaner(pgUrl, userName, passWord, expiredTime));

        streamOperator.keyBy(value -> value.table_id + "/" + value.partition_desc)
                .process(new ProcessClean(pgUrl, userName, passWord, expiredTime, ontimerInterval));

        env.execute("clean job");

    }

    public static class ProcessClean extends KeyedProcessFunction<String, PartitionInfo, String> {
        private transient MapState<String, PartitionInfo.WillStateValue> willState;
        private transient ValueState<Boolean> timerInitializedState;
        private transient MapState<String, Long> compactNewState;
        private transient ValueState<Boolean> compactionVersionState;

        private final String pgUrl;
        private final String userName;
        private final String password;
        private final int expiredTime;
        private final int ontimerInterval;

        private transient Connection pgConnection;

        public ProcessClean(String pgUrl, String userName, String password, int expiredTime, int ontimerInterval) {
            this.pgUrl = pgUrl;
            this.userName = userName;
            this.password = password;
            this.expiredTime = expiredTime;
            this.ontimerInterval = ontimerInterval;
        }

        @Override
        public void open(Configuration parameters) throws SQLException, ClassNotFoundException {
            // 初始化状态变量
            MapStateDescriptor<String, PartitionInfo.WillStateValue> willStateDesc =
                    new MapStateDescriptor<>("willStateDesc", String.class, PartitionInfo.WillStateValue.class);
            willState = getRuntimeContext().getMapState(willStateDesc);

            MapStateDescriptor<String, Long> compactNewStateDesc =
                    new MapStateDescriptor<>("newCompactDesc", String.class, Long.class);
            compactNewState = getRuntimeContext().getMapState(compactNewStateDesc);

            ValueStateDescriptor<Boolean> initDesc =
                    new ValueStateDescriptor<>("timerInit", Boolean.class, false);
            timerInitializedState = getRuntimeContext().getState(initDesc);

            ValueStateDescriptor<Boolean> compactVersionDesc =
                    new ValueStateDescriptor<>("compactVersion", Boolean.class, true);
            compactionVersionState = getRuntimeContext().getState(compactVersionDesc);

            //PGConnectionPool.init(pgUrl, userName, password);
            //pgConnection = PGConnectionPool.getConnection();
            Class.forName("org.postgresql.Driver");
            pgConnection = DriverManager.getConnection(pgUrl,userName,password);
            cleanUtils = new CleanUtils();

        }

        @Override
        public void processElement(PartitionInfo value, KeyedProcessFunction<String, PartitionInfo, String>.Context ctx, Collector<String> out) throws Exception {
            String tableId = value.table_id;
            String partitionDesc = value.partition_desc;
            String commitOp = value.commit_op;
            long timestamp = value.timestamp;
            int version = value.version;
            List<String> snapshot = value.snapshot;
            if (commitOp.equals("CompactionCommit") || commitOp.equals("UpdateCommit") ){
                if ( snapshot.size() == 1) {
                    boolean isOldCompaction = cleanUtils.getCompactVersion(tableId, partitionDesc, version, pgConnection);
                    log.info("当前识别出来为旧版压缩："+ isOldCompaction);
                    compactionVersionState.update(isOldCompaction);
                }
            }
            boolean compactVersion = compactionVersionState.value();
            PartitionInfo.WillStateValue willStateValue = new PartitionInfo.WillStateValue(timestamp, snapshot);
            if (!timerInitializedState.value()) {
                timerInitializedState.update(true);
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + ontimerInterval);
            }
            if (commitOp.equals("AppendCommit") || commitOp.equals("MergeCommit")) {
                if (compactNewState.contains(tableId + "/" + partitionDesc)) {
                    long compactTime = compactNewState.get(tableId + "/" + partitionDesc);
                    if (timestamp < compactTime - expiredTime) {
                        log.info("1:当前压缩版本是旧版本：" + compactVersion);
                        cleanUtils.deleteFileAndDataCommitInfo(snapshot, tableId, partitionDesc, pgConnection, compactVersion);
                        cleanUtils.cleanPartitionInfo(tableId, partitionDesc, version, pgConnection);
                    } else {
                        willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                    }
                } else {
                    willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                }
            }
            if (commitOp.equals("CompactionCommit") || commitOp.equals("UpdateCommit")) {
                if (snapshot.size() == 1) {
                    if (compactNewState.contains(tableId + "/" + partitionDesc)) {
                        long compactTime = compactNewState.get(tableId + "/" + partitionDesc) ;
                        if (timestamp > compactTime) {
                            compactNewState.put(tableId + "/" + partitionDesc, timestamp);
                            willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                        } else {
                            if (timestamp < compactTime - expiredTime) {
                                log.info("2:当前压缩版本是旧版本：" + compactVersion);
                                cleanUtils.deleteFileAndDataCommitInfo(snapshot, tableId, partitionDesc, pgConnection, compactVersion);
                                cleanUtils.cleanPartitionInfo(tableId, partitionDesc, version, pgConnection);
                            } else {
                                willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                            }
                        }
                    } else {
                        compactNewState.put(tableId + "/" + partitionDesc, timestamp);
                        willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                    }
                } else if (snapshot.size() > 1) {
                    log.info("识别出当前compaction为并发提交，忽略");
                    if (compactNewState.contains(tableId + "/" + partitionDesc)) {
                        long compactTime = compactNewState.get(tableId + "/" + partitionDesc);
                        if (timestamp < compactTime - expiredTime) {
                            log.info("3 当前压缩版本是旧版本： " + compactVersion);
                            cleanUtils.deleteFileAndDataCommitInfo(snapshot, tableId, partitionDesc, pgConnection, compactVersion);
                            cleanUtils.cleanPartitionInfo(tableId, partitionDesc, version, pgConnection);
                        } else {
                            willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                        }
                    } else {
                        willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                    }
                }

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            for (Map.Entry<String, Long> entry : compactNewState.entries()) {
                String compactId = entry.getKey();
                Long commitTime = entry.getValue();
                long expiredThreshold = commitTime - expiredTime;
                Iterator<Map.Entry<String, PartitionInfo.WillStateValue>> willStateIterator = willState.iterator();
                while (willStateIterator.hasNext()) {
                    Map.Entry<String, PartitionInfo.WillStateValue> willStateEntry = willStateIterator.next();
                    String[] keys = willStateEntry.getKey().split("/");
                    String tableId = keys[0];
                    String partitionDesc = keys[1];
                    if (cleanUtils.partitionExist(tableId, partitionDesc, pgConnection)){
                        int version = Integer.parseInt(keys[2]);
                        List<String> snapshot = willStateEntry.getValue().snapshot;
                        String compositeKey = tableId + "/" + partitionDesc;
                        if (compositeKey.equals(compactId)) {
                            PartitionInfo.WillStateValue stateValue = willStateEntry.getValue();
                            if (stateValue.timestamp < expiredThreshold) {
                                log.info("4 当前压缩版本为旧版本：" + compactionVersionState.value());
                                cleanUtils.deleteFileAndDataCommitInfo(snapshot, tableId, partitionDesc, pgConnection, compactionVersionState.value());
                                cleanUtils.cleanPartitionInfo(tableId, partitionDesc, version, pgConnection);
                                willStateIterator.remove();
                            }
                        }
                    } else {
                        log.info("检测到table_id :" + tableId + " 对应的分区： " + partitionDesc + "已经被删除，清理该分区的所有状态");
                        compactNewState.clear();
                        willState.clear();
                        break;
                    }
                }

            }
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + ontimerInterval);
        }

        @Override
        public void close() throws Exception {
            if (pgConnection != null && !pgConnection.isClosed()) {
                pgConnection.close();
                System.out.println("✅ PostgreSQL connection returned to pool.");
            }
        }
    }
}

