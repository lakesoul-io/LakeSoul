// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0


package org.apache.flink.lakesoul.entry;

import com.dmetasoul.lakesoul.meta.external.oracle.OracleDBManager;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.BinaryDebeziumDeserializationSchema;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.lakesoul.types.BinarySourceRecordSerializer;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BUCKET_PARALLELISM;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SOURCE_PARALLELISM;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.WAREHOUSE_PATH;

public class OracleCdc {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String dbName = parameter.get(SOURCE_DB_DB_NAME.key());
        String userName = parameter.get(SOURCE_DB_USER.key());
        String passWord = parameter.get(SOURCE_DB_PASSWORD.key());
        String[] schemaList = parameter.get(SOURCE_DB_SCHEMA_LIST.key()).split(",");
        String host = parameter.get(SOURCE_DB_HOST.key());
        int splitSize = parameter.getInt(SOURCE_DB_SPLIT_SIZE.key(), SOURCE_DB_SPLIT_SIZE.defaultValue());
        int port = parameter.getInt(SOURCE_DB_PORT.key(), OracleDBManager.DEFAULT_ORACLE_PORT);
        String databasePrefixPath = parameter.get(WAREHOUSE_PATH.key());
        String serverTimezone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());
        int sourceParallelism = parameter.getInt(SOURCE_PARALLELISM.key());
        int bucketParallelism = parameter.getInt(BUCKET_PARALLELISM.key());
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(),
                JOB_CHECKPOINT_INTERVAL.defaultValue());
        String tableList = parameter.get(SOURCE_DB_SCHEMA_TABLES.key());
        String flinkCheckpoint = parameter.get(FLINK_CHECKPOINT.key());

        String[] tables = tableList.split(",");
        String[] userTables = new String[tables.length];
        for (int i = 0; i < tables.length; i++) {
            userTables[i] = tables[i].toUpperCase();
        }

        OracleDBManager dbManager = new OracleDBManager(dbName,
                userName,
                passWord,
                host,
                Integer.toString(port));

        for (String schema : schemaList) {
            dbManager.importOrSyncLakeSoulNamespace(schema);
        }
        Configuration conf = new Configuration();
        conf.set(LakeSoulSinkOptions.USE_CDC, true);
        conf.set(LakeSoulSinkOptions.isMultiTableSource, true);
        conf.set(SOURCE_PARALLELISM, sourceParallelism);
        conf.set(BUCKET_PARALLELISM, bucketParallelism);
        conf.set(SERVER_TIME_ZONE, serverTimezone);
        conf.set(WAREHOUSE_PATH, databasePrefixPath);
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().registerTypeWithKryoSerializer(BinarySourceRecord.class, BinarySourceRecordSerializer.class);

        ParameterTool pt = ParameterTool.fromMap(conf.toMap());
        env.getConfig().setGlobalJobParameters(pt);
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4034);
        CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
        if (JOB_CHECKPOINT_MODE.defaultValue().equals("AT_LEAST_ONCE")) {
            checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
        }
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        env.getCheckpointConfig().setCheckpointStorage(flinkCheckpoint);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.of(10, TimeUnit.MINUTES),
                Time.of(20, TimeUnit.SECONDS)
        ));

        LakeSoulRecordConvert lakeSoulRecordConvert = new LakeSoulRecordConvert(conf, conf.getString(SERVER_TIME_ZONE));

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("log.mining.strategy", "online_catalog");
        debeziumProperties.setProperty("log.mining.continuous.mine", "true");
        JdbcIncrementalSource<BinarySourceRecord> oracleChangeEventSource =
                new OracleSourceBuilder()
                        .hostname(host)
                        .schemaList(schemaList)
                        .tableList(userTables)
                        .databaseList(dbName)
                        .port(port)
                        .username(userName)
                        .password(passWord)
                        .deserializer(new BinaryDebeziumDeserializationSchema(lakeSoulRecordConvert, conf.getString(WAREHOUSE_PATH)))
                        .includeSchemaChanges(true) // output the schema changes as well
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(debeziumProperties)
                        .splitSize(splitSize)
                        .build();

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = conf;
        LakeSoulMultiTableSinkStreamBuilder builder = new LakeSoulMultiTableSinkStreamBuilder(oracleChangeEventSource, context, lakeSoulRecordConvert);
        DataStreamSource<BinarySourceRecord> source = builder.buildMultiTableSource("Oracle Source");
        DataStream<BinarySourceRecord> stream = builder.buildHashPartitionedCDCStream(source);
        DataStreamSink<BinarySourceRecord> dmSink = builder.buildLakeSoulDMLSink(stream);
        env.execute("LakeSoul CDC Sink From Oracle Database" + dbName);
    }
}