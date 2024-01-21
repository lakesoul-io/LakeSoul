// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry;

import com.dmetasoul.lakesoul.meta.external.NameSpaceManager;
import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
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

public class JdbcCDC {
    private static String host;
    private static String dbName;
    private static String userName;
    private static String passWord;
    private static int port;
    private static int splitSize;
    private static String slotName;
    private static String[] schemaList;
    private static String[] tableList;
    private static String serverTimezone;
    private static String pluginName;

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String dbType = parameter.get(SOURCE_DB_TYPE.key(), SOURCE_DB_TYPE.defaultValue());
        dbName = parameter.get(SOURCE_DB_DB_NAME.key());
        userName = parameter.get(SOURCE_DB_USER.key());
        passWord = parameter.get(SOURCE_DB_PASSWORD.key());
        host = parameter.get(SOURCE_DB_HOST.key());
        port = parameter.getInt(SOURCE_DB_PORT.key(), MysqlDBManager.DEFAULT_MYSQL_PORT);
        //Postgres Oracle
        if (dbType.equals("orcale") || dbType.equalsIgnoreCase("postgresql")) {
            schemaList = parameter.get(SOURCE_DB_SCHEMA_LIST.key()).split(",");
            String[] tables = parameter.get(SOURCE_DB_SCHEMA_TABLES.key()).split(",");
            tableList = new String[tables.length];
            for (int i = 0; i < tables.length; i++) {
                tableList[i] = tables[i].toUpperCase();
            }
            splitSize = parameter.getInt(SOURCE_DB_SPLIT_SIZE.key(), SOURCE_DB_SPLIT_SIZE.defaultValue());
        }
        if (dbType.equalsIgnoreCase("sqlserver")){
            tableList = parameter.get(SOURCE_DB_SCHEMA_TABLES.key()).split(",");
        }
        pluginName = parameter.get(PLUGIN_NAME.key(), PLUGIN_NAME.defaultValue());
        //flink
        String databasePrefixPath = parameter.get(WAREHOUSE_PATH.key());
        serverTimezone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());
        int sourceParallelism = parameter.getInt(SOURCE_PARALLELISM.key());
        int bucketParallelism = parameter.getInt(BUCKET_PARALLELISM.key());
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(),
                JOB_CHECKPOINT_INTERVAL.defaultValue());//mill second

        Configuration conf = new Configuration();
        // parameters for mutil tables ddl sink
        conf.set(SOURCE_DB_DB_NAME, dbName);
        conf.set(SOURCE_DB_USER, userName);
        conf.set(SOURCE_DB_PASSWORD, passWord);
        conf.set(SOURCE_DB_HOST, host);
        conf.set(SOURCE_DB_PORT, port);
        conf.set(WAREHOUSE_PATH, databasePrefixPath);
        conf.set(SERVER_TIME_ZONE, serverTimezone);

        // parameters for mutil tables dml sink
        conf.set(LakeSoulSinkOptions.USE_CDC, true);
        conf.set(LakeSoulSinkOptions.isMultiTableSource, true);
        conf.set(LakeSoulSinkOptions.WAREHOUSE_PATH, databasePrefixPath);
        conf.set(LakeSoulSinkOptions.SOURCE_PARALLELISM, sourceParallelism);
        conf.set(LakeSoulSinkOptions.BUCKET_PARALLELISM, bucketParallelism);
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.getConfig().registerTypeWithKryoSerializer(BinarySourceRecord.class, BinarySourceRecordSerializer.class);

        ParameterTool pt = ParameterTool.fromMap(conf.toMap());
        env.getConfig().setGlobalJobParameters(pt);

        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);

        CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
        if (parameter.get(JOB_CHECKPOINT_MODE.key(), JOB_CHECKPOINT_MODE.defaultValue()).equals("AT_LEAST_ONCE")) {
            checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
        }
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setCheckpointStorage(parameter.get(FLINK_CHECKPOINT.key()));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, // max failures per interval
                Time.of(10, TimeUnit.MINUTES), //time interval for measuring failure rate
                Time.of(20, TimeUnit.SECONDS) // delay
        ));
        LakeSoulRecordConvert lakeSoulRecordConvert = new LakeSoulRecordConvert(conf, conf.getString(SERVER_TIME_ZONE));

        if (dbType.equalsIgnoreCase("mysql")) {
            mysqlCdc(lakeSoulRecordConvert, conf, env);
        }
        if (dbType.equalsIgnoreCase("postgres")) {
            slotName = parameter.get(SOURCE_DB_SLOT_NAME.key(), SOURCE_DB_SLOT_NAME.defaultValue());
            postgresCdc(lakeSoulRecordConvert, conf, env);
        }
        if (dbType.equalsIgnoreCase("oracle")) {
            oracleCdc(lakeSoulRecordConvert, conf, env);
        }
        if (dbType.equalsIgnoreCase("sqlserver")){
            sqlserverCdc(lakeSoulRecordConvert, conf, env);
        }

    }

    private static void mysqlCdc(LakeSoulRecordConvert lakeSoulRecordConvert, Configuration conf, StreamExecutionEnvironment env) throws Exception {
        MySqlSourceBuilder<BinarySourceRecord> sourceBuilder = MySqlSource.<BinarySourceRecord>builder()
                .hostname(host)
                .port(port)
                .databaseList(dbName) // set captured database
                .tableList(dbName + ".*") // set captured table
                .serverTimeZone(serverTimezone)  // default -- Asia/Shanghai
                //.scanNewlyAddedTableEnabled(true)
                .username(userName)
                .password(passWord);
        sourceBuilder.deserializer(new BinaryDebeziumDeserializationSchema(lakeSoulRecordConvert,
                conf.getString(WAREHOUSE_PATH)));
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("allowPublicKeyRetrieval", "true");
        jdbcProperties.put("useSSL", "false");
        sourceBuilder.jdbcProperties(jdbcProperties);
        MySqlSource<BinarySourceRecord> mySqlSource = sourceBuilder.build();

        NameSpaceManager manager = new NameSpaceManager();
        manager.importOrSyncLakeSoulNamespace(dbName);

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = conf;
        LakeSoulMultiTableSinkStreamBuilder
                builder =
                new LakeSoulMultiTableSinkStreamBuilder(mySqlSource, context, lakeSoulRecordConvert);
        DataStreamSource<BinarySourceRecord> source = builder.buildMultiTableSource("MySQL Source");

        DataStream<BinarySourceRecord> stream = builder.buildHashPartitionedCDCStream(source);
        DataStreamSink<BinarySourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
        env.execute("LakeSoul CDC Sink From MySQL Database " + dbName);

    }

    private static void postgresCdc(LakeSoulRecordConvert lakeSoulRecordConvert, Configuration conf, StreamExecutionEnvironment env) throws Exception {
        JdbcIncrementalSource<BinarySourceRecord> pgSource = PostgresSourceBuilder.PostgresIncrementalSource.<BinarySourceRecord>builder()
                .hostname(host)
                .schemaList(schemaList)
                .tableList(tableList)
                .database(dbName)
                .port(port)
                .username(userName)
                .password(passWord)
                .decodingPluginName(pluginName)
                .splitSize(splitSize)
                .slotName(slotName)
                .deserializer(new BinaryDebeziumDeserializationSchema(lakeSoulRecordConvert, conf.getString(WAREHOUSE_PATH)))
                .build();

        NameSpaceManager manager = new NameSpaceManager();
        for (String schema : schemaList) {
            manager.importOrSyncLakeSoulNamespace(schema);
        }
        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = conf;
        LakeSoulMultiTableSinkStreamBuilder
                builder =
                new LakeSoulMultiTableSinkStreamBuilder(pgSource, context, lakeSoulRecordConvert);
        DataStreamSource<BinarySourceRecord> source = builder.buildMultiTableSource("Postgres Source");

        DataStream<BinarySourceRecord> stream = builder.buildHashPartitionedCDCStream(source);
        DataStreamSink<BinarySourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
        env.execute("LakeSoul CDC Sink From Postgres Database " + dbName);
    }

    private static void oracleCdc(LakeSoulRecordConvert lakeSoulRecordConvert, Configuration conf, StreamExecutionEnvironment env) throws Exception {

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("log.mining.strategy", "online_catalog");
        debeziumProperties.setProperty("log.mining.continuous.mine", "true");
        JdbcIncrementalSource<BinarySourceRecord> oracleChangeEventSource =
                new OracleSourceBuilder()
                        .hostname(host)
                        .schemaList(schemaList)
                        .tableList(tableList)
                        .databaseList(dbName)
                        .port(port)
                        .username(userName)
                        .serverTimeZone(serverTimezone)
                        .password(passWord)
                        .deserializer(new BinaryDebeziumDeserializationSchema(lakeSoulRecordConvert, conf.getString(WAREHOUSE_PATH)))
                        .includeSchemaChanges(true) // output the schema changes as well
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(debeziumProperties)
                        .splitSize(splitSize)
                        .build();

        NameSpaceManager manager = new NameSpaceManager();
        for (String schema : schemaList) {
            manager.importOrSyncLakeSoulNamespace(schema);
        }

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = conf;
        LakeSoulMultiTableSinkStreamBuilder
                builder =
                new LakeSoulMultiTableSinkStreamBuilder(oracleChangeEventSource, context, lakeSoulRecordConvert);
        DataStreamSource<BinarySourceRecord> source = builder.buildMultiTableSource("Postgres Source");

        DataStream<BinarySourceRecord> stream = builder.buildHashPartitionedCDCStream(source);
        DataStreamSink<BinarySourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
        env.execute("LakeSoul CDC Sink From Oracle Database " + dbName);
    }

    public static void sqlserverCdc(LakeSoulRecordConvert lakeSoulRecordConvert, Configuration conf, StreamExecutionEnvironment env) throws Exception {
        SqlServerSourceBuilder.SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder()
                        .hostname(host)
                        .port(port)
                        .databaseList(dbName)
                        .tableList(tableList)
                        .username(userName)
                        .password(passWord)
                        .deserializer(new BinaryDebeziumDeserializationSchema(lakeSoulRecordConvert, conf.getString(WAREHOUSE_PATH)))
                        .startupOptions(StartupOptions.initial())
                        .build();
        NameSpaceManager manager = new NameSpaceManager();
        manager.importOrSyncLakeSoulNamespace(dbName);
        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = conf;
        LakeSoulMultiTableSinkStreamBuilder
                builder =
                new LakeSoulMultiTableSinkStreamBuilder(sqlServerSource, context, lakeSoulRecordConvert);

        DataStreamSource<BinarySourceRecord> source = builder.buildMultiTableSource("Sqlserver Source");

        DataStream<BinarySourceRecord> stream = builder.buildHashPartitionedCDCStream(source);
        DataStreamSink<BinarySourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
        env.execute("LakeSoul CDC Sink From sqlserver Database " + dbName);
    }
}