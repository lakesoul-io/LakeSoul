// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry;

import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.entry.sql.flink.LakeSoulInAndOutputJobListener;
import org.apache.flink.lakesoul.entry.sql.utils.FileUtil;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.tool.JobOptions;
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

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions.*;

public class MysqlCdc {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        String dbName = parameter.get(SOURCE_DB_DB_NAME.key());
        String userName = parameter.get(SOURCE_DB_USER.key());
        String passWord = parameter.get(SOURCE_DB_PASSWORD.key());
        String host = parameter.get(SOURCE_DB_HOST.key());
        int port = parameter.getInt(SOURCE_DB_PORT.key(), MysqlDBManager.DEFAULT_MYSQL_PORT);
        String sinkDBName = parameter.get(SINK_DBNAME.key(), SINK_DBNAME.defaultValue());
        String databasePrefixPath = parameter.get(WAREHOUSE_PATH.key());
        String serverTimezone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());
        int sourceParallelism = parameter.getInt(SOURCE_PARALLELISM.key());
        int bucketParallelism = parameter.getInt(BUCKET_PARALLELISM.key());
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(),
                JOB_CHECKPOINT_INTERVAL.defaultValue());     //mill second
        LakeSoulInAndOutputJobListener listener = null;
        MysqlDBManager mysqlDBManager = new MysqlDBManager(dbName,
                userName,
                passWord,
                host,
                Integer.toString(port),
                new HashSet<>(),
                databasePrefixPath,
                bucketParallelism,
                true);

        mysqlDBManager.importOrSyncLakeSoulNamespace(dbName);

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
        conf.set(LakeSoulSinkOptions.HASH_BUCKET_NUM, bucketParallelism);
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        StreamExecutionEnvironment env;
        //String lineageUrl = "http://localhost:5000";
        String lineageUrl = System.getenv("LINEAGE_URL");
        String appName = null;
        String nameSpace = null;
        if (lineageUrl != null){
            conf.set(JobOptions.transportTypeOption, "http");
            conf.set(JobOptions.urlOption, lineageUrl);
            conf.set(JobOptions.execAttach, false);
            conf.set(lineageOption, true);
            env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
            appName = FileUtil.getSubNameFromBatch(env.getConfiguration().get(JobOptions.KUBE_CLUSTER_ID));
            listener = new LakeSoulInAndOutputJobListener(lineageUrl);
            listener.jobName(appName, dbName);
            listener.addInputTable("mysql."+dbName+".mysqlTables", dbName,null,null);
            env.registerJobListener(listener);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        }
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

        MySqlSourceBuilder<BinarySourceRecord> sourceBuilder = MySqlSource.<BinarySourceRecord>builder()
                .hostname(host)
                .port(port)
                .databaseList(dbName) // set captured database
                .tableList(dbName + ".*") // set captured table
                .serverTimeZone(serverTimezone)  // default -- Asia/Shanghai
                .scanNewlyAddedTableEnabled(true)
                .username(userName)
                .includeSchemaChanges(true)
                .password(passWord);

        LakeSoulRecordConvert lakeSoulRecordConvert = new LakeSoulRecordConvert(conf, conf.getString(SERVER_TIME_ZONE));
        sourceBuilder.deserializer(new BinaryDebeziumDeserializationSchema(lakeSoulRecordConvert,
                conf.getString(WAREHOUSE_PATH), sinkDBName));
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("allowPublicKeyRetrieval", "true");
        jdbcProperties.put("useSSL", "false");
        sourceBuilder.jdbcProperties(jdbcProperties);
        MySqlSource<BinarySourceRecord> mySqlSource = sourceBuilder.build();

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        if (lineageUrl !=null){
            Map<String, String> confs = ((Configuration) env.getConfiguration()).toMap();
            confs.put(linageJobName.key(), appName);
            confs.put(linageJobNamespace.key(), dbName);
            confs.put(lineageJobUUID.key(), listener.getRunId());
            confs.put(lineageOption.key(), "true");

            context.conf = Configuration.fromMap(confs);
        } else {
            context.conf = (Configuration) env.getConfiguration();
        }
        LakeSoulMultiTableSinkStreamBuilder
                builder =
                new LakeSoulMultiTableSinkStreamBuilder(mySqlSource, context, lakeSoulRecordConvert);
        DataStreamSource<BinarySourceRecord> source = builder.buildMultiTableSource("MySQL Source");
        DataStream<BinarySourceRecord> stream = builder.buildHashPartitionedCDCStream(source);
        DataStreamSink<BinarySourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
        env.execute("LakeSoul CDC Sink From MySQL Database " + dbName);
    }
}
