/*
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.lakesoul.entry.source;

import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.sink.writer.NewLakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.BinaryDebeziumDeserializationSchema;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions.SOURCE_DB_PORT;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BUCKET_PARALLELISM;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SOURCE_PARALLELISM;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.WAREHOUSE_PATH;

public class MysqlSourceCdc {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        String dbName = parameter.get(SOURCE_DB_DB_NAME.key());
        String userName = parameter.get(SOURCE_DB_USER.key());
        String passWord = parameter.get(SOURCE_DB_PASSWORD.key());
        String host = parameter.get(SOURCE_DB_HOST.key());
        int port = parameter.getInt(SOURCE_DB_PORT.key(), MysqlDBManager.DEFAULT_MYSQL_PORT);
        String databasePrefixPath = parameter.get(WAREHOUSE_PATH.key());
        String serverTimezone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());
        int sourceParallelism = parameter.getInt(SOURCE_PARALLELISM.key());
        int bucketParallelism = parameter.getInt(BUCKET_PARALLELISM.key());
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(),
                JOB_CHECKPOINT_INTERVAL.defaultValue());     //mill second

//        MysqlDBManager mysqlDBManager = new MysqlDBManager(dbName,
//                userName,
//                passWord,
//                host,
//                Integer.toString(port),
//                new HashSet<>(),
//                databasePrefixPath,
//                bucketParallelism,
//                true);

//        mysqlDBManager.importOrSyncLakeSoulNamespace(dbName);
        //syncing mysql tables to lakesoul

//        List<String> tableList = mysqlDBManager.listTables();
//        if (tableList.isEmpty()) {
//            throw new IllegalStateException("Failed to discover captured tables");
//        }
//        tableList.forEach(mysqlDBManager::importOrSyncLakeSoulTable);

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

        StreamExecutionEnvironment env;
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool pt = ParameterTool.fromMap(conf.toMap());
        env.getConfig().setGlobalJobParameters(pt);

        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);

        CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
        if (parameter.get(JOB_CHECKPOINT_MODE.key(), JOB_CHECKPOINT_MODE.defaultValue()).equals("AT_LEAST_ONCE")) {
            checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
        }
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setCheckpointStorage(parameter.get(FLINK_CHECKPOINT.key()));
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        MySqlSourceBuilder<BinarySourceRecord> sourceBuilder = MySqlSource.<BinarySourceRecord>builder()
                .hostname(host)
                .port(port)
                .databaseList(dbName) // set captured database
                .tableList(dbName + ".*") // set captured table
                .serverTimeZone(serverTimezone)  // default -- Asia/Shanghai
                .username(userName)
                .password(passWord);

        sourceBuilder.includeSchemaChanges(true);
        sourceBuilder.scanNewlyAddedTableEnabled(true);
        LakeSoulRecordConvert lakeSoulRecordConvert = new LakeSoulRecordConvert(conf.getBoolean(USE_CDC), conf.getString(SERVER_TIME_ZONE));
        sourceBuilder.deserializer(new BinaryDebeziumDeserializationSchema(lakeSoulRecordConvert, conf.getString(WAREHOUSE_PATH)));
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("allowPublicKeyRetrieval", "true");
        jdbcProperties.put("useSSL", "false");
        sourceBuilder.jdbcProperties(jdbcProperties);
        MySqlSource<BinarySourceRecord> mySqlSource = sourceBuilder.build();

        NewLakeSoulMultiTableSinkStreamBuilder.Context context = new NewLakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = conf;
        NewLakeSoulMultiTableSinkStreamBuilder builder = new NewLakeSoulMultiTableSinkStreamBuilder(mySqlSource, context, lakeSoulRecordConvert);
        DataStreamSource<BinarySourceRecord> source = builder.buildMultiTableSource("MySQL Source");

        DataStream<BinarySourceRecord> stream = builder.buildHashPartitionedCDCStream(source);
        DataStreamSink<BinarySourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
        env.execute("LakeSoul CDC Sink From MySQL Database " + dbName);
    }
}
