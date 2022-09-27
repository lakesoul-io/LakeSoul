/*
 *
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
 *
 *
 */

package org.apache.flink.lakesoul.entry;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.sink.LakeSoulDDLSink;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.tool.JobOptions;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.JsonSourceRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;
import java.util.HashSet;

import static org.apache.flink.lakesoul.tool.JobOptions.FLINK_CHECKPOINT;
import static org.apache.flink.lakesoul.tool.JobOptions.JOB_CHECKPOINT_INTERVAL;
import static org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions.*;


public class MysqlCdc {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        String DBName = parameter.get(SOURCE_DB_DB_NAME.key());
        String userName = parameter.get(SOURCE_DB_USER.key());
        String passWrd = parameter.get(SOURCE_DB_PASSWORD.key());
        String host = parameter.get(SOURCE_DB_HOST.key());
        int port = parameter.getInt(SOURCE_DB_PORT.key(), MysqlDBManager.DEFAULT_MYSQL_PORT);
        String databasePrefixPath = parameter.get(WAREHOUSE_PATH.key());
        int parallelism = parameter.getInt(SOURCE_PARALLELISM.key());
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(), JOB_CHECKPOINT_INTERVAL.defaultValue());     //mill second


        MysqlDBManager mysqlDBManager = new MysqlDBManager(DBName,
                                                           userName,
                                                           passWrd,
                                                           host,
                                                           Integer.toString(port),
                                                           new HashSet<>(),
                                                            databasePrefixPath);
        DBManager dbManager = new DBManager();
        dbManager.cleanMeta();
        mysqlDBManager.importOrSyncLakeSoulNamespace(DBName);
        //syncing mysql tables to lakesoul
        mysqlDBManager.listTables().forEach(mysqlDBManager::importOrSyncLakeSoulTable);

        Configuration conf = new Configuration();

        // parameters for mutil tables ddl sink
        conf.set(SOURCE_DB_DB_NAME, DBName);
        conf.set(SOURCE_DB_USER, userName);
        conf.set(SOURCE_DB_PASSWORD, passWrd);
        conf.set(SOURCE_DB_HOST, host);
        conf.set(SOURCE_DB_PORT, port);
        conf.set(WAREHOUSE_PATH, databasePrefixPath);

        // parameters for mutil tables dml sink
        conf.set(LakeSoulSinkOptions.USE_CDC, true);
        conf.set(LakeSoulSinkOptions.BUCKET_PARALLELISM, parallelism);
        conf.set(LakeSoulSinkOptions.WAREHOUSE_PATH,databasePrefixPath);
        conf.set(LakeSoulSinkOptions.SOURCE_PARALLELISM,parallelism);
        conf.set(LakeSoulSinkOptions.SINK_PARALLELISM,parallelism);

        StreamExecutionEnvironment env;
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool pt = ParameterTool.fromMap(conf.toMap());
        env.getConfig().setGlobalJobParameters(pt);

        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);
        env.getCheckpointConfig().setCheckpointStorage(parameter.get(FLINK_CHECKPOINT.key()));
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        MySqlSourceBuilder<JsonSourceRecord> sourceBuilder = MySqlSource.<JsonSourceRecord>builder()
                                                                        .hostname(host)
                                                                        .port(port)
                                                                        .databaseList(DBName) // set captured database
                                                                        .tableList(DBName + ".*") // set captured table
                                                                        .username(userName)
                                                                        .password(passWrd);
        System.out.println(DBName); // set captured database
        System.out.println(DBName + ".*");

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.sourceBuilder = sourceBuilder;
        context.conf = conf;
        LakeSoulMultiTableSinkStreamBuilder builder = new LakeSoulMultiTableSinkStreamBuilder(context);
        DataStreamSource<JsonSourceRecord> source = builder.buildMultiTableSource();
        Tuple2<DataStream<JsonSourceRecord>, DataStream<JsonSourceRecord>> streams = builder.buildCDCAndDDLStreamsFromSource(source);
        DataStream<JsonSourceRecord> stream = builder.buildHashPartitionedCDCStream(streams.f0);
        DataStreamSink<JsonSourceRecord> dmlSink = builder.buildLakeSoulDMLSink(stream);
        DataStreamSink<JsonSourceRecord> ddlSink = builder.buildLakeSoulDDLSink(streams.f1);
        env.execute("Print MySQL Snapshot + Binlog");
    }
}