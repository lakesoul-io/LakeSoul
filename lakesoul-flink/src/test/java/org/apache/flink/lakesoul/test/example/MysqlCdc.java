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
package org.apache.flink.lakesoul.test.example;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.sink.LakeSoulDDLSink;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.types.JsonSourceRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.HashSet;


public class MysqlCdc {

    public static void main(String[] args) throws Exception {
        String DBName = "test_cdc";
        String userName = "root";
        String passWrd = "mysql123";
        String host = "localhost";
        int port = 3306;

        MysqlDBManager mysqlDBManager = new MysqlDBManager(DBName,
                userName,
                passWrd,
                host,
                Integer.toString(port),
                new HashSet<>(),
                MysqlDBManager.DEFAULT_LAKESOUL_TABLE_PATH_PREFIX);
        DBManager dbManager = new DBManager();
        dbManager.cleanMeta();
        mysqlDBManager.importOrSyncLakeSoulNamespace(DBName);
        mysqlDBManager.listTables().forEach(mysqlDBManager::importOrSyncLakeSoulTable);


        Configuration conf= new Configuration();
        conf.setString(LakeSoulDDLSink.ConfKey.DB_NAME, DBName);
        conf.setString(LakeSoulDDLSink.ConfKey.DB_USER, userName);
        conf.setString(LakeSoulDDLSink.ConfKey.DB_PASSWORD, passWrd);
        conf.setString(LakeSoulDDLSink.ConfKey.DB_HOST, host);
        conf.setInteger(LakeSoulDDLSink.ConfKey.DB_PORT, port);
        conf.setString(LakeSoulDDLSink.ConfKey.LAKESOUL_TABLE_PATH_PREFIX, MysqlDBManager.DEFAULT_LAKESOUL_TABLE_PATH_PREFIX);

        StreamExecutionEnvironment env;
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool pt = ParameterTool.fromMap(conf.toMap());
        env.getConfig().setGlobalJobParameters(pt);


        env.setParallelism(1);
        env.enableCheckpointing(5021);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);

        MySqlSourceBuilder<JsonSourceRecord> sourceBuilder = MySqlSource.<JsonSourceRecord>builder()
                .hostname(host)
                .port(port)
                .databaseList(DBName) // set captured database
                .tableList(DBName+".*") // set captured table
                .username(userName)
                .password(passWrd);

        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.sourceBuilder = sourceBuilder;

        LakeSoulMultiTableSinkStreamBuilder builder = new LakeSoulMultiTableSinkStreamBuilder(context);

        DataStreamSource<JsonSourceRecord> source = builder.buildMultiTableSource();

        Tuple2<DataStream<JsonSourceRecord>, DataStream<JsonSourceRecord>> streams = builder.buildCDCAndDDLStreamsFromSource(source);

        builder.printStream(streams.f0, "Print CDC Stream");
        streams.f1.addSink(new LakeSoulDDLSink()).setParallelism(1);
        env.execute("Print MySQL Snapshot + Binlog");
    }
}