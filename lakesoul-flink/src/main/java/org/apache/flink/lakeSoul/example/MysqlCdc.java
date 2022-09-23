package org.apache.flink.lakeSoul.example;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakeSoul.sink.LakeSoulDDLSink;
import org.apache.flink.lakeSoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakeSoul.types.JsonSourceRecord;
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
//        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        ParameterTool pt = ParameterTool.fromMap(conf.toMap());
        env.getConfig().setGlobalJobParameters(pt);

//        env.getConfig().setGlobalJobParameters(conf);

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
//        builder.printStream(streams.f1, "Print DDL Stream");
        streams.f1.addSink(new LakeSoulDDLSink()).setParallelism(1);
        env.execute("Print MySQL Snapshot + Binlog");
        //Thread.sleep(300000);
    }
}