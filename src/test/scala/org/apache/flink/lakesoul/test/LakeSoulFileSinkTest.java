package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.meta.DBManager;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.LakesoulCatalog;
import org.apache.flink.lakesoul.LakesoulFileSink;
import org.apache.flink.lakesoul.sink.LakeSoulStreamWrite;
import org.apache.flink.lakesoul.tools.LakeSoulKeyGen;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;




public class LakeSoulFileSinkTest {

    private StreamTableEnvironment tEnvs;
    TableEnvironment tEnv;
    private StreamExecutionEnvironment env;
    private final String LAKESOUL = "lakesoul";


    @Before
    public void before() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1010);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhyang/Downloads/flink");
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        tEnv = TableEnvironment.create(settings);
        tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        Catalog lakesoulCatalog = new LakesoulCatalog();
        tEnv.registerCatalog(LAKESOUL, lakesoulCatalog);
        tEnv.useCatalog(LAKESOUL);
        tEnvs.registerCatalog(LAKESOUL, lakesoulCatalog);
        tEnvs.useCatalog(LAKESOUL);
    }


    @Test
    public void createStreamingSinkTest() throws Exception {
        //   tableEnv.executeSql( "CREATE TABLE user_behavior ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with('connector' = 'lakesoul','format'='parquet','path'='D://lakesoultest','lakesoul_cdc'='true')" );

//        tEnvs.executeSql( "CREATE TABLE tt1223 ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with ('connector' = 'lakesoul','format'='parquet','path'='/Users/zhyang/Downloads','lakesoul_cdc_change_column'='name','lakesoul_meta_host'='127.0.0.2','lakesoul_meta_host_port'='9043','lakesoul_cdc'='true','key'='user_id')" );
//        tEnvs.executeSql("insert into tt1223 values (1666,'1key12','value1'),(26667,'key12','value2'),(36663,'1key3','value3')");
//
        tEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("user_id", DataTypes.BIGINT())
                        .column("dt",DataTypes.STRING())
                        .column("name",DataTypes.STRING())
                        .build())
                .build());
        Table table = tEnv.sqlQuery("SELECT * FROM SourceTable");
        table.executeInsert("tt1223");
//        tEnvs.toDataStream(table).print();
        env.execute();



    }


}


