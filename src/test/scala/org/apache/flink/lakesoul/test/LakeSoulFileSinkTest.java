package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.meta.DBManager;
import org.apache.flink.lakesoul.metaData.LakesoulCatalog;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.*;
import org.junit.Before;
import org.junit.Test;


public class LakeSoulFileSinkTest {

    private StreamTableEnvironment tEnvs;
    private StreamExecutionEnvironment env;
    private final String LAKESOUL = "lakesoul";


    @Before
    public void before() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1001);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15003);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhyang/Downloads/flink");
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

        Catalog lakesoulCatalog = new LakesoulCatalog();
        tEnvs.registerCatalog(LAKESOUL, lakesoulCatalog);
        tEnvs.useCatalog(LAKESOUL);
    }


    @Test
    public void createStreamingSinkTest() throws Exception {
        String PATH ="/Users/zhyang/Downloads/tmp";
//        tEnvs.executeSql( "CREATE TABLE tt122343 ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with ('connector' = 'lakesoul','format'='parquet','path'='"+PATH+"','lakesoul_cdc_change_column'='name','lakesoul_meta_host'='127.0.0.2','lakesoul_meta_host_port'='9043','lakesoul_cdc'='true','key'='user_id')" );
//        tEnvs.executeSql("insert into tt1223 values (1,'key1','value1'),(1987,'key1','value1'),(2,'key2','value2'),(92,'key2','value2'),(3,'key3','value3'),(4,'key3','value3'),(5,'key3','value3'),(6,'key3','value3'),(7,'key3','value3'),(8,'key3','value3'),(9,'key3','value1'),(10,'key3','value2'),(11,'key3','value3'),(12,'key3','value3'),(13,'key3','value3'),(14,'key3','value3'),(15,'key3','value3'),(16,'key3','value3'),(17,'key3','value3'),(18,'key3','value3'),(19,'key3','value1'),(20,'key3','value2')");
//
//
//
//        Thread.sleep(8000);

        //String key test
        tEnvs.executeSql( "CREATE TABLE tableTest14 ( user_id STRING, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with ('connector' = 'lakesoul','format'='parquet','path'='"+PATH+"','lakesoul_cdc_change_column'='name','lakesoul_meta_host'='127.0.0.2','lakesoul_meta_host_port'='9043','lakesoul_cdc'='true','key'='user_id','table_name'='MetaCommon.DATA_BASE().tableTest14')" );
//        tEnvs.executeSql("insert into tableTest1 values ('1','key1','value1'),('1987','key1','value1'),('2','key2','value2'),('92','key2','value2'),('3','key3','value3'),('4','key3','value3'),('5','key3','value3'),('6','key3','value3'),('7','key3','value3'),('8','key3','value3'),('9','key3','value1'),('10','key3','value2'),('11','key3','value3'),('12','key3','value3'),('13','key3','value3'),('14','key3','value3'),('15','key3','value3'),('16','key3','value3'),('17','key3','value3'),('18','key3','value3'),('19','key3','value1'),('20','key3','value2')");
//        Thread.sleep(8000);

////
        tEnvs.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("user_id", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .column("name", DataTypes.STRING())
                        .build()).option("rows-per-second","300").option("fields.dt.length","1")
                        .option("fields.user_id.length","8").option("fields.name.length","4")
                .build());
        Table table2 = tEnvs.from("SourceTable");

        table2.executeInsert("tableTest14");




        Thread.sleep(100000000);


//        tEnvs.executeSql("select * from tableTest1");
    }
}


