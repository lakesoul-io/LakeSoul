package org.apache.flink.lakeSoul.test;

import org.apache.flink.lakeSoul.metaData.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.*;
import org.junit.Test;


public class LakeSoulFileSinkTest {

  @Test
  public void flinkCdcSinkTest() throws InterruptedException {
    StreamTableEnvironment tEnvs;
    StreamExecutionEnvironment env;
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(201);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(403);
    env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhyang/Downloads/flink");
    tEnvs = StreamTableEnvironment.create(env);
    tEnvs.getConfig().getConfiguration().set(
        ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

    //source
    tEnvs.executeSql("create table mysql_test_1(\n" +
        "id INTEGER primary key NOT ENFORCED ," +
        "name string," +
        " dt string)" +
        " with (\n" +
        "'connector'='mysql-cdc'," +
        "'hostname'='127.0.0.1'," +
        "'port'='3306'," +
        "'server-id'='1'," +
        "'username'='root',\n" +
        "'password'='root',\n" +
        "'database-name'='zhyang_test',\n" +
        "'table-name'='test5'\n" +
        ")");

    Catalog lakesoulCatalog = new LakeSoulCatalog();
    tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
    tEnvs.useCatalog("lakeSoul");
    String tableName = "flinkI" + (int) (Math.random() * 156439750) % 2235;
    String PATH = "/Users/zhyang/Downloads/tmp/" + tableName;

    //target
    tEnvs.executeSql(
        "CREATE TABLE " + tableName + "( id int," +
            " name string," +
            " dt string," +
            "primary key (id) NOT ENFORCED ) " +
            "PARTITIONED BY (dt)" +
            " with ('connector' = 'lakeSoul'," +
            "'format'='parquet','path'='" +
            PATH + "'," +
            "'useCDC'='true'," +
            "'bucket_num'='2')");

    tEnvs.executeSql("show tables ").print();

    tEnvs.useCatalog("default_catalog");

    tEnvs.executeSql("insert into lakeSoul.test_lakesoul_meta." + tableName + " select * from mysql_test_1 ");

    Thread.sleep(100000000);
  }

}


