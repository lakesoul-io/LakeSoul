package org.apache.flink.lakeSoul.test;

import org.apache.flink.lakeSoul.metaData.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.*;
import org.junit.Before;
import org.junit.Test;


public class LakeSoulFileSinkTest {

  private StreamTableEnvironment tEnvs;
  private StreamExecutionEnvironment env;


  @Before
  public void before() {

    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    env.enableCheckpointing(1001);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10003);
    env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhyang/Downloads/flink");
    tEnvs = StreamTableEnvironment.create(env);
    tEnvs.getConfig().getConfiguration().set(
        ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

    Catalog lakesoulCatalog = new LakeSoulCatalog();
    String LAKESOUL = "lakesoul";
    tEnvs.registerCatalog(LAKESOUL, lakesoulCatalog);
    tEnvs.useCatalog(LAKESOUL);
  }

  @Test
  public void createStreamingSinkTest() throws Exception {
    String tableName = "flinkI1";
    String PATH = "/Users/zhyang/Downloads/tmp2/" + tableName;

    tEnvs.executeSql(
        "CREATE TABLE " + tableName + "( user_id STRING, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with ('connector' = 'lakesoul','format'='parquet','path'='" +
            PATH + "','lakesoul_cdc_change_column'='name','lakesoul_cdc'='true','bucket_num'='2')");

    tEnvs.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
        .schema(Schema.newBuilder()
            .column("user_id", DataTypes.STRING())
            .column("dt", DataTypes.STRING())
            .column("name", DataTypes.STRING())
            .build()).option("rows-per-second", "300").option("fields.dt.length", "1")
        .option("fields.user_id.length", "8").option("fields.name.length", "4")
        .build());
    Table table2 = tEnvs.from("SourceTable");




    table2.executeInsert(tableName);

    Thread.sleep(100000000);


  }
}


