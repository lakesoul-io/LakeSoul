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

package org.apache.flink.lakesoul.sink;

import org.apache.flink.lakesoul.metaData.LakesoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;

public class Main {

  public static void main(String[] args) throws InterruptedException {
     StreamTableEnvironment tEnvs;
     StreamExecutionEnvironment env;
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    env.enableCheckpointing(1001);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10003);
    env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhyang/Downloads/flink");
    tEnvs = StreamTableEnvironment.create(env);
    tEnvs.getConfig().getConfiguration().set(
        ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

    Catalog lakesoulCatalog = new LakesoulCatalog();
    String LAKESOUL = "lakesoul";
    tEnvs.registerCatalog(LAKESOUL, lakesoulCatalog);
    tEnvs.useCatalog(LAKESOUL);

    String tableName = "flinkI121";
    int i = (int) (Math.random() * 1000) % 23;
    tableName+=i;
    String PATH = "/home/zehy/Downloads/tmp2/" + tableName;

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
