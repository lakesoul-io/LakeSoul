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

package org.apache.flink.lakesoul.test;

import org.apache.flink.lakesoul.metaData.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;

public class Main {

  public static void main(String[] args) throws Exception {
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
    tEnvs.registerCatalog("lakesoul", lakesoulCatalog);
    tEnvs.useCatalog("lakesoul");
    String tableName = "flinkI" + (int) (Math.random() * 156439750) % 2235;
    String PATH = "/Users/zhyang/Downloads/tmp/" + tableName;

    //target
    tEnvs.executeSql(
        "CREATE TABLE " + tableName + "( id int," +
            " name string," +
            " dt string," +
            "primary key (id) NOT ENFORCED ) " +
            "PARTITIONED BY (dt)" +
            " with ('connector' = 'lakesoul'," +
            "'format'='parquet','path'='" +
            PATH + "'," +
            "'lakesoul_cdc_change_column'='name'," +

            "'lakesoul_cdc'='true'," +
            "'bucket_num'='2')");

    tEnvs.useCatalog("default_catalog");

    tEnvs.executeSql("insert into lakesoul.test_lakesoul_meta." + tableName + " select * from mysql_test_1 ");

  }
}
