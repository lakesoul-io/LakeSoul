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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
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
    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    env.setParallelism(1);
    env.enableCheckpointing(10000);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
    env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink/test");
    tEnvs = StreamTableEnvironment.create(env);
    tEnvs.getConfig().getConfiguration().set(
        ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

    //source
    tEnvs.executeSql("create table mysql_test_1(\n" +
        "id INTEGER primary key NOT ENFORCED ," +
        "name string," +
        " dt int)" +
        " with (\n" +
        "'connector'='mysql-cdc'," +
        "'hostname'='127.0.0.1'," +
        "'port'='3306'," +
        "'server-id'='1'," +
        "'username'='root',\n" +
        "'password'='root',\n" +
        "'database-name'='test_cdc',\n" +
        "'table-name'='mysql_test_1'\n" +
        ")");

    Catalog lakesoulCatalog = new LakeSoulCatalog();
    tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
    tEnvs.useCatalog("lakeSoul");
    String tableName = "flinkI1715";
    String PATH = "/tmp/lakesoul/" + tableName;

    //target
    tEnvs.executeSql(
        "CREATE TABLE " + tableName + "( id int," +
            " name string," +
            " dt int," +
            "primary key (id) NOT ENFORCED ) " +
            "PARTITIONED BY (dt)" +
            " with ('connector' = 'lakeSoul'," +
            "'format'='parquet','path'='" +
            PATH + "'," +
            "'useCDC'='true'," +
            "'hashBucketNum'='2')");

    tEnvs.executeSql("show tables ").print();

    tEnvs.useCatalog("default_catalog");

    tEnvs.executeSql("insert into lakeSoul.test_lakesoul_meta." + tableName + " select * from mysql_test_1 ");

    Thread.sleep(100000000);
  }

}


