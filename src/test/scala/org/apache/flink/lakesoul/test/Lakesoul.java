/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.lakesoul.*;
import org.apache.flink.lakesoul.LakesoulCatalogFactory;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Lakesoul {
    private Map<String, String> props;
    private StreamTableEnvironment tEnvs;
    private final String LAKESOUL = "lakesoul";
    private DBManager DbManage;

    @Before
    public void before() {
        props = new HashMap<>();
        props.put("type", LAKESOUL);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakesoulCatalog();
        tEnvs.registerCatalog(LAKESOUL,lakesoulCatalog);
        tEnvs.useCatalog(LAKESOUL);
        DbManage = new DBManager();
    }

    @Test
    public void LakesoulCatalog(){
        LakesoulCatalogFactory catalogFactory =new LakesoulCatalogFactory();
        Catalog lakesoulCatalog = catalogFactory.createCatalog(LAKESOUL,props);
        assertTrue(lakesoulCatalog instanceof LakesoulCatalog);
    }

    @Test
    public void registerCatalog(){
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bbSettings);
        Catalog lakesoulCatalog = new LakesoulCatalog();
        tableEnv.registerCatalog(LAKESOUL,lakesoulCatalog);
        tableEnv.useCatalog(LAKESOUL);
        System.out.println(tableEnv.getCurrentCatalog());
        assertEquals(true, tableEnv.getCatalog(LAKESOUL).get() instanceof LakesoulCatalog);
    }


    @Test
    public void createTable(){
        tEnvs.executeSql( "CREATE TABLE user_behaviorgg ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with ('lakesoul_cdc_change_column'='name','lakesoul_meta_host'='127.0.0.2','lakesoul_meta_host_port'='9043')" );
        tEnvs.executeSql("show tables").print();
        TableInfo info = DbManage.getTableInfo("MetaCommon.DATA_BASE().user_behaviorgg");
        System.out.println(info.getTableSchema());
    }

    @Test
    public void dropTable() {
        tEnvs.executeSql("drop table user_behavior7464434");
        tEnvs.executeSql("show tables").print();
    }


    @Test
    public void sqlLakesoulTableSink() throws InterruptedException {
        tEnvs.executeSql( "CREATE TABLE user_behaviors1t4433 ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with ('connector' = 'lakesoul','format'='parquet','path'='/Users/zhyang/Downloads','lakesoul_cdc_change_column'='name','lakesoul_meta_host'='127.0.0.2','lakesoul_meta_host_port'='9043')" );
        tEnvs.executeSql("insert into user_behaviors1t4433 values (1666,'key1','value1'),(2666,'key1','value2'),(3666,'key3','value3')");
        tEnvs.executeSql("insert into user_behaviors1t4433 values (11,'key1','value1'),(22,'key1','value2'),(33,'key3','value3')");
        tEnvs.executeSql("insert into user_behaviors1t4433 values (1119,'key19','value1'),(222,'key91','value2'),(3393,'key973','value3')");
        Thread.sleep(10000000);
    }

    @Test
    public void sqlDefaultSink(){
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bbSettings);
        tableEnv.executeSql( "CREATE TABLE user_behavior27 ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with('connector' = 'filesystem','format'='parquet','path'='/Users/zhyang/Downloads')" );
        tableEnv.executeSql("insert into user_behavior27 values (1,'key1','value1'),(2,'key1','value2'),(3,'key3','value3')");
        tableEnv.executeSql("show tables").print();
    }
}