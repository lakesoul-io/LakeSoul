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
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.metadata.LakesoulCatalogDatabase;
import org.apache.flink.lakesoul.table.LakeSoulCatalogFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LakeSoulCatalogTest {
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
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        lakesoulCatalog.open();

        try {
            lakesoulCatalog.createDatabase("test_lakesoul_meta", new LakesoulCatalogDatabase(), false);
        } catch (DatabaseAlreadyExistException e) {
            throw new RuntimeException(e);
        }
        tEnvs.registerCatalog(LAKESOUL, lakesoulCatalog);
        tEnvs.useCatalog(LAKESOUL);
        tEnvs.useDatabase("test_lakesoul_meta");
        DbManage = new DBManager();
    }

    @Test
    public void LakesoulCatalog() {
        LakeSoulCatalogFactory catalogFactory = new LakeSoulCatalogFactory();
        Catalog lakesoulCatalog = catalogFactory.createCatalog(LAKESOUL, props);
        assertTrue(lakesoulCatalog instanceof LakeSoulCatalog);
    }

    @Test
    public void registerCatalog() {
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bbSettings);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tableEnv.registerCatalog(LAKESOUL, lakesoulCatalog);
        tableEnv.useCatalog(LAKESOUL);
        System.out.println(tableEnv.getCurrentCatalog());
        assertTrue(tableEnv.getCatalog(LAKESOUL).get() instanceof LakeSoulCatalog);
    }


    @Test
    public void createTable() {
        tEnvs.executeSql("CREATE TABLE user_behaviorgg ( user_id BIGINT, dt STRING, name STRING,primary key (user_id)" +
                         " NOT ENFORCED ) PARTITIONED BY (dt) with ('lakesoul_cdc_change_column'='name'," +
                         "'lakesoul_meta_host'='127.0.0.2','lakesoul_meta_host_port'='9043')");
        tEnvs.executeSql("show tables").print();
        TableInfo info = DbManage.getTableInfoByPath("MetaCommon.DATA_BASE().user_behaviorgg");
        System.out.println(info.getTableSchema());
    }

    @Test
    public void dropTable() {
        tEnvs.executeSql("drop table user_behavior7464434");
        tEnvs.executeSql("show tables").print();
    }


    @Test
    public void sqlDefaultSink() {

        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tableEnv.executeSql(
                "CREATE TABLE GeneratedTable "
                + "("
                + "  name STRING,"
                + "  score INT,"
                + "  event_time TIMESTAMP_LTZ(3),"
                + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
                + ")"
                + "WITH ('connector'='datagen')");

        Table table = tableEnv.from("GeneratedTable");
        tableEnv.toDataStream(table).print();
        tableEnv.executeSql("insert into user_behavior27 values (1,'key1','value1'),(2,'key1','value2'),(3,'key3'," +
                            "'value3')");
    }
}