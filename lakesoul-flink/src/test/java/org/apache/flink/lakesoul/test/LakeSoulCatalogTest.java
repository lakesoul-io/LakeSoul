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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LakeSoulCatalogTest extends AbstractTestBase {
    private final String LAKESOUL = "lakesoul";
    private Map<String, String> props;
    private StreamTableEnvironment tEnvs;
    private DBManager DbManage;

    @Before
    public void before() {
        props = new HashMap<>();
        props.put("type", LAKESOUL);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnvs = StreamTableEnvironment.create(env);
        LakeSoulCatalog lakesoulCatalog = new LakeSoulCatalog();
        lakesoulCatalog.cleanForTest();
        lakesoulCatalog.open();

        lakesoulCatalog.createDatabase("test_lakesoul_meta", new LakesoulCatalogDatabase(), true);
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
        assertTrue(tableEnv.getCatalog(LAKESOUL).get() instanceof LakeSoulCatalog);
    }


    @Test
    public void createTable() {
        tEnvs.executeSql(
                "CREATE TABLE if not exists user_behaviorgg ( user_id BIGINT, dt STRING, name STRING,primary key (user_id)" +
                        " NOT ENFORCED ) PARTITIONED BY (dt) with ('lakesoul_cdc_change_column'='name', 'hashBucketNum'='2'," +
                        "'lakesoul_meta_host'='127.0.0.2','lakesoul_meta_host_port'='9043', 'path'='" +
                        getTempDirUri("/user_behaviorgg") +
                        "')");
        tEnvs.executeSql("show tables").print();
        TableInfo info = DbManage.getTableInfoByNameAndNamespace("user_behaviorgg", "test_lakesoul_meta");
        assertEquals(info.getTableSchema(),
                new StructType().add("user_id", LongType, false).add("dt", StringType).add("name", StringType)
                        .json());
        tEnvs.executeSql("DROP TABLE user_behaviorgg");
    }

    @Test
    public void createTableWithLike() {
        tEnvs.executeSql("CREATE TABLE if not exists user_behaviorgg ( user_id BIGINT, dt STRING, name STRING NOT NULL,primary key (user_id)" +
                " NOT ENFORCED ) PARTITIONED BY (dt) with ('lakesoul_cdc_change_column'='name', 'hashBucketNum'='2'," +
                "'lakesoul_meta_host_port'='9043', 'path'='/tmp/user_behaviorgg', 'use_cdc'='true')");

        TableInfo info = DbManage.getTableInfoByNameAndNamespace("user_behaviorgg", "test_lakesoul_meta");
        Assertions.assertThat(info.getTableSchema()).isEqualTo(new StructType().add("user_id", LongType, false).add("dt", StringType).add("name", StringType, false).json());

        tEnvs.executeSql("CREATE TABLE if not exists like_table with ('path'='/tmp/like_table') like user_behaviorgg");
        TableInfo info2 = DbManage.getTableInfoByNameAndNamespace("like_table", "test_lakesoul_meta");
        Assertions.assertThat(info2.getTableSchema()).isEqualTo(new StructType().add("user_id", LongType, false).add("dt", StringType).add("name", StringType, false).json());
        Assertions.assertThat(info.getProperties().get("lakesoul_cdc_change_column")).isEqualTo(info2.getProperties().get("lakesoul_cdc_change_column"));
        Assertions.assertThat(info.getProperties().get("path")).isEqualTo("/tmp/user_behaviorgg");
        Assertions.assertThat(info2.getProperties().get("path")).isEqualTo("/tmp/like_table");
        System.out.println(info);
        System.out.println(info2);

        tEnvs.executeSql("DROP TABLE user_behaviorgg");
        tEnvs.executeSql("DROP TABLE like_table");
    }

    @Test
    public void dropTable() {
        tEnvs.executeSql("drop table if exists user_behavior7464434");
        tEnvs.executeSql("show tables").print();
    }
}