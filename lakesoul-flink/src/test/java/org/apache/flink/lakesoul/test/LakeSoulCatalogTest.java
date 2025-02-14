// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import com.alibaba.fastjson.JSON;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.metadata.LakesoulCatalogDatabase;
import org.apache.flink.lakesoul.table.LakeSoulCatalogFactory;
import org.apache.flink.lakesoul.test.flinkSource.TestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.spark.sql.arrow.ArrowUtils;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.lakesoul.test.flinkSource.TestUtils.BATCH_TYPE;
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
                ArrowUtils.toArrowSchema(new StructType().add("user_id", LongType, false).add("dt", StringType).add("name", StringType), "UTC").toJson());
        tEnvs.executeSql("DROP TABLE user_behaviorgg");
    }

    @Test
    public void createTableWithLike() {
        tEnvs.executeSql("CREATE TABLE if not exists user_behaviorgg ( user_id BIGINT, dt STRING, name STRING NOT NULL,primary key (user_id)" +
                " NOT ENFORCED ) PARTITIONED BY (dt) with ('lakesoul_cdc_change_column'='name', 'hashBucketNum'='2'," +
                "'lakesoul_meta_host_port'='9043', 'path'='/tmp/user_behaviorgg', 'use_cdc'='true')");

        TableInfo info = DbManage.getTableInfoByNameAndNamespace("user_behaviorgg", "test_lakesoul_meta");
        Assertions.assertThat(info.getTableSchema()).isEqualTo(ArrowUtils.toArrowSchema(new StructType().add("name", StringType, false).add("user_id", LongType, false).add("dt", StringType), "UTC").toJson());

        tEnvs.executeSql("CREATE TABLE if not exists like_table with ('path'='/tmp/like_table') like user_behaviorgg");
        TableInfo info2 = DbManage.getTableInfoByNameAndNamespace("like_table", "test_lakesoul_meta");
        Assertions.assertThat(info2.getTableSchema()).isEqualTo(ArrowUtils.toArrowSchema(new StructType().add("name", StringType, false).add("user_id", LongType, false).add("dt", StringType), "UTC").toJson());
        Assertions.assertThat(JSON.parseObject(info.getProperties()).get("lakesoul_cdc_change_column")).isEqualTo(JSON.parseObject(info2.getProperties()).get("lakesoul_cdc_change_column"));
        Assertions.assertThat(JSON.parseObject(info.getProperties()).get("path")).isEqualTo("/tmp/user_behaviorgg");
        Assertions.assertThat(JSON.parseObject(info2.getProperties()).get("path")).isEqualTo("/tmp/like_table");

        tEnvs.executeSql("DROP TABLE user_behaviorgg");
        tEnvs.executeSql("DROP TABLE like_table");
    }

    @Test
    public void createTableWithComments() {
        tEnvs.executeSql("create table table_with_comments (" +
                "user_id BIGINT PRIMARY KEY NOT ENFORCED COMMENT 'user id'," +
                "name STRING COMMENT 'user name') " +
                "COMMENT 'this is user table' " +
                "WITH (" +
                "'connector'='lakesoul'," +
                "'hashBucketNum'='2'," +
                "'path'='file:///tmp/table_with_comments')");
        List<Row> showCreateTableResult = CollectionUtil.iteratorToList(
            tEnvs.executeSql("show create table table_with_comments").collect());
        Assertions.assertThat(showCreateTableResult.get(0).getField(0).toString())
                        .contains("COMMENT 'this is user table'");
        List<Row> descTableResult = CollectionUtil.iteratorToList(
            tEnvs.executeSql("desc table_with_comments").collect());
        Assertions.assertThat(descTableResult.get(0).getField(6).toString())
                .contains("user id");
        Assertions.assertThat(descTableResult.get(1).getField(6).toString())
                .contains("user name");
        tEnvs.executeSql("drop table table_with_comments").print();
    }

    @Test
    public void testListPartitions()
            throws TableNotPartitionedException, TableNotExistException, ExecutionException, InterruptedException {
        LakeSoulCatalogFactory catalogFactory = new LakeSoulCatalogFactory();
        Catalog lakesoulCatalog = catalogFactory.createCatalog(LAKESOUL, props);
        TableEnvironment streamTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        String createUserSql = "create table test_table (" +
                "    id INT," +
                "    val STRING" +
                ") WITH (" +
                "    'connector'='lakesoul'," +
                "    'path'='" + getTempDirUri("/lakeSource/time_test") +
                "' )";
        streamTableEnv.executeSql(createUserSql);
        streamTableEnv.executeSql("INSERT INTO test_table VALUES " +
                "(1, '1'), (2, '2')").await();
        assertTrue("empty partition", lakesoulCatalog.listPartitions(ObjectPath.fromString("default.test_table")).isEmpty());
        streamTableEnv.executeSql("DROP TABLE if exists test_table");
        createUserSql = "create table test_table (" +
                "    id INT," +
                "    val STRING" +
                ") PARTITIONED BY (id) WITH (" +
                "    'connector'='lakesoul'," +
                "    'path'='" + getTempDirUri("/lakeSource/time_test") +
                "' )";
        streamTableEnv.executeSql(createUserSql);
        streamTableEnv.executeSql("INSERT INTO test_table VALUES " +
                "(1, '1'), (2, '2')").await();
        List<CatalogPartitionSpec> partitionSpecs = lakesoulCatalog
                .listPartitions(ObjectPath.fromString("default.test_table"));
        assertEquals(2, partitionSpecs.size());
        for (CatalogPartitionSpec partitionSpec : partitionSpecs) {
            assertEquals(1, partitionSpec.getPartitionSpec().size());
            assertTrue(partitionSpec.getPartitionSpec().get("id").equals("1") ||
                    partitionSpec.getPartitionSpec().get("id").equals("2"));
        }
        streamTableEnv.executeSql("DROP TABLE if exists test_table");
    }


    @Test
    public void dropTable() {
        tEnvs.executeSql("drop table if exists user_behavior7464434");
        tEnvs.executeSql("show tables").print();
    }
}