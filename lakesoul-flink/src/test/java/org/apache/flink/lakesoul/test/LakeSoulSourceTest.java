package org.apache.flink.lakesoul.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class LakeSoulSourceTest {

    private String BATCH_TYPE = "batch";
    private String STREAMING_TYPE = "streaming";

    @Test
    public void testLakesoulSourceIncrementalStream() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = createTableEnvironment();
        createLakeSoulSourceStreamTestTable(createTableEnv);
        String testSql = String.format("select name,score from test_stream /*+ OPTIONS('readstarttime'='%s','readtype'='incremental','timezone'='Africa/Accra')*/",
                getCurrentDateTime());
        StreamTableEnvironment tEnvs = createTableEnv(STREAMING_TYPE);
        tEnvs.executeSql(testSql).print();
    }

    @Test
    public void testLakesoulSourceSelectMultiRangeAndHash() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = createTableEnvironment();
        createLakeSoulSourceMultiPartitionTable(createTableEnv);
        String testMultiRangeSelect = "select * from user_multi where `region`='UK' and score > 80";
        String testMultiHashSelect = "select name,`date`,region from user_multi where score > 80";
        StreamTableEnvironment tEnvs = createTableEnv(BATCH_TYPE);
        TableImpl flinkTable1 = (TableImpl) tEnvs.sqlQuery(testMultiRangeSelect);
        List<Row> results1 = CollectionUtil.iteratorToList(flinkTable1.execute().collect());
        checkEqualInAnyOrder(results1, new String[]{"+I[3, Amy, 95, 0320, UK]"});
        TableImpl flinkTable2 = (TableImpl) tEnvs.sqlQuery(testMultiHashSelect);
        List<Row> results2 = CollectionUtil.iteratorToList(flinkTable2.execute().collect());
        checkEqualInAnyOrder(results2, new String[]{"+I[Amy, 0320, UK]", "+I[Bob, 0320, China]"});
    }

    @Test
    public void testLakesoulSourceSelectWhere() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = createTableEnvironment();
        createLakeSoulSourceTableUser(createTableEnv);
        String testSelectWhere = "select * from user_info where order_id=3";
        StreamTableEnvironment tEnvs = createTableEnv(BATCH_TYPE);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectWhere);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        checkEqualInAnyOrder(results, new String[]{"+I[3, Jack, 75]", "+I[3, Amy, 95]"});
    }

    @Test
    public void testLakesoulSourceSelectJoin() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = createTableEnvironment();
        createLakeSoulSourceTableUser(createTableEnv);
        createLakeSoulSourceTableOrder(createTableEnv);
        String testSelectJoin = "select ui.order_id,sum(oi.price) as total_price,count(*) as total " +
                "from user_info as ui inner join order_info as oi " +
                "on ui.order_id=oi.id group by ui.order_id having ui.order_id>2";
        StreamTableEnvironment tEnvs = createTableEnv(BATCH_TYPE);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectJoin);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        checkEqualInAnyOrder(results, new String[]{"+I[3, 30, 2]", "+I[4, 25, 1]", "+I[5, 15, 1]"});
    }

    @Test
    public void testLakesoulSourceSelectDistinct() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = createTableEnvironment();
        createLakeSoulSourceTableUser(createTableEnv);
        String testSelectDistinct = "select distinct order_id from user_info where order_id<5";
        StreamTableEnvironment tEnvs = createTableEnv(BATCH_TYPE);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectDistinct);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        checkEqualInAnyOrder(results, new String[]{"+I[1]", "+I[2]", "+I[3]", "+I[4]"});
    }

    @Test
    public void testLakesoulSourceSelectNoPK() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = createTableEnvironment();
        createLakeSoulSourceTableWithoutPK(createTableEnv);
        String testSelectNoPK = "select * from order_noPK";
        StreamTableEnvironment tEnvs = createTableEnv(BATCH_TYPE);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectNoPK);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        checkEqualInAnyOrder(results, new String[]{"+I[1, apple, 20]", "+I[2, tomato, 10]", "+I[3, water, 15]"});
    }


    @Test
    public void testLakeSoulSourceCdc() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = createTableEnvironment();
        createLakeSoulSourceCdcTable(createTableEnv);
        String createSql = "create table user_sink (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score INT" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'use_cdc'='false'," +
                "    'path'='/tmp/lakeSource/cdc_sink' )";
        String cdcSql = "insert into user_sink select * from user_cdc";
        String selectSql = "select * from user_sink";
        StreamTableEnvironment tEnvs = createTableEnv(BATCH_TYPE);
        tEnvs.executeSql("DROP TABLE if exists user_sink");
        tEnvs.executeSql(createSql);
        tEnvs.executeSql(cdcSql);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(selectSql);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        checkEqualInAnyOrder(results, new String[]{"+I[1, Bob, 90]", "+I[2, Alice, 80]", "+I[3, Jack, 75]"});
    }

    private void createLakeSoulSourceStreamTestTable(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createUserSql = "create table test_stream (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score INT" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'use_cdc'='false'," +
                "    'path'='/tmp/lakeSource/test_stream' )";
        tEnvs.executeSql("DROP TABLE if exists test_stream");
        tEnvs.executeSql(createUserSql);
        tEnvs.executeSql("INSERT INTO test_stream VALUES (1, 'Bob', 90), (2, 'Alice', 80)").await();
        Thread.sleep(2000l);
        tEnvs.executeSql("INSERT INTO test_stream VALUES(3, 'Jack', 75)").await();
        Thread.sleep(2000l);
        tEnvs.executeSql("INSERT INTO test_stream VALUES (4, 'Jack', 95),(5, 'Tom', 75)").await();
    }

    private void createLakeSoulSourceTableUser(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_info (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score INT" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'use_cdc'='false'," +
                "    'path'='/tmp/lakeSource/user' )";
        tEnvs.executeSql("DROP TABLE if exists user_info");
        tEnvs.executeSql(createUserSql);
        tEnvs.executeSql("INSERT INTO user_info VALUES (1, 'Bob', 90), (2, 'Alice', 80), (3, 'Jack', 75), (3, 'Amy', 95),(5, 'Tom', 75), (4, 'Mike', 70)").await();
    }

    private void createLakeSoulSourceTableOrder(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createOrderSql = "create table order_info (" +
                "    `id` INT PRIMARY KEY NOT ENFORCED," +
                "    price INT" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'use_cdc'='false'," +
                "    'path'='/tmp/lakeSource/order' )";
        tEnvs.executeSql("DROP TABLE if exists order_info");
        tEnvs.executeSql(createOrderSql);
        tEnvs.executeSql("INSERT INTO order_info VALUES (1, 20), (2, 10), (3, 15), (4, 25), (5, 15)").await();
    }

    private void createLakeSoulSourceCdcTable(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createSql = "create table user_cdc (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score INT" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'use_cdc'='false'," +
                "    'path'='/tmp/lakeSource/cdc' )";
        tEnvs.executeSql("DROP TABLE if exists user_cdc");
        tEnvs.executeSql(createSql);
        tEnvs.executeSql("INSERT INTO user_cdc VALUES (1, 'Bob', 90), (2, 'Alice', 80), (3, 'Jack', 75)").await();
    }

    private void createLakeSoulSourceMultiPartitionTable(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createSql = "create table user_multi (" +
                "    `id` INT," +
                "    name STRING," +
                "    score INT," +
                "    `date` STRING," +
                "    region STRING," +
                "PRIMARY KEY (`id`,`name`) NOT ENFORCED" +
                ") " +
                "PARTITIONED BY (`region`,`date`)" +
                "WITH (" +
                "    'format'='lakesoul'," +
                "    'use_cdc'='false'," +
                "    'path'='/tmp/lakeSource/multi_range_hash' )";
        tEnvs.executeSql("DROP TABLE if exists user_multi");
        tEnvs.executeSql(createSql);
        tEnvs.executeSql("INSERT INTO user_multi VALUES (1, 'Bob', 90, '0320', 'China'), (2, 'Alice', 80, '0321', 'China'), " +
                "(3, 'Jack', 75, '0322', 'China'), (3, 'Amy', 95, '0320','UK'), (5, 'Tom', 75, '0320', 'UK'), (4, 'Mike', 70, '0321', 'UK')").await();
    }

    private void createLakeSoulSourceTableWithoutPK(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createOrderSql = "create table order_noPK (" +
                "    `id` INT," +
                "    name STRING," +
                "    price INT" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'use_cdc'='false'," +
                "    'path'='/tmp/lakeSource/noPK' )";
        tEnvs.executeSql("DROP TABLE if exists order_noPK");
        tEnvs.executeSql(createOrderSql);
        tEnvs.executeSql("INSERT INTO order_noPK VALUES (1,'apple',20), (2,'tomato',10), (3,'water',15)").await();
    }

    private TableEnvironment createTableEnvironment() {
        TableEnvironment createTableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        createTableEnv.registerCatalog("lakeSoul", lakesoulCatalog);
        createTableEnv.useCatalog("lakeSoul");
        createTableEnv.useDatabase("default");
        return createTableEnv;
    }

    private StreamTableEnvironment createTableEnv(String envType) {
        StreamTableEnvironment tEnvs;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        if (envType.equals(STREAMING_TYPE)) {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");
        return tEnvs;
    }

    private String getCurrentDateTime() {
        Instant instant = Instant.ofEpochMilli(System.currentTimeMillis() - 3000l);
        ZoneId zoneId = ZoneId.of("Africa/Accra");
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return zonedDateTime.format(formatter);
    }

    private void checkEqualInAnyOrder(List<Row> results, String[] expectedResult) {
        assertThat(results.stream().map(row -> row.toString()).collect(Collectors.toList())).
                containsExactlyInAnyOrder(expectedResult);
    }

}
