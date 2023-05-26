package org.apache.flink.lakesoul.test.flinkSource;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchReadSuite {
    private String BATCH_TYPE = "batch";
    private String startTime;
    private String endTime;

    @Test
    public void testLakesoulSourceSnapshotRead() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTestTable(createTableEnv);
        String testSql = String.format("select * from user_test /*+ OPTIONS('readendtime'='%s','readtype'='snapshot','timezone'='Africa/Accra')*/",
                endTime);
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSql);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, Jack, 75]", "+I[2, Alice, 80]", "+I[1, Bob, 90]"});
    }

    @Test
    public void testLakesoulSourceIncrementalRead() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTestTable(createTableEnv);
        String testSql = String.format("select * from user_test /*+ OPTIONS('readstarttime'='%s','readendtime'='%s','readtype'='incremental','timezone'='Africa/Accra')*/",
                startTime, endTime);
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSql);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, Jack, 75]"});
    }

    @Test
    public void testLakesoulSourceSelectNoPK() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableWithoutPK(createTableEnv);
        String testSelectNoPK = "select * from order_noPK";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectNoPK);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[1, apple, 20.00]", "+I[2, tomato, 10.00]", "+I[3, water, 15.00]"});
    }

    @Test
    public void testLakesoulSourceSelectMultiRangeAndHash() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        TestUtils.createLakeSoulSourceMultiPartitionTable(createTableEnv);
        String testMultiRangeSelect = "select * from user_multi where `region`='UK' and score > 80";
        String testMultiHashSelect = "select name,`date`,region from user_multi where score > 80";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        TableImpl flinkTable1 = (TableImpl) tEnvs.sqlQuery(testMultiRangeSelect);
        List<Row> results1 = CollectionUtil.iteratorToList(flinkTable1.execute().collect());
        TestUtils.checkEqualInAnyOrder(results1, new String[]{"+I[3, Amy, 95, 1995-10-10, UK]"});
        TableImpl flinkTable2 = (TableImpl) tEnvs.sqlQuery(testMultiHashSelect);
        List<Row> results2 = CollectionUtil.iteratorToList(flinkTable2.execute().collect());
        TestUtils.checkEqualInAnyOrder(results2, new String[]{"+I[Amy, 1995-10-10, UK]", "+I[Bob, 1995-10-01, China]"});
    }

    @Test
    public void testLakesoulSourceSelectWhere() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        TestUtils.createLakeSoulSourceTableUser(createTableEnv);
        String testSelectWhere = "select * from user_info where order_id=3";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectWhere);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, Jack, 75]", "+I[3, Amy, 95]"});
    }

    @Test
    public void testLakesoulSourceSelectJoin() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        TestUtils.createLakeSoulSourceTableUser(createTableEnv);
        TestUtils.createLakeSoulSourceTableOrder(createTableEnv);
        String testSelectJoin = "select ui.order_id,sum(oi.price) as total_price,count(*) as total " +
                "from user_info as ui inner join order_info as oi " +
                "on ui.order_id=oi.id group by ui.order_id having ui.order_id>2";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectJoin);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, 30.7, 2]", "+I[4, 25.24, 1]", "+I[5, 15.04, 1]"});
    }

    @Test
    public void testLakesoulSourceSelectDistinct() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        TestUtils.createLakeSoulSourceTableUser(createTableEnv);
        String testSelectDistinct = "select distinct order_id from user_info where order_id<5";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectDistinct);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[1]", "+I[2]", "+I[3]", "+I[4]"});
    }

    private void createLakeSoulSourceTestTable(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_test (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score INT" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='/tmp/lakeSource/user_test' )";
        tEnvs.executeSql("DROP TABLE if exists user_test");
        tEnvs.executeSql(createUserSql);
        tEnvs.executeSql("INSERT INTO user_test VALUES (1, 'Bob', 90), (2, 'Alice', 80)").await();
        Thread.sleep(1000l);
        startTime = TestUtils.getDateTimeFromTimestamp(Instant.ofEpochMilli(System.currentTimeMillis()));
        tEnvs.executeSql("INSERT INTO user_test VALUES(3, 'Jack', 75)").await();
        Thread.sleep(1000l);
        endTime = TestUtils.getDateTimeFromTimestamp(Instant.ofEpochMilli(System.currentTimeMillis()));
        tEnvs.executeSql("INSERT INTO user_test VALUES (4, 'Jack', 95),(5, 'Tom', 75)").await();
    }

    private void createLakeSoulSourceTableWithoutPK(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createOrderSql = "create table order_noPK (" +
                "    `id` INT," +
                "    name STRING," +
                "    price DECIMAL(8,2)" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'path'='/tmp/lakeSource/noPK' )";
        tEnvs.executeSql("DROP TABLE if exists order_noPK");
        tEnvs.executeSql(createOrderSql);
        tEnvs.executeSql("INSERT INTO order_noPK VALUES (1,'apple',20), (2,'tomato',10), (3,'water',15)").await();
    }
}
