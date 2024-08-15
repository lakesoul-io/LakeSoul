// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.flinkSource;

import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.lakesoul.test.flinkSource.TestUtils.BATCH_TYPE;

public class DMLSuite extends AbstractTestBase {

    @Test
    public void testInsertSQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUser(tEnv);
        tEnv.executeSql("INSERT INTO user_info VALUES (2, 'Alice', 80),(3, 'Jack', 75)").await();
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]", "+I[3, Jack, 75]"});
        tEnv.executeSql("INSERT INTO user_info VALUES (4, 'Mike', 70)").await();
        TableImpl flinkTable1 = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results1 = CollectionUtil.iteratorToList(flinkTable1.execute().collect());
        TestUtils.checkEqualInAnyOrder(results1,
                new String[]{"+I[2, Alice, 80]", "+I[3, Jack, 75]", "+I[4, Mike, 70]"});
    }

    @Test
    public void testInsertPkPartitionTableSQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUserWithRange(tEnv);
        tEnv.executeSql("INSERT INTO user_info_1 VALUES (2, 'Alice', 80),(3, 'Jack', 75)").await();
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info_1";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]", "+I[3, Jack, 75]"});
        tEnv.executeSql("INSERT INTO user_info_1 VALUES (4, 'Mike', 70)").await();
        TableImpl flinkTable1 = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results1 = CollectionUtil.iteratorToList(flinkTable1.execute().collect());
        TestUtils.checkEqualInAnyOrder(results1,
                new String[]{"+I[2, Alice, 80]", "+I[3, Jack, 75]", "+I[4, Mike, 70]"});
        List<Row>
                results2 =
                CollectionUtil.iteratorToList(tEnv.executeSql("select order_id from user_info_1").collect());
        TestUtils.checkEqualInAnyOrder(results2,
                new String[]{"+I[2]", "+I[3]", "+I[4]"});
        List<Row>
                results3 =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("select order_id, sum(score) from user_info_1 group by order_id").collect());
        TestUtils.checkEqualInAnyOrder(results3,
                new String[]{"+I[2, 80]", "+I[3, 75]", "+I[4, 70]"});
    }


    @Test
    public void testUpdateNonPkAndPartitionSQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUser(tEnv);
        tEnv.executeSql("INSERT INTO user_info VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Mike', 70)")
                .await();
        try {
            tEnv.executeSql("UPDATE user_info set score = 100 where order_id = 3").await();
        } catch (Throwable e) {
            System.out.println("Unsupported UPDATE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results,
                new String[]{"+I[2, Alice, 80]", "+I[3, Amy, 100]", "+I[3, Jack, 100]", "+I[4, Mike, 70]"});
    }

    @Test
    public void testNonPkPartitionedTableSQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceNonPkWithPartitionTableUser(tEnv);
        tEnv.executeSql(
                        "INSERT INTO user_info_3 VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Mike', 70)")
                .await();
        List<Row>
                results1 =
                CollectionUtil.iteratorToList(tEnv.executeSql("select order_id from user_info_3").collect());
        TestUtils.checkEqualInAnyOrder(results1,
                new String[]{"+I[2]", "+I[3]", "+I[3]", "+I[4]"});
        List<Row>
                results2 =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("select order_id, sum(score) from user_info_3 group by order_id").collect());
        TestUtils.checkEqualInAnyOrder(results2,
                new String[]{"+I[2, 80]", "+I[3, 170]", "+I[4, 70]"});
    }

    @Test
    public void testUpdate() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceNonPkTableUser(tEnv);
        tEnv.executeSql(
                        "INSERT INTO user_info_2 VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Mike', 70)")
                .await();
        try {
            tEnv.executeSql("UPDATE user_info_2 set name = cast('Johny' as varchar) where order_id = 4").await();
        } catch (Throwable e) {
            System.out.println("Unsupported UPDATE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info_2";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results,
                new String[]{"+I[2, Alice, 80]", "+I[3, Amy, 95]", "+I[3, Jack, 75]", "+I[4, Johny, 70]"});
    }

    @Test
    public void testUpdatePkSQLNotSupported() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUser(tEnv);
        tEnv.executeSql("INSERT INTO user_info VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Mike', 70)")
                .await();
        try {
            tEnv.executeSql("UPDATE user_info set name = 'John' where order_id = 4").await();
        } catch (Throwable e) {
            System.out.println("Unsupported UPDATE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results,
                new String[]{"+I[2, Alice, 80]",
                        "+I[3, Amy, 95]",
                        "+I[3, Jack, 75]",
                        "+I[4, John, 70]",
                        "+I[4, Mike, 70]"});
    }

    @Test
    public void testUpdatePartitionSQLNotSupported() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUserWithRange(tEnv);
        tEnv.executeSql(
                        "INSERT INTO user_info_1 VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Mike', 70)")
                .await();
        try {
            tEnv.executeSql("UPDATE user_info_1 set order_id = 1 where score = 75").await();
        } catch (Throwable e) {
            System.out.println("Unsupported UPDATE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info_1";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results,
                new String[]{"+I[2, Alice, 80]",
                        "+I[1, Jack, 75]",
                        "+I[3, Jack, 75]",
                        "+I[3, Amy, 95]",
                        "+I[4, Mike, 70]"});
    }

    @Test
    public void testUpdateCDCTableSQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulCDCSourceTableUser(tEnv);
        tEnv.executeSql("INSERT INTO user_info VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Mike', 70)")
                .await();
        try {
            tEnv.executeSql("UPDATE user_info set score = 85 where name = 'Alice'").await();
        } catch (Throwable e) {
            System.out.println("Unsupported Update SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results,
                new String[]{"+I[2, Alice, 85]", "+I[3, Amy, 95]", "+I[3, Jack, 75]", "+I[4, Mike, 70]"});
    }

    @Test
    public void testDeleteNonPkAndPartitionSQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUser(tEnv);
        tEnv.executeSql("INSERT INTO user_info VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95)").await();
        try {
            tEnv.executeSql("DELETE FROM user_info where order_id = 3").await();
        } catch (Throwable e) {
            System.out.println("Unsupported DELETE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]"});
    }

    @Test
    public void testDeletePkSQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUser(tEnv);
        tEnv.executeSql("INSERT INTO user_info VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Bob', 110)")
                .await();
        try {
            tEnv.executeSql("DELETE FROM user_info where name = 'Jack'").await();
        } catch (Throwable e) {
            System.out.println("Unsupported DELETE SQL");
        }
        String testSelect = "select * from user_info";
        TableImpl flinkTable = (TableImpl) tEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]", "+I[3, Amy, 95]", "+I[4, Bob, 110]"});
    }

    @Test
    public void testDeleteCDCPkSQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulCDCSourceTableUser(tEnv);
        tEnv.executeSql("INSERT INTO user_info VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Bob', 110)")
                .await();
        try {
            tEnv.executeSql("DELETE FROM user_info where name = 'Jack'").await();
        } catch (Throwable e) {
            System.out.println("Unsupported DELETE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]", "+I[3, Amy, 95]", "+I[4, Bob, 110]"});
    }

    @Test
    public void testDeletePartitionAndPkSQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUserWithRange(tEnv);
        tEnv.executeSql(
                        "INSERT INTO user_info_1 VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Bob', 110)")
                .await();
        try {
            // LakeSoulTableSource::applyPartition will not be called and LakeSoulTableSource::applyFilters will be called
            tEnv.executeSql("DELETE FROM user_info_1 where order_id = 3 and name = 'Jack'").await();
        } catch (Throwable e) {
            System.out.println("Unsupported DELETE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info_1";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]", "+I[3, Amy, 95]", "+I[4, Bob, 110]"});
    }

    @Test
    public void testDeletePartitionOnlySQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUserWithRange(tEnv);
        tEnv.executeSql(
                        "INSERT INTO user_info_1 VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Bob', 110)")
                .await();
        try {
            // LakeSoulTableSource::applyPartition will be called and LakeSoulTableSource::applyFilters will not be called
            tEnv.executeSql("DELETE FROM user_info_1 where order_id = 3").await();
        } catch (Throwable e) {
            System.out.println("Unsupported DELETE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info_1";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]", "+I[4, Bob, 110]"});
    }

    @Test
    public void testDeleteAllPartitionedDataExactlySQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUserWithRange(tEnv);
        tEnv.executeSql(
                        "INSERT INTO user_info_1 VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Bob', 110)")
                .await();
        try {
            // LakeSoulTableSource::applyPartition will be called and LakeSoulTableSource::applyFilters will not be called
            tEnv.executeSql("DELETE FROM user_info_1 where order_id = 3 and score > 60").await();
        } catch (Throwable e) {
            System.out.println("Unsupported DELETE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info_1";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]", "+I[4, Bob, 110]"});
    }

    @Test
    public void testDeletePartitionedCdcTable() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulPartitionedCDCSourceTableUser(tEnv);
        tEnv.executeSql(
                        "INSERT INTO user_info_1 VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Bob', 110)")
                .await();
        try {
            // LakeSoulTableSource::applyPartition will be called and LakeSoulTableSource::applyFilters will not be called
            tEnv.executeSql("DELETE FROM user_info_1 where order_id = 3").await();
        } catch (Throwable e) {
            System.out.println("Unsupported DELETE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info_1";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]", "+I[4, Bob, 110]"});
    }

    @Test
    public void testPKTypes() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        tEnv.getConfig().setLocalTimeZone(TimeZone.getTimeZone("Asia/Shanghai").toZoneId());
        String createUserSql = "create table test_pk_types (" +
                "    id1 DECIMAL(10, 5)," +
                "    id2 DATE," +
                "    id3 TIMESTAMP_LTZ(6)," +
                "    v STRING," +
                "CONSTRAINT `PK_pk` PRIMARY KEY (`id1`, `id2`, `id3`) NOT ENFORCED" +
                ")" +
                " WITH (" +
                "    'connector'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'use_cdc'='true'," +
                "    'path'='" + getTempDirUri("/lakeSource/test_pk_types") +
                "' )";
        tEnv.executeSql("DROP TABLE if exists test_pk_types");
        tEnv.executeSql(createUserSql);
        tEnv.executeSql("INSERT INTO test_pk_types VALUES " +
                        "(CAST(2 as DECIMAL(10, 5)), DATE '2024-01-01', TIMESTAMP '2024-02-03 01:01:01.123456', 'value1')," +
                        "(CAST('1234.56' as DECIMAL(10, 5)), DATE '2024-02-01', TIMESTAMP '2024-03-03 01:01:01.123456', 'value2')," +
                        "(CAST(55.78 as DECIMAL(10, 5)), DATE '2024-01-03', TIMESTAMP '2024-02-04 01:01:01.123456', 'value3')," +
                        "(CAST('123' as DECIMAL(10, 5)), DATE '2024-05-01', TIMESTAMP '2024-02-05 01:01:01.123456', 'value4')")
                .await();
        String testSelect = "select * from test_pk_types";
        TableImpl flinkTable = (TableImpl) tEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{
                "+I[2.00000, 2024-01-01, 2024-02-02T17:01:01.123456Z, value1]",
                "+I[123.00000, 2024-05-01, 2024-02-04T17:01:01.123456Z, value4]",
                "+I[55.78000, 2024-01-03, 2024-02-03T17:01:01.123456Z, value3]",
                "+I[1234.56000, 2024-02-01, 2024-03-02T17:01:01.123456Z, value2]"
        });
    }

    private void createLakeSoulSourceNonPkTableUser(TableEnvironment tEnvs)
            throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_info_2 (" +
                "    order_id INT," +
                "    name varchar," +
                "    score DECIMAL" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='" + getTempDirUri("/lakeSource/user2") +
                "' )";
        tEnvs.executeSql("DROP TABLE if exists user_info_2");
        tEnvs.executeSql(createUserSql);
    }

    private void createLakeSoulSourceNonPkWithPartitionTableUser(TableEnvironment tEnvs)
            throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_info_3 (" +
                "    order_id INT," +
                "    name varchar," +
                "    score DECIMAL" +
                ") PARTITIONED BY ( order_id )" +
                " WITH (" +
                "    'format'='lakesoul'," +
                "    'path'='" + getTempDirUri("/lakeSource/user_nonpk_partitioned") +
                "' )";
        tEnvs.executeSql("DROP TABLE if exists user_info_3");
        tEnvs.executeSql(createUserSql);
    }

    private void createLakeSoulSourceTableUser(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_info (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score DECIMAL" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='" + getTempDirUri("/lakeSource/user") +
                "' )";
        tEnvs.executeSql("DROP TABLE if exists user_info");
        tEnvs.executeSql(createUserSql);
    }

    private void createLakeSoulSourceTableUserWithRange(TableEnvironment tEnvs)
            throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_info_1 (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score DECIMAL" +
                ") PARTITIONED BY ( order_id )" +
                "WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='" + getTempDirUri("/lakeSource/user1") +
                "' )";
        tEnvs.executeSql("DROP TABLE if exists user_info_1");
        tEnvs.executeSql(createUserSql);
    }

    private void createLakeSoulCDCSourceTableUser(TableEnvironment tEnvs)
            throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_info (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score DECIMAL" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'use_cdc'='true'," +
                "    'path'='" + getTempDirUri("/lakeSource/user") +
                "' )";
        tEnvs.executeSql("DROP TABLE if exists user_info");
        tEnvs.executeSql(createUserSql);
    }

    private void createLakeSoulPartitionedCDCSourceTableUser(TableEnvironment tEnvs)
            throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_info (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score DECIMAL" +
                ") PARTITIONED BY ( order_id )" +
                " WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'use_cdc'='true'," +
                "    'path'='" + getTempDirUri("/lakeSource/user") +
                "' )";
        tEnvs.executeSql("DROP TABLE if exists user_info");
        tEnvs.executeSql(createUserSql);
    }

}
