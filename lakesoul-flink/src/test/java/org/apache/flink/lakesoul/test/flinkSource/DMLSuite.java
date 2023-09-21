// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.flinkSource;

import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class DMLSuite extends AbstractTestBase {
    private final String BATCH_TYPE = "batch";

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
                new String[]{"+I[2, Alice, 80]", "+I[3, Amy, 95]", "+I[3, Jack, 75]", "+I[4, John, 70]","+I[4, Mike, 70]"});
    }
    @Test
    public void testUpdatePartitionSQLNotSupported() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUserWithRange(tEnv);
        tEnv.executeSql("INSERT INTO user_info_1 VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95),(4, 'Mike', 70)")
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
                new String[]{"+I[2, Alice, 80]", "+I[1, Jack, 75]", "+I[3, Jack, 75]", "+I[3, Amy, 95]", "+I[4, Mike, 70]"});
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
        tEnv.executeSql("INSERT INTO user_info VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95)").await();
        try {
            tEnv.executeSql("DELETE FROM user_info where name = 'Jack'").await();
        } catch (Throwable e) {
            System.out.println("Unsupported DELETE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]", "+I[3, Amy, 95]"});
    }

    @Test
    public void testDeleteCDCPkSQL() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulCDCSourceTableUser(tEnv);
        tEnv.executeSql("INSERT INTO user_info VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95)").await();
        try {
            tEnv.executeSql("DELETE FROM user_info where name = 'Jack'").await();
        } catch (Throwable e) {
            System.out.println("Unsupported DELETE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]", "+I[3, Amy, 95]"});
    }

    @Test
    public void testDeletePartitionSQLNotSupported() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUserWithRange(tEnv);
        tEnv.executeSql("INSERT INTO user_info_1 VALUES (2, 'Alice', 80),(3, 'Jack', 75),(3, 'Amy', 95)").await();
        try {
            tEnv.executeSql("DELETE FROM user_info_1 where order_id = 3").await();
        } catch (Throwable e) {
            System.out.println("Unsupported DELETE SQL");
        }
        StreamTableEnvironment streamEnv = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String testSelect = "select * from user_info_1";
        TableImpl flinkTable = (TableImpl) streamEnv.sqlQuery(testSelect);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[2, Alice, 80]","+I[3, Jack, 75]","+I[3, Amy, 95]"});
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

    private void createLakeSoulSourceTableUserWithRange(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
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
    private void createLakeSoulCDCSourceTableUser(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
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

}
