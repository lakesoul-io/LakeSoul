// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.flinkSource;

import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.lakesoul.test.flinkSource.TestUtils.BATCH_TYPE;
import static org.apache.flink.lakesoul.test.flinkSource.TestUtils.STREAMING_TYPE;

public class DDLSuite extends AbstractTestBase {

    @Test
    public void dropTable() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUser(tEnv);
        tEnv.executeSql("SHOW TABLES");
        tEnv.executeSql("DROP TABLE if exists user_info");
        tEnv.executeSql("SHOW TABLES");
    }

    @Test
    public void dropView() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUser(tEnv);
        createLakeSoulSourceTableViewUser(tEnv);
        tEnv.executeSql("SHOW VIEWS");
        tEnv.executeSql("DROP VIEW user_info_view");
        tEnv.executeSql("SHOW VIEWS");
    }

    @Test
    public void dropTablePartition() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createDropPartitionTable(tEnv);
        tEnv.executeSql(
                        "INSERT INTO user_info VALUES" +
                                "(1, 'Bob', 90, TO_DATE('1995-10-01'))," +
                                "(2, 'Alice', 80, TO_DATE('1995-10-01')), " +
                                "(3, 'Jack', 75,  TO_DATE('1995-10-15'))," +
                                "(3, 'Amy', 95,  TO_DATE('1995-10-10')), " +
                                "(5, 'Tom', 75,  TO_DATE('1995-10-01'))," +
                                "(4, 'Mike', 70, TO_DATE('1995-10-02'))")
                .await();
        List<Row> results = CollectionUtil.iteratorToList(
                tEnv.executeSql("select * from user_info").collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{
                "+I[3, Amy, 95.0, 1995-10-10]",
                "+I[1, Bob, 90.0, 1995-10-01]",
                "+I[2, Alice, 80.0, 1995-10-01]",
                "+I[5, Tom, 75.0, 1995-10-01]",
                "+I[4, Mike, 70.0, 1995-10-02]",
                "+I[3, Jack, 75.0, 1995-10-15]"});
        tEnv.executeSql("alter table user_info drop partition (`date`='1995-10-01')");
        results = CollectionUtil.iteratorToList(
            tEnv.executeSql("select * from user_info").collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{
                "+I[3, Amy, 95.0, 1995-10-10]",
                "+I[4, Mike, 70.0, 1995-10-02]",
                "+I[3, Jack, 75.0, 1995-10-15]"});
        tEnv.executeSql("drop table if exists user_info").await();
    }

    @Test
    public void alterTableNotSupported() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUser(tEnv);
        try {
            tEnv.executeSql("ALTER TABLE user_info RENAME TO NewUsers");
        } catch (CatalogException e) {
            System.out.println("Rename lakesoul table not supported now");
        }
    }

    @Test
    public void describeTable() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUser(tEnv);
        tEnv.executeSql("DESC user_info");
        tEnv.executeSql("DESCRIBE user_info");
    }

    @Test
    public void explainTable() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableUser(tEnv);
        String explaination = tEnv.explainSql("SELECT * FROM user_info WHERE order_id > 3");
        System.out.println(explaination);
    }

    @Test
    public void loadLakeSoulModuleNotSupported() {
        StreamTableEnvironment streamTableEnv = TestUtils.createStreamTableEnv(STREAMING_TYPE);
        try {
            streamTableEnv.executeSql("LOAD MODULE lakesoul WITH ('format'='lakesoul')");
        } catch (ValidationException e) {
            System.out.println("LOAD lakesoul module not supported now");
        }
    }

    @Test
    public void unloadModuleTest() {
        StreamTableEnvironment streamTableEnv = TestUtils.createStreamTableEnv(STREAMING_TYPE);
        try {
            streamTableEnv.executeSql("UNLOAD MODULE core");
            streamTableEnv.executeSql("SHOW MODULES");
        } catch (ValidationException e) {
            System.out.println("UNLOAD lakesoul module not supported now");
        }
    }

    @Test
    public void timeTypeTest() throws ExecutionException, InterruptedException {
        TableEnvironment streamTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        String createUserSql = "create table time_test_table (" +
                "    t1 TIME," +
                "    t2 TIME(3)," +
                "    t3 TIME(6)," +
                "    t4 TIME(9)" +
                ") WITH (" +
                "    'connector'='lakesoul'," +
                "    'path'='" + getTempDirUri("/lakeSource/time_test") +
                "' )";
        streamTableEnv.executeSql(createUserSql);
        streamTableEnv.executeSql("INSERT INTO time_test_table VALUES " +
                "(" +
                "time '10:10:10.999', " +
                "time '10:10:11.999', " +
                "time '10:10:12.999', " +
                "time '10:10:13.999' " +
                ")").await();
        List<Row> results = CollectionUtil.iteratorToList(
                streamTableEnv.executeSql("select * from time_test_table").collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{
                "+I[10:10:10, 10:10:11, 10:10:12, 10:10:13]"});
        streamTableEnv.executeSql("DROP TABLE if exists time_test_table");
    }

    @Test
    public void primaryKeyAndPartitionKeyOverlapTest() {
        StreamTableEnvironment streamTableEnv = TestUtils.createStreamTableEnv(STREAMING_TYPE);
        Assert.assertThrows("cannot overlap", TableException.class, () -> {
            streamTableEnv.executeSql("drop table if exists test_table");
            streamTableEnv.executeSql(
                    "create table test_table (id int, primary key(id) not enforced) partitioned by (id) with " +
                            "('connector'='lakesoul'," +
                            "'hashBucketNum'='2')");
        });
    }

    @Test
    public void typeValidationTest() {
        validateTablePKType("TIMESTAMP(9)", true);
        validateTablePKType("TIMESTAMP(9)", false);
        validateTablePKType("ARRAY<STRING>", true);
        validateTablePKType("MAP<STRING, STRING>", true);
        validateTablePKType("ROW<n0 STRING, n1 STRING>", true);
    }

    private void validateTablePKType(String type, boolean isPK) {
        StreamTableEnvironment streamTableEnv = TestUtils.createStreamTableEnv(STREAMING_TYPE);
        Assert.assertThrows("type validation failed", TableException.class, () -> {
            streamTableEnv.executeSql("drop table if exists test_table");
            streamTableEnv.executeSql("create table test_table (\n" +
                    String.format("  `pk` %s %s \n", type, isPK ? "PRIMARY KEY NOT ENFORCED" : "") +
                    ")\n" +
                    "with (\n" +
                    "  'connector' = 'lakesoul',\n" +
                    "  'path' = 'file:///tmp/test_table',\n" +
                    "  'hashBucketNum' = '1'\n" +
                    ");"
            );
        });
    }

    private void createLakeSoulSourceTableUser(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_info (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score FLOAT" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='" + getTempDirUri("/lakeSource/user") +
                "' )";
        tEnvs.executeSql("DROP TABLE if exists user_info");
        tEnvs.executeSql(createUserSql);
    }

    private void createLakeSoulSourceTableViewUser(TableEnvironment tEnvs)
            throws ExecutionException, InterruptedException {
        String createUserSql = "create view if not exists user_info_view as select * from user_info";
        tEnvs.executeSql("DROP view if exists user_info_view");
        tEnvs.executeSql(createUserSql);
    }

    private void createDropPartitionTable(TableEnvironment tEnvs) {
        String createUserSql = "create table user_info (" +
                "    order_id INT PRIMARY KEY NOT ENFORCED, " +
                "    name STRING, " +
                "    score FLOAT, " +
                "   `date` DATE" +
                ") PARTITIONED BY (`date`) WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='" + getTempDirUri("/lakeSource/user") +
                "' )";
        tEnvs.executeSql("DROP TABLE if exists user_info");
        tEnvs.executeSql(createUserSql);
    }
}
