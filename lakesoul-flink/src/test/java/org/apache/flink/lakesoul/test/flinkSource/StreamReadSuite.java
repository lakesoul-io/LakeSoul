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

package org.apache.flink.lakesoul.test.flinkSource;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class StreamReadSuite {

    private String STREAMING_TYPE = "streaming";

    @Test
    public void testLakesoulSourceIncrementalStream() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv();
        createLakeSoulSourceStreamTestTable(createTableEnv);
        String testSql = String.format("select * from test_stream /*+ OPTIONS('readstarttime'='%s','readtype'='incremental','timezone'='Africa/Accra')*/",
                TestUtils.getCurrentDateTimeForIncremental());
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(STREAMING_TYPE);
        try {
            TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSql);
            flinkTable.execute().await(3l, TimeUnit.SECONDS);
            List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
            TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, Jack, 75]", "+I[2, Alice, 80]", "+I[1, Bob, 90]"});
        } catch (TimeoutException e) {
            System.out.println("Stream test IncrementalStream is done.");
        }
    }

    @Test
    public void testLakesoulSourceSelectMultiRangeAndHash() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv();
        createLakeSoulSourceMultiPartitionTable(createTableEnv);
        String testMultiRangeSelect = "select * from user_multi where `region`='UK' and score > 80";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(STREAMING_TYPE);
        try {
            TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testMultiRangeSelect);
            flinkTable.execute().await(3l, TimeUnit.SECONDS);
            List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
            TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, Amy, 95, 1995-10-10, UK]"});
        } catch (TimeoutException e) {
            System.out.println("Stream test SelectMultiRangeAndHash is done.");
        }
    }

    @Test
    public void testLakesoulSourceSelectWhere() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv();
        createLakeSoulSourceTableUser(createTableEnv);
        String testSelectWhere = "select * from user_info where order_id=3";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(STREAMING_TYPE);
        try {
            TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectWhere);
            flinkTable.execute().await(3l, TimeUnit.SECONDS);
            List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
            TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, Jack, 75]", "+I[3, Amy, 95]"});
        } catch (TimeoutException e) {
            System.out.println("Stream test SelectWhere is done.");
        }
    }

    @Test
    public void testLakesoulSourceSelectJoin() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv();
        createLakeSoulSourceTableUser(createTableEnv);
        createLakeSoulSourceTableOrder(createTableEnv);
        String testSelectJoin = "select ui.order_id,sum(oi.price) as total_price,count(*) as total " +
                "from user_info as ui inner join order_info as oi " +
                "on ui.order_id=oi.id group by ui.order_id having ui.order_id>2";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(STREAMING_TYPE);
        try {
            TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectJoin);
            flinkTable.execute().await(3l, TimeUnit.SECONDS);
            List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
            TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, 30.7, 2]", "+I[4, 25.24, 1]", "+I[5, 15.04, 1]"});
        } catch (TimeoutException e) {
            System.out.println("Stream test SelectJoin is done.");
        }
    }

    @Test
    public void testLakesoulSourceSelectDistinct() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv();
        createLakeSoulSourceTableUser(createTableEnv);
        String testSelectDistinct = "select distinct order_id from user_info where order_id<5";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(STREAMING_TYPE);
        try {
            TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSelectDistinct);
            flinkTable.execute().await(3l, TimeUnit.SECONDS);
            List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
            TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[1]", "+I[2]", "+I[3]", "+I[4]"});
        } catch (TimeoutException e) {
            System.out.println("Stream test SelectDistinct is done.");
        }
    }

    private void createLakeSoulSourceStreamTestTable(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createUserSql = "create table test_stream (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score INT" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='/tmp/lakeSource/test_stream' )";
        tEnvs.executeSql("DROP TABLE if exists test_stream");
        tEnvs.executeSql(createUserSql);
        tEnvs.executeSql("INSERT INTO test_stream VALUES (1, 'Bob', 90), (2, 'Alice', 80)").await();
        Thread.sleep(1000l);
        tEnvs.executeSql("INSERT INTO test_stream VALUES(3, 'Jack', 75)").await();
        Thread.sleep(1000l);
        tEnvs.executeSql("INSERT INTO test_stream VALUES (4, 'Jack', 95),(5, 'Tom', 75)").await();
    }

    private void createLakeSoulSourceTableUser(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_info (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score DECIMAL" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='/tmp/lakeSource/user' )";
        tEnvs.executeSql("DROP TABLE if exists user_info");
        tEnvs.executeSql(createUserSql);
        tEnvs.executeSql("INSERT INTO user_info VALUES (1, 'Bob', 90), (2, 'Alice', 80)").await();
        Thread.sleep(1000l);
        tEnvs.executeSql("INSERT INTO user_info VALUES (3, 'Jack', 75), (3, 'Amy', 95)").await();
        Thread.sleep(1000l);
        tEnvs.executeSql("INSERT INTO user_info VALUES (5, 'Tom', 75), (4, 'Mike', 70)").await();
    }

    private void createLakeSoulSourceTableOrder(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createOrderSql = "create table order_info (" +
                "    `id` INT PRIMARY KEY NOT ENFORCED," +
                "    price DOUBLE" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='/tmp/lakeSource/order' )";
        tEnvs.executeSql("DROP TABLE if exists order_info");
        tEnvs.executeSql(createOrderSql);
        tEnvs.executeSql("INSERT INTO order_info VALUES (1, 20.12), (2, 10.88)").await();
        Thread.sleep(1000l);
        tEnvs.executeSql("INSERT INTO order_info VALUES (3, 15.35)").await();
        Thread.sleep(1000l);
        tEnvs.executeSql("INSERT INTO order_info VALUES (4, 25.24), (5, 15.04)").await();
    }

    private void createLakeSoulSourceMultiPartitionTable(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createSql = "create table user_multi (" +
                "    `id` INT," +
                "    name STRING," +
                "    score INT," +
                "    `date` DATE," +
                "    region STRING," +
                "PRIMARY KEY (`id`,`name`) NOT ENFORCED" +
                ") " +
                "PARTITIONED BY (`region`,`date`)" +
                "WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='/tmp/lakeSource/multi_range_hash' )";
        tEnvs.executeSql("DROP TABLE if exists user_multi");
        tEnvs.executeSql(createSql);
        tEnvs.executeSql("INSERT INTO user_multi VALUES (1, 'Bob', 90, TO_DATE('1995-10-01'), 'China'), (2, 'Alice', 80, TO_DATE('1995-10-10'), 'China')").await();
        Thread.sleep(1000l);
        tEnvs.executeSql("INSERT INTO user_multi VALUES (3, 'Jack', 75,  TO_DATE('1995-10-15'), 'China')").await();
        Thread.sleep(1000l);
        tEnvs.executeSql("INSERT INTO user_multi VALUES (3, 'Amy', 95,  TO_DATE('1995-10-10'),'UK'), (4, 'Mike', 70, TO_DATE('1995-10-15'), 'UK')").await();
    }

}
