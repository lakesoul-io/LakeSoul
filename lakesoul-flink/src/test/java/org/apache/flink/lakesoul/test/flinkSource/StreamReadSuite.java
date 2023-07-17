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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class StreamReadSuite extends AbstractTestBase {

    private final List<Tuple2<Integer, Integer>> BUCKET_NUM_AND_PARALLELISM = Arrays.asList(
            new Tuple2<>(3, 2),
            new Tuple2<>(2, 3)
    );

    @Test
    public void testLakesoulSourceIncrementalStream() {
        for (Tuple2<Integer, Integer> tup : BUCKET_NUM_AND_PARALLELISM) {
            int hashBucketNum = tup.f0;
            int parallelism = tup.f1;
            System.out.println(
                    "testLakesoulSourceIncrementalStream with hashBucketNum=" + hashBucketNum + ", parallelism=" +
                            parallelism);
            TableEnvironment createTableEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
            LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);
            LakeSoulTestUtils.registerLakeSoulCatalog(createTableEnv, lakeSoulCatalog);

            String createUserSql = "create table test_stream (" +
                    "    order_id INT," +
                    "    name STRING PRIMARY KEY NOT ENFORCED," +
                    "    score INT" +
                    ") WITH (" +
                    "    'connector'='lakesoul'," +
                    String.format("    'hashBucketNum'='%d',", hashBucketNum) +
                    "    'path'='" + getTempDirUri("/lakeSource/test_stream") +
                    "' )";
            createTableEnv.executeSql("DROP TABLE if exists test_stream");
            createTableEnv.executeSql(createUserSql);

            String testSql = String.format(
                    "select * from test_stream /*+ OPTIONS('readstarttime'='%s','readtype'='incremental'," +
                            "'timezone'='Africa/Accra', 'discoveryinterval'='1000')*/",
                    TestUtils.getDateTimeFromTimestamp(Instant.ofEpochMilli(System.currentTimeMillis())));

            StreamTableEnvironment tEnvs = LakeSoulTestUtils.createTableEnvInStreamingMode(
                    LakeSoulTestUtils.createStreamExecutionEnvironment(parallelism, 1000L, 1000L), parallelism);
            LakeSoulTestUtils.registerLakeSoulCatalog(tEnvs, lakeSoulCatalog);
            LakeSoulTestUtils.checkStreamingQueryAnswer(
                    tEnvs,
                    "test_stream",
                    testSql,
                    "    order_id INT," +
                            "    name STRING PRIMARY KEY NOT ENFORCED," +
                            "    score INT",
                    (s) -> {
                        try {
                            createTableEnv.executeSql("INSERT INTO test_stream VALUES (1, 'Bob', 90), (2, 'Alice', 80)")
                                    .await();
                            createTableEnv.executeSql("INSERT INTO test_stream VALUES (3, 'Jack', 75)").await();
                            createTableEnv.executeSql("INSERT INTO test_stream VALUES (4, 'Jack', 95),(5, 'Tom', 75)")
                                    .await();
                            createTableEnv.executeSql("INSERT INTO test_stream VALUES (6, 'Tom', 100)").await();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }

                    },
                    "[+I[1, Bob, 90], +I[2, Alice, 80], +I[4, Jack, 95], +I[6, Tom, 100]]",
                    20L
            );
        }
    }

    @Test
    public void testLakesoulSourceSelectMultiRangeAndHash() {
        for (Tuple2<Integer, Integer> tup : BUCKET_NUM_AND_PARALLELISM) {
            int hashBucketNum = tup.f0;
            int parallelism = tup.f1;
            System.out.println(
                    "testLakesoulSourceSelectMultiRangeAndHash with hashBucketNum=" + hashBucketNum + ", parallelism=" +
                            parallelism);
            TableEnvironment createTableEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
            LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);
            LakeSoulTestUtils.registerLakeSoulCatalog(createTableEnv, lakeSoulCatalog);

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
                    "    'connector'='lakesoul'," +
                    String.format("    'hashBucketNum'='%d',", hashBucketNum) +
                    "    'path'='" + getTempDirUri("/lakeSource/multi_range_hash") +
                    "' )";
            createTableEnv.executeSql("DROP TABLE if exists user_multi");
            createTableEnv.executeSql(createSql);

            String testMultiRangeSelect = "select * from user_multi" +
                    " /*+ OPTIONS('discoveryinterval'='1000')*/ " +
                    "where `region`='UK' and score > 80";


            StreamTableEnvironment tEnvs = LakeSoulTestUtils.createTableEnvInStreamingMode(
                    LakeSoulTestUtils.createStreamExecutionEnvironment(parallelism, 1000L, 1000L), parallelism);
            LakeSoulTestUtils.registerLakeSoulCatalog(tEnvs, lakeSoulCatalog);
            LakeSoulTestUtils.checkStreamingQueryAnswer(
                    tEnvs,
                    "user_multi",
                    testMultiRangeSelect,
                    "    `id` INT," +
                            "    name STRING," +
                            "    score INT," +
                            "    `date` DATE," +
                            "    region STRING," +
                            "PRIMARY KEY (`id`,`name`) NOT ENFORCED",
                    (s) -> {
                        try {
                            createTableEnv.executeSql(
                                            "INSERT INTO user_multi VALUES (1, 'Bob', 90, TO_DATE('1995-10-01'), 'China'), (2, 'Alice', 80, TO_DATE('1995-10-10'), 'China')")
                                    .await();
                            createTableEnv.executeSql(
                                            "INSERT INTO user_multi VALUES (3, 'Jack', 75,  TO_DATE('1995-10-15'), 'China')")
                                    .await();
                            createTableEnv.executeSql(
                                            "INSERT INTO user_multi VALUES (3, 'Amy', 95,  TO_DATE('1995-10-10'),'UK'), (4, 'Mike', 70, TO_DATE('1995-10-15'), 'UK')")
                                    .await();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }

                    },
                    "[+I[1, Bob, 90, 1995-10-01, UK], +I[3, Amy, 95, 1995-10-10, UK]]",
                    20L
            );
        }
    }

    @Test
    public void testLakesoulSourceSelectWhere() {
        for (Tuple2<Integer, Integer> tup : BUCKET_NUM_AND_PARALLELISM) {
            int hashBucketNum = tup.f0;
            int parallelism = tup.f1;
            System.out.println("testLakesoulSourceSelectWhere with hashBucketNum=" + hashBucketNum + ", parallelism=" +
                    parallelism);
            TableEnvironment createTableEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
            LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);
            LakeSoulTestUtils.registerLakeSoulCatalog(createTableEnv, lakeSoulCatalog);

            String createUserSql = "create table user_info (" +
                    "    order_id INT," +
                    "    name STRING PRIMARY KEY NOT ENFORCED," +
                    "    score DECIMAL" +
                    ") WITH (" +
                    "    'connector'='lakesoul'," +
                    String.format("    'hashBucketNum'='%d',", hashBucketNum) +
                    "    'path'='" + getTempDirUri("/lakeSource/user_info") +
                    "' )";
            createTableEnv.executeSql("DROP TABLE if exists user_info");
            createTableEnv.executeSql(createUserSql);

            String testSelectWhere = "select * from user_info" +
                    " /*+ OPTIONS('discoveryinterval'='1000')*/ " +
                    " where order_id=3";


            StreamTableEnvironment tEnvs = LakeSoulTestUtils.createTableEnvInStreamingMode(
                    LakeSoulTestUtils.createStreamExecutionEnvironment(parallelism, 1000L, 1000L), parallelism);
            LakeSoulTestUtils.registerLakeSoulCatalog(tEnvs, lakeSoulCatalog);
            LakeSoulTestUtils.checkStreamingQueryAnswer(
                    tEnvs,
                    "user_info",
                    testSelectWhere,
                    "    order_id INT," +
                            "    name STRING PRIMARY KEY NOT ENFORCED," +
                            "    score DECIMAL",
                    (s) -> {
                        try {
                            createTableEnv.executeSql("INSERT INTO user_info VALUES (1, 'Bob', 90), (2, 'Alice', 80)")
                                    .await();
                            createTableEnv.executeSql("INSERT INTO user_info VALUES (3, 'Jack', 75), (3, 'Amy', 95)")
                                    .await();
                            createTableEnv.executeSql("INSERT INTO user_info VALUES (5, 'Tom', 75), (4, 'Mike', 70)")
                                    .await();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }

                    },
                    "[+I[3, Amy, 95], +I[3, Jack, 75]]",
                    20L
            );
        }
    }

    @Test
    public void testLakesoulSourceSelectJoin() {
        for (Tuple2<Integer, Integer> tup : BUCKET_NUM_AND_PARALLELISM) {
            int hashBucketNum = tup.f0;
            int parallelism = tup.f1;
            System.out.println("testLakesoulSourceSelectJoin with hashBucketNum=" + hashBucketNum + ", parallelism=" +
                    parallelism);
            TableEnvironment createTableEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
            LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);
            LakeSoulTestUtils.registerLakeSoulCatalog(createTableEnv, lakeSoulCatalog);

            String createUserSql = "create table user_info2 (" +
                    "    order_id INT," +
                    "    name STRING PRIMARY KEY NOT ENFORCED," +
                    "    score DECIMAL" +
                    ") WITH (" +
                    "    'connector'='lakesoul'," +
                    String.format("    'hashBucketNum'='%d',", hashBucketNum) +
                    "    'path'='" + getTempDirUri("/lakeSource/user_info2") +
                    "' )";
            createTableEnv.executeSql("DROP TABLE if exists user_info2");
            createTableEnv.executeSql(createUserSql);

            String createOrderSql = "create table order_info (" +
                    "    `id` INT PRIMARY KEY NOT ENFORCED," +
                    "    price DOUBLE" +
                    ") WITH (" +
                    "    'connector'='lakesoul'," +
                    String.format("    'hashBucketNum'='%d',", hashBucketNum) +
                    "    'path'='" + getTempDirUri("/lakeSource/order") +
                    "' )";
            createTableEnv.executeSql("DROP TABLE if exists order_info");
            createTableEnv.executeSql(createOrderSql);

            String testSelectJoin = "select ui.order_id,sum(oi.price) as total_price,count(*) as total " +
                    "from user_info2" +
                    " /*+ OPTIONS('discoveryinterval'='1000')*/ " +
                    " as ui inner join " +
                    "order_info " +
                    " /*+ OPTIONS('discoveryinterval'='1000')*/ " +
                    "as oi " +
                    "on ui.order_id=oi.id group by ui.order_id having ui.order_id>2";


            StreamTableEnvironment tEnvs = LakeSoulTestUtils.createTableEnvInStreamingMode(
                    LakeSoulTestUtils.createStreamExecutionEnvironment(parallelism, 1000L, 1000L), parallelism);
            LakeSoulTestUtils.registerLakeSoulCatalog(tEnvs, lakeSoulCatalog);
            LakeSoulTestUtils.checkStreamingQueryAnswer(
                    tEnvs,
                    "multi_join",
                    testSelectJoin,
                    "    order_id INT," +
                            "    total_price DOUBLE," +
                            "    total BIGINT NOT NULL",
                    (s) -> {
                        try {
                            createTableEnv.executeSql("INSERT INTO user_info2 VALUES (1, 'Bob', 90), (2, 'Alice', 80)")
                                    .await();
                            createTableEnv.executeSql("INSERT INTO order_info VALUES (1, 20.12), (2, 10.88)").await();

                            createTableEnv.executeSql("INSERT INTO user_info2 VALUES (3, 'Jack', 75), (3, 'Amy', 95)")
                                    .await();
                            createTableEnv.executeSql("INSERT INTO order_info VALUES (3, 15.35)").await();

                            createTableEnv.executeSql("INSERT INTO user_info2 VALUES (5, 'Tom', 75), (4, 'Mike', 70)")
                                    .await();
                            createTableEnv.executeSql("INSERT INTO order_info VALUES (4, 25.24), (5, 15.04)").await();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }

                    },
                    "[+I[3, 30.7, 2], +I[4, 25.24, 1], +I[5, 15.04, 1]]",
                    20L
            );
        }
    }

    @Test
    public void testLakesoulSourceSelectDistinct() {
        for (Tuple2<Integer, Integer> tup : BUCKET_NUM_AND_PARALLELISM) {
            int hashBucketNum = tup.f0;
            int parallelism = tup.f1;
            System.out.println("testLakesoulSourceSelectJoin with hashBucketNum=" + hashBucketNum + ", parallelism=" +
                    parallelism);
            TableEnvironment createTableEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
            LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);
            LakeSoulTestUtils.registerLakeSoulCatalog(createTableEnv, lakeSoulCatalog);

            String createUserSql = "create table user_info3 (" +
                    "    order_id INT," +
                    "    name STRING PRIMARY KEY NOT ENFORCED," +
                    "    score DECIMAL" +
                    ") WITH (" +
                    "    'connector'='lakesoul'," +
                    String.format("    'hashBucketNum'='%d',", hashBucketNum) +
                    "    'path'='" + getTempDirUri("/lakeSource/user_info3") +
                    "' )";
            createTableEnv.executeSql("DROP TABLE if exists user_info3");
            createTableEnv.executeSql(createUserSql);

            String testSelectDistinct = "select distinct order_id from user_info3" +
                    " /*+ OPTIONS('discoveryinterval'='1000')*/ " +
                    " where order_id<5";


            StreamTableEnvironment tEnvs = LakeSoulTestUtils.createTableEnvInStreamingMode(
                    LakeSoulTestUtils.createStreamExecutionEnvironment(parallelism, 1000L, 1000L), parallelism);
            LakeSoulTestUtils.registerLakeSoulCatalog(tEnvs, lakeSoulCatalog);
            LakeSoulTestUtils.checkStreamingQueryAnswer(
                    tEnvs,
                    "user_info3",
                    testSelectDistinct,
                    "dist INT",
                    (s) -> {
                        try {
                            createTableEnv.executeSql("INSERT INTO user_info3 VALUES (1, 'Bob', 90), (2, 'Alice', 80)")
                                    .await();
                            createTableEnv.executeSql("INSERT INTO user_info3 VALUES (3, 'Jack', 75), (3, 'Amy', 95)")
                                    .await();
                            createTableEnv.executeSql("INSERT INTO user_info3 VALUES (5, 'Tom', 75), (4, 'Mike', 70)")
                                    .await();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }

                    },
                    "[+I[1], +I[2], +I[3], +I[4]]",
                    20L
            );

        }
    }
}
