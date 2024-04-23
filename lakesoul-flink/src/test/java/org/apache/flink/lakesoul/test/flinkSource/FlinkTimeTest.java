// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.flinkSource;

import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.lakesoul.test.flinkSource.TestUtils.BATCH_TYPE;
import static org.apache.flink.lakesoul.test.flinkSource.TestUtils.STREAMING_TYPE;

public class FlinkTimeTest extends AbstractTestBase {

    @Test
    public void testWatermark() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        String createUserSql = "create table user_actions (" +
                "    id INT," +
                "    user_name STRING PRIMARY KEY NOT ENFORCED," +
                // watermark only support millisecond timestamp
                "  user_action_time TIMESTAMP(3), " +
                "  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '10' SECOND " +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='" + getTempDirUri("/LakeSource/user_actions") +
                "' )";
        tEnv.executeSql("DROP TABLE if exists user_actions");
        tEnv.executeSql(createUserSql);

        tEnv.executeSql("INSERT INTO user_actions VALUES (2, 'Alice', TO_TIMESTAMP('1999-01-01 12:09:59')), (3, 'Jack', TO_TIMESTAMP('1999-01-01 12:11:16')),(3, 'Amy', TO_TIMESTAMP('1999-01-01 12:10:00')),(4, 'Mike', TO_TIMESTAMP('1999-01-01 12:10:01'))").await();

        String querySql = "SELECT " +
                "TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), " +
                "COUNT(user_name), " +
                "COUNT(id)\n" +
                "FROM user_actions\n" +
                "GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);\n";
        tEnv.executeSql(querySql).print();
        List<Row> rows = CollectionUtil.iteratorToList(tEnv.executeSql(querySql).collect());
        TestUtils.checkEqualInAnyOrder(rows,
                new String[]{"+I[1999-01-01T12:00, 1, 1]", "+I[1999-01-01T12:10, 3, 3]"});

    }

    @Test
    public void testProcTime() throws ExecutionException, InterruptedException {
        TableEnvironment tEnv = TestUtils.createTableEnv(BATCH_TYPE);
        String createUserSql = "create table user_actions (" +
                "    score INT," +
                "    user_name STRING PRIMARY KEY NOT ENFORCED," +
                //   Declare an additional field as a processing-time attribute.
                "  user_action_time as proctime() " +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'discoveryinterval'='1000'," +
                "    'path'='" + getTempDirUri("/LakeSource/user_actions") +
                "' )";
        tEnv.executeSql("DROP TABLE if exists user_actions");
        tEnv.executeSql(createUserSql);


        String querySql = "SELECT " +
                "TUMBLE_START(user_action_time, INTERVAL '5' SECOND) as window_starter, " +
                "COUNT(user_name) as user_count, " +
                "SUM(score) as score_sum " +
                "FROM user_actions\n" +
                "GROUP BY TUMBLE(user_action_time, INTERVAL '5' SECOND);\n";

        StreamTableEnvironment sEnv = TestUtils.createStreamTableEnv(STREAMING_TYPE);
//        try {
//            sEnv.executeSql(querySql).await(10, TimeUnit.SECONDS);
//        } catch (TimeoutException e) {
//            throw new RuntimeException(e);
//        }

        // TODO: 2024/4/23 incorrect sum result
        LakeSoulTestUtils.checkStreamingQueryAnswer(
                sEnv,
                "user_actions",
                querySql,
                "    window_starter TIMESTAMP(3) NOT NULL," +
                        "    user_count BIGINT NOT NULL, " +
                        " score_sum INT",
                (s) -> {
                    try {

                        Thread.sleep(20000);
                        tEnv.executeSql("INSERT INTO user_actions VALUES (1, 'Bob'), (10, 'Alice')")
                                .await();
                        System.out.println("1st insert finished " + LocalDateTime.now());
                        Thread.sleep(10000);
                        tEnv.executeSql("INSERT INTO user_actions VALUES (100, 'Jack')").await();
                        System.out.println("2nd insert finished " + LocalDateTime.now());
                        Thread.sleep(10000);

                        tEnv.executeSql("INSERT INTO user_actions VALUES (1000, 'Jack'),(10000, 'Tom')")
                                .await();
                        System.out.println("3rd insert finished" + LocalDateTime.now());
                        Thread.sleep(10000);
                        tEnv.executeSql("INSERT INTO user_actions VALUES (100000, 'Tom')").await();
                        System.out.println("4th insert finished " + LocalDateTime.now());
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }

                },
                "",
                100L
        );
        tEnv.executeSql("select * from user_actions").print();
    }
}
