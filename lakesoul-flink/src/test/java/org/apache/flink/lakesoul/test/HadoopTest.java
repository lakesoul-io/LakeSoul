// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.tool.JobOptions;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

public class HadoopTest {

    public static void main(String[] args) {
        StreamTableEnvironment tEnvs;
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
//        env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8181, "/Users/dudongfeng/.m2/repository/com/dmetasoul/lakesoul-flink/2.2.0-flink-1.14-SNAPSHOT/lakesoul-flink-2.2.0-flink-1.14-SNAPSHOT.jar");
//        env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8181, "/Users/dudongfeng/Desktop/lakesoul-flink-2.3.0-flink-1.14-SNAPSHOT.jar");
//        env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8181, "/Users/dudongfeng/Desktop/lakesoul-flink-2.3.0-flink-1.14-SNAPSHOT-new.jar");
        env.enableCheckpointing(2 * 1000);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().getConfiguration()
                .set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
                .set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true)
                .set(LakeSoulSinkOptions.USE_CDC, true);

        LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(true);
        LakeSoulTestUtils.registerLakeSoulCatalog(tEnvs, lakeSoulCatalog);


        tEnvs.executeSql("CREATE TABLE event (\n" +
                "    act_id       int       NOT NULL,\n" +
                "    id       int       NOT NULL,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"  +
                "WITH (" +
                String.format("'format'='lakesoul','path'='%s', 'hashBucketNum'='2', '%s'='false' ", "hdfs:///tmp/lakesoul/event",LakeSoulSinkOptions.USE_CDC.key()) +
                ")");

        tEnvs.executeSql("CREATE TABLE action (\n" +
                "    id       int       NOT NULL,\n" +
                "    score       int       NOT NULL,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"  +
                "WITH (" +
                String.format("'format'='lakesoul','path'='%s', '%s'='%s', 'hashBucketNum'='2', '%s'='false' ", "hdfs:///tmp/lakesoul/action", JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), 5 ,LakeSoulSinkOptions.USE_CDC.key()) +
                ")");


//        String testSql = String.format("select kind,sum(cost) from new_table group by kind ");
//        String testSql = String.format("select * from new_table t join new_table_1 m on t.name = m.name1");
//        tEnvs.executeSql("SELECT ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) AS revenue, o_entry_d FROM `customer`, `new_order`, `oorder`, `order_line` WHERE c_state LIKE 'A%' AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND no_w_id = o_w_id AND no_d_id = o_d_id AND no_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND o_entry_d > TIMESTAMP '2007-01-02 00:00:00.000000' GROUP BY ol_o_id, ol_w_id, ol_d_id, o_entry_d ORDER BY revenue DESC , o_entry_d").print();
        tEnvs.executeSql("show tables").print();


        String selectTable = "select " +
                "e.*, a.score " +
                "from " +
                "(SELECT *, proctime() as proctime FROM event ) e" +
                "   left join action FOR SYSTEM_TIME AS OF e.proctime a on e.act_id = a.id "
//                "   left join product FOR SYSTEM_TIME AS OF e.proctime p on e.pro_id = p.id "
                ;

//        Thread thread = new Thread(()-> {
        TableResult tableResult = tEnvs.executeSql(String.format("%s", selectTable));
//        });
//        thread.start();

        Thread thread = new Thread(()-> {
            TableEnvironment batchEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
            LakeSoulTestUtils.registerLakeSoulCatalog(batchEnv, lakeSoulCatalog);
            batchEnv.getConfig().getConfiguration()
                    .set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
                    .set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
//                    .set(LakeSoulSinkOptions.USE_CDC, true);
            try {
                batchEnv.executeSql("insert into action values(1, 1), (2,2)").await();
                Thread.sleep(6000);
                batchEnv.executeSql("insert into event values(11, 1), (22,2), (33,3)").await();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });


//        try {
//            thread.join(1000 * 120);
//        } catch (InterruptedException e) {
//            System.out.println("1000 * 120 mills passed");
//        } finally {
//        tableResult.print();
        thread.start();

        try {
            Thread.sleep(1000 * 120);
            thread.join();
        } catch (InterruptedException e) {

            System.out.println("canceling job client at " + Instant.now());

        } finally {

            tableResult.getJobClient().get().cancel();
            System.exit(0);
        }

//        }
    }
}
