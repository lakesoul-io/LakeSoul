package org.apache.flink.lakesoul.entry;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;

import java.util.concurrent.TimeUnit;

public class LakeSoulStreamTest {
    public static void main(String[] args) {
        StreamTableEnvironment tEnvs;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(10, TimeUnit.SECONDS)));
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1023);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flinkCHK");

        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");
        String testSql = "select id,score,name from user_big";
        tEnvs.executeSql(testSql).print();
//        tEnvs.executeSql(
//                "CREATE TABLE CsvSinkTable ("
//                        + "id INT,"
//                        + "score INT,"
//                        + "name STRING"
//                        + ") WITH ("
//                        + "'connector' = 'filesystem',"
//                        + "'path' = 'file:///tmp/csv/',"
//                        + "'format' = 'csv'"
//                        + ")"
//        );
//
//        Table res = tEnvs.sqlQuery(testSql);
//        tEnvs.executeSql("INSERT INTO CsvSinkTable SELECT * FROM "+res);
    }
}
