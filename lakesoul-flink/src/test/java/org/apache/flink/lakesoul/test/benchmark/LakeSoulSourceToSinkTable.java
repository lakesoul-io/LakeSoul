// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.benchmark;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;

import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.lakesoul.tool.JobOptions.FLINK_CHECKPOINT;
import static org.apache.flink.lakesoul.tool.JobOptions.JOB_CHECKPOINT_INTERVAL;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;

public class LakeSoulSourceToSinkTable {

    /**
     * param example:
     * --source.database.name test_cdc
     * --source.table.name default_init
     * --sink.database.name flink_sink
     * --sink.table.name default_init
     * --use.cdc true
     * --hash.bucket.number 2
     * --job.checkpoint_interval 10000
     * --server_time_zone UTC
     * --warehouse.path /tmp/data
     * --flink.checkpoint /tmp/chk
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        String sourceDBName = parameter.get("source.database.name");
        String sourceTableName = parameter.get("source.table.name");
        String sinkDBName = parameter.get("sink.database.name");
        String sinkTableName = parameter.get("sink.table.name");
        int hashBucketNum = parameter.getInt("hash.bucket.number");
        int checkpointInterval =
                parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(), JOB_CHECKPOINT_INTERVAL.defaultValue());
        String timeZone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());
        String warehousePath = parameter.get("warehouse.path");
        String checkpointPath = parameter.get(FLINK_CHECKPOINT.key());
        boolean useCDC = parameter.getBoolean("use.cdc");

        Configuration configuration = new Configuration();
        configuration.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
        env.setParallelism(hashBucketNum);
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().setLocalTimeZone(TimeZone.getTimeZone(timeZone).toZoneId());

        Catalog lakeSoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakesoul", lakeSoulCatalog);
        tEnvs.useCatalog("lakesoul");
        tEnvs.executeSql("create database if not exists " + sinkDBName);

        String createTableSql;
        if (useCDC) {
            createTableSql =
                    "create table %s.%s " + "with (" + "   'connector'='lakesoul'," + "   'hashBucketNum'='%s'," +
                            "   'use_cdc'='%s'," + "   'path'='%s'" + ") like %s.%s";
            tEnvs.executeSql(String.format("drop table if exists %s.%s", sinkDBName, sinkTableName));
            tEnvs.executeSql(
                    String.format(createTableSql, sinkDBName, sinkTableName, hashBucketNum, "true", warehousePath,
                            sourceDBName, sourceTableName));
        } else {
            createTableSql = "create table %s.%s " + "with (" + "   'format'='lakesoul'," + "   'use_cdc'='%s'," +
                    "   'path'='%s'" + ") like %s.%s";
            tEnvs.executeSql(String.format("drop table if exists %s.%s", sinkDBName, sinkTableName));
            tEnvs.executeSql(
                    String.format(createTableSql, sinkDBName, sinkTableName, hashBucketNum, "false", warehousePath,
                            sourceDBName, sourceTableName));
        }

        String writeSql = "INSERT INTO %s.%s SELECT * from %s.%s";
        String sql = String.format(writeSql, sinkDBName, sinkTableName, sourceDBName, sourceTableName);
        System.out.println(tEnvs.explainSql(sql, ExplainDetail.CHANGELOG_MODE, ExplainDetail.JSON_EXECUTION_PLAN));
        tEnvs.executeSql(sql); // we should not await here to not block the process exiting
    }
}
