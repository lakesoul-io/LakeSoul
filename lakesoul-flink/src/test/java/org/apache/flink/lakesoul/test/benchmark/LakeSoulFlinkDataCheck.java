// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.benchmark;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.assertj.core.api.Assertions;

import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.apache.flink.lakesoul.tool.JobOptions.FLINK_CHECKPOINT;
import static org.apache.flink.lakesoul.tool.JobOptions.JOB_CHECKPOINT_INTERVAL;

public class LakeSoulFlinkDataCheck {

    /**
     * param example:
     * --mysql.hostname localhost
     * --mysql.database.name test_cdc
     * --mysql.table.name default_init
     * --mysql.username root
     * --mysql.password root
     * --mysql.port 3306
     * --server.time.zone UTC
     * --single.table.contract true
     * --lakesoul.database.name lakesoul_test
     * --lakesoul.table.name lakesoul_table
     * --flink.checkpoint file:///tmp/chk
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        String mysqlHost = parameter.get("mysql.hostname");
        String mysqlDB = parameter.get("mysql.database.name");
        String mysqlTable = parameter.get("mysql.table.name", "");
        String mysqlUser = parameter.get("mysql.username");
        String mysqlPassword = parameter.get("mysql.password");
        int mysqlPort = parameter.getInt("mysql.port", 3306);
        String serverTimeZone = parameter.get("server.time.zone");
        boolean isSingleTable = parameter.getBoolean("single.table.contract");

        String lakeSoulDB = parameter.get("lakesoul.database.name", "");
        String lakeSoulTable = parameter.get("lakesoul.table.name", "");
        int checkpointInterval =
                parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(), JOB_CHECKPOINT_INTERVAL.defaultValue());
        String checkpointPath = parameter.get(FLINK_CHECKPOINT.key());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().setLocalTimeZone(TimeZone.getTimeZone(serverTimeZone).toZoneId());

        String mysqlUrl = String.format("jdbc:mysql://%s:%s", mysqlHost, mysqlPort);
        JdbcCatalog catalog = new JdbcCatalog("mysql", mysqlDB, mysqlUser, mysqlPassword, mysqlUrl);
        tEnvs.registerCatalog("mysql_catalog", catalog);

        Catalog lakeSoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakesoul", lakeSoulCatalog);
        tEnvs.useCatalog("lakesoul");

        if (isSingleTable) {
            checkDataBetweenLakeSoulAndMysql(tEnvs, mysqlDB, mysqlTable, lakeSoulDB, lakeSoulTable);
        } else {
            tEnvs.executeSql("use " + mysqlDB);
            CloseableIterator<Row> tableList = tEnvs.executeSql("show tables").collect();
            tableList.forEachRemaining(new Consumer<Row>() {
                @Override
                public void accept(Row row) {
                    String tableName = (String) row.getField(0);
                    checkDataBetweenLakeSoulAndMysql(tEnvs, mysqlDB, tableName);
                }
            });
        }
    }

    public static void checkDataBetweenLakeSoulAndMysql(StreamTableEnvironment tEnvs, String mysqlDatabase, String mysqlTable,
                                                        String lakeSoulDatabase, String lakeSoulTable) {
        String sql = "select * from `%s`.`%s`.`%s`";
        TableResult mysqlTableResult = tEnvs.executeSql(String.format(sql, "mysql_catalog", mysqlDatabase, mysqlTable));
//        mysqlTableResult.print();
        TableResult lakeSoulTableResult = tEnvs.executeSql(String.format(sql, "lakesoul", lakeSoulDatabase, lakeSoulTable));
//        lakeSoulTableResult.print();
        List<Row> rows = CollectionUtil.iteratorToList(mysqlTableResult.collect());
        List<Row> rows1 = CollectionUtil.iteratorToList(lakeSoulTableResult.collect());
        Assertions.assertThat(rows).isNotNull().containsExactlyInAnyOrderElementsOf(rows1);
        System.out.println("======= all data check are right! =======");
    }

    public static void checkDataBetweenLakeSoulAndMysql(StreamTableEnvironment tEnvs, String database, String tableName) {
        String sql = "select * from %s.%s.%s";
        TableResult mysqlTableResult = tEnvs.executeSql(String.format(sql, "mysql_catalog", database, tableName));
//        mysqlTableResult.print();
        TableResult lakeSoulTableResult = tEnvs.executeSql(String.format(sql, "lakesoul", database, tableName));
//        lakeSoulTableResult.print();
        List<Row> rows = CollectionUtil.iteratorToList(mysqlTableResult.collect());
        List<Row> rows1 = CollectionUtil.iteratorToList(lakeSoulTableResult.collect());
        Assertions.assertThat(rows).isNotNull().containsExactlyInAnyOrderElementsOf(rows1);
        System.out.println("======= all data check are right! =======");
    }
}
