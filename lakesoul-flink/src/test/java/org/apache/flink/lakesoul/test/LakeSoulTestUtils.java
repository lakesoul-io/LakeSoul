// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

public class LakeSoulTestUtils {

    public static void registerLakeSoulCatalog(TableEnvironment env, LakeSoulCatalog catalog) {
        env.registerCatalog(catalog.getName(), catalog);
        env.useCatalog(catalog.getName());
    }

    public static LakeSoulCatalog createLakeSoulCatalog() {
        return createLakeSoulCatalog(false);
    }

    public static LakeSoulCatalog createLakeSoulCatalog(boolean cleanAll) {
        LakeSoulCatalog lakeSoulCatalog = new LakeSoulCatalog();
        if (cleanAll) lakeSoulCatalog.cleanForTest();
        return lakeSoulCatalog;
    }

    public static TableEnvironment createTableEnvInBatchMode() {
        return createTableEnvInBatchMode(SqlDialect.DEFAULT);
    }

    public static TableEnvironment createTableEnvInBatchMode(SqlDialect dialect) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tableEnv.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        tableEnv.getConfig().setSqlDialect(dialect);
        return tableEnv;
    }

    public static StreamTableEnvironment createTableEnvInStreamingMode(StreamExecutionEnvironment env) {
        return createTableEnvInStreamingMode(env, SqlDialect.DEFAULT);
    }

    public static StreamTableEnvironment createTableEnvInStreamingMode(StreamExecutionEnvironment env,
                                                                       int parallelism) {
        return createTableEnvInStreamingMode(env, SqlDialect.DEFAULT, parallelism);
    }

    public static StreamExecutionEnvironment createStreamExecutionEnvironment() {
        return createStreamExecutionEnvironment(2, 5000, 5000);
    }

    public static StreamExecutionEnvironment createStreamExecutionEnvironment(int parallelism, long checkpointInterval, long checkpointTimeout) {
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointStorage(AbstractTestBase.getTempDirUri("/flinkchk"));
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
        env.getCheckpointConfig().configure(config);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));
        return env;
    }

    public static StreamTableEnvironment createTableEnvInStreamingMode(StreamExecutionEnvironment env,
                                                                       SqlDialect dialect) {
        return createTableEnvInStreamingMode(env, dialect, 2);
    }

    public static StreamTableEnvironment createTableEnvInStreamingMode(StreamExecutionEnvironment env,
                                                                       SqlDialect dialect, int parallelism) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), parallelism);
        tableEnv.getConfig().setSqlDialect(dialect);
        return tableEnv;
    }

    public static TableEnvironment createTableEnvWithLakeSoulCatalog(LakeSoulCatalog catalog) {
        TableEnvironment tableEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(catalog.getName(), catalog);
        tableEnv.useCatalog(catalog.getName());
        return tableEnv;
    }

    public static void checkStreamingQueryAnswer(StreamTableEnvironment tableEnv, String sourceTable, String querySql,
                                                 String schemaString,
                                                 Consumer<String> f, String expectedAnswer, long timeout) {
        tableEnv.executeSql(
                String.format("DROP TABLE IF EXISTS default_catalog.default_database.%s_sink", sourceTable));
        tableEnv.executeSql(String.format("CREATE TABLE default_catalog.default_database.%s_sink(", sourceTable) +
                schemaString +
                ")WITH (" +
                "'connector' = 'values', 'sink-insert-only' = 'false'" +
                ")");
        TestValuesTableFactory.clearAllData();
        final TableResult execute =
                tableEnv.executeSql(
                        String.format("INSERT INTO default_catalog.default_database.%s_sink ", sourceTable) + querySql);
        Thread thread = new Thread(() -> {
            try {
                execute.await(timeout, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            } catch (TimeoutException e) {

            } finally {
                execute.getJobClient().get().cancel();
            }
        });
        thread.start();
        f.accept("");
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<String> results = TestValuesTableFactory.getResults(String.format("%s_sink", sourceTable));
        results.sort(Comparator.comparing(
                row -> Integer.valueOf(row.substring(3, (row.contains(",")) ? row.indexOf(",") : row.length() - 1))));
        assertThat(results.toString()).isEqualTo(expectedAnswer);

    }

}
