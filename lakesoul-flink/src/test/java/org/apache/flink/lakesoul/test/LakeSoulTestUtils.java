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

package org.apache.flink.lakesoul.test;

import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

public class LakeSoulTestUtils {

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
        tableEnv.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tableEnv.getConfig().setSqlDialect(dialect);
        return tableEnv;
    }

    public static StreamTableEnvironment createTableEnvInStreamingMode(StreamExecutionEnvironment env) {
        return createTableEnvInStreamingMode(env, SqlDialect.DEFAULT);
    }

    public static StreamExecutionEnvironment createStreamExecutionEnvironment() {
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        return env;
    }

    public static StreamTableEnvironment createTableEnvInStreamingMode(StreamExecutionEnvironment env, SqlDialect dialect) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);
        tableEnv.getConfig().setSqlDialect(dialect);
        return tableEnv;
    }

    public static TableEnvironment createTableEnvWithLakeSoulCatalog(LakeSoulCatalog catalog) {
        TableEnvironment tableEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(catalog.getName(), catalog);
        tableEnv.useCatalog(catalog.getName());
        return tableEnv;
    }

}
