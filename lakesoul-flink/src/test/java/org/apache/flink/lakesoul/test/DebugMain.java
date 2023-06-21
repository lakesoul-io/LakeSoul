/*
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
 */
package org.apache.flink.lakesoul.test;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

/**
 * This is for local manual testing use.
 */
public class DebugMain {
    public static void main(String[] args) {
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        config.setString("s3.endpoint", "http://localhost:9000");
        config.setString("s3.access-key", "minioadmin1");
        config.setString("s3.secret-key", "minioadmin1");
        config.setString("s3.path.style.access", "true");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(2);
        env.enableCheckpointing(15000);
        env.getCheckpointConfig().setCheckpointStorage(AbstractTestBase.getTempDirUri("/flinkchk"));
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        FileSystem.initialize(config, null);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 2);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        LakeSoulCatalog lakeSoulCatalog = LakeSoulTestUtils.createLakeSoulCatalog(false);
        LakeSoulTestUtils.registerLakeSoulCatalog(tableEnv, lakeSoulCatalog);
        System.out.println(tableEnv.explainSql("select * from `test_cdc`.`mysql_test_1`",
                ExplainDetail.CHANGELOG_MODE));
        Table table = tableEnv.sqlQuery("select * from `test_cdc`.`mysql_test_1`");
        table.execute().print();
    }
}
