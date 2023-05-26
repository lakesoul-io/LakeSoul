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

package org.apache.flink.lakesoul.entry.sql.flink;


import org.apache.flink.lakesoul.entry.sql.common.FlinkOption;
import org.apache.flink.lakesoul.entry.sql.common.JobType;
import org.apache.flink.lakesoul.entry.sql.common.SubmitOption;
import org.apache.flink.lakesoul.entry.sql.Submitter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.flink.lakesoul.entry.sql.utils.FileUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.lakesoul.entry.sql.utils.SqlSplitter;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkSqlSubmitter extends Submitter {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlSubmitter.class);

    public FlinkSqlSubmitter(SubmitOption submitOption) {
        super(submitOption);

    }

    @Override
    public void submit() throws IOException, URISyntaxException {
        EnvironmentSettings settings = null;
        TableEnvironment tEnv = null;
        if (submitOption.getJobType().equals(JobType.STREAM.getType())) {
            settings = EnvironmentSettings
                    .newInstance()
                    .inStreamingMode()
                    .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            tEnv = StreamTableEnvironment.create(env, settings);
            this.setCheckpoint(env);
        } else if (submitOption.getJobType().equals(JobType.BATCH.getType())) {
            settings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inBatchMode()
                    .build();
            tEnv = TableEnvironment.create(settings);

        } else {
            throw new RuntimeException("jobType is not supported");
        }

        String sql = FileUtil.readHDFSFile(submitOption.getSqlFilePath());
        LOG.info("sql: \n" + sql);
        SqlSplitter sqlSplitter = new SqlSplitter();
        List<String> sqlList = sqlSplitter.splitSql(sql);
        ExecuteSql.exeSql(sqlList, tEnv);
    }

    private void setCheckpoint(StreamExecutionEnvironment env) {
        FlinkOption flinkOption = submitOption.getFlinkOption();
        env.enableCheckpointing(flinkOption.getCheckpointInterval());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);
        CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
        if (flinkOption.getCheckpointingMode().equals("AT_LEAST_ONCE")) {
            checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
        }
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(flinkOption.getCheckpointPath());

        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env.configure(config);

    }

}
