// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.flink;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.entry.sql.Submitter;
import org.apache.flink.lakesoul.entry.sql.common.FlinkOption;
import org.apache.flink.lakesoul.entry.sql.common.JobType;
import org.apache.flink.lakesoul.entry.sql.common.SubmitOption;
import org.apache.flink.lakesoul.entry.sql.utils.FileUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class FlinkSqlSubmitter extends Submitter {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlSubmitter.class);

    public FlinkSqlSubmitter(SubmitOption submitOption) {
        super(submitOption);

    }

    @Override
    public void submit() throws IOException, URISyntaxException, ExecutionException, InterruptedException {
        EnvironmentSettings settings = null;
        TableEnvironment tEnv = null;
        if (submitOption.getJobType().equals(JobType.STREAM.getType())) {
            settings = EnvironmentSettings
                    .newInstance()
                    .inStreamingMode()
                    .build();

            Configuration conf = new Configuration();
            conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
            this.setCheckpoint(env);
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (submitOption.getJobType().equals(JobType.BATCH.getType())) {
            settings = EnvironmentSettings.newInstance()
                    .inBatchMode()
                    .build();
            tEnv = TableEnvironment.create(settings);

        } else {
            throw new RuntimeException("jobType is not supported");
        }

        String sql = FileUtil.readHDFSFile(submitOption.getSqlFilePath());
        System.out.println(
                MessageFormatter.format("\n======SQL Script Content from file {}:\n{}",
                        submitOption.getSqlFilePath(), sql).getMessage());
        ExecuteSql.executeSqlFileContent(sql, tEnv);
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
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(flinkOption.getCheckpointPath());
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, // max failures per interval
                Time.of(10, TimeUnit.MINUTES), //time interval for measuring failure rate
                Time.of(20, TimeUnit.SECONDS) // delay
        ));
    }

}
