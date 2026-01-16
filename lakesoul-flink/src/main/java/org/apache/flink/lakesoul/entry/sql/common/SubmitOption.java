// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.common;

import org.apache.flink.api.java.utils.ParameterTool;

import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.JobOptions.JOB_CHECKPOINT_MODE;

public class SubmitOption {
    private final String submitType;
    private final String jobType;
    private final String language;
    private final String sqlFilePath;
    private final Long scheduleTime;
    private FlinkOption flinkOption;

    public SubmitOption(ParameterTool params) {
        this.submitType = params.get("submit_type");
        this.jobType = params.get("job_type");
        this.language = params.get("language");
        this.sqlFilePath = params.get("sql_file_path");
        this.scheduleTime = params.getLong("scheduleTime", 0L);
        this.checkParam();
        if (SubmitType.getSubmitType(submitType) == SubmitType.FLINK) {
            setFlinkOption(params, this);
        }
    }

    public Long getScheduleTime() {
        return scheduleTime;
    }

    public String getSqlFilePath() {
        return sqlFilePath;
    }

    public String getSubmitType() {
        return submitType;
    }

    public String getJobType() {
        return jobType;
    }

    public String getLanguage() {
        return language;
    }


    public FlinkOption getFlinkOption() {
        return flinkOption;
    }

    public void setFlinkOption(FlinkOption flinkOption) {
        this.flinkOption = flinkOption;
    }


    public void checkParam() {
        if (SubmitType.getSubmitType(submitType) == null) {
            throw new RuntimeException(String.format("submitType: %s is not supported. Supported submitTypes: %s", submitType, SubmitType.getSupportSubmitType()));
        }
        if (JobType.getJobType(jobType) == null) {
            throw new RuntimeException("jobType is not supported. Supported jobType: " + JobType.getSupportJobType());
        }
        if (LanguageType.getLanguageType(language) == null) {
            throw new RuntimeException("language is not supported. Supported language: " + LanguageType.getSupportLanguage());
        }
    }

    private void setFlinkOption(ParameterTool params, SubmitOption submitOption) {
        String checkpointPath = params.get(FLINK_CHECKPOINT.key());
        String savepointPath = params.get(FLINK_SAVEPOINT.key());
        long checkpointInterval = params.getLong(JOB_CHECKPOINT_INTERVAL.key(), JOB_CHECKPOINT_INTERVAL.defaultValue());
        String checkpointingMode = params.get(JOB_CHECKPOINT_MODE.key(), JOB_CHECKPOINT_MODE.defaultValue());
        FlinkOption flinkOption = new FlinkOption();
        flinkOption.setCheckpointPath(checkpointPath);
        flinkOption.setSavepointPath(savepointPath);
        flinkOption.setCheckpointInterval(checkpointInterval);
        flinkOption.setCheckpointingMode(checkpointingMode);
        submitOption.setFlinkOption(flinkOption);
    }

}
