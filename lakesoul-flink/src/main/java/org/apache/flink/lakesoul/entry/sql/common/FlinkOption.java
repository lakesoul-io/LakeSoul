// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.common;

import org.apache.flink.configuration.ConfigOption;

public class FlinkOption {
    private String checkpointPath;

    private String savepointPath;

    private long checkpointInterval;

    private String checkpointingMode;

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public String getSavepointPath() {
        return savepointPath;
    }

    public void setSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
    }

    public void setCheckpointPath(String checkpointPath) {
        this.checkpointPath = checkpointPath;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public String getCheckpointingMode() {
        return checkpointingMode;
    }

    public void setCheckpointingMode(String checkpointingMode) {
        this.checkpointingMode = checkpointingMode;
    }

}
