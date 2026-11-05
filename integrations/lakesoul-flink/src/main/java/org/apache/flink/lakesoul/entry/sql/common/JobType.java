// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.common;

public enum JobType {
    STREAM("stream"),

    BATCH("batch");

    private final String type;

    JobType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static JobType getJobType(String type) {
        if (type == null) {
            return null;
        }
        for (JobType jobType : JobType.values()) {
            if (jobType.getType().equals(type)) {
                return jobType;
            }
        }
        return null;
    }

    public static String getSupportJobType() {
        StringBuilder sb = new StringBuilder();
        for (JobType jobType : JobType.values()) {
            sb.append(jobType.getType()).append(" ");
        }
        return sb.toString();
    }
}
