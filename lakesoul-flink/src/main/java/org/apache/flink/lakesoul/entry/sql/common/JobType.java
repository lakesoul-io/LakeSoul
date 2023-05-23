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
