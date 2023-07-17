// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.common;

public enum SubmitType {
    FLINK("flink");
    private final String type;

    SubmitType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static SubmitType getSubmitType(String type) {
        if (type == null) {
            return null;
        }
        for (SubmitType submitType : SubmitType.values()) {
            if (submitType.getType().equals(type)) {
                return submitType;
            }
        }
        return null;
    }

    public static String getSupportSubmitType() {
        StringBuilder sb = new StringBuilder();
        for (SubmitType submitType : SubmitType.values()) {
            sb.append(submitType.getType()).append(" ");
        }
        return sb.toString();
    }

    public static SubmitType stringToEnum(String str) {
        for (SubmitType constant : values()) {
            if (constant.getType().equals(str)) {
                return constant;
            }
        }
        return null;
    }

}
