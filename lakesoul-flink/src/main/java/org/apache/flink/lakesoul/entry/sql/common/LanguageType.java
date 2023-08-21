// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.common;

public enum LanguageType {
    SQL("sql");
    private final String type;

    LanguageType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static LanguageType getLanguageType(String type) {
        if (type == null) {
            return null;
        }
        for (LanguageType languageType : LanguageType.values()) {
            if (languageType.getType().equals(type)) {
                return languageType;
            }
        }
        return null;
    }

    public static String getSupportLanguage() {
        StringBuilder sb = new StringBuilder();
        for (LanguageType languageType : LanguageType.values()) {
            sb.append(languageType.getType()).append(" ");
        }
        return sb.toString();
    }

    public static LanguageType stringToEnum(String str) {
        for (LanguageType constant : values()) {
            if (constant.getType().equals(str)) {
                return constant;
            }
        }
        return null;
    }

}
