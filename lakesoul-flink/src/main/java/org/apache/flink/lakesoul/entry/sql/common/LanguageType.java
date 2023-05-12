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
