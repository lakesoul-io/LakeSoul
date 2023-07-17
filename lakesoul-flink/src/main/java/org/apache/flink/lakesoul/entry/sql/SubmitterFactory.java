// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql;

import org.apache.flink.lakesoul.entry.sql.common.LanguageType;
import org.apache.flink.lakesoul.entry.sql.common.SubmitType;
import org.apache.flink.lakesoul.entry.sql.flink.FlinkSqlSubmitter;
import org.apache.flink.lakesoul.entry.sql.common.SubmitOption;

public class SubmitterFactory {
    public static Submitter createSubmitter(SubmitOption submitOption) {
        switch (SubmitType.stringToEnum(submitOption.getSubmitType())) {
            case FLINK:
                switch (LanguageType.stringToEnum(submitOption.getLanguage())) {
                    case SQL:
                        return new FlinkSqlSubmitter(submitOption);
                    default:
                        throw new RuntimeException("language is not supported");
                }
            default:
                throw new RuntimeException("submitType is not supported");
        }
    }


}
