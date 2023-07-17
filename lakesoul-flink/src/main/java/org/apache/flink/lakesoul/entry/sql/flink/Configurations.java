// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.flink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
public class Configurations {

    public static void setSingleConfiguration(TableEnvironment tEnv, String key, String value) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return;
        }
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString(key, value);

    }


}
