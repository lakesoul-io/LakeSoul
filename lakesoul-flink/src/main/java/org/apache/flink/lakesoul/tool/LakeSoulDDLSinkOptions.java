/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.tool;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class LakeSoulDDLSinkOptions extends LakeSoulSinkOptions{

    public static final ConfigOption<String> SOURCE_DB_DB_NAME = ConfigOptions
            .key("source_db.db_name")
            .stringType()
            .noDefaultValue()
            .withDescription("source database name");


    public static final ConfigOption<String> SOURCE_DB_USER = ConfigOptions
            .key("source_db.user")
            .stringType()
            .noDefaultValue()
            .withDescription("source database user_name");

    public static final ConfigOption<String> SOURCE_DB_PASSWORD = ConfigOptions
            .key("source_db.password")
            .stringType()
            .noDefaultValue()
            .withDescription("source database access password");


    public static final ConfigOption<String> SOURCE_DB_HOST = ConfigOptions
            .key("source_db.host")
            .stringType()
            .noDefaultValue()
            .withDescription("source database access host_name");

    public static final ConfigOption<Integer> SOURCE_DB_PORT = ConfigOptions
            .key("source_db.port")
            .intType()
            .noDefaultValue()
            .withDescription("source database access port");

    public static final ConfigOption<String> SOURCE_DB_EXCLUDE_TABLES = ConfigOptions
            .key("source_db.exclude_tables")
            .stringType()
            .defaultValue("")
            .withDescription("list of source database excluded tables. Comma-Separated string");


}
