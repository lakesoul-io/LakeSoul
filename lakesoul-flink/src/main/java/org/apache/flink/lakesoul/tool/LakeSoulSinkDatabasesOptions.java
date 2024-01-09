// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0




package org.apache.flink.lakesoul.tool;


import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;


public class LakeSoulSinkDatabasesOptions extends LakeSoulSinkOptions {


    public static final ConfigOption<String> TARGET_DB_URL = ConfigOptions
            .key("target_db.url")
            .stringType()
            .noDefaultValue()
            .withDescription("source database url");


    public static final ConfigOption<String> SOURCE_DB_DB_NAME = ConfigOptions
            .key("source_db.db_name")
            .stringType()
            .noDefaultValue()
            .withDescription("source database name");


    public static final ConfigOption<String> TARGET_DB_USER = ConfigOptions
            .key("target_db.user")
            .stringType()
            .noDefaultValue()
            .withDescription("source database user_name");


    public static final ConfigOption<String> TARGET_DB_PASSWORD = ConfigOptions
            .key("target_db.password")
            .stringType()
            .noDefaultValue()
            .withDescription("source database access password");


    public static final ConfigOption<String> TARGET_DATABASE_TYPE = ConfigOptions
            .key("target_db.db_type")
            .stringType()
            .noDefaultValue()
            .withDescription("mysql,postgres,doris");


    public static final ConfigOption<String> TARGET_DB_DB_NAME = ConfigOptions
            .key("target_db.db_name")
            .stringType()
            .noDefaultValue()
            .withDescription("target ddatabase name");


    public static final ConfigOption<String> SOURCE_DB_LAKESOUL_TABLE = ConfigOptions
            .key("source_db.table_name")
            .stringType()
            .noDefaultValue()
            .withDescription("lakesoul table");


    public static final ConfigOption<String> TARGET_DB_TABLE_NAME = ConfigOptions
            .key("target_db.table_name")
            .stringType()
            .noDefaultValue()
            .withDescription("target database table");


    public static final ConfigOption<Integer> DORIS_REPLICATION_NUM = ConfigOptions
            .key("doris_replication.num")
            .intType()
            .defaultValue(1)
            .withDescription("doris table replication num");


    public static final ConfigOption<Integer> SINK_PARALLELISM = ConfigOptions
            .key("sink_parallelism")
            .intType()
            .defaultValue(1)
            .withDescription("parallelism settings for out-of-the-lake");


    public static final ConfigOption<Boolean> BATHC_STREAM_SINK = ConfigOptions
            .key("use_batch")
            .booleanType()
            .defaultValue(true)
            .withDescription("batch or stream for out-of-lake");


    public static final ConfigOption<String> DORIS_FENODES = ConfigOptions
            .key("doris.fenodes")
            .stringType()
            .defaultValue("127.0.0.1:8030");
}