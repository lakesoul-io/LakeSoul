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
