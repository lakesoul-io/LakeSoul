package org.apache.flink.lakesoul.entry;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class SourceOptions {
    public static final ConfigOption<String> SOURCE_DB_DB_NAME = ConfigOptions.key("source_db.dbName").stringType().noDefaultValue().withDescription("source database name");
    public static final ConfigOption<String> SOURCE_DB_USER = ConfigOptions.key("source_db.user").stringType().noDefaultValue().withDescription("source database user_name");
    public static final ConfigOption<String> SOURCE_DB_PASSWORD = ConfigOptions.key("source_db.password").stringType().noDefaultValue().withDescription("source database access password");
    public static final ConfigOption<String> SOURCE_DB_HOST = ConfigOptions.key("source_db.host").stringType().noDefaultValue().withDescription("source database access host_name");
    public static final ConfigOption<Integer> SOURCE_DB_PORT = ConfigOptions.key("source_db.port").intType().defaultValue(5432).withDescription("source database access port");
    public static final ConfigOption<Integer> SPLIT_SIZE = ConfigOptions.key("splitSize").intType().defaultValue(1024).withDescription("split size of flink postgresql cdc");
    public static final ConfigOption<Integer> FETCH_SIZE = ConfigOptions.key("fetchSize").intType().defaultValue(1024).withDescription("fetch size");
    public static final ConfigOption<String> SLOT_NAME = ConfigOptions.key("slotName").stringType().noDefaultValue().withDescription("slot name of pg");
    public static final ConfigOption<String> PLUG_NAME = ConfigOptions.key("plugName").stringType().noDefaultValue().withDescription("plug name of postgresql");
    public static final ConfigOption<String> SCHEMA_LIST = ConfigOptions.key("schemaList").stringType().defaultValue("public").withDescription("");
    public static final ConfigOption<String> PG_URL = ConfigOptions.key("url").stringType().noDefaultValue().withDescription("");
    public static final ConfigOption<Integer> SOURCE_PARALLELISM = ConfigOptions.key("source.parallelism").intType().defaultValue(1).withDescription("job Parallelism");
    public static final ConfigOption<Integer> DATA_EXPIRED_TIME = ConfigOptions.key("dataExpiredTime").intType().defaultValue(3).withDescription("");
    public static final ConfigOption<String> STARTUP_OPTIONS_CONFIG_OPTION = ConfigOptions.key("start_option").stringType().defaultValue("initial").withDescription("");
    public static final ConfigOption<Integer> JDBC_SINK_BATCH_SIZE = ConfigOptions.key("jdbcBatchSize").intType().defaultValue(2000).withDescription("jdbc sink batch size");
    public static final ConfigOption<Integer> SINK_INTERVAL = ConfigOptions.key("sinkInterval").intType().defaultValue(1).withDescription("");
    public static final ConfigOption<Boolean> DEALING_WITH_DATA_SKEW = ConfigOptions.key("deal_data_skey").defaultValue(false).withDescription("Dealing with data skew");
    public static final ConfigOption<Long> ONTIMER_INTERVAL = ConfigOptions.key("ontimer_interval").longType().noDefaultValue();
    public static final ConfigOption<String> TARGET_IDS = ConfigOptions.key("targetTableID").stringType().noDefaultValue();
    public static final ConfigOption<String> TARGET_TABLES =ConfigOptions.key("targetTableName").stringType().noDefaultValue();
    public SourceOptions() {
    }
}