// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.tool;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.fs.Path;

import java.time.Duration;

public class LakeSoulSinkOptions {

    public static final String FACTORY_IDENTIFIER = "lakesoul";

    public static final String HASH_PARTITIONS = "hashPartitions";

    public static final String MERGE_COMMIT_TYPE = "MergeCommit";

    public static final String APPEND_COMMIT_TYPE = "AppendCommit";

    public static final String FILE_OPTION_ADD = "add";

    public static final String CDC_CHANGE_COLUMN = "lakesoul_cdc_change_column";

    public static final String CDC_CHANGE_COLUMN_DEFAULT = "rowKinds";

    public static final String SORT_FIELD = "__sort_filed__";

    public static final Long DEFAULT_BUCKET_ROLLING_SIZE = 20000L;

    public static final Long DEFAULT_BUCKET_ROLLING_TIME = 2000000L;

    public static final String DELETE = "delete";

    public static final String DELETE_CDC = "delete_cdc";

    public static final String UPDATE = "update";

    public static final String INSERT = "insert";
    public static final String LAKESOUL_VIEW_KIND = "flink";
    public static final String VIEW_ORIGINAL_QUERY = "original_query";
    public static final String VIEW_EXPANDED_QUERY = "expand_query";


    public static final ConfigOption<String> CATALOG_PATH = ConfigOptions
            .key("path")
            .stringType()
            .noDefaultValue()
            .withDescription("The path of a directory");

    public static final ConfigOption<Boolean> LOGICALLY_DROP_COLUM = ConfigOptions
            .key("logically.drop.column")
            .booleanType()
            .defaultValue(false)
            .withDescription("If true, Meta TableInfo will keep dropped column at schema and mark the column as \"dropped\", otherwise column will be dropped from schema");

    public static final ConfigOption<Integer> SOURCE_PARALLELISM = ConfigOptions
            .key("source.parallelism")
            .intType()
            .defaultValue(4)
            .withDescription("source number parallelism");

    public static final ConfigOption<String> WAREHOUSE_PATH = ConfigOptions
            .key("warehouse_path")
            .stringType()
            .defaultValue(new Path(System.getProperty("java.io.tmpdir"), "lakesoul").toString())
            .withDescription("warehouse path for LakeSoul");

    public static final ConfigOption<Integer> BUCKET_PARALLELISM = ConfigOptions
            .key("sink.parallelism")
            .intType()
            .defaultValue(4)
            .withDescription("bucket number parallelism");

    public static final ConfigOption<Integer> HASH_BUCKET_NUM = ConfigOptions
            .key("hashBucketNum")
            .intType()
            .defaultValue(4)
            .withDescription("bucket number for table");

    public static final ConfigOption<Long> FILE_ROLLING_SIZE = ConfigOptions
            .key("file_rolling_size")
            .longType()
            .defaultValue(20000L)
            .withDescription("file rolling size ");

    public static final ConfigOption<Long> FILE_ROLLING_TIME = ConfigOptions
            .key("file_rolling_time")
            .longType()
            .defaultValue(Duration.ofMinutes(10).toMillis())
            .withDescription("file rolling time ");

    public static final ConfigOption<Long> BUCKET_CHECK_INTERVAL = ConfigOptions
            .key("bucket_check_interval")
            .longType()
            .defaultValue(Duration.ofMinutes(1).toMillis())
            .withDescription("file rolling time ");

    public static final ConfigOption<Boolean> USE_CDC = ConfigOptions
            .key("use_cdc")
            .booleanType()
            .defaultValue(false)
            .withDescription("use cdc column ");
    public static final ConfigOption<Boolean> isMultiTableSource = ConfigOptions
            .key("Multi_Table_Source")
            .booleanType()
            .defaultValue(false)
            .withDescription("use cdc table source");
    public static final ConfigOption<String> SERVER_TIME_ZONE = ConfigOptions
            .key("server_time_zone")
            .stringType()
            .defaultValue("Asia/Shanghai")
            .withDescription("server time zone");
    public static final ConfigOption<String> DMLTYPE = ConfigOptions
            .key("dml_type")
            .stringType()
            .defaultValue(INSERT)
            .withDescription("dmltype");
    public static final ConfigOption<Boolean> LAKESOUL_VIEW = ConfigOptions
            .key("lakesoul_view")
            .booleanType()
            .defaultValue(false)
            .withDescription("lakesoul view");

    public static final ConfigOption<String> LAKESOUL_VIEW_TYPE = ConfigOptions
            .key("lakesoul_view_type")
            .stringType()
            .defaultValue(LAKESOUL_VIEW_KIND)
            .withDescription("lakesoul view_type");

    public static final ConfigOption<String> FLINK_WAREHOUSE_DIR = ConfigOptions
            .key("flink.warehouse.dir")
            .stringType()
            .defaultValue(null)
            .withDescription("flink_warehouse_dir");

    public static final ConfigOption<String> SOURCE_DB_SCHEMA_LIST = ConfigOptions
            .key("source_db.schemaList")
            .stringType()
            .noDefaultValue()
            .withDescription("source database schemaList");

    public static final ConfigOption<String> SOURCE_DB_SCHEMA_TABLES = ConfigOptions
            .key("source_db.schema_tables")
            .stringType()
            .defaultValue("")
            .withDescription("list tables of a schema");
    public static final ConfigOption<String> SOURCE_DB_SLOT_NAME = ConfigOptions
            .key("source_db.slot_name")
            .stringType()
            .defaultValue("flink")
            .withDescription("source db slot name");
    public static final ConfigOption<Integer> SOURCE_DB_SPLIT_SIZE = ConfigOptions
            .key("source_db.splitSize")
            .intType()
            .defaultValue(1024)
            .withDescription("The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.");

    //for pg
    public static final ConfigOption<String> PLUGIN_NAME = ConfigOptions
            .key("pluginName")
            .stringType()
            .defaultValue("decoderbufs")
            .withDescription("The name of the Postgres logical decoding plug-in installed on the server.");

}




