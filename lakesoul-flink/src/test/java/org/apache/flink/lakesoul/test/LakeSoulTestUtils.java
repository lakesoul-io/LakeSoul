package org.apache.flink.lakesoul.test;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.sql.parser.SqlPartitionUtils;
import org.apache.flink.sql.parser.hive.ddl.SqlAddHivePartitions;
import org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImpl;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

public class LakeSoulTestUtils {

    public static LakeSoulCatalog createLakeSoulCatalog() {
        return createLakeSoulCatalog(false);
    }
    public static LakeSoulCatalog createLakeSoulCatalog(boolean cleanAll) {
        LakeSoulCatalog lakeSoulCatalog = new LakeSoulCatalog();
        if (cleanAll) lakeSoulCatalog.cleanForTest();
        return lakeSoulCatalog;
    }

    public static TableEnvironment createTableEnvInBatchMode() {
        return createTableEnvInBatchMode(SqlDialect.DEFAULT);
    }

    public static TableEnvironment createTableEnvInBatchMode(SqlDialect dialect) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tableEnv.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tableEnv.getConfig().setSqlDialect(dialect);
        return tableEnv;
    }

    public static StreamTableEnvironment createTableEnvInStreamingMode(StreamExecutionEnvironment env) {
        return createTableEnvInStreamingMode(env, SqlDialect.DEFAULT);
    }

    public static StreamExecutionEnvironment createStreamExecutionEnvironment() {
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        return env;
    }

    public static StreamTableEnvironment createTableEnvInStreamingMode(StreamExecutionEnvironment env, SqlDialect dialect) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);
        tableEnv.getConfig().setSqlDialect(dialect);
        return tableEnv;
    }

    public static TableEnvironment createTableEnvWithLakeSoulCatalog(LakeSoulCatalog catalog) {
        TableEnvironment tableEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(catalog.getName(), catalog);
        tableEnv.useCatalog(catalog.getName());
        return tableEnv;
    }

}
