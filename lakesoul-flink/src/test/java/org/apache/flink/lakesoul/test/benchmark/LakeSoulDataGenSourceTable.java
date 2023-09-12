// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test.benchmark;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;

import static org.apache.flink.lakesoul.tool.JobOptions.FLINK_CHECKPOINT;
import static org.apache.flink.lakesoul.tool.JobOptions.JOB_CHECKPOINT_INTERVAL;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

/**
 * this is used to create random data sinking to a LakeSoul table without primary key
 */
public class LakeSoulDataGenSourceTable {
    /**
     * param example:
     * --sink.database.name flink_source
     * --sink.table.name source_table
     * --job.checkpoint_interval 10000
     * --server_time_zone UTC
     * --sink.parallel 2
     * --warehouse.path file:///tmp/data
     * --flink.checkpoint file:///tmp/chk
     * --data.size 1000
     * --write.time 5
     */
    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);

        String sinkDBName = parameter.get("sink.database.name", "flink_source");
        String sinkTableName = parameter.get("sink.table.name", "source_table");
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(),
                JOB_CHECKPOINT_INTERVAL.defaultValue());
        String timeZone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());
        String warehousePath = parameter.get("warehouse.path", "file:///tmp/data");
        String checkpointPath = parameter.get(FLINK_CHECKPOINT.key(), "file:///tmp/chk");
        int sinkParallel = parameter.getInt("sink.parallel", 2);
        int dataSize = parameter.getInt("data.size", 1000);
        int writeTime = parameter.getInt("write.time", 5);

        Configuration configuration = new Configuration();
        configuration.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        TableEnvironment tEnvs = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnvs.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        tEnvs.getConfig().getConfiguration().setString("table.local-time-zone", timeZone);

        final Schema schema = Schema.newBuilder()
                .column("col_1", DataTypes.INT())
//                .column("col_2", DataTypes.STRING())
                .column("col_3", DataTypes.VARCHAR(10))
                .column("col_4", DataTypes.STRING())
                .column("col_5", DataTypes.BOOLEAN())
                .column("col_6", DataTypes.DECIMAL(10, 3))
                .column("col_7", DataTypes.TINYINT())
                .column("col_8", DataTypes.SMALLINT())
                .column("col_9", DataTypes.INT())
                .column("col_10", DataTypes.BIGINT())
                .column("col_11", DataTypes.FLOAT())
                .column("col_12", DataTypes.DOUBLE())
                .column("col_13", DataTypes.DATE())
//                .column("col_14", DataTypes.TIMESTAMP())
                .column("col_15", DataTypes.TIMESTAMP_LTZ())
                .build();

        DataType structured = DataTypes.STRUCTURED(
                DataExample.class,
                DataTypes.FIELD("col_1", DataTypes.INT()),
//                        DataTypes.FIELD("col_2", DataTypes.STRING()),
                DataTypes.FIELD("col_3", DataTypes.VARCHAR(50)),
                DataTypes.FIELD("col_4", DataTypes.STRING()),
                DataTypes.FIELD("col_5", DataTypes.BOOLEAN()),
                DataTypes.FIELD("col_6", DataTypes.DECIMAL(10, 3).bridgedTo(BigDecimal.class)),
                DataTypes.FIELD("col_7", DataTypes.TINYINT()),
                DataTypes.FIELD("col_8", DataTypes.SMALLINT()),
                DataTypes.FIELD("col_9", DataTypes.INT()),
                DataTypes.FIELD("col_10", DataTypes.BIGINT()),
                DataTypes.FIELD("col_11", DataTypes.FLOAT()),
                DataTypes.FIELD("col_12", DataTypes.DOUBLE()),
                DataTypes.FIELD("col_13", DataTypes.DATE().bridgedTo(Date.class)),
//                DataTypes.FIELD("col_14", DataTypes.TIMESTAMP().bridgedTo(Timestamp.class)),
                DataTypes.FIELD("col_15", DataTypes.TIMESTAMP_LTZ(6)));

        Table data_gen_table = tEnvs.from(
                TableDescriptor.forConnector("datagen")
                        .option("number-of-rows", String.valueOf(dataSize)) // make the source bounded
                        .option("fields.col_1.kind", "sequence")
                        .option("fields.col_1.start", "1")
                        .option("fields.col_1.end", String.valueOf(dataSize))
//                        .option("fields.col_2.length", "1")
                        .option("fields.col_3.length", "10")
                        .option("fields.col_4.length", "20")
                        .schema(schema)
                        .build());

        tEnvs.createTemporaryTable("csv_table", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", warehousePath + "csv/")
                .option("parquet.utc-timezone", "true")
                .format(FormatDescriptor.forFormat("parquet")
                        .build())
                .build());

        Catalog lakeSoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakesoul", lakeSoulCatalog);
        tEnvs.useCatalog("lakesoul");
        tEnvs.executeSql("create database if not exists " + sinkDBName);

        tEnvs.executeSql("drop table if exists `default`." + sinkTableName);
        tEnvs.createTable(sinkTableName, TableDescriptor.forConnector("lakesoul")
                .schema(schema)
                .option("path", warehousePath + sinkTableName)
                .build());

        while (writeTime > 0) {
            writeData(tEnvs, data_gen_table, sinkTableName);
            writeTime--;
        }
    }

    public static void writeData(TableEnvironment tEnvs, Table sourceTable, String sinkTableName) throws Exception {
        List<Row> rows = IteratorUtils.toList(sourceTable.execute().collect());
        Table table = tEnvs.fromValues(rows);

        table.executeInsert("default_catalog.default_database.csv_table").await();
        table.executeInsert(sinkTableName).await();
    }
}
