/*
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
 */

package org.apache.flink.lakesoul.test.benchmark;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.types.DataType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.TimeZone;

import static org.apache.flink.lakesoul.tool.JobOptions.FLINK_CHECKPOINT;
import static org.apache.flink.lakesoul.tool.JobOptions.JOB_CHECKPOINT_INTERVAL;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;

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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
        env.setParallelism(sinkParallel);
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().setLocalTimeZone(TimeZone.getTimeZone(timeZone).toZoneId());

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
                        .option("number-of-rows", "200") // make the source bounded
                        .option("fields.col_1.kind", "sequence")
                        .option("fields.col_1.start", "1")
                        .option("fields.col_1.end", String.valueOf(dataSize))
//                        .option("fields.col_2.length", "1")
                        .option("fields.col_3.length", "10")
                        .option("fields.col_4.length", "20")
                        .schema(schema)
                        .build());

        DataStream<DataExample> dataExampleDataStream = tEnvs.toDataStream(data_gen_table, structured);

        tEnvs.createTemporaryTable("csv_table", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", warehousePath + "csv/")
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", ",")
                        .build())
                .build());

        Catalog lakeSoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakesoul", lakeSoulCatalog);
        tEnvs.useCatalog("lakesoul");
        tEnvs.executeSql("create database if not exists " + sinkDBName);

        tEnvs.createTable(sinkTableName, TableDescriptor.forConnector("lakesoul")
                .schema(schema)
                .option("path", warehousePath + sinkTableName)
                .build());

        while (writeTime > 0) {
            writeData(env, tEnvs, dataExampleDataStream, sinkTableName, dataSize);
            writeTime--;
        }
    }

    public static void writeData(StreamExecutionEnvironment env, StreamTableEnvironment tEnvs, DataStream<DataExample> dataStream, String sinkTableName, int dataSize) throws Exception {
        List<DataExample> list = dataStream.executeAndCollect(dataSize);
        // test data show
        // list.forEach(System.out::println);

        DataStream<DataExample> dataExampleDataStreamSource = env.fromCollection(list);
        Table table = tEnvs.fromDataStream(dataExampleDataStreamSource);

        table.executeInsert("default_catalog.default_database.csv_table");
        table.executeInsert(sinkTableName);
    }
}
