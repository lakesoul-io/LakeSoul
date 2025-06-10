/*
 * SPDX-FileCopyrightText: 2025 LakeSoul Contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.dmetasoul.e2e;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.ExecutionException;

/**
 * @author mag1cian
 */
public class FlinkDataSink {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        var csvFileTable = "CREATE TABLE csv_source (" +
                // number
                "f_int INT," + "f_bigint BIGINT," + "f_smallint SMALLINT," + "f_tinyint TINYINT," + "f_float FLOAT," + "f_double DOUBLE," + "f_decimal DECIMAL(10, 2)," +
                // string
                "f_string STRING," + "f_char CHAR(10)," + "f_varchar VARCHAR(20)," +
                // bool
                "f_boolean BOOLEAN," +
                // time
                "f_date DATE," + "f_time TIME," + "f_timestamp TIMESTAMP(3)," +
                // bytes
                "f_bytes BINARY," +
                // array
                "f_array ARRAY<INT>," +
                // row
                "f_row ROW<f1 INT, f2 STRING>" + ") WITH (\n" + "'connector' = 'filesystem',\n" + "'path' = 'file:///tmp/lakesoul/e2e/data.csv',\n" + "'format' ='csv',\n" + "'csv.null-literal' ='NULL'\n" + ")\n";


        tableEnv.executeSql(csvFileTable);
        tableEnv.executeSql("create catalog lakesoul with('type'='lakesoul')");
        tableEnv.executeSql("use catalog lakesoul");
        tableEnv.executeSql("drop table  if exists lakesoul_e2e_test;");
        var createTable = "CREATE TABLE lakesoul_e2e_test\n" + "WITH (\n" + "'connector' = 'lakesoul',\n" + "'path'='file:///tmp/lakesoul/e2e/flink/sink'\n" + ")\n" + "LIKE default_catalog.default_database.csv_source;";
        tableEnv.executeSql(createTable);
        tableEnv.executeSql("insert into lakesoul_e2e_test select * from default_catalog.default_database.csv_source;");
        tableEnv.executeSql("select * from lakesoul_e2e_test");
    }
}
