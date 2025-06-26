/*
 * SPDX-FileCopyrightText: 2025 LakeSoul Contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.dmetasoul.e2e;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * @author mag1cian
 */
public class FlinkDataSource {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    var parquetFileTable =
        "CREATE TABLE parquet_source ("
            +
            // number
            "f_int INT,"
            + "f_bigint BIGINT,"
            + "f_smallint SMALLINT,"
            + "f_tinyint TINYINT,"
            + "f_float FLOAT,"
            + "f_double DOUBLE,"
            + "f_decimal DECIMAL(10, 2),"
            +
            // string
            "f_string STRING,"
            + "f_char CHAR(10),"
            + "f_varchar VARCHAR(20),"
            +
            // bool
            "f_boolean BOOLEAN,"
            +
            // time
            "f_date DATE,"
            + "f_time TIME,"
            + "f_timestamp TIMESTAMP(3),"
            +
            // bytes
            "f_bytes BINARY,"
            +
            // array
            "f_array ARRAY<INT>,"
            +
            // row
            "f_row ROW<f1 INT, f2 STRING>"
            + ") WITH (\n"
            + "'connector' = 'filesystem',\n"
            + "'path' = 'file:///tmp/lakesoul/e2e/data/',\n"
            + "'format' ='parquet'\n"
            + ")\n";
    tableEnv.executeSql(parquetFileTable).await();

    tableEnv.executeSql("create catalog lakesoul with('type'='lakesoul')").await();

    tableEnv.executeSql("use catalog lakesoul").await();
    // 执行差异查询
    var res1 =
        tableEnv.executeSql(
            "select count(*) FROM default_catalog.default_database.parquet_source; ");
    var res2 = tableEnv.executeSql("select count(*) FROM lakesoul_e2e_test;");
    var c1 = Objects.requireNonNull(res1.collect().next().getField(0)).toString();
    var c2 = Objects.requireNonNull(res2.collect().next().getField(0)).toString();
    if (!c1.equals(c2)) {
      throw new RuntimeException("Sink data != Source Data");
    }
  }
}
