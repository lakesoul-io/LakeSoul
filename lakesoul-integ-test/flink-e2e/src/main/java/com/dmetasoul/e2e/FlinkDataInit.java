/*
 * SPDX-FileCopyrightText: 2025 LakeSoul Contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.dmetasoul.e2e;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author mag1cian
 */
public class FlinkDataInit {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    var dataGenSql =
        "CREATE TABLE data_gen_source ("
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
            + ") WITH ("
            + "'connector' = 'datagen',"
            + "'number-of-rows' = '1000',"
            +
            // number option
            "'fields.f_int.min' = '1',"
            + "'fields.f_int.max' = '100',"
            + "'fields.f_bigint.min' = '1',"
            + "'fields.f_bigint.max' = '1000',"
            + "'fields.f_smallint.min' = '1',"
            + "'fields.f_smallint.max' = '100',"
            + "'fields.f_tinyint.min' = '1',"
            + "'fields.f_tinyint.max' = '10',"
            + "'fields.f_float.min' = '1.0',"
            + "'fields.f_float.max' = '100.0',"
            + "'fields.f_double.min' = '1.0',"
            + "'fields.f_double.max' = '100.0',"
            + "'fields.f_decimal.min' = '1.0',"
            + "'fields.f_decimal.max' = '100.0',"
            +
            // string option
            "'fields.f_string.var-len' = 'true',"
            + "'fields.f_varchar.var-len' = 'true',"
            +
            // array option
            "'fields.f_array.element.min' = '1',"
            + "'fields.f_array.element.max' = '100',"
            + "'fields.f_array.element.null-rate' = '10'"
            + ")";

    //        // 2. 创建DataGen源表
    tableEnv.executeSql(dataGenSql);

    var creatCsv =
        "CREATE TABLE data_file\n ("
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
            "f_row ROW<f1 INT, f2 STRING> )"
            + "WITH (\n"
            + "'connector' = 'filesystem',\n"
            + "'path' = 's3://dmetasoul-bucket/lakesoul/lakesoul-e2e/data/',\n"
            + "'format' = 'parquet'\n"
            + ")\n";
    tableEnv.executeSql(creatCsv);
    tableEnv.executeSql("insert into data_file select * from data_gen_source").await();
  }
}
