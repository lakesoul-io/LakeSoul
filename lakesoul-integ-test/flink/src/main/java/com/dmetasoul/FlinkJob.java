/*
 * SPDX-FileCopyrightText: 2025 LakeSoul Contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.dmetasoul;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;

/**
 * @author mag1cian
 */
public class FlinkJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        var dataGenSql =
                "CREATE TABLE all_type (" +
                        // number
                        "f_int INT," +
                        "f_bigint BIGINT," +
                        "f_smallint SMALLINT," +
                        "f_tinyint TINYINT," +
                        "f_float FLOAT," +
                        "f_double DOUBLE," +
                        "f_decimal DECIMAL(10, 2)," +
                        // string
                        "f_string STRING," +
                        "f_char CHAR(10)," +
                        "f_varchar VARCHAR(20)," +
                        // bool
                        "f_boolean BOOLEAN," +
                        // time
                        "f_date DATE," +
                        "f_time TIME," +
                        "f_timestamp TIMESTAMP(3)," +
                        // bytes
                        "f_bytes BINARY," +
                        // array
                        "f_array ARRAY<INT>," +
                        // row
                        "f_row ROW<f1 INT, f2 STRING>" +
                        ") WITH (" +
                        "'connector' = 'datagen'," +
//                        "'rows-per-second' = '50'," +
                        "'number-of-rows' = '1000'," +
// number conf
                        "'fields.f_int.min' = '1'," +
                        "'fields.f_int.max' = '100'," +
                        "'fields.f_bigint.min' = '1'," +
                        "'fields.f_bigint.max' = '1000'," +
                        "'fields.f_smallint.min' = '1'," +
                        "'fields.f_smallint.max' = '100'," +

                        "'fields.f_tinyint.min' = '1'," +
                        "'fields.f_tinyint.max' = '10'," +
                        "'fields.f_float.min' = '1.0'," +
                        "'fields.f_float.max' = '100.0'," +
                        "'fields.f_double.min' = '1.0'," +
                        "'fields.f_double.max' = '100.0'," +
                        "'fields.f_decimal.min' = '1.0'," +
                        "'fields.f_decimal.max' = '100.0'," +
// string conf
                        "'fields.f_string.var-len' = 'true'," +
                        "'fields.f_varchar.var-len' = 'true'," +
                        //
                        "'fields.f_array.element.min' = '1'," +
                        "'fields.f_array.element.max' = '100'," +
                        "'fields.f_array.element.null-rate' = '10'" +
                        ")";


//        // 2. 创建DataGen源表
        tableEnv.executeSql(dataGenSql);
//        tableEnv.executeSql("select * from all_type").print();

        tableEnv.executeSql("create catalog lakesoul with('type'='lakesoul')");
        tableEnv.executeSql("use catalog lakesoul");
        tableEnv.executeSql("drop table  if exists all_type_lfs;");
        var createTable = "CREATE TABLE all_type_lfs\n" +
                "WITH (\n" +
                "'connector' = 'lakesoul',\n" +
                "'path'='file:///tmp/lakesoul/flink/sink/test'\n" +
                ")\n" +
                "LIKE default_catalog.default_database.all_type;";
        tableEnv.executeSql(createTable);
        tableEnv.executeSql("insert into all_type_lfs select * from default_catalog.default_database.all_type");
        var res1 = tableEnv.executeSql("select count(*) from all_type_lfs");
        System.out.println(res1);
//        var res2 = tableEnv.executeSql("select count(*) from default_catalog.default_database.all_type");
//
//        var rows1 = new ArrayList<>();
//        var rows2 = new ArrayList<>();
//        try (
//                var it1 = res1.collect();
//                var it2 = res2.collect();
//        ) {
//            it1.forEachRemaining(rows1::add);
//            it2.forEachRemaining(rows2::add);
//            if (!rows1.equals(rows2)) {
//                System.out.println("Error");
//            }
//        }

    }
}