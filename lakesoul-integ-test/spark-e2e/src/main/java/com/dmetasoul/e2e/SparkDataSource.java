/*
 * SPDX-FileCopyrightText: 2025 LakeSoul Contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.dmetasoul.e2e;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * @author mag1cian
 */
public class SparkDataSource {
    public static void main(String[] args) {
        var spark = SparkSession.builder().config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
                .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
                .config("spark.sql.defaultCatalog", "lakesoul")
                .getOrCreate();
//        var df = spark.sql("select * from lakesoul_e2e_test");
//        df.show();


        // 创建数据集合
        List<Row> data = Arrays.asList(
                RowFactory.create("Alice", 25),
                RowFactory.create("Bob", 30),
                RowFactory.create("Charlie", 35)
        );

        // 定义 Schema
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        );
        StructType schema = DataTypes.createStructType(fields);

        // 创建 DataFrame
        Dataset<Row> df = spark.createDataFrame(data, schema);
        df.registerTempTable("test_data");

        spark.sql("drop database if exists spark");
        spark.sql("create database spark;");
        spark.sql("create table spark.data (name string, age int) using lakesoul location 'file:///tmp/lakesoul/e2e/spark'" );
        spark.sql("insert into spark.data select name, age from test_data");
        spark.sql("select * from spark.data").show();

//        df.write().mode("append").format("lakesoul").option("shortTableName", "spark.lakesoul_e2e_test_from_spark").save("file:///tmp/lakesoul/e2e/spark");
        spark.stop();
    }
}
