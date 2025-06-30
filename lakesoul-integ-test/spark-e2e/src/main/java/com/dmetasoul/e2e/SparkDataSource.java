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
    var spark =
        SparkSession.builder()
            .config(
                "spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
            .config(
                "spark.sql.catalog.lakesoul",
                "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
            .config("spark.sql.defaultCatalog", "lakesoul")
            .getOrCreate();
    var parquetPath = "s3://lakesoul-test-bucket/lakesoul/e2e/data/";
    StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("f_int", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_bigint", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_smallint", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_tinyint", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_float", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_double", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_decimal", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_string", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_char", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_varchar", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_boolean", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_date", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_time", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_timestamp", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_bytes", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_array", DataTypes.IntegerType, false),
              DataTypes.createStructField("f_row", DataTypes.IntegerType, false),
            });
    Dataset<Row> origin = spark.read().schema(schema).option("inferSchema", "true").csv(parquetPath);
    origin.registerTempTable("parquet_source");
    var c1 = spark.sql("select * from parquet_source;").count();
    var c2 = spark.sql("select * from lakesoul_e2e_test;").count();
    if (c1!=c2) {
      throw new RuntimeException("Sink data != Source Data");
    }
    spark.stop();
  }
}
