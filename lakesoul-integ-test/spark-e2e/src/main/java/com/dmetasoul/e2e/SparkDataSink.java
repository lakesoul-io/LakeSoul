/*
 * SPDX-FileCopyrightText: 2025 LakeSoul Contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.dmetasoul.e2e;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author mag1cian
 */
public class SparkDataSink {
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
    spark.sql("DROP TABLE IF EXISTS lakesoul_e2e_test");
    String parquetPath = "s3://lakesoul-test-bucket/lakesoul/e2e/data";
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
    Dataset<Row> origin = spark.read().schema(schema).option("inferSchema", "true").parquet(parquetPath);
    var tablePath = "s3://lakesoul-test-bucket/lakesoul/e2e/spark/sink";
    origin
        .write()
        .mode("overwrite")
        .format("lakesoul")
        .option("shortTableName", "lakesoul_e2e_test")
        .save(tablePath);
    spark.stop();
  }
}
