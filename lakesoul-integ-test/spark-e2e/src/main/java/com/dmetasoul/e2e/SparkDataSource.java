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
            .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
            .config("spark.sql.defaultCatalog", "lakesoul")
            .getOrCreate();
    var parquetPath = "s3://lakesoul-test-bucket/lakesoul/e2e/data/";
    StructType schema =
            DataTypes.createStructType(
                    new StructField[] {
                            DataTypes.createStructField("f_int", DataTypes.IntegerType, true),
                            DataTypes.createStructField("f_bigint", DataTypes.LongType, true),
                            DataTypes.createStructField("f_smallint", DataTypes.ShortType, true),
                            DataTypes.createStructField("f_tinyint", DataTypes.ByteType, true),
                            DataTypes.createStructField("f_float", DataTypes.FloatType, true),
                            DataTypes.createStructField("f_double", DataTypes.DoubleType, true),
                            DataTypes.createStructField("f_decimal", DataTypes.createDecimalType(10,2), true),
                            DataTypes.createStructField("f_string", DataTypes.StringType, true),
                            DataTypes.createStructField("f_char", DataTypes.StringType, true),
                            DataTypes.createStructField("f_varchar", DataTypes.StringType, true),
                            DataTypes.createStructField("f_boolean", DataTypes.BooleanType, true),
                            DataTypes.createStructField("f_date", DataTypes.DateType, true),
                            DataTypes.createStructField("f_time", DataTypes.IntegerType, true),
                            DataTypes.createStructField("f_timestamp", DataTypes.TimestampType, true),
                            DataTypes.createStructField("f_bytes", DataTypes.BinaryType, true),
                            DataTypes.createStructField(
                                    "f_array", DataTypes.createArrayType(DataTypes.IntegerType, true), true),
                            DataTypes.createStructField("f_row", DataTypes.createStructType(new StructField[]{
                                    DataTypes.createStructField("f1",DataTypes.IntegerType,true),
                                    DataTypes.createStructField("f2",DataTypes.StringType,true)
                            }), true),
                    });

    Dataset<Row> origin = spark.read().schema(schema).option("inferSchema", "true").parquet(parquetPath);
    origin.registerTempTable("parquet_source");
    var c1 = spark.sql("select * from parquet_source;").count();
    var c2 = spark.sql("select * from lakesoul_e2e_test;").count();
    if (c1!=c2) {
      throw new RuntimeException("Sink data != Source Data");
    }
    spark.stop();
  }
}
