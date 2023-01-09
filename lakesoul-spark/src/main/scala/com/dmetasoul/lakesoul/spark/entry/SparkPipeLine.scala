/*
 *
 *
 *   Copyright [2022] [DMetaSoul Team]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.spark.entry

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

object SparkPipeLine {

  var dbName = "test_streamRead"
  val dbPath = "s3a://bucket-name/table/path/is/also/table/name"

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("STREAM PIPELINE TEST")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .config("spark.default.parallelism", "4")
    val spark = builder.getOrCreate()

    val tableName = "testTable"
    val field = "testField"
    val tablePath = dbPath + "/" + tableName
    val query = spark.readStream.format("lakesoul")
      .option(LakeSoulOptions.PARTITION_DESC, "range = range1")
      .option(LakeSoulOptions.READ_START_TIME, "2022-01-01 15:15:15")
      .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
      .load(tablePath)

    getMaxRecord(query,tableName, field)
  }

  def getMaxRecord(query: DataFrame, table: String, field: String): String = {
    query.agg(functions.max(query(field)).as("max " + field)).toString()
  }

  def getSumRecord(query: DataFrame, table: String, field: String): String = {
    query.agg(functions.sum(query(field)).as("sum " + field)).toString()
  }
}
