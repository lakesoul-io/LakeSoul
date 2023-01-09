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

  def main(args: Array[String]): Unit = {

    // all args will be organized into an Array,and filed will be an Array[String]
    val dbName = "test_cdc"
    val tableName = "mysql_test_1"
    val field = "type"
    val tablePath = "/tmp/lakesoul/" + dbName + "/" + tableName
    /** test table schema
     * +----+-------+------+---------+
     *  | id | name  | type | new_col |
     *  +----+-------+------+---------+
     *  | 1  | Peter | 10   | 8.8     |
     *  | 2  | Alice | 20   | 9.9     |
     *  | 3  | Dab   | 25   | 9.5     |
     *  | 4  | Smith | 16   | 9.2     |
     *  +----+-------+------+---------+
     * */

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

    val query = spark.readStream.format("lakesoul")
      //      .option(LakeSoulOptions.READ_START_TIME, "2022-01-01 15:15:15")
      .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
      .load(tablePath)

    getMaxRecord(query, tableName, field).show()
  }

  def getMaxRecord(query: DataFrame, table: String, field: String): DataFrame = {
    query.agg(functions.max(query(field)).as("max " + field))
  }

  def getSumRecord(query: DataFrame, table: String, field: String): String = {
    query.agg(functions.sum(query(field)).as("sum " + field)).toString()
  }
}
