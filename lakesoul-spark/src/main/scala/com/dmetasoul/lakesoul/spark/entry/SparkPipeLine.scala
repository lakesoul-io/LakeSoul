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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.lakesoul.LakeSoulOptions.{READ_TYPE, ReadType}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._


object SparkPipeLine {

  val fromDataSourcePath = "/tmp/lakesoul/test_cdc/mysql_test_2"
  val toDataSourcePath = "file:///home/yongpeng/result"
  val checkpointLocation = "file:///home/yongpeng/chk"
  val partitionDesc = ""
  val readStartTime = "2023-01-01 15:20:15"
  val sourceType = "lakesoul"
  val outputMode = "update"
  val processType = ProcessOptions.STREAM
  val field = "id"

  def main(args: Array[String]): Unit = {

    /** test table
     * +-------+----------+-------+-------+
     * | name  | phone_no | score | hash  |
     * +-------+----------+-------+-------+
     * | Alice | 20       | 9.9   | hash1 |
     * | Bob   | 10010    | 9.8   | hash2 |
     * | Carl  | 1020     | 9.4   | hash3 |
     * | Smith | 8820     | 9.2   | hash4 |
     * | Bob   | 9988     | 8.9   | hash5 |
     * +-------+----------+-------+-------+
     * primaryKey is hash
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
      .config("park.sql.warehouse.dir", "/tmp/lakesoul")
    val spark = builder.getOrCreate()
    val query = sourceFromDataSource(spark, sourceType, partitionDesc, readStartTime, ReadType.INCREMENTAL_READ, fromDataSourcePath, processType)
    //    query.createOrReplaceTempView("testView")
    //    val data = spark.sql("select hash,score from testView")
    val operators = Map("groupBy" -> "hash", "sum" -> "score","max" -> "score")
    val data = processDataFrameByOperators(query, "", operators)

    sinkToDataSource(data, sourceType, outputMode, checkpointLocation, partitionDesc, toDataSourcePath, processType)
  }

  def sourceFromDataSource(spark: SparkSession,
                           source: String,
                           partitionDesc: String,
                           readStartTime: String,
                           readType: String,
                           fromDataSourcePath: String,
                           processType: String): DataFrame = {
    processType match {
      case "batch" => spark.read.format(source)
        .option(LakeSoulOptions.PARTITION_DESC, partitionDesc)
        .option(LakeSoulOptions.READ_START_TIME, readStartTime)
        .option(LakeSoulOptions.READ_TYPE, readType)
        .load(fromDataSourcePath)

      case "stream" => spark.readStream.format(source)
        .option(LakeSoulOptions.PARTITION_DESC, partitionDesc)
        .option(LakeSoulOptions.READ_START_TIME, readStartTime)
        .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
        .load(fromDataSourcePath)
    }
  }

  def sinkToDataSource(query: DataFrame,
                       source: String,
                       outputMode: String,
                       checkpointLocation: String,
                       partitionDesc: String,
                       toDataSourcePath: String,
                       processType: String): Unit = {
    processType match {
      case "batch" =>
        query.write.format(source)
          .option(LakeSoulOptions.PARTITION_DESC, partitionDesc)
          .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
          .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
          .option("path", toDataSourcePath)
      case "stream" =>
        query.writeStream.format(source)
          .outputMode(outputMode)
          .option("checkpointLocation", checkpointLocation)
          .option(LakeSoulOptions.PARTITION_DESC, partitionDesc)
          .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
          .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
          .option("path", toDataSourcePath)
          .trigger(Trigger.Once())
          .start().awaitTermination()
    }
  }


  /** --operators groupby:op;max:id
   * */
  def processDataFrameByOperators(query: DataFrame, table: String, operators: Map[String, String]): DataFrame = {
    val ops = operators.keys.toSeq
    val fields = operators.values.toSeq
    ops(0) match {
      case "groupBy" => query.groupBy(fields(0)).agg(sum(query(fields(1))).as(getComputeOps(ops(1)) + fields(1))
        ,max(query(fields(2))).as(getComputeOps(ops(2)) + fields(2)))

      case "orderBy" => query.orderBy(fields(0)).agg(sum(query(fields(1))).as(getComputeOps(ops(1)) + fields(1)))
    }
  }

  def getComputeOps(ops: String): String = {
    ops match {
      case "sum" => "sum"
      case "max" => "max"
      case "min" => "min"
      case "avg" => "avg"
    }
  }
}

object ProcessOptions {
  val BATCH = "batch"
  val STREAM = "stream"
}
