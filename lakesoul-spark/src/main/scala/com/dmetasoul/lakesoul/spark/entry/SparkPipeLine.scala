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
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._


object SparkPipeLine {

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
      .config("park.sql.warehouse.dir", "/tmp/lakesoul")
    val spark = builder.getOrCreate()

    val parameter = ParametersTool.fromArgs(args)
    val fromDataSourcePath = parameter.get(SparkPipeLineOptions.FROM_DATASOURCE_PATH)
    val toDataSourcePath = parameter.get(SparkPipeLineOptions.TO_DATASOURCE_PATH)
    val readStartTime = parameter.get(SparkPipeLineOptions.READ_START_TIMESTAMP, "1970-01-01 00:00:00")
    val sourceType = parameter.get(SparkPipeLineOptions.SOURCE_TYPE, "lakesoul")
    val sinkType = parameter.get(SparkPipeLineOptions.SINK_TYPE, "lakesoul")
    val outputMode = parameter.get(SparkPipeLineOptions.OUTPUT_MODE, "complete")
    val processType = parameter.get(SparkPipeLineOptions.PROCESS_TYPE, "stream")
    val processFields = parameter.get(SparkPipeLineOptions.PROCESS_FIELDS)
    val hashPartitions = parameter.get(SparkPipeLineOptions.HASH_PARTITIONS_NAME)
    // unrequested parameter
    val partitionDesc = parameter.get(SparkPipeLineOptions.PARTITION_DESCRIBE, "")
    val hashBucketNum = parameter.getInt(SparkPipeLineOptions.HASH_BUCKET_NUMBER, 2)
    val checkpointLocation = parameter.get(SparkPipeLineOptions.CHECKPOINT_LOCATION, "file:///tmp/chk")
    val triggerTime = parameter.getLong(SparkPipeLineOptions.TRIGGER_TIME, 2000)
    val sinkTableName = parameter.get(SparkPipeLineOptions.SINK_TABLE_NAME, "")

    val query = sourceFromDataSource(spark, sourceType, partitionDesc, readStartTime, ReadType.INCREMENTAL_READ, fromDataSourcePath, processType)
    query.createOrReplaceTempView("testView")
    /** process operators and fields
     * eg：--operator:field  groupby:id;sum:score;max:score
     * */
    val processFieldsSeq = processFields.split(";").toSeq
    var groupby = ""
    var aggs = ""
    for (i <- 0 to processFieldsSeq.length - 1) {
      val op = processFieldsSeq(i).split(":")(0)
      val field = processFieldsSeq(i).split(":")(1)
      op match {
        case "groupBy" => groupby += "group by " + field
        case "sum" => aggs += "sum(" + field + ") as sum,"
        case "max" => aggs += "max(" + field + ") as max,"
        case "min" => aggs += "min(" + field + ") as min,"
        case "avg" => aggs += "avg(" + field + ") as avg,"
      }
    }
    val sqlString = "select " + aggs + hashPartitions + " from testView " + groupby
    val query1 = spark.sql(sqlString)
    sinkToDataSource(query1, sinkType, outputMode, checkpointLocation, partitionDesc, toDataSourcePath, processType, hashPartitions, hashBucketNum, triggerTime, sinkTableName)
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
                       sinkSource: String,
                       outputMode: String,
                       checkpointLocation: String,
                       partitionDesc: String,
                       toDataSourcePath: String,
                       processType: String,
                       hashPartitions: String,
                       hashBucketNum: Int,
                       triggerTime: Long,
                       sinkTableName: String): Unit = {
    processType match {
      case "batch" =>
        query.write.format(sinkSource)
          .option(LakeSoulOptions.PARTITION_DESC, partitionDesc)
          .option(LakeSoulOptions.HASH_PARTITIONS, hashPartitions)
          .option(LakeSoulOptions.HASH_BUCKET_NUM, hashBucketNum)
          .option("path", toDataSourcePath)
      case "stream" =>
        query.writeStream.format(sinkSource)
          .outputMode(outputMode)
          .option("mergeSchema", "true")
          .option(SparkPipeLineOptions.CHECKPOINT_LOCATION, checkpointLocation)
          .option(LakeSoulOptions.PARTITION_DESC, partitionDesc)
          .option(LakeSoulOptions.HASH_PARTITIONS, hashPartitions)
          .option(LakeSoulOptions.HASH_BUCKET_NUM, hashBucketNum)
          .option("path", toDataSourcePath)
          .option(SparkPipeLineOptions.SINK_TABLE_NAME, sinkTableName)
          .trigger(Trigger.ProcessingTime(triggerTime))
          .start().awaitTermination()
    }
  }

  def getComputeDataFrame(query: DataFrame, op: String, field: String): DataFrame = {
    op match {
      case "sum" => query.agg(sum(query(field).as("sum " + field)))
      case "max" => query.agg(max(query(field).as("max " + field)))
      case "min" => query.agg(min(query(field).as("min " + field)))
      case "avg" => query.agg(avg(query(field).as("avg " + field)))
    }
  }
}

object SparkPipeLineOptions {
  val FROM_DATASOURCE_PATH = "sourcePath"
  val TO_DATASOURCE_PATH = "sinkPath"
  val CHECKPOINT_LOCATION = "checkpointLocation"
  val READ_START_TIMESTAMP = "readStartTime"
  val SOURCE_TYPE = "sourceType"
  val SINK_TYPE = "sinkType"
  val OUTPUT_MODE = "outputmode"
  val PROCESS_TYPE = "processType"
  val PROCESS_FIELDS = "fields"
  // selected parameter
  val PARTITION_DESCRIBE = "partitionDesc"
  val HASH_PARTITIONS_NAME = "hashPartition"
  val HASH_BUCKET_NUMBER = "hashBucketNum"
  val TRIGGER_TIME = "trigger_time"
  val SINK_TABLE_NAME = "sinkTableName"
}
