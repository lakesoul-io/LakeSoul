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

import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.lakesoul.LakeSoulOptions.{READ_TYPE, ReadType}
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
    val readStartTime = parameter.get(SparkPipeLineOptions.READ_START_TIMESTAMP)
    val sourceType = parameter.get(SparkPipeLineOptions.SOURCE_TYPE)
    val sinkType = parameter.get(SparkPipeLineOptions.SINK_TYPE)
    val outputMode = parameter.get(SparkPipeLineOptions.OUTPUT_MODE)
    val processType = parameter.get(SparkPipeLineOptions.PROCESS_TYPE)
    val processFields = parameter.get(SparkPipeLineOptions.PROCESS_FIELDS)
    // unrequested parameter
    val partitionDesc = parameter.get(SparkPipeLineOptions.PARTITION_DESCRIBE, "")
    val hashPartitions = parameter.get(SparkPipeLineOptions.HASH_PARTITIONS_NAME)
    val hashBucketNum = parameter.getInt(SparkPipeLineOptions.HASH_BUCKET_NUMBER)
    // default parameter
    val checkpointLocation = SparkPipeLineOptions.CHECKPOINT_LOCATION

    val query = sourceFromDataSource(spark, sourceType, partitionDesc, readStartTime, ReadType.INCREMENTAL_READ, fromDataSourcePath, processType)
    //    query.createOrReplaceTempView("testView")
    //    val data = spark.sql("select hash,name,score from testView")
    val data = processDataFrameByOperators(query, "", processOperatorsAndFields(processFields))
    sinkToDataSource(data, sinkType, outputMode, checkpointLocation, partitionDesc, toDataSourcePath, processType, hashPartitions, hashBucketNum)
  }

  def processOperatorsAndFields(processFields: String): Map[String, String] = {
    var operators = Map[String, String]()
    val processFieldsSeq = processFields.split(";").toSeq
    processFieldsSeq.foreach(fieldCouple => {
      operators += (fieldCouple.split(":")(0) -> fieldCouple.split(":")(1))
    })
    operators
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
                       hashBucketNum: Int): Unit = {
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
          .option("checkpointLocation", checkpointLocation)
          .option(LakeSoulOptions.PARTITION_DESC, partitionDesc)
          .option(LakeSoulOptions.HASH_PARTITIONS, hashPartitions)
          .option(LakeSoulOptions.HASH_BUCKET_NUM, hashBucketNum)
          .option("path", toDataSourcePath)
          .trigger(Trigger.ProcessingTime(1000))
          .start().awaitTermination()
    }
  }


  /** eg：--operator:field  groupby:id;sum:score;max:score
   * */
  def processDataFrameByOperators(query: DataFrame, table: String, operators: Map[String, String]): DataFrame = {
    val ops = operators.keys.toSeq
    val fields = operators.values.toSeq
    ops(0) match {
      case "groupBy" => getComputeOps(getCombinationOps(query, ops(0), fields(0)), query, ops(1), fields(1))
    }
  }

  def getCombinationOps(query: DataFrame, op: String, field: String): RelationalGroupedDataset = {
    op match {
      case "groupBy" => query.groupBy(field)
    }
  }

  def getComputeOps(relationSet: RelationalGroupedDataset, query: DataFrame, op: String, field: String): DataFrame = {
    op match {
      case "sum" => relationSet.agg(sum(query(field).as("sum " + field)))
      case "max" => relationSet.agg(max(query(field).as("max " + field)))
      case "min" => relationSet.agg(min(query(field).as("min " + field)))
      case "avg" => relationSet.agg(avg(query(field).as("avg " + field)))
    }
  }
}

object SparkPipeLineOptions {
  var FROM_DATASOURCE_PATH = "sourcePath"
  var TO_DATASOURCE_PATH = "sinkPath"
  var CHECKPOINT_LOCATION = "file:///tmp/chk"
  var READ_START_TIMESTAMP = "readStartTime"
  var SOURCE_TYPE = "sourceType"
  var SINK_TYPE = "sinkType"
  var OUTPUT_MODE = "outputmode"
  var PROCESS_TYPE = "processType"
  var PROCESS_FIELDS = "fields"
  // selected parameter
  var PARTITION_DESCRIBE = "partitionDesc"
  var HASH_PARTITIONS_NAME = "hashPartition"
  var HASH_BUCKET_NUMBER = "hashBucketNum"
}
