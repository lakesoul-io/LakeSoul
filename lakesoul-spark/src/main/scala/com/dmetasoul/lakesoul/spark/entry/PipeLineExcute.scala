/*
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.streaming.Trigger

object PipeLineExcute {
  def main(args: Array[String]): Unit = {
    val parameter = ParametersTool.fromArgs(args)
    val yamlPath = parameter.get(PipeLineOption.YamlPath, "./PipeLine.yml")
    val sparkSession = getSparkSession()
    val deployMode = sparkSession.sparkContext.getConf.get("spark.submit.deployMode")

    val pipeLineContainer = new PipelineParser().parserYaml(yamlPath, deployMode)

    buildStep(pipeLineContainer.getSteps, pipeLineContainer.getSink, sparkSession)
  }

  def buildStep(operators: java.util.List[Operator], sink: PipelineSink, sparkSession: SparkSession): Unit = {
    var preViewName = ""
    for (i <- 0 until operators.size) {
      val op = operators.get(i)
      if (null != op.getSourceTableName) {
        getSource(op.getSourceOption, op.getTableNameWithDatabase, sparkSession).createOrReplaceTempView(op.getSourceTableName)
        preViewName = ""
      } else {
        op.setSourceTableName(preViewName)
      }
      if (null != op.getSinkTableName) {
        toSink(sparkSession.sql(op.toSql), sink)
      } else {
        sparkSession.sql(op.toSql).createOrReplaceTempView(op.getViewName)
        preViewName = op.getViewName
      }
    }
  }

  def getSparkSession(): SparkSession = {
    val builder = SparkSession.builder()
//      .appName("STREAM PIPELINE")
 //     .config("spark.master","local[2]")
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 4)
      .config("spark.sql.parquet.mergeSchema", value = true)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .config("spark.default.parallelism", "4")
//      .config("spark.sql.warehouse.dir", "/tmp/lakesoul")
   //   .config("spark.files", "d:\\test.yml")
    builder.getOrCreate()

  }

  def getSource(sourceOption: SourceOption, sourceTableName: String, spark: SparkSession): DataFrame = {
    sourceOption.getProcessType match {
      case "batch" =>
        spark.read.format("lakesoul")
          .option(LakeSoulOptions.PARTITION_DESC, "")
          .option(LakeSoulOptions.READ_START_TIME, sourceOption.getReadStartTime)
          .option(LakeSoulOptions.READ_TYPE, sourceOption.getProcessType)
          .table(sourceTableName)
      case "stream" =>
        spark.readStream.format("lakesoul")
          .option(LakeSoulOptions.PARTITION_DESC, "")
          .option(LakeSoulOptions.READ_START_TIME, sourceOption.getReadStartTime)
          .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
          .table(sourceTableName)
    }
  }

  def toSink(sinkDF: DataFrame, sink: PipelineSink): Unit = {

    sink.getProcessType match {
      case "batch" =>
        sinkDF.write.format("lakesoul")
          .option(LakeSoulOptions.PARTITION_DESC, String.join(",", sink.getRangePartition))
          .option(LakeSoulOptions.HASH_PARTITIONS, String.join(",", sink.getHashPartition))
          .option(LakeSoulOptions.HASH_BUCKET_NUM, sink.getHashBucketNum)
          .option("path", sink.getSinkPath)
          .option("shortTableName", sink.getSinkTableName)
      case "stream" =>
        //todo failover情况，batch无法恢复，需要测试
        //        sinkDF.writeStream.format("console")
        //          .outputMode(sink.getOutputmode)
        //          .foreachBatch { (query: DataFrame, _: Long) => {
        //            query.show(1000)
        //          }
        //          }
        //          .trigger(Trigger.ProcessingTime(sink.getTriggerTime))
        //          .start()
        //          .awaitTermination()
        sinkDF.writeStream.format("lakesoul")
          .outputMode(sink.getOutputmode)
          .option("mergeSchema", "true")
          .option(SparkPipeLineOptions.CHECKPOINT_LOCATION, sink.getCheckpointLocation)
          .option(LakeSoulOptions.PARTITION_DESC,String.join(",", sink.getRangePartition))
          .option(LakeSoulOptions.HASH_PARTITIONS, String.join(",", sink.getHashPartition))
          .option(LakeSoulOptions.HASH_BUCKET_NUM, sink.getHashBucketNum)
          .option("path", sink.getSinkPath)
          .option("shortTableName", sink.getSinkTableName)
          .trigger(Trigger.ProcessingTime(sink.getTriggerTime))
          .start()
          .awaitTermination()
    }

  }

}

object PipeLineOption {
  val YamlPath = "yamlPath"
}