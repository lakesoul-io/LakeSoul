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

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.streaming.Trigger

import scala.collection.JavaConverters

object PipeLineExecute {
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
      if (null != op.getSourceTableName && "" != op.getSourceTableName) {
        getSource(op.getSourceOption, op.getTableNameWithDatabase, sparkSession).createOrReplaceTempView(op.getSourceTableName)
        preViewName = ""
      } else {
        op.setSourceTableName(preViewName)
      }
      if (null != op.getSinkTableName && "" != op.getSinkTableName) {
        println(op.toSql)
        toSink(sparkSession.sql(op.toSql), sink, op.getOperation.getProcessType, sparkSession)
      } else {
        println(op.toSql)
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
      .config("spark.sql.parquet.mergeSchema", value = true)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .config("spark.default.parallelism", 4)
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

  def toSink(sinkDF: DataFrame, sink: PipelineSink, processType: String, spark: SparkSession): Unit = {

    val namespace = sink.getSinkDatabaseName
    spark.sql("use " + namespace)

    LakeSoulCatalog.showCurrentNamespace()(0)
    spark.sessionState.catalogManager.currentNamespace(0)

    val rangePartition = if (sink.getRangePartition != null && sink.getRangePartition.size() > 0) String.join(",", sink.getRangePartition) else ""
    val hashPartition = if (sink.getHashPartition != null && sink.getHashPartition.size() > 0) String.join(",", sink.getHashPartition) else ""
    processType match {
      case "batch" =>
        sinkDF.write.format("lakesoul")
          .mode("overwrite")
          .option(LakeSoulOptions.PARTITION_DESC, rangePartition)
          .option(LakeSoulOptions.HASH_PARTITIONS, hashPartition)
          .option(LakeSoulOptions.HASH_BUCKET_NUM, sink.getHashBucketNum)
          .option("path", sink.getSinkPath)
          .option("shortTableName", sink.getSinkTableName)
          .save()
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
          .option(LakeSoulOptions.PARTITION_DESC, rangePartition)
          .option(LakeSoulOptions.HASH_PARTITIONS, hashPartition)
          .option(LakeSoulOptions.HASH_BUCKET_NUM, sink.getHashBucketNum)
          .option("path", sink.getSinkPath)
          .option("shortTableName", sink.getSinkTableName)
          .trigger(Trigger.ProcessingTime(sink.getTriggerTime))
          .start()
          .awaitTermination()
      case "distinctStreamBatch" =>
        assert(sink.getHashPartition != null && sink.getHashPartition.size() > 0, "distinct operate must set key column!")
        var firstBatch = true
        val distinctColumn = sink.getHashPartition.get(0)
        val groupByColumnList = sink.getHashPartition.subList(1, sink.getHashPartition.size())
        val groupByColumnSeq = JavaConverters.asScalaIterator(groupByColumnList.iterator()).toSeq.map(s => col(s))
        val timeInterval = sink.getIntervalTime
        var lastTime = System.currentTimeMillis()

        val tableName = sink.getSinkTableName
        val distinctTableName = tableName + "_distinct_" + distinctColumn
        val distinctTablePath = sink.getSinkPath + "_distinct_" + distinctColumn
        sinkDF.writeStream.foreachBatch {
          (batchDF: DataFrame, _: Long) => {
            if (!batchDF.rdd.isEmpty()) {
              batchDF.toDF().show()
              if (LakeSoulTable.isLakeSoulTable(distinctTablePath)) {
                val distinctTable = LakeSoulTable.forName(distinctTableName, namespace)
                distinctTable.upsert(batchDF)
              } else {
                batchDF.write
                  .mode("append")
                  .format("lakesoul")
                  .option("mergeSchema", "true")
                  .option(LakeSoulOptions.PARTITION_DESC, rangePartition)
                  .option(LakeSoulOptions.HASH_PARTITIONS, hashPartition)
                  .option(LakeSoulOptions.HASH_BUCKET_NUM, sink.getHashBucketNum)
                  .option("path", distinctTablePath)
                  .option("shortTableName", distinctTableName)
                  .save()
              }
              if (System.currentTimeMillis() - lastTime > timeInterval || firstBatch) {
                lastTime = System.currentTimeMillis()
                firstBatch = false
                val table = LakeSoulTable.forName(distinctTableName)
                table.toDF.show()
                val countResult = table.toDF.groupBy(groupByColumnSeq: _*).count().withColumnRenamed("count", "count_distinct_" + distinctColumn)
                countResult.toDF().show()
                countResult.write
                  .mode("overwrite")
                  .format("lakesoul")
                  .option("mergeSchema", "true")
                  .option(LakeSoulOptions.PARTITION_DESC, rangePartition)
                  .option(LakeSoulOptions.HASH_PARTITIONS, String.join(",", groupByColumnList))
                  .option(LakeSoulOptions.HASH_BUCKET_NUM, sink.getHashBucketNum)
                  .option("path", sink.getSinkPath)
                  .option("shortTableName", tableName)
                  .save()
              }
            }
          }
        }
          .option(SparkPipeLineOptions.CHECKPOINT_LOCATION, sink.getCheckpointLocation)
          .start()
          .awaitTermination()
    }

  }

}

object PipeLineOption {
  val YamlPath = "yamlPath"
}