// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.benchmark

import com.dmetasoul.lakesoul.spark.ParametersTool
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.SparkSession

/**
 * this class is used to check flink write data: with the same two data, one write to csv, the other write to LakeSoul.
 */
object FlinkWriteDataCheck {

  var csvPath = "file:///tmp/csv/"
  var lakeSoulPath = "file:///tmp/lakesoul/"
  var serverTimeZone = "UTC"

  val printLine = " ******** "

  /**
   * param example:
   * --csv.path file:///tmp/csv/
   * --lakesoul.table.path file:///tmp/lakesoul/
   * --server.time.zone UTC
   */
  def main(args: Array[String]): Unit = {
    val parameter = ParametersTool.fromArgs(args)
    csvPath = parameter.get("csv.path", "file:///tmp/csv/")
    lakeSoulPath = parameter.get("lakesoul.table.path", "file:///tmp/lakesoul/")
    serverTimeZone = parameter.get("server.time.zone", serverTimeZone)

    val builder = SparkSession.builder()
      .appName("FLINK_DATA_CHECK")
      .master("local[4]")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a_staging")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3.buffer.dir", "/opt/spark/work-dir/s3")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
      .config("spark.hadoop.fs.s3a.fast.upload", value = true)
      .config("spark.hadoop.fs.s3a.multipart.size", 67108864)
      .config("spark.hadoop.fs.s3a.connection.maximum", 100)
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
      .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://lakesoul-test-bucket/")
      .config("spark.sql.session.timeZone", serverTimeZone)
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .config("spark.default.parallelism", "16")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val lakeSoulTablePath = SparkUtil.makeQualifiedTablePath(new Path(lakeSoulPath)).toString
    val csvTablePath = SparkUtil.makeQualifiedTablePath(new Path(csvPath)).toString

    val lakeSoulDF = LakeSoulTable.forPath(lakeSoulTablePath).toDF
    val csvDF = spark.read.schema(lakeSoulDF.schema).format("parquet").load(csvTablePath)

    val diff1 = lakeSoulDF.rdd.subtract(csvDF.rdd)
    val result = lakeSoulDF.count() == csvDF.count() && diff1.count() == 0

    if (!result) {
      println(printLine)
      println(s"CSV count ${csvDF.count()}, LakeSoul count ${lakeSoulDF.count()}")
      println("*************diff1**************")
      spark.createDataFrame(diff1, lakeSoulDF.schema).show()
      println("data verification ERROR!!!")
      System.exit(1)
    } else {
      println(printLine + "data verification SUCCESS!!!" + printLine)
    }
  }
}
