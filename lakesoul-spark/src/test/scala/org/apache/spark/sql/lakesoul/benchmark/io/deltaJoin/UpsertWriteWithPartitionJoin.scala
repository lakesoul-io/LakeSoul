// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.benchmark.io.deltaJoin

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.functions.{col, lit}

object UpsertWriteWithPartitionJoin {
  val tablePathLeft = "s3://lakesoul-test-bucket/datalake_table/left"
  val tablePathRight = "s3://lakesoul-test-bucket/datalake_table/right"
  val tablePathJoin = "s3://lakesoul-test-bucket/datalake_table/join"
  val tablePathGt = "s3://lakesoul-test-bucket/datalake_table/gt"

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CCF BDCI 2022 DataLake Contest")
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
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://lakesoul-test-bucket/datalake_table/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .config("spark.hadoop.fs.s3a.connection.maximum", 400)

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, "true")
    SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_READER_AWAIT_TIMEOUT.key, "60000")

    val dataPath0 = "/opt/spark/work-dir/data/base-0.parquet"
    val dataPath1 = "/opt/spark/work-dir/data/base-1.parquet"
    val dataPath2 = "/opt/spark/work-dir/data/base-2.parquet"
    val dataPath3 = "/opt/spark/work-dir/data/base-3.parquet"
    val dataPath4 = "/opt/spark/work-dir/data/base-4.parquet"
    val dataPath5 = "/opt/spark/work-dir/data/base-5.parquet"
    val dataPath6 = "/opt/spark/work-dir/data/base-6.parquet"
    val dataPath7 = "/opt/spark/work-dir/data/base-7.parquet"
    val dataPath8 = "/opt/spark/work-dir/data/base-8.parquet"
    val dataPath9 = "/opt/spark/work-dir/data/base-9.parquet"
    val dataPath10 = "/opt/spark/work-dir/data/base-10.parquet"
    val DeltaDataPathList = scala.util.Random.shuffle(List(
      "l2" -> dataPath4,
      "l2" -> dataPath2,
      "l1" -> dataPath3,
      "r2" -> dataPath6,
      "r1" -> dataPath4,
      "r2" -> dataPath3,
      "r1" -> dataPath5,
    ))


    spark.time({
      spark.read.format("parquet").load(dataPath1).selectExpr("uuid", "substring(uuid, 1) as pk").select(col("uuid"), col("pk"), lit("1").as("rangeA"))
        .write.format("lakesoul")
        .option("rangePartitions", "rangeA")
        .option("hashPartitions", "pk")
        .option("hashBucketNum", 4)
        .partitionBy("rangeA")
        .mode("Overwrite").save(tablePathLeft)

      spark.read.format("parquet").load(dataPath1).select(col("uuid"), col("ip"), col("requests"), col("city"), lit("1").as("rangeB"))
        .write.format("lakesoul")
        .option("rangePartitions", "rangeB")
        .option("hashPartitions", "uuid")
        .option("hashBucketNum", 4)
        .partitionBy("rangeB")
        .mode("Overwrite").save(tablePathRight)

      val df = LakeSoulTable.forPath(tablePathLeft).toDF.join(LakeSoulTable.forPath(tablePathRight).toDF, Seq("uuid"), "left_outer")
      df
        .write.format("lakesoul")
        .option("rangePartitions", "rangeA")
        .option("hashPartitions", "pk")
        .option("hashBucketNum", 4)
        .partitionBy("rangeA")
        .mode("Overwrite").save(tablePathJoin)

      DeltaDataPathList.foreach(tup =>
        if (tup._1 == "r1") {
          val df = upsertTableRightRangeOne(spark, tup._2)
          joinLeftTablePartitionOne(df)
        } else if (tup._1 == "r2") {
          val df = upsertTableRightRangeTwo(spark, tup._2)
          joinLeftTablePartitionTwo(df)
        } else if (tup._1 == "l1") {
          val df = upsertTableLeftRangeOne(spark, tup._2)
          joinRightTablePartitionOne(df)
        } else {
          val df = upsertTableLeftRangeTwo(spark, tup._2)
          joinRightTablePartitionTwo(df)
        }
      )

      val rightDf1 = upsertTableRightRangeTwo(spark, dataPath6)
      val leftDf1 = upsertTableLeftRangeTwo(spark, dataPath7)
      joinRightTablePartitionTwo(leftDf1)
      joinLeftTablePartitionTwo(rightDf1)

      println("saving gt")
      val gt1 = LakeSoulTable.forPath(tablePathLeft).toDF.filter("rangeA=1").join(LakeSoulTable.forPath(tablePathRight).toDF.filter("rangeB=1"), Seq("uuid"), "left_outer")
      val gt2 = LakeSoulTable.forPath(tablePathLeft).toDF.filter("rangeA=2").join(LakeSoulTable.forPath(tablePathRight).toDF.filter("rangeB=2"), Seq("uuid"), "left_outer")
      val gt  = gt1.union(gt2)
      gt.write.format("lakesoul")
        .option("rangePartitions", "rangeA")
        .option("hashPartitions", "pk")
        .option("hashBucketNum", 4)
        .partitionBy("rangeA")
        .mode("Overwrite").save(tablePathGt)
      println("saving gt done")
    })
  }

  private def upsertTableRightRangeOne(spark: SparkSession, path: String): DataFrame = {
    println(s"upsertTableRightRange1: $path")
    val deltaRight = spark.read.parquet(path).select(col("uuid"), col("ip"), col("requests"), col("city"), lit("1").as("rangeB"))
    LakeSoulTable.forPath(tablePathRight).upsert(deltaRight)
    deltaRight
  }

  private def upsertTableRightRangeTwo(spark: SparkSession, path: String): DataFrame = {
    println(s"upsertTableRightRange2: $path")
    val deltaRight = spark.read.parquet(path).select(col("uuid"), col("ip"), col("requests"), col("city"), lit("2").as("rangeB"))
    LakeSoulTable.forPath(tablePathRight).upsert(deltaRight)
    deltaRight
  }

  private def upsertTableLeftRangeOne(spark: SparkSession, path: String): DataFrame = {
    println(s"upsertTableLeftRange1: $path")
    val deltaLeft = spark.read.parquet(path).selectExpr("uuid", "substring(uuid, 1) as pk").select(col("uuid"), col("pk"), lit("1").as("rangeA"))
    LakeSoulTable.forPath(tablePathLeft).upsert(deltaLeft)
    deltaLeft
  }

  private def upsertTableLeftRangeTwo(spark: SparkSession, path: String): DataFrame = {
    println(s"upsertTableLeftRange2: $path")
    val deltaLeft = spark.read.parquet(path).selectExpr("uuid", "substring(uuid, 1) as pk").select(col("uuid"), col("pk"), lit("2").as("rangeA"))
    LakeSoulTable.forPath(tablePathLeft).upsert(deltaLeft)
    deltaLeft
  }

  private def joinLeftTablePartitionOne(deltaRightDF: DataFrame): Unit = {
    LakeSoulTable.forPath(tablePathJoin).upsertOnJoinKey(deltaRightDF, Seq("uuid"), Seq("rangeA=1"))
  }

  private def joinLeftTablePartitionTwo(deltaRightDF: DataFrame): Unit = {
    LakeSoulTable.forPath(tablePathJoin).upsertOnJoinKey(deltaRightDF, Seq("uuid"), Seq("rangeA=2"))
  }

  private def joinRightTablePartitionOne(deltaLeftDF: DataFrame): Unit = {
    LakeSoulTable.forPath(tablePathJoin).joinWithTablePathsAndUpsert(deltaLeftDF, Seq(tablePathRight), Seq(Seq("rangeB=1")))
  }

  private def joinRightTablePartitionTwo(deltaLeftDF: DataFrame): Unit = {
    LakeSoulTable.forPath(tablePathJoin).joinWithTablePathsAndUpsert(deltaLeftDF, Seq(tablePathRight), Seq(Seq("rangeB=2")))
  }
}

