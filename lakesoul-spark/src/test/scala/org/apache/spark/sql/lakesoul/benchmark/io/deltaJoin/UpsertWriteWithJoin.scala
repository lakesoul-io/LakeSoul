// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.benchmark.io.deltaJoin

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf

object UpsertWriteWithJoin {

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
    SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_READER_AWAIT_TIMEOUT.key, "300000")


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
      "l"->dataPath4,
      "l"->dataPath2, "l"->dataPath3,
//      "r"->dataPath1,
      "r"->dataPath4,
      "r"->dataPath3,
      "r"->dataPath2,
//      dataPath4, dataPath5, dataPath6, dataPath7, dataPath8, dataPath9, dataPath10
    ))

    spark.time({
      spark.read.format("parquet").load(dataPath1)
        .write.format("lakesoul")
        .option("hashPartitions", "uuid")
        .option("hashBucketNum", 4)
        .mode("Overwrite").save(tablePathRight)

      spark.read.format("parquet").load(dataPath1).selectExpr("uuid", "substring(uuid, 1) as pk")
        .write.format("lakesoul")
        .option("hashPartitions", "pk")
        .option("hashBucketNum", 4)
        .mode("Overwrite").save(tablePathLeft)

      LakeSoulTable.forPath(tablePathLeft).toDF.join(LakeSoulTable.forPath(tablePathRight).toDF, Seq("uuid"), "left_outer").repartition(1)
        .write.format("lakesoul")
        .option("hashPartitions", "pk")
        .option("hashBucketNum", 4)
        .mode("Overwrite").save(tablePathJoin)

      // Code for concurrent test, left and right table will upsert concurrently,
      // which means one side can upsert itself when another is upserting, after both sides finish upsert, update JoinTable
      val deltaDF0 = upsertTableRight(spark, dataPath2)
      val deltaDF1 = upsertTableLeft(spark, dataPath2)
      val deltaDF2 = upsertTableRight(spark, dataPath3)
      val deltaDF3 = upsertTableLeft(spark, dataPath3)
      joinLeftTable(deltaDF0)
      joinRightTable(deltaDF1)
      joinLeftTable(deltaDF2)
      joinRightTable(deltaDF3)

      val deltaDF4 = upsertTableLeft(spark, dataPath4)
      val deltaDF5 = upsertTableRight(spark, dataPath5)
      val deltaDF6 = upsertTableRight(spark, dataPath4)
      joinLeftTable(deltaDF5)
      joinLeftTable(deltaDF6)
      joinRightTable(deltaDF4)

      val deltaDF7 = upsertTableRight(spark, dataPath8)
      val deltaDF8 = upsertTableLeft(spark, dataPath7)
      joinLeftTable(deltaDF7)
      joinRightTable(deltaDF8)
      val deltaDF9 = upsertTableLeft(spark, dataPath6)
      val deltaDF10 = upsertTableRight(spark, dataPath9)
      joinLeftTable(deltaDF10)
      joinRightTable(deltaDF9)

      val deltaDF11 = upsertTableLeft(spark, dataPath10)
      joinRightTable(deltaDF11)

      val deltaDF12 = upsertTableRight(spark, dataPath7)
      joinLeftTable(deltaDF12)

      val deltaDF13 = upsertTableRight(spark, dataPath10)
      val deltaDF14 = upsertTableLeft(spark, dataPath8)
      joinLeftTable(deltaDF13)
      joinRightTable(deltaDF14)

      val deltaDF15 = upsertTableLeft(spark, dataPath5)
      val deltaDF16 = upsertTableRight(spark, dataPath6)
      val deltaDF17 = upsertTableLeft(spark, dataPath9)
      joinRightTable(deltaDF15)
      joinRightTable(deltaDF17)
      joinLeftTable(deltaDF16)

      println("saving gt")

      val gt = LakeSoulTable.forPath(tablePathLeft).toDF.join(LakeSoulTable.forPath(tablePathRight).toDF, Seq("uuid"), "left_outer").repartition(1)
      println(gt.queryExecution)
      gt.write.format("lakesoul")
        .option("hashPartitions", "pk")
        .option("hashBucketNum", 4)
        .mode("Overwrite").save(tablePathGt)

      println("saving gt done")
    })
  }


  private def upsertTableRight(spark: SparkSession, path: String): DataFrame = {
    println(s"upsertTableRight: $path")
    val deltaRight = spark.read.parquet(path)
    LakeSoulTable.forPath(tablePathRight).upsert(deltaRight)
    deltaRight
  }

  private def upsertTableLeft(spark: SparkSession, path: String): DataFrame = {
    println(s"upsertTableLeft: $path")
    val deltaLeft = spark.read.parquet(path).selectExpr("uuid", "substring(uuid, 1) as pk")
    LakeSoulTable.forPath(tablePathLeft).upsert(deltaLeft)
    deltaLeft
  }

  private def joinLeftTable(deltaRightDF: DataFrame): Unit = {
    LakeSoulTable.forPath(tablePathJoin).upsertOnJoinKey(deltaRightDF, Seq("uuid"))
  }

  private def joinRightTable(deltaLeftDF: DataFrame): Unit = {
    LakeSoulTable.forPath(tablePathJoin).joinWithTablePathsAndUpsert(deltaLeftDF, Seq(tablePathRight))
  }

}
