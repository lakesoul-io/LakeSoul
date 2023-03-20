package org.apache.spark.sql.lakesoul.benchmark.io.concurrent

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import javolution.util.ReentrantLock
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.benchmark.io.concurrent.UpsertWrite.{dataPath1, dataPath2, dataPath3}
import org.apache.spark.sql.lakesoul.benchmark.io.deltaJoin.UpsertWriteWithJoin.upsertTableLeft
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf

object UpsertWriteAndJoin {
  val tablePathLeft = "s3://lakesoul-test-bucket/datalake_table/left"
  val tablePathRight = "s3://lakesoul-test-bucket/datalake_table/right"
  val tablePathJoin = "s3://lakesoul-test-bucket/datalake_table/join"
  val tablePathGt = "s3://lakesoul-test-bucket/datalake_table/gt"

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
  lazy private val lock = new ReentrantLock()

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

      val threadLeft = new UpsertThreadLeft(spark)
      val threadRight = new UpsertThreadRight(spark)
      threadLeft.start()
      threadRight.start()
      threadLeft.join()
      threadRight.join()

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

  class UpsertThreadLeft(spark: SparkSession) extends Thread {

    def doUpsert(path: String): Unit = {
      println(s"upsertTableLeft: $path")
      val deltaLeft = spark.read.parquet(path).selectExpr("uuid", "substring(uuid, 1) as pk")
      LakeSoulTable.forPath(spark, tablePathLeft).upsert(deltaLeft)
      val deltaJoin = broadcast(deltaLeft).join(LakeSoulTable.forPath(spark, tablePathRight).toDF, Seq("uuid"), "left_outer")
      lock.lock()
      try {
        LakeSoulTable.forPath(spark, tablePathJoin).upsert(deltaJoin)
      } finally {
        lock.unlock()
      }
    }

    override def run() {
      doUpsert(dataPath2)
      doUpsert(dataPath3)
      doUpsert(dataPath3)
    }
  }

  class UpsertThreadRight(spark: SparkSession) extends Thread {

    def doUpsert(path: String): Unit = {
      println(s"upsertTableRight: $path")
      val deltaRight = spark.read.parquet(path)
      LakeSoulTable.forPath(spark, tablePathRight).upsert(deltaRight)
      val deltaJoin = LakeSoulTable.forPath(spark, tablePathLeft).toDF.join(broadcast(deltaRight), Seq("uuid"), "inner")
      lock.lock()
      try {
        //        println(deltaJoin.queryExecution)
        LakeSoulTable.forPath(spark, tablePathJoin).upsert(deltaJoin)
      } finally {
        lock.unlock()
      }
    }

    override def run() {
      doUpsert(dataPath2)
      doUpsert(dataPath3)
      doUpsert(dataPath4)
    }
  }
}
