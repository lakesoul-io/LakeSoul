// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.benchmark.io

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf

/**
 * Run with following commands with local minio env:
 *
 * mvn package -Prelease-linux-x86-64 -pl lakesoul-spark -am -DskipTests
 * docker run --cpus=1 --rm -ti --net host -v /opt/spark/work-dir/data:/opt/spark/work-dir/data -v $PWD/lakesoul-spark/target:/opt/spark/work-dir/jars bitnami/spark:3.3.1 spark-submit --driver-memory 4g --jars /opt/spark/work-dir/jars/lakesoul-spark-2.4.0-spark-3.3-SNAPSHOT.jar --class org.apache.spark.sql.lakesoul.benchmark.io.ParquetScanBenchmark /opt/spark/work-dir/jars/lakesoul-spark-2.4.0-spark-3.3-SNAPSHOT-tests.jar --localtest
 */
object ParquetScanBenchmark {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("ParquetScanBenchmark")
      .master("local[1]")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a_staging")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3.buffer.dir", "/tmp")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
      .config("spark.hadoop.fs.s3a.fast.upload", value = true)
      .config("spark.hadoop.fs.s3a.threads.max", value = 2)
      .config("spark.hadoop.fs.s3a.multipart.size", 33554432)
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.sql.files.maxPartitionBytes", "2g")
      .config("spark.default.parallelism", 1)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://lakesoul-test-bucket/data/benchmark")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")

    var bucketName = "lakesoul-test-bucket"
    if (args.length >= 1 && args(0) == "--localtest") {
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")
    } else {
      if (args.length >= 1 && args(0) == "--bucketname") {
        bucketName = args(1)
      }
    }

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val dataPath0 = "/opt/spark/work-dir/data/base-0.parquet"
    val tablePath = s"s3://$bucketName/data/benchmark/parquet-scan"
    println(s"tablePath: $tablePath")

    var tableExist = true
    try {
      val _ = LakeSoulTable.forPath(tablePath)
      tableExist = true
    } catch {
      case _: Throwable => tableExist = false
    }

    if (!tableExist) {
      println(s"LakeSoul table not exist, upload from local file")
      val df = spark.read.format("parquet").load(dataPath0).repartition(1)
      df.write.format("lakesoul")
        .mode("Overwrite").save(tablePath)
    }

    println(s"Reading with parquet-mr")
    // spark parquet-mr read
    SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, "false")
    spark.time({
      spark.read.format("lakesoul").load(tablePath).write.format("noop").mode("Overwrite").save()
    })
    println(s"Reading with native io")
    SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, "true")
    spark.time({
      spark.read.format("lakesoul").load(tablePath).write.format("noop").mode("Overwrite").save()
    })
  }
}
