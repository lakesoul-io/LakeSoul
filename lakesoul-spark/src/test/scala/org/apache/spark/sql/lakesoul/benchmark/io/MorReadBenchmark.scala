// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.benchmark.io

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf

object MorReadBenchmark {
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
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.default.parallelism", 8)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://lakesoul-test-bucket/datalake_table/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .config("spark.hadoop.fs.s3a.connection.maximum", 1000)

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, "true")

//    LakeSoulTable.registerMergeOperator(spark, "org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOpLong", "longSumMerge")
//    LakeSoulTable.registerMergeOperator(spark, "org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeNonNullOp", "stringNonNullMerge")
    val tablePath= "s3://lakesoul-test-bucket/datalake_table/test"
//    val table = LakeSoulTable.forPath(tablePath)

    if (args.length >= 2 ) {
      val is_gt = args(1)
      val tablePath = if (is_gt == "true") "s3://lakesoul-test-bucket/datalake_table/gt" else "s3://lakesoul-test-bucket/datalake_table/join"
      println(s"tablePath = $tablePath")
      val table = LakeSoulTable.forPath(tablePath)
      SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, "true")
      SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_READER_AWAIT_TIMEOUT.key, "60000")

      println(s"=====Reading with NATIVE_IO_ENABLE=true =====")

      spark.time({
        val path = "/tmp/result/ccf/result"
        println(s"writing local parquet in $path")
        table.toDF
          .write.parquet(path)
      })
    } else{
      println(s"=====Reading with native io=====")
      val table = LakeSoulTable.forPath(tablePath)
      SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, "true")
      SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_READER_AWAIT_TIMEOUT.key, "10000")
      spark.time({
        println("counting df")

        val rows = table.toDF.count()
        println(s"row count = $rows")
      })
      spark.time({
        println("writing noop")
        table.toDF
          //.withColumn("requests", expr("longSumMerge(requests)"))
          //        .withColumn("name", expr("stringNonNullMerge(name)"))
          .write.mode("Overwrite")
          .format("noop")
          .save()
      })
//      spark.time({
//        println("writing local parquet")
//        table.toDF
//          .withColumn("requests", expr("longSumMerge(requests)"))
//          //        .withColumn("name", expr("stringNonNullMerge(name)"))
//          .write.parquet("/tmp/result/ccf/")
//      })

      //    spark.time({
      //      val path = "/tmp/result/ccf/result"
      //      println(s"writing local parquet in $path")
      //      table.toDF
      //        .withColumn("requests", expr("longSumMerge(requests)"))
      ////        .withColumn("name", expr("stringNonNullMerge(name)"))
      ////        .select("uuid","name")
      ////        .where("uuid ='000007dc-d5fe-426a-acb8-dd5a5bfc042c'")
      ////        .show()
      //        .write.parquet(path)
      //    })

      println(s"=====Reading with parquet-mr=====")
      // spark parquet-mr read
      SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, "false")

      //    spark.time({
      //      println("counting df")
      //      //        .withColumn("requests", expr("longSumMerge(requests)"))
      //      //        .withColumn("name", expr("stringNonNullMerge(name)"))
      //      val rows = table.toDF.count()
      //      println(s"row count = $rows")
      //    })
      //    spark.time({
      //      println("writing noop")
      //      table.toDF
      //        .write.mode("Overwrite")
      //        .format("noop")
      //        .save()
      //    })
    }
  }
}
