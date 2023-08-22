// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.{MetaVersion, StreamingRecord}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.time.SpanSugar._
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import java.util.Locale

@RunWith(classOf[JUnitRunner])
class LakeSoulSinkSuite extends StreamTest with LakeSoulTestUtils {
  override val streamingTimeout = 60.seconds

  import testImplicits._


  private def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        f(file1, file2)
      }
    }
  }

  test("append mode") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[Int]
        val df = inputData.toDF()
        val query = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("lakesoul")
          .start(outputDir.getCanonicalPath)
        try {
          inputData.addData(1)
          query.processAllAvailable()

          val outputDf = spark.read.format("lakesoul").load(outputDir.getCanonicalPath)
          checkDatasetUnorderly(outputDf.as[Int], 1)

          val snapshotManagement = SnapshotManagement(outputDir.getCanonicalPath)
          //          val tableId = snapshotManagement.snapshot.getTableInfo.table_id
          //          var info = StreamingRecord.getStreamingInfo(tableId)
          //          assert(info._1.equals(query.id.toString) && info._2 == 0L)

          inputData.addData(2)
          query.processAllAvailable()

          checkDatasetUnorderly(outputDf.as[Int], 1, 2)
          //          info = StreamingRecord.getStreamingInfo(tableId)
          //          assert(info._1.equals(query.id.toString) && info._2 == 1L)

          inputData.addData(3)
          query.processAllAvailable()

          checkDatasetUnorderly(outputDf.as[Int], 1, 2, 3)

          //          info = StreamingRecord.getStreamingInfo(tableId)
          //          assert(info._1.equals(query.id.toString) && info._2 == 2L)
        } finally {
          query.stop()
        }
      }
    }
  }

  test("complete mode") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[Int]
        val df = inputData.toDF()
        val query =
          df.groupBy().count()
            .writeStream
            .outputMode("complete")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("lakesoul")
            .start(outputDir.getCanonicalPath)
        try {
          inputData.addData(1)
          query.processAllAvailable()

          val outputDf = spark.read.format("lakesoul").load(outputDir.getCanonicalPath)
          checkDatasetUnorderly(outputDf.as[Long], 1L)

          val snapshotManagement = SnapshotManagement(outputDir.getCanonicalPath)
          //          val tableId = snapshotManagement.snapshot.getTableInfo.table_id
          //          var info = StreamingRecord.getStreamingInfo(tableId)
          //          assert(info._1.equals(query.id.toString) && info._2 == 0L)

          inputData.addData(2)
          query.processAllAvailable()

          checkDatasetUnorderly(outputDf.as[Long], 2L)
          //          info = StreamingRecord.getStreamingInfo(tableId)
          //          assert(info._1.equals(query.id.toString) && info._2 == 1L)

          inputData.addData(3)
          query.processAllAvailable()

          checkDatasetUnorderly(outputDf.as[Long], 3L)
          //          info = StreamingRecord.getStreamingInfo(tableId)
          //          assert(info._1.equals(query.id.toString) && info._2 == 2L)
        } finally {
          query.stop()
        }
      }
    }
  }

  test("update mode: only supported with hash partition") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()

      val e = intercept[AnalysisException] {
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .outputMode("update")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("lakesoul")
          .start(outputDir.getCanonicalPath)
      }

      assert(e.getMessage().contains("only support Update output mode with hash partition"))

      val query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .outputMode("update")
          .option("hashPartitions", "id")
          .option("hashBucketNum", "2")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("lakesoul")
          .start(outputDir.getCanonicalPath)
      try {

        inputData.addData(1, 2, 3)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        val outputDf = spark.read.format("lakesoul").load(outputDir.getCanonicalPath)
          .select("id", "value")
        val expectedSchema = new StructType()
          .add(StructField("id", IntegerType, false))
          .add(StructField("value", IntegerType))
        assert(outputDf.schema === expectedSchema)



        // Verify the data is correctly read
        checkDatasetUnorderly(
          outputDf.as[(Int, Int)],
          (1, 1000), (2, 2000), (3, 3000))

        val snapshotManagement = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(outputDir.getCanonicalPath)).toString)
        val tableInfo = MetaVersion.getTableInfo(snapshotManagement.table_path)
        assert(tableInfo.hash_column.equals("id")
          && tableInfo.range_column.isEmpty
          && tableInfo.bucket_num == 2)

      } finally {
        if (query != null) {
          query.stop()
        }
      }
    }
  }


  test("path not specified") {
    failAfter(streamingTimeout) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[Int]
        val df = inputData.toDF()
        val e = intercept[IllegalArgumentException] {
          df.writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("lakesoul")
            .start()
        }
        Seq("path", " not specified").foreach { msg =>
          assert(e.getMessage.toLowerCase(Locale.ROOT).contains(msg))
        }
      }
    }
  }


  test("range partitioned writing and batch reading") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .partitionBy("id")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("lakesoul")
          .start(outputDir.getCanonicalPath)
      try {

        inputData.addData(1, 2, 3)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        val outputDf = spark.read.format("lakesoul").load(outputDir.getCanonicalPath)
          .select("id", "value")
        val expectedSchema = new StructType()
          .add(StructField("id", IntegerType, false))
          .add(StructField("value", IntegerType))
        assert(outputDf.schema === expectedSchema)



        // Verify the data is correctly read
        checkDatasetUnorderly(
          outputDf.as[(Int, Int)],
          (1, 1000), (2, 2000), (3, 3000))

        val snapshotManagement = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(outputDir.getCanonicalPath)).toString)
        val tableInfo = MetaVersion.getTableInfo(snapshotManagement.table_path)
        assert(tableInfo.range_column.equals("id")
          && tableInfo.hash_column.isEmpty
          && tableInfo.bucket_num == -1)

      } finally {
        if (query != null) {
          query.stop()
        }
      }
    }
  }

  test("range and hash partitioned writing and batch reading") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val query =
        ds.map(i => (i, i, i * 1000))
          .toDF("range", "hash", "value")
          .writeStream
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "2")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("lakesoul")
          .start(outputDir.getCanonicalPath)
      try {

        inputData.addData(1, 2, 2, 3)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        val outputDf = spark.read.format("lakesoul").load(outputDir.getCanonicalPath)
          .select("range", "hash", "value")
        val expectedSchema = new StructType()
          .add(StructField("range", IntegerType, false))
          .add(StructField("hash", IntegerType, false))
          .add(StructField("value", IntegerType))
        assert(outputDf.schema === expectedSchema)

        // Verify the data is correctly read
        checkDatasetUnorderly(
          outputDf.as[(Int, Int, Int)],
          (1, 1, 1000), (2, 2, 2000), (3, 3, 3000))

        val snapshotManagement = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(outputDir.getCanonicalPath)).toString)
        val tableInfo = MetaVersion.getTableInfo(snapshotManagement.table_path)
        assert(tableInfo.range_column.equals("range")
          && tableInfo.hash_column.equals("hash")
          && tableInfo.bucket_num == 2)

      } finally {
        if (query != null) {
          query.stop()
        }
      }
    }
  }


  test("work with aggregation + watermark") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Long]
      val inputDF = inputData.toDF.toDF("time")
      val outputDf = inputDF
        .selectExpr("CAST(time AS timestamp) AS timestamp")
        .withWatermark("timestamp", "10 seconds")
        .groupBy(window($"timestamp", "5 seconds"))
        .count()
        .select("window.start", "window.end", "count")

      val query =
        outputDf.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("lakesoul")
          .start(outputDir.getCanonicalPath)
      try {
        def addTimestamp(timestampInSecs: Int*): Unit = {
          inputData.addData(timestampInSecs.map(_ * 1L): _*)
          failAfter(streamingTimeout) {
            query.processAllAvailable()
          }
        }

        def check(expectedResult: ((Long, Long), Long)*): Unit = {
          val outputDf = spark.read.format("lakesoul").load(outputDir.getCanonicalPath)
            .selectExpr(
              "CAST(start as BIGINT) AS start",
              "CAST(end as BIGINT) AS end",
              "count")
          checkDatasetUnorderly(
            outputDf.as[(Long, Long, Long)],
            expectedResult.map(x => (x._1._1, x._1._2, x._2)): _*)
        }

        addTimestamp(100) // watermark = None before this, watermark = 100 - 10 = 90 after this
        addTimestamp(104, 123) // watermark = 90 before this, watermark = 123 - 10 = 113 after this

        addTimestamp(140) // wm = 113 before this, emit results on 100-105, wm = 130 after this
        check((100L, 105L) -> 2L, (120L, 125L) -> 1L) // no-data-batch emits results on 120-125

      } finally {
        if (query != null) {
          query.stop()
        }
      }
    }
  }


  test("throw exception when users are trying to write in batch with different partitioning") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .partitionBy("id")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("lakesoul")
          .start(outputDir.getCanonicalPath)
      try {

        inputData.addData(1, 2, 3)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        val e = intercept[AnalysisException] {
          spark.range(100)
            .select('id.cast("integer"), 'id % 4 as 'by4, 'id.cast("integer") * 1000 as 'value)
            .write
            .format("lakesoul")
            .partitionBy("id", "by4")
            .mode("append")
            .save(outputDir.getCanonicalPath)
        }
        assert(e.getMessage.contains("Range partition column `id` was already set"))

      } finally {
        query.stop()
      }
    }
  }

  test("incompatible schema merging throws errors - first streaming then batch") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .partitionBy("id")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("lakesoul")
          .start(outputDir.getCanonicalPath)
      try {

        inputData.addData(1, 2, 3)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        val e = intercept[AnalysisException] {
          spark.range(100).select('id, ('id * 3).cast("string") as 'value)
            .write
            .partitionBy("id")
            .format("lakesoul")
            .mode("append")
            .save(outputDir.getCanonicalPath)
        }
        assert(e.getMessage.contains("incompatible"))
      } finally {
        query.stop()
      }
    }
  }

  test("incompatible schema merging throws errors - first batch then streaming") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val dsWriter =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("lakesoul")
      spark.range(100).select('id, ('id * 3).cast("string") as 'value)
        .write
        .format("lakesoul")
        .mode("append")
        .save(outputDir.getCanonicalPath)

      val wrapperException = intercept[StreamingQueryException] {
        val q = dsWriter.start(outputDir.getCanonicalPath)
        inputData.addData(1, 2, 3)
        q.processAllAvailable()
      }
      assert(wrapperException.cause.isInstanceOf[AnalysisException])
      assert(wrapperException.cause.getMessage.contains("incompatible"))
    }
  }

  test("can't write out with all columns being partition columns") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .partitionBy("id", "value")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("lakesoul")
          .start(outputDir.getCanonicalPath)
      val e = intercept[StreamingQueryException] {
        inputData.addData(1)
        query.awaitTermination(10000)
      }
      assert(e.cause.isInstanceOf[AnalysisException]
        && e.getMessage.contains("Cannot use all columns for partition columns"))
    }
  }


}
