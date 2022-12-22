/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.arrow.lakesoul.io.NativeIOWrapper
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{AnalysisException, QueryTest, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf.NATIVE_IO_ENABLE
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession, LakeSoulTestUtils}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch, NativeIOUtils}
import org.scalatest.BeforeAndAfter

import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.language.implicitConversions



trait NativeIOReaderTests
  extends QueryTest
    with SharedSparkSession
    with LakeSoulTestUtils
    with BeforeAndAfter {

  import testImplicits._



  before{
    LakeSoulCatalog.cleanMeta()
  }

  val testSrcFilePath =
    "/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
//    "s3a://lakesoul-test-s3/part-00002-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
  val testDeltaFilePath = "/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
  val testParquetRowCount = 1000
  val testParquetColNum = 12
  val testHashBucketNum = String.valueOf(1)



  test("[NativeIOWrapper]Read local file") {
    val wrapper = new NativeIOWrapper()
    wrapper.initialize()
    wrapper.addFile("/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet")
//    wrapper.setThreadNum(2)
    wrapper.setBatchSize(10)
    wrapper.setBufferSize(1)
    wrapper.createReader()
    wrapper.startReader(_ => {})
    val reader = LakeSoulArrowReader(
      wrapper = wrapper
    )
    var cnt = 0
    while (reader.hasNext) {
      val result = reader.next()
      result match {
        case Some(vsr) =>
          assert(vsr.getFieldVectors.size()==testParquetColNum)
          val vectors = NativeIOUtils.asArrayColumnVector(vsr)

          val batch = {
            new ColumnarBatch(vectors, vsr.getRowCount)
          }
          assert(batch.numRows() == 10)
          cnt += 1
          println(cnt)
        case None =>
//          assert(false)
      }
    }
  }

  test("[NativeIOWrapper]Read s3 file") {
    val wrapper = new NativeIOWrapper()
    wrapper.initialize()
    wrapper.addFile("s3://lakesoul-test-s3/large_file.parquet")
    wrapper.setThreadNum(2)
    wrapper.setBatchSize(8192)
    wrapper.setBufferSize(1)
    wrapper.setObjectStoreOptions("minioadmin1", "minioadmin1", "us-east-1", "lakesoul-test-s3", "http://localhost:9002")
    wrapper.createReader()
    wrapper.startReader(_ => {})
    val reader = LakeSoulArrowReader(
      wrapper = wrapper
    )
    var cnt = 0
    while (reader.hasNext) {
      val result = reader.next()
      result match {
        case Some(vsr) =>
          assert(vsr.getFieldVectors.size()==31)
          val vectors = NativeIOUtils.asArrayColumnVector(vsr)

          val batch = {
            new ColumnarBatch(vectors, vsr.getRowCount)
          }
          assert(batch.numRows() <= 8192)
          cnt += 1
          println(cnt)
        case None =>
        //          assert(false)
      }
    }
  }

  test("[Small file test]without hash_key and range_key") {
    withTempDir { dir =>
        val tablePath = dir.toString
        val df = spark
          .read
          .format("parquet")
          .load(testSrcFilePath)
          .toDF()
        df
          .write
          .format("lakesoul")
          .mode("Overwrite")
          .save(tablePath)

//        assert(spark.read.format("lakesoul").load(tablePath).schema.size == testParquetColNum)
//        assert(spark.read.format("lakesoul").load(tablePath).count() == testParquetRowCount)
      val native_df = spark.read.format("lakesoul").load(tablePath).toDF().collect()
      println("load native_df done")
      assert(native_df.head.length==testParquetColNum)
      assert(native_df.length==testParquetRowCount)
      spark.sessionState.conf.setConf(NATIVE_IO_ENABLE, false)
      val orig_df = spark.read.format("lakesoul").load(tablePath).toDF().collect()
      println("load orig_df done")
      orig_df
        .zipAll(native_df, InternalRow(), InternalRow())
        .foreach(zipped=>assert(zipped._1 == zipped._2))
      spark.sessionState.conf.setConf(NATIVE_IO_ENABLE, true)

    }
  }


  test("[Small file test]with hash_key and range_key") {
    withTempDir { dir =>
      val tablePath = "s3a://lakesoul-test-s3/" + dir.getName
      val df = spark
        .read
        .format("parquet")
        .load(testSrcFilePath)
        .toDF()
      println(spark.sessionState.conf.getConfString("spark.hadoop.fs.s3a.aws.credentials.provider"))
      df
        .write
        .format("lakesoul")
        .mode("Overwrite")
        .option("rangePartitions","gender")
        .option("hashPartitions","id")
        .option("hashBucketNum",2)
        .save(tablePath)

      //        assert(spark.read.format("lakesoul").load(tablePath).schema.size == testParquetColNum)
      //        assert(spark.read.format("lakesoul").load(tablePath).count() == testParquetRowCount)
      val native_df = spark.read.format("lakesoul").load(tablePath).toDF().collect()
      println("load native_df done")
      assert(native_df.head.length==testParquetColNum)
      assert(native_df.length==testParquetRowCount)
      spark.sessionState.conf.setConf(NATIVE_IO_ENABLE, false)
      val orig_df = spark.read.format("lakesoul").load(tablePath).toDF().collect()
      println("load orig_df done")
      orig_df
        .zipAll(native_df, InternalRow(), InternalRow())
        .foreach(zipped=>assert(zipped._1 == zipped._2))
      spark.sessionState.conf.setConf(NATIVE_IO_ENABLE, true)

    }
  }

  test("[Large file test]without hash_key and range_key") {
    withTempDir { dir =>
      val tablePath = dir.toString
      val testSrcFilePath = "/Users/ceng/PycharmProjects/write_parquet/large_file.parquet"    // ccf data_contest base file
      val df = spark
        .read
        .format("parquet")
        .load(testSrcFilePath)
        .toDF()
      df
        .write
        .format("lakesoul")
        .mode("Overwrite")
        .save(tablePath)
      println("write lakesoul table done")
      val start = System.nanoTime()
//      val table = spark.read.format("lakesoul").load(tablePath)
      spark.sparkContext.getConf.set("spark.dmetasoul.lakesoul.native.io.prefetch.buffer.size", "64")
      val loop=20
      for (i <- 1 to loop) {
        println(i)
        assert(spark.read.format("lakesoul")
          .load(tablePath)
          .select("*")
          .where(
            col("str0").isNotNull
              and col("str1").isNotNull
              and col("str2").isNotNull
              and col("str3").isNotNull
              and col("str4").isNotNull
              and col("str5").isNotNull
              and col("str6").isNotNull
              and col("str7").isNotNull
              and col("str8").isNotNull
              and col("str9").isNotNull
              and col("str10").isNotNull
              and col("str11").isNotNull
              and col("str12").isNotNull
              and col("str13").isNotNull
              and col("str14").isNotNull
              and col("int0").isNotNull
              and col("int1").isNotNull
              and col("int2").isNotNull
              and col("int3").isNotNull
              and col("int4").isNotNull
              and col("int5").isNotNull
              and col("int6").isNotNull
              and col("int7").isNotNull
              and col("int8").isNotNull
              and col("int9").isNotNull
              and col("int10").isNotNull
              and col("int11").isNotNull
              and col("int12").isNotNull
              and col("int13").isNotNull
              and col("int14").isNotNull
          )
          .count()==2592000)
        sql("CLEAR CACHE")
      }

      println(s"count ${loop} times complete, using ${NANOSECONDS.toSeconds(System.nanoTime() - start)}s")
    }
  }

  test("[Large file test]with hash_key") {
    withTempDir { dir =>
      val tablePath = dir.toString
      val testSrcFilePath = "/Users/ceng/base-0-0.parquet"    // ccf data_contest base file
      val df = spark
        .read
        .format("parquet")
        .load(testSrcFilePath)
        .toDF()
      df
        .write
        .format("lakesoul")
        .mode("Overwrite")
        .option("hashPartitions","uuid")
        .option("hashBucketNum",2)
        .save(tablePath)
      println("write lakesoul table done")
      //      val table = spark.read.format("lakesoul").load(tablePath)
      for (_ <- 1 to 10) {
        assert(spark.read.format("lakesoul")
          .load(tablePath)
          .select("*")
          .where(col("uuid").isNotNull
            and col("ip").isNotNull
            and col("hostname").isNotNull
            and col("requests").isNotNull
            and col("name").isNotNull
            and col("job").isNotNull
            and col("city").isNotNull
            and col("phonenum").isNotNull
          )
          .count()==10000000)
        sql("CLEAR CACHE")
      }

    }
  }

  test("[MultiPartitionMergeScan test]is single file") {
    withTempDir { dir =>
      val tablePath = dir.toString
      val df = spark
        .read
        .format("parquet")
        .load(testSrcFilePath)
        .toDF()
      df
        .write
        .format("lakesoul")
        .option("rangePartitions","gender")
        .option("hashPartitions","id")
        .option("hashBucketNum", testHashBucketNum)
        .mode("Overwrite")
        .save(tablePath)
      val readTable = spark.read.format("lakesoul").load(tablePath)
      assert(readTable.schema.size == testParquetColNum)
      assert(readTable.count() == testParquetRowCount)
    }
  }

  test("[MultiPartitionMergeScan test]multiple file") {
    withTempDir { dir =>
      val tablePath =SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
      Seq(("range1", "hash1", "insert"),("range2", "hash2", "insert"),("range3", "hash2", "insert"),("range4", "hash2", "insert"),("range4", "hash4", "insert"), ("range3", "hash3", "insert"))
        .toDF("range", "hash", "op")
        .write
        .mode("append")
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .option("lakesoul_cdc_change_column", "op")
        .partitionBy("range","op")
        .save(tablePath)
      val lake = LakeSoulTable.forPath(tablePath)
      val tableForUpsert = Seq(("range1", "hash1", "delete"), ("range3", "hash3", "update"))
        .toDF("range", "hash", "op")
      lake.upsert(tableForUpsert)

      val data1 = spark.read.format("lakesoul").load(tablePath)
      val data2 = data1.select("range","hash","op")
      checkAnswer(data2, Seq(("range2", "hash2", "insert"),("range3", "hash2", "insert"),("range4", "hash2", "insert"),("range4", "hash4", "insert"), ("range3", "hash3", "update")).toDF("range", "hash", "op"))
    }
  }

  test("s3") {
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
      .config("spark.sql.warehouse.dir", "s3://ccf-datalake-contest/datalake_table/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9002")
      .config("fs.s3a.endpoint", "http://localhost:9002")
//      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    val spark = builder.getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
//    withTempDir { dir =>
//      val tablePath = dir.toString
      val df = spark
        .read
        .format("parquet")
        .load("s3a://lakesoul-test-s3/part-00002-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet")
        .toDF()
//      df
//        .write
//        .format("lakesoul")
//        .option("rangePartitions", "gender")
//        .option("hashPartitions", "id")
//        .option("hashBucketNum", testHashBucketNum)
//        .mode("Overwrite")
//        .save(tablePath)
//    }
  }
}

class NativeIOReaderSuite
  extends NativeIOReaderTests {
  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
    session.conf.set(NATIVE_IO_ENABLE.key, true)
    session.conf.set("spark.sql.parquet.columnarReaderBatchSize", 8192)
    session.conf.set("spark.master", "local[3]")
    session.conf.set("spark.default.parallelism","2")
//    session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, false)
    session.sparkContext.setLogLevel("ERROR")

    session
  }
}