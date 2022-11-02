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
import org.apache.spark.sql.{AnalysisException, QueryTest, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf.NATIVE_IO_ENABLE
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession, LakeSoulTestUtils}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ArrowUtils, ColumnVector, ColumnarBatch}
import org.scalatest.BeforeAndAfter

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

  val testSrcFilePath = "/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
  val testDeltaFilePath = "/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
  val testParquetRowCount = 1000
  val testParquetColNum = 12
  val testHashBucketNum = String.valueOf(1)



  test("[ArrowCDataWrapper test]Read ColumnarBatch from test file") {
    val wrapper = new NativeIOWrapper()
    wrapper.initialize()
    //        wrapper.addFile("/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io/test/test.snappy.parquet")
    wrapper.addFile("/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet")
//    wrapper.setThreadNum(2)
    wrapper.setBatchSize(10)
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
          val vectors = ArrowUtils.asArrayColumnVector(vsr)

          val batch = {
            new ColumnarBatch(vectors, vsr.getRowCount)
          }
          assert(batch.numRows() == 10)
        case None =>
//          assert(false)
      }
    }
  }

  test("[NativeParquetScan test]Read ColumnarBatch from test file") {
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
        assert(spark.read.format("lakesoul").load(tablePath).schema.size == testParquetColNum)
        assert(spark.read.format("lakesoul").load(tablePath).count() == testParquetRowCount)
    }
  }

  test("[NativeParquetScan test]Read ColumnarBatch from large test file") {
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
        .save(tablePath)
      println("write lakesoul table done")
//      val table = spark.read.format("lakesoul").load(tablePath)
      for (_ <- 1 to 10) {
        assert(spark.read.format("lakesoul")
          .load(tablePath)
          .select("*")
          .where(col("uuid").isNotNull and
            col("ip").isNotNull and
            col("hostname").isNotNull and
            col("requests").isNotNull and
            col("name").isNotNull and
            col("job").isNotNull and
            col("city").isNotNull and
            col("phonenum").isNotNull)
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
      // TODO:  NATIVE_IO_ENABLE for MultiPartitionMergeScan not implemented
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
      val lake = LakeSoulTable.forPath(tablePath);
      val tableForUpsert = Seq(("range1", "hash1", "delete"), ("range3", "hash3", "update"))
        .toDF("range", "hash", "op")
      lake.upsert(tableForUpsert)

      // TODO:  NATIVE_IO_ENABLE for MultiPartitionMergeScan not implemented
      val data1 = spark.read.format("lakesoul").load(tablePath)
      val data2 = data1.select("range","hash","op")
      checkAnswer(data2, Seq(("range2", "hash2", "insert"),("range3", "hash2", "insert"),("range4", "hash2", "insert"),("range4", "hash4", "insert"), ("range3", "hash3", "update")).toDF("range", "hash", "op"))
    }
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
    session.sparkContext.setLogLevel("ERROR")

    session
  }
}