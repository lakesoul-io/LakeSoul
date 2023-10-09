// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.schema

import com.dmetasoul.lakesoul.meta.DataFileInfo
import org.apache.hadoop.fs.Path

import java.io.File
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException}
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Dataset, QueryTest, Row}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CaseSensitivitySuite extends QueryTest
  with SharedSparkSession with SQLTestUtils with LakeSoulSQLCommandTest {

  import testImplicits._

  private def testWithCaseSensitivity(name: String)(f: => Unit): Unit = {
    test(name) {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        f
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        f
      }
    }
  }

  private def getPartitionValues(allFiles: Dataset[DataFileInfo], colName: String): Array[String] = {
    allFiles.select(col(s"range_partitions")).distinct().as[String].collect()
  }


  test("set range partition columns with option - rangePartitions") {
    withTempDir { tempDir =>
      val p = tempDir.getCanonicalPath
      val path = SparkUtil.makeQualifiedTablePath(new Path(p)).toString
      Seq((1, "a"), (2, "b")).toDF("Key", "val").write
        .option("rangePartitions", "key")
        .format("lakesoul").mode("append").save(path)

      checkAnswer(
        spark.read.format("lakesoul").load(path).select("Key", "val"),
        Row(1, "a") :: Row(2, "b") :: Nil
      )

      val tableInfo = SnapshotManagement(path).getTableInfoOnly
      assert(tableInfo.schema == new StructType()
        .add("Key", "int", false)
        .add("val", "string"))
      assert(tableInfo.range_column.equals("Key"))

    }
  }

  test("set range partition columns with partitionBy") {
    withTempDir { tempDir =>
      val p = tempDir.getCanonicalPath
      val path = SparkUtil.makeQualifiedTablePath(new Path(p)).toString
      Seq((1, "a"), (2, "b")).toDF("Key", "val").write
        .partitionBy("key")
        .format("lakesoul").mode("append").save(path)

      checkAnswer(
        spark.read.format("lakesoul").load(path).select("Key", "val"),
        Row(1, "a") :: Row(2, "b") :: Nil
      )

      val tableInfo = SnapshotManagement(path).getTableInfoOnly
      assert(tableInfo.schema == new StructType()
        .add("Key", "int", false)
        .add("val", "string"))
      assert(tableInfo.range_column.equals("Key"))
    }
  }

  test("set range partition columns - rangePartitions has higher priority than partitionBy") {
    withTempDir { tempDir =>
      val p = tempDir.getCanonicalPath
      val path = SparkUtil.makeQualifiedTablePath(new Path(p)).toString
      Seq((1, "a"), (2, "b")).toDF("Key", "val").write
        .option("rangePartitions", "val")
        .partitionBy("key")
        .format("lakesoul").mode("append").save(path)

      checkAnswer(
        spark.read.format("lakesoul").load(path).select("Key", "val"),
        Row(1, "a") :: Row(2, "b") :: Nil
      )

      val tableInfo = SnapshotManagement(path).getTableInfoOnly
      assert(tableInfo.schema == new StructType()
        .add("Key", "int")
        .add("val", "string", false))
      assert(tableInfo.range_column.equals("val"))

    }
  }

  test("set hash partition columns with option- hashPartitions and hashBucketNum") {
    withTempDir { tempDir =>
      val p = tempDir.getCanonicalPath
      val path = SparkUtil.makeQualifiedTablePath(new Path(p)).toString

      val e1 = intercept[AnalysisException] {
        Seq((1, "a"), (2, "d")).toDF("key", "val").write
          .format("lakesoul")
          .mode("overwrite")
          .option("hashPartitions", "key")
          .save(path)
      }
      assert(e1.getMessage.contains("You must set the bucket num"))


      //first commit can use hash partition with append mode
      Seq((1, "a"), (2, "b")).toDF("key", "val").write
        .option("hashPartitions", "key")
        .option("hashBucketNum", 2)
        .format("lakesoul")
        .mode("append").save(path)

      checkAnswer(
        spark.read.format("lakesoul").load(path).select("key", "val"),
        Row(1, "a") :: Row(2, "b") :: Nil
      )

      val tableInfo = SnapshotManagement(path).getTableInfoOnly
      assert(tableInfo.schema == new StructType()
        .add("key", "int", false)
        .add("val", "string"))
      assert(tableInfo.hash_column.equals("key"))
      assert(tableInfo.bucket_num == 2)


      //don't support append when use hash partition and not first commit
      val e2 = intercept[AnalysisException] {
        Seq((3, "d")).toDF("key", "val").write
          .format("lakesoul")
          .mode("append")
          .save(path)
      }
      assert(e2.getMessage
        .contains("When use hash partition and not first commit, `Append` mode is not supported"))
    }
  }

  test("set hash partition columns") {
    withTempDir { tempDir =>
      val p = tempDir.getCanonicalPath
      val path = SparkUtil.makeQualifiedTablePath(new Path(p)).toString

      Seq((1, "a", "1"), (2, "b", "2")).toDF("key", "val", "hash").write
        .partitionBy("key")
        //        .option("rangePartitions","key")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", 2)
        .format("lakesoul")
        .mode("overwrite")
        .save(path)

      val tableInfo = SnapshotManagement(path).getTableInfoOnly
      assert(tableInfo.schema == new StructType()
        .add("key", "int", false)
        .add("val", "string")
        .add("hash", "string", false))
      assert(tableInfo.range_column.equals("key"))
      assert(tableInfo.hash_column.equals("hash"))


      //don't need to define partition columns when table exists
      Seq((3, "d", "3")).toDF("key", "val", "hash").write
        .format("lakesoul")
        .mode("overwrite")
        .save(path)

      checkAnswer(
        spark.read.format("lakesoul").load(path).select("key", "val", "hash"),
        Row(3, "d", "3") :: Nil
      )

      //can't change partition columns
      val e1 = intercept[AnalysisException] {
        Seq((4, "e", "4")).toDF("key", "val", "hash").write
          .format("lakesoul")
          .mode("overwrite")
          .option("rangePartitions", "val")
          .save(path)
      }
      assert(e1.getMessage.contains("was already set when creating table, it conflicts with your partition columns"))

      val e2 = intercept[AnalysisException] {
        Seq((4, "e", "4")).toDF("key", "val", "hash").write
          .format("lakesoul")
          .mode("overwrite")
          .option("hashPartitions", "val")
          .save(path)
      }
      assert(e2.getMessage.contains("Hash partition column"))

    }
  }

  test("set partition columns - case sensitive") {
    withTempDir { tempDir =>
      val p = tempDir.getCanonicalPath
      val path = SparkUtil.makeQualifiedTablePath(new Path(p)).toString

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        Seq((1, "a", "1"), (2, "b", "2")).toDF("key", "val", "hash").write
          .format("lakesoul")
          .mode("overwrite")
          .option("rAngeParTitionS", "key") // note the different case
          .option("HaSHParTitionS", "hash")
          .option("HAshBucketNUM", 2)
          .save(path)
      }

      checkAnswer(
        spark.read.format("lakesoul").load(path).select("key", "val", "hash"),
        Row(1, "a", "1") :: Row(2, "b", "2") :: Nil
      )

      val tableInfo = SnapshotManagement(path).getTableInfoOnly
      assert(tableInfo.schema == new StructType()
        .add("key", "int", false)
        .add("val", "string")
        .add("hash", "string", false))
      assert(tableInfo.range_column.equals("key"))
      assert(tableInfo.hash_column.equals("hash"))

    }
  }

  test("set partition columns - case insensitive") {
    withTempDir { tempDir =>
      val p = tempDir.getCanonicalPath
      val path = SparkUtil.makeQualifiedTablePath(new Path(p)).toString

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        Seq((1, "a", "1"), (2, "b", "2")).toDF("key", "val", "hash").write
          .format("lakesoul")
          .mode("overwrite")
          .option("rAngeParTitionS", "key") // note the different case
          .option("HaSHParTitionS", "hash")
          .option("HAshBucketNUM", 2)
          .save(path)
      }

      checkAnswer(
        spark.read.format("lakesoul").load(path).select("key", "val", "hash"),
        Row(1, "a", "1") :: Row(2, "b", "2") :: Nil
      )

      val tableInfo = SnapshotManagement(path).getTableInfoOnly
      assert(tableInfo.schema == new StructType()
        .add("key", "int", false)
        .add("val", "string")
        .add("hash", "string", false))
      assert(tableInfo.range_column.equals("key"))
      assert(tableInfo.hash_column.equals("hash"))

    }
  }

  testWithCaseSensitivity("case sensitivity of partition fields") {
    withTempDir { tempDir =>
      val query = "SELECT id + 1 as Foo, id as Bar FROM RANGE(1)"
      sql(query).write
        //        .partitionBy("foo")
        .option("rangePartitions", "foo")
        .format("lakesoul").save(tempDir.getAbsolutePath)
      checkAnswer(
        sql(query),
        spark.read.format("lakesoul").load(tempDir.getAbsolutePath).select("Foo", "Bar")
      )


      val allFiles = SparkUtil.allDataInfo(SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tempDir.getAbsolutePath))).snapshot).toSeq.toDS()
      assert(getPartitionValues(allFiles, "Foo") === Array("Foo=1"))
      checkAnswer(
        spark.read.format("lakesoul").load(tempDir.getAbsolutePath).select("Foo", "Bar"),
        Row(1L, 0L)
      )
    }
  }

  test("case sensitivity of partition fields (stream)") {
    // DataStreamWriter auto normalizes partition columns, therefore we don't need to check
    // case sensitive case
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir(checkpointDir => {
        withTempDir { tempDir =>
          val memSource = MemoryStream[(Long, Long)]
          val stream1 = startStream(memSource.toDF().toDF("Foo", "Bar"), tempDir, checkpointDir)
          try {
            memSource.addData((1L, 0L))
            stream1.processAllAvailable()
          } finally {
            stream1.stop()
          }

          checkAnswer(
            spark.read.format("lakesoul").load(tempDir.getAbsolutePath).select("foo", "bar"),
            Row(1L, 0L)
          )

          val allFiles = SparkUtil.allDataInfo(SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tempDir.getAbsolutePath))).snapshot).toSeq.toDS()
          assert(getPartitionValues(allFiles, "Foo") === Array("Foo=1"))
        }

      })
    }
  }

  testWithCaseSensitivity("two fields with same name") {
    withTempDir { tempDir =>
      intercept[AnalysisException] {
        val query = "SELECT id as Foo, id as foo FROM RANGE(1)"
        //        sql(query).write.partitionBy("foo").format("lakesoul").save(tempDir.getAbsolutePath)
        sql(query).write.option("rangePartitions", "foo").format("lakesoul").save(tempDir.getAbsolutePath)
      }
    }
  }

  testWithCaseSensitivity("two fields with same name (stream)") {
    withTempDir(checkpointDir => {
      withTempDir { tempDir =>
        val memSource = MemoryStream[(Long, Long)]
        val stream1 = startStream(memSource.toDF().toDF("Foo", "foo"), tempDir, checkpointDir)
        try {
          val e = intercept[StreamingQueryException] {
            memSource.addData((0L, 0L))
            stream1.processAllAvailable()
          }
          assert(e.cause.isInstanceOf[AnalysisException])
        } finally {
          stream1.stop()
        }
      }

    })
  }

  testWithCaseSensitivity("schema merging is case insenstive but preserves original case") {
    withTempDir { tempDir =>
      val query1 = "SELECT id as foo, id as bar FROM RANGE(1)"
      sql(query1).write.format("lakesoul").save(tempDir.getAbsolutePath)

      val query2 = "SELECT id + 1 as Foo, id as bar FROM RANGE(1)" // notice how 'F' is capitalized
      sql(query2).write.format("lakesoul").mode("append").save(tempDir.getAbsolutePath)

      val query3 = "SELECT id as bAr, id + 2 as Foo FROM RANGE(1)" // changed order as well
      sql(query3).write.format("lakesoul").mode("append").save(tempDir.getAbsolutePath)

      val df = spark.read.format("lakesoul").load(tempDir.getAbsolutePath)
      checkAnswer(
        df.select("foo", "bar"),
        Row(0, 0) :: Row(1, 0) :: Row(2, 0) :: Nil
      )
      assert(df.schema.fieldNames === Seq("foo", "bar"))
    }
  }

  testWithCaseSensitivity("schema merging preserving column case (stream)") {
    withTempDir(checkpointDir => {
      withTempDir { tempDir =>
        val memSource = MemoryStream[(Long, Long)]
        val stream1 = startStream(memSource.toDF().toDF("Foo", "Bar"), tempDir, checkpointDir, None)
        try {
          memSource.addData((0L, 0L))
          stream1.processAllAvailable()
        } finally {
          stream1.stop()
        }
        val stream2 = startStream(memSource.toDF().toDF("foo", "Bar"), tempDir, checkpointDir, None)
        try {
          memSource.addData((1L, 2L))
          stream2.processAllAvailable()
        } finally {
          stream2.stop()
        }

        val df = spark.read.format("lakesoul").load(tempDir.getAbsolutePath)
        checkAnswer(
          df,
          Row(0L, 0L) :: Row(1L, 2L) :: Nil
        )
        assert(df.schema.fieldNames === Seq("Foo", "Bar"))
      }

    })
  }


  test("replaceWhere predicate should be case insensitive") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      Seq((1, "a"), (2, "b")).toDF("Key", "val").write
        //        .partitionBy("key")
        .option("rangePartitions", "key")
        .format("lakesoul").mode("append").save(path)

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        Seq((2, "c")).toDF("Key", "val").write
          .format("lakesoul")
          .mode("overwrite")
          .option("replaceWhere", "key = 2") // note the different case
          .save(path)
      }

      checkAnswer(
        spark.read.format("lakesoul").load(path).select("key", "val"),
        Row(1, "a") :: Row(2, "c") :: Nil
      )

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val e = intercept[AnalysisException] {
          Seq((2, "d")).toDF("Key", "val").write
            .format("lakesoul")
            .mode("overwrite")
            .option("replaceWhere", "key = 2") // note the different case
            .save(path)
        }
        assert(e.getMessage.contains("Key"))
      }

      checkAnswer(
        spark.read.format("lakesoul").load(path).select("key", "val"),
        Row(1, "a") :: Row(2, "c") :: Nil
      )
    }
  }

  private def startStream(df: Dataset[_],
                          tempDir: File,
                          checkpointDir: File,
                          partitionBy: Option[String] = Some("foo")): StreamingQuery = {
    val writer = df.writeStream
      .option("checkpointLocation", new File(checkpointDir, "_checkpoint").getAbsolutePath)
      .format("lakesoul")
    partitionBy.foreach(writer.partitionBy(_))
    writer.start(tempDir.getAbsolutePath)
  }


}
