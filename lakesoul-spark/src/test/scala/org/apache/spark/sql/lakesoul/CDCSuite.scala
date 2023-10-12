// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.{LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.test.{LakeSoulTestSparkSession, LakeSoulTestUtils}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.types.StructType
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.text.SimpleDateFormat
import scala.language.implicitConversions

@RunWith(classOf[JUnitRunner])
class CDCSuite
  extends QueryTest
    with SharedSparkSession
    with LakeSoulTestUtils {

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, "lakesoul")
    session.conf.set(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, true)
    session.sparkContext.setLogLevel("ERROR")

    session
  }

  import testImplicits._

  val format = "lakesoul"

  private implicit def toTableIdentifier(tableName: String): TableIdentifier = {
    spark.sessionState.sqlParser.parseTableIdentifier(tableName)
  }

  protected def getTablePath(tableName: String): String = {
    LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(
      TableIdentifier(tableName, Some("default"))).get
  }

  protected def getDefaultTablePath(tableName: String): String = {
    SparkUtil.getDefaultTablePath(TableIdentifier(tableName, Some("default"))).toString
  }

  protected def getPartitioningColumns(tableName: String): Seq[String] = {
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[LakeSoulCatalog]
      .loadTable(Identifier.of(Array("default"), tableName)).partitioning()
      .map(_.asInstanceOf[IdentityTransform].ref.asInstanceOf[FieldReference].fieldNames()(0))
  }

  protected def getSchema(tableName: String): StructType = {
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[LakeSoulCatalog]
      .loadTable(Identifier.of(Array("default"), tableName)).schema()
  }

  protected def getSnapshotManagement(path: Path): SnapshotManagement = {
    SnapshotManagement(path)
  }

  Seq("true").distinct.foreach { nativeIOEnabled =>
    test(s"test cdc with MultiPartitionMergeScan(native_io_enabled=$nativeIOEnabled) ") {
      withTable("tt") {
        withTempDir(dir => {
          val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
          withSQLConf(
            LakeSoulSQLConf.NATIVE_IO_ENABLE.key -> nativeIOEnabled) {
            Seq(("range1", "hash1", "insert"), ("range2", "hash2", "insert"), ("range3", "hash2", "insert"), ("range4", "hash2", "insert"), ("range4", "hash4", "insert"), ("range3", "hash3", "insert"))
              .toDF("range", "hash", "op")
              .write
              .mode("append")
              .format("lakesoul")
              .option("rangePartitions", "range")
              .option("hashPartitions", "hash")
              .option("hashBucketNum", "2")
              .option("lakesoul_cdc_change_column", "op")
              .save(tablePath)
            val lake = LakeSoulTable.forPath(tablePath);
            val tableForUpsert = Seq(("range1", "hash1", "delete"), ("range3", "hash3", "update"))
              .toDF("range", "hash", "op")
            lake.upsert(tableForUpsert)
            val data1 = spark.read.format("lakesoul").load(tablePath)
            val data2 = data1.select("range", "hash", "op")
            checkAnswer(data2, Seq(("range2", "hash2", "insert"), ("range3", "hash2", "insert"), ("range4", "hash2", "insert"), ("range4", "hash4", "insert"), ("range3", "hash3", "update")).toDF("range", "hash", "op"))
          }
        })
      }
    }

    test(s"test cdc with OnePartitionMergeBucketScan(native_io_enabled=$nativeIOEnabled) ") {
      withTable("tt") {
        withTempDir(dir => {
          val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
          withSQLConf(
            LakeSoulSQLConf.NATIVE_IO_ENABLE.key -> nativeIOEnabled) {
            Seq(("range1", "hash1", "insert"), ("range1", "hash2", "insert"), ("range1", "hash3", "insert"), ("range1", "hash4", "insert"), ("range1", "hash5", "insert"))
              .toDF("range", "hash", "op")
              .write
              .mode("append")
              .format("lakesoul")
              .option("rangePartitions", "range")
              .option("hashPartitions", "hash")
              .option("hashBucketNum", "2")
              .option("lakesoul_cdc_change_column", "op")
              .save(tablePath)
            val lake = LakeSoulTable.forPath(tablePath);
            val tableForUpsert = Seq(("range1", "hash1", "delete"), ("range1", "hash3", "update"), ("range1", "hash5", "insert"))
              .toDF("range", "hash", "op")
            lake.upsert(tableForUpsert)
            val data1 = spark.read.format("lakesoul").load(tablePath)
            val data2 = data1.select("range", "hash", "op")
            checkAnswer(data2, Seq(("range1", "hash2", "insert"), ("range1", "hash3", "update"), ("range1", "hash4", "insert"), ("range1", "hash5", "insert")).toDF("range", "hash", "op"))
          }
        })
      }
    }

    test(s"test cdc with MultiPartitionMergeBucketScan(native_io_enabled=$nativeIOEnabled)") {
      withTable("tt") {
        withTempDir(dir => {
          val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
          withSQLConf(
            LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true",
            LakeSoulSQLConf.NATIVE_IO_ENABLE.key -> nativeIOEnabled) {
            Seq(("range1", "hash1", "insert"), ("range1", "hash2", "insert"), ("range1", "hash3", "insert"), ("range2", "hash3", "insert"), ("range2", "hash4", "insert"))
              .toDF("range", "hash", "op")
              .write
              .mode("append")
              .format("lakesoul")
              .option("rangePartitions", "range")
              .option("hashPartitions", "hash")
              .option("hashBucketNum", "2")
              .option("lakesoul_cdc_change_column", "op")
              .save(tablePath)
            val lake = LakeSoulTable.forPath(tablePath);
            val tableForUpsert = Seq(("range1", "hash1", "update"), ("range1", "hash3", "update"), ("range2", "hash1", "insert"), ("range2", "hash3", "update"))
              .toDF("range", "hash", "op")
            lake.upsert(tableForUpsert)
            val tableForUpsert1 = Seq(("range1", "hash1", "delete"), ("range1", "hash4", "update"), ("range2", "hash4", "insert"), ("range2", "hash3", "delete"))
              .toDF("range", "hash", "op")
            lake.upsert(tableForUpsert1)
            val data1 = spark.read.format("lakesoul").load(tablePath)
            val data2 = data1.select("range", "hash", "op")
            checkAnswer(data2, Seq(("range1", "hash2", "insert"), ("range2", "hash1", "insert"), ("range1", "hash3", "update"), ("range2", "hash4", "insert"), ("range1", "hash4", "update")).toDF("range", "hash", "op"))
          }
        })
      }
    }

  }

  test("test cdc with snapshot") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        withSQLConf(
          LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true") {
          Seq(("range1", "hash1-1", "insert"), ("range2", "hash2-1", "insert"))
            .toDF("range", "hash", "op")
            .write
            .mode("append")
            .format("lakesoul")
            .option(LakeSoulOptions.RANGE_PARTITIONS, "range")
            .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
            .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
            .option("lakesoul_cdc_change_column", "op")
            .save(tablePath)
          val lake = LakeSoulTable.forPath(tablePath)
          val tableForUpsert = Seq(("range1", "hash1-1", "delete"), ("range1", "hash1-5", "insert"),
            ("range2", "hash2-1", "delete"), ("range2", "hash2-5", "insert"))
            .toDF("range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert)
          val tableForUpsert1 = Seq(("range1", "hash1-2", "update"), ("range2", "hash2-2", "update"))
            .toDF("range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert1)
          Thread.sleep(1000)
          val timeA = System.currentTimeMillis()
          Thread.sleep(1000)
          val tableForUpsert2 = Seq(("range1", "hash1-3", "insert"), ("range2", "hash2-3", "insert"))
            .toDF("range", "hash", "op")
          lake.upsert(tableForUpsert2)

          val versionA: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeA)
          val parDesc = "range=range1"
          // snapshot startVersion default to 0
          val lake1 = LakeSoulTable.forPathSnapshot(tablePath, parDesc, versionA, "")
          val data1 = lake1.toDF.select("range", "hash", "op")
          val lake2 = spark.read.format("lakesoul")
            .option(LakeSoulOptions.PARTITION_DESC, parDesc)
            .option(LakeSoulOptions.READ_END_TIME, versionA)
            .option(LakeSoulOptions.READ_TYPE, ReadType.SNAPSHOT_READ)
            .load(tablePath)
          val data2 = lake2.toDF.select("range", "hash", "op")
          checkAnswer(data1, Seq(("range1", "hash1-2", "update"), ("range1", "hash1-5", "insert")).toDF("range", "hash", "op"))
          checkAnswer(data2, Seq(("range1", "hash1-2", "update"), ("range1", "hash1-5", "insert")).toDF("range", "hash", "op"))
        }
      })
    }
  }

  test("test cdc with incremental") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        withSQLConf(
          LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true") {
          Seq(("range1", "hash1-1", "insert"), ("range2", "hash2-1", "insert"))
            .toDF("range", "hash", "op")
            .write
            .mode("append")
            .format("lakesoul")
            .option(LakeSoulOptions.RANGE_PARTITIONS, "range")
            .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
            .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
            .option("lakesoul_cdc_change_column", "op")
            .save(tablePath)
          val lake = LakeSoulTable.forPath(tablePath)
          val tableForUpsert = Seq(("range1", "hash1-2", "update"), ("range1", "hash1-5", "insert"), ("range2", "hash2-2", "insert"), ("range2", "hash2-5", "insert"))
            .toDF("range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert)
          val tableForUpsert1 = Seq(("range1", "hash1-1", "delete"), ("range2", "hash2-10", "delete"))
            .toDF("range", "hash", "op")
          Thread.sleep(2000)
          val timeB = System.currentTimeMillis()
          lake.upsert(tableForUpsert1)

          val tableForUpsert2 = Seq(("range1", "hash1-13", "insert"), ("range2", "hash2-13", "update"))
            .toDF("range", "hash", "op")
          Thread.sleep(3000)
          lake.upsert(tableForUpsert2)
          val tableForUpsert3 = Seq(("range1", "hash1-15", "insert"), ("range2", "hash2-15", "update"))
            .toDF("range", "hash", "op")
          lake.upsert(tableForUpsert3)
          Thread.sleep(1000)
          val timeC = System.currentTimeMillis()

          val versionB: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeB)
          val versionC: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeC)
          val parDesc = "range=range1"
          val lake1 = LakeSoulTable.forPathIncremental(tablePath, parDesc, versionB, versionC, "America/Los_Angeles")
          val data1 = lake1.toDF.select("range", "hash", "op")
          val lake2 = spark.read.format("lakesoul")
            .option(LakeSoulOptions.PARTITION_DESC, parDesc)
            .option(LakeSoulOptions.READ_START_TIME, versionB)
            .option(LakeSoulOptions.READ_END_TIME, versionC)
            .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
            .load(tablePath)
          val data2 = lake2.toDF.select("range", "hash", "op")
          checkAnswer(data1, Seq(("range1", "hash1-1", "delete"),
            ("range1", "hash1-13", "insert"), ("range1", "hash1-15", "insert")).toDF("range", "hash", "op"))
          checkAnswer(data2, Seq(("range1", "hash1-1", "delete"),
            ("range1", "hash1-13", "insert"), ("range1", "hash1-15", "insert")).toDF("range", "hash", "op"))
        }
      })
    }
  }

  test("test df.count after cdc ") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      val df = Seq(("2021-01-01", 1, "rice", "insert"), ("2021-01-01", 2, "bread", "insert")).toDF("date", "id", "name", "op")
      df.write
        .mode("append")
        .format("lakesoul")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("lakesoul_cdc_change_column", "op")
        .save(tablePath)
      val lake = LakeSoulTable.forPath(tablePath)
      lake.toDF.show(false)
      assert(lake.toDF.count() == 2)
    })

  }
}
