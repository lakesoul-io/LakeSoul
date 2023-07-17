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
import org.apache.spark.sql.lakesoul.LakeSoulOptions.{READ_TYPE, ReadType}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.{LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.text.SimpleDateFormat

@RunWith(classOf[JUnitRunner])
class ReadSuite extends QueryTest
  with SharedSparkSession
  with LakeSoulTestUtils {

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

  test("test snapshot read with OnePartition") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        Seq(("range1", "hash1-1", "insert"), ("range2", "hash2-1", "insert"))
          .toDF("range", "hash", "op")
          .write
          .mode("append")
          .format("lakesoul")
          .option(LakeSoulOptions.RANGE_PARTITIONS, "range")
          .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
          .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
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
        val lake1 = LakeSoulTable.forPathSnapshot(tablePath, parDesc, versionA)
        val data1 = lake1.toDF.select("range", "hash", "op")
        val lake2 = spark.read.format("lakesoul")
          .option(LakeSoulOptions.PARTITION_DESC, parDesc)
          .option(LakeSoulOptions.READ_END_TIME, versionA)
          .option(LakeSoulOptions.READ_TYPE, ReadType.SNAPSHOT_READ)
          .load(tablePath)
        val data2 = lake2.toDF.select("range", "hash", "op")
        lake2.createOrReplaceTempView("testView")
        val data3 = sql(s"select range,hash,op from testView where op='delete'")
        checkAnswer(data1, Seq(("range1", "hash1-1", "delete"), ("range1", "hash1-2", "update"), ("range1", "hash1-5", "insert")).toDF("range", "hash", "op"))
        checkAnswer(data2, Seq(("range1", "hash1-1", "delete"), ("range1", "hash1-2", "update"), ("range1", "hash1-5", "insert")).toDF("range", "hash", "op"))
        checkAnswer(data3, Seq(("range1", "hash1-1", "delete")).toDF("range", "hash", "op"))
      })
    }
  }

  test("test snapshot read with MultiPartition") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        Seq((1, "range1", "hash1-1", "insert"), (2, "range2", "hash2-1", "insert"))
          .toDF("id", "range", "hash", "op")
          .write
          .mode("overwrite")
          .format("lakesoul")
          .option(LakeSoulOptions.RANGE_PARTITIONS, "range,op")
          .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
          .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
          .save(tablePath)
        val lake = LakeSoulTable.forPath(tablePath)
        val tableForUpsert = Seq((3, "range1", "hash1-2", "update"), (4, "range1", "hash1-5", "insert"), (5, "range2", "hash2-2", "insert"), (6, "range2", "hash2-5", "insert"))
          .toDF("id", "range", "hash", "op")
        Thread.sleep(2000)
        lake.upsert(tableForUpsert)
        val tableForUpsert1 = Seq((7, "range1", "hash1-1", "delete"), (8, "range2", "hash2-10", "delete"))
          .toDF("id", "range", "hash", "op")
        Thread.sleep(2000)
        val timeA = System.currentTimeMillis()
        lake.upsert(tableForUpsert1)
        val tableForUpsert2 = Seq((9, "range1", "hash1-13", "insert"), (10, "range2", "hash2-13", "update"))
          .toDF("id", "range", "hash", "op")
        Thread.sleep(2000)
        lake.upsert(tableForUpsert2)
        val tableForUpsert3 = Seq((11, "range1", "hash1-15", "insert"), (12, "range2", "hash2-15", "update"))
          .toDF("id", "range", "hash", "op")
        Thread.sleep(2000)
        lake.upsert(tableForUpsert3)
        Thread.sleep(1000)
        val versionA: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeA)
        // snapshot startVersion default to 0
        val lake2 = spark.read.format("lakesoul")
          .option(LakeSoulOptions.PARTITION_DESC, "range=range1,op=insert")
          .option(LakeSoulOptions.READ_END_TIME, versionA)
          .option(LakeSoulOptions.READ_TYPE, ReadType.SNAPSHOT_READ)
          .load(tablePath)
        val data2 = lake2.toDF.select("id", "range", "hash", "op")
        lake2.createOrReplaceTempView("testView")
        val data3 = sql(s"select id,range,hash,op from testView where hash='hash1-5'")
        checkAnswer(data2, Seq((1, "range1", "hash1-1", "insert"), (4, "range1", "hash1-5", "insert")).toDF("id", "range", "hash", "op"))
        checkAnswer(data3, Seq((4, "range1", "hash1-5", "insert")).toDF("id", "range", "hash", "op"))
      })
    }
  }

  test("test snapshot read without Partition") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        Seq(("range1", "hash1-1", "insert"), ("range2", "hash2-1", "insert"))
          .toDF("range", "hash", "op")
          .write
          .mode("append")
          .format("lakesoul")
          .option(LakeSoulOptions.RANGE_PARTITIONS, "range")
          .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
          .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
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
        // snapshot startVersion default to 0
        val lake2 = spark.read.format("lakesoul")
          .option(LakeSoulOptions.READ_END_TIME, versionA)
          .option(LakeSoulOptions.READ_TYPE, ReadType.SNAPSHOT_READ)
          .load(tablePath)
        val data2 = lake2.toDF.select("range", "hash", "op")
        checkAnswer(data2, Seq(("range1", "hash1-1", "delete"), ("range1", "hash1-2", "update"), ("range1", "hash1-5", "insert"),
          ("range2", "hash2-1", "delete"), ("range2", "hash2-2", "update"), ("range2", "hash2-5", "insert")).toDF("range", "hash", "op"))
      })
    }
  }

  test("test incremental read with OnePartition") {
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
          val lake1 = LakeSoulTable.forPathIncremental(tablePath, parDesc, versionB, versionC)
          val data1 = lake1.toDF.select("range", "hash", "op")
          val lake2 = spark.read.format("lakesoul")
            .option(LakeSoulOptions.PARTITION_DESC, parDesc)
            .option(LakeSoulOptions.READ_START_TIME, versionB)
            .option(LakeSoulOptions.READ_END_TIME, versionC)
            .option(LakeSoulOptions.TIME_ZONE,"America/Los_Angeles")
            .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
            .load(tablePath)
          val data2 = lake2.toDF.select("range", "hash", "op")
          lake2.createOrReplaceTempView("testView")
          val data3 = sql(s"select range,hash,op from testView where op='delete'")
          checkAnswer(data1, Seq(("range1", "hash1-1", "delete"),
            ("range1", "hash1-13", "insert"), ("range1", "hash1-15", "insert")).toDF("range", "hash", "op"))
          checkAnswer(data2, Seq(("range1", "hash1-1", "delete"),
            ("range1", "hash1-13", "insert"), ("range1", "hash1-15", "insert")).toDF("range", "hash", "op"))
          checkAnswer(data3, Seq(("range1", "hash1-1", "delete")).toDF("range", "hash", "op"))
        }
      })
    }
  }

  test("test incremental read with MultiPartition") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        Seq((1, "range1", "hash1-1", "insert"), (2, "range2", "hash2-1", "insert"))
          .toDF("id", "range", "hash", "op")
          .write
          .mode("overwrite")
          .format("lakesoul")
          .option(LakeSoulOptions.RANGE_PARTITIONS, "range,op")
          .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
          .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
          .save(tablePath)
        val lake = LakeSoulTable.forPath(tablePath)
        val tableForUpsert = Seq((3, "range1", "hash1-2", "update"), (4, "range1", "hash1-5", "insert"), (5, "range2", "hash2-2", "insert"), (6, "range2", "hash2-5", "insert"))
          .toDF("id", "range", "hash", "op")
        Thread.sleep(2000)
        lake.upsert(tableForUpsert)
        val tableForUpsert1 = Seq((7, "range1", "hash1-1", "delete"), (8, "range2", "hash2-10", "delete"))
          .toDF("id", "range", "hash", "op")
        Thread.sleep(2000)
        val timeB = System.currentTimeMillis()
        lake.upsert(tableForUpsert1)
        val tableForUpsert2 = Seq((9, "range1", "hash1-13", "insert"), (10, "range2", "hash2-13", "update"))
          .toDF("id", "range", "hash", "op")
        Thread.sleep(2000)
        lake.upsert(tableForUpsert2)
        val tableForUpsert3 = Seq((11, "range1", "hash1-15", "insert"), (12, "range2", "hash2-15", "update"))
          .toDF("id", "range", "hash", "op")
        Thread.sleep(2000)
        val timeC = System.currentTimeMillis()
        lake.upsert(tableForUpsert3)
        Thread.sleep(1000)
        val versionB: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeB)
        val versionC: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeC)
        val lake2 = spark.read.format("lakesoul")
          .option(LakeSoulOptions.PARTITION_DESC, "range=range1,op=delete")
          .option(LakeSoulOptions.READ_START_TIME, versionB)
          .option(LakeSoulOptions.READ_END_TIME, versionC)
          .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
          .load(tablePath)
        val data2 = lake2.toDF.select("id", "range", "hash", "op")
        checkAnswer(data2, Seq((7, "range1", "hash1-1", "delete")).toDF("id", "range", "hash", "op"))
      })
    }
  }

  test("test incremental read without Partition") {
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
          val lake2 = spark.read.format("lakesoul")
            .option(LakeSoulOptions.READ_START_TIME, versionB)
            .option(LakeSoulOptions.READ_END_TIME, versionC)
            .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
            .load(tablePath)
          val data2 = lake2.toDF.select("range", "hash", "op")
          checkAnswer(data2, Seq(("range1", "hash1-1", "delete"), ("range1", "hash1-13", "insert"), ("range1", "hash1-15", "insert"),
            ("range2", "hash2-10", "delete"), ("range2", "hash2-13", "update"), ("range2", "hash2-15", "update")).toDF("range", "hash", "op"))
        }
      })
    }
  }

  class CreateStreamReadTableWithOnePartition extends Thread {
    override def run(): Unit = {
      withTable("tt") {
        withTempDir(dir => {
          val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
          Seq((1, "range1", "hash1-1", "insert"), (2, "range2", "hash2-1", "insert"))
            .toDF("id", "range", "hash", "op")
            .write
            .mode("append")
            .format("lakesoul")
            .option(LakeSoulOptions.RANGE_PARTITIONS, "range")
            .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
            .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
            .save(tablePath)
          val time = System.currentTimeMillis()
          val lake = LakeSoulTable.forPath(tablePath)
          val tableForUpsert = Seq((3, "range1", "hash1-2", "update"), (4, "range2", "hash2-2", "insert"))
            .toDF("id", "range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert)
          val testStreamRead = new TestStreamRead
          testStreamRead.setTablePath(tablePath, time - 4000, "range=range1")
          testStreamRead.start()
          val tableForUpsert1 = Seq((5, "range1", "hash1-3", "insert"), (6, "range2", "hash2-5", "insert"), (7, "range1", "hash1-1", "delete"), (8, "range2", "hash2-5", "delete"))
            .toDF("id", "range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert1)
          val tableForUpsert2 = Seq((9, "range1", "hash1-13", "insert"), (10, "range2", "hash2-13", "update"))
            .toDF("id", "range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert2)
          Thread.sleep(1000)
        })
      }
    }
  }

  class CreateStreamReadTableWithMultiPartition extends Thread {
    override def run(): Unit = {
      withTable("tt") {
        withTempDir(dir => {
          val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
          Seq((1, "range1", "hash1-1", "insert"), (2, "range2", "hash2-1", "insert"))
            .toDF("id", "range", "hash", "op")
            .write
            .mode("append")
            .format("lakesoul")
            .option(LakeSoulOptions.RANGE_PARTITIONS, "range,op")
            .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
            .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
            .save(tablePath)
          val time = System.currentTimeMillis()
          val lake = LakeSoulTable.forPath(tablePath)
          val tableForUpsert = Seq((3, "range1", "hash1-2", "update"), (4, "range1", "hash1-5", "insert"), (5, "range2", "hash2-2", "insert"), (6, "range2", "hash2-5", "insert"))
            .toDF("id", "range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert)
          val testStreamRead = new TestStreamRead
          testStreamRead.setTablePath(tablePath, time - 4000, "range=range1,op=insert")
          testStreamRead.start()
          val tableForUpsert1 = Seq((7, "range1", "hash1-1", "delete"), (8, "range2", "hash2-10", "delete"))
            .toDF("id", "range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert1)
          val tableForUpsert2 = Seq((9, "range1", "hash1-13", "insert"), (10, "range2", "hash2-13", "update"))
            .toDF("id", "range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert2)
          Thread.sleep(1000)
        })
      }
    }
  }

  class CreateStreamReadTableWithoutPartition extends Thread {
    override def run(): Unit = {
      withTable("tt") {
        withTempDir(dir => {
          val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
          Seq((1, "range1", "hash1-1", "insert"))
            .toDF("id", "range", "hash", "op")
            .write
            .mode("append")
            .format("lakesoul")
            .option(LakeSoulOptions.RANGE_PARTITIONS, "range")
            .option(LakeSoulOptions.HASH_PARTITIONS, "hash")
            .option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
            .save(tablePath)
          val time = System.currentTimeMillis()
          val lake = LakeSoulTable.forPath(tablePath)
          val tableForUpsert = Seq((2, "range2", "hash2-1", "insert"))
            .toDF("id", "range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert)
          val testStreamRead = new TestStreamRead
          testStreamRead.setTablePath(tablePath, time - 4000, "")
          testStreamRead.start()
          val tableForUpsert1 = Seq((3, "range1", "hash1-2", "update"), (4, "range2", "hash2-2", "insert"), (5, "range1", "hash1-5", "insert"), (6, "range1", "hash1-1", "delete"))
            .toDF("id", "range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert1)
          val tableForUpsert2 = Seq((7, "range2", "hash2-5", "insert"), (8, "range2", "hash2-10", "delete"))
            .toDF("id", "range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert2)
          Thread.sleep(1000)
        })
      }
    }
  }

  class TestStreamRead extends Thread {
    private var tablePath: String = ""
    private var startTime: Long = 0
    private var partitionDes: String = ""

    def setTablePath(path: String, startTime: Long, partitionDes: String): Unit = {
      this.tablePath = path
      this.startTime = startTime
      this.partitionDes = partitionDes
    }

    override def run(): Unit = {
      val versionB: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTime)
      var batch = 0
      val query = spark.readStream.format("lakesoul")
        .option(LakeSoulOptions.PARTITION_DESC, partitionDes)
        .option(LakeSoulOptions.READ_START_TIME, versionB)
        .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
        .load(tablePath)
      query
        .writeStream.foreachBatch { (query: DataFrame, _: Long) => {
        batch += 1
        val data = query.select("id", "range", "hash", "op")
        if (partitionDes.equals("range=range1")) {
          if (batch == 1) {
            checkAnswer(data, Seq((1, "range1", "hash1-1", "insert"), (3, "range1", "hash1-2", "update")).toDF("id", "range", "hash", "op"))
          }
        } else if (partitionDes.equals("range=range1,op=insert")) {
          if (batch == 1) {
            checkAnswer(data, Seq((1, "range1", "hash1-1", "insert"), (4, "range1", "hash1-5", "insert")).toDF("id", "range", "hash", "op"))
          }
        } else {
          if (batch == 1) {
            checkAnswer(data, Seq((1, "range1", "hash1-1", "insert"), (2, "range2", "hash2-1", "insert")).toDF("id", "range", "hash", "op"))
          }
        }
      }
      }
        .trigger(Trigger.Once())
        .start()
        .awaitTermination()
    }
  }

  test("test stream read with OnePartition") {
    new Thread(new CreateStreamReadTableWithOnePartition).run()
  }

  test("test stream read with MultiPartition") {
    new Thread(new CreateStreamReadTableWithMultiPartition).run()
  }

  test("test stream read without Partition") {
    new Thread(new CreateStreamReadTableWithoutPartition).run()
  }
}
