// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.spark.clean.CleanUtils.{readDataCommitInfo, readPartitionInfo, setPartitionInfoTimestamp}
import com.dmetasoul.lakesoul.spark.clean.CleanExpiredData.{cleanAllPartitionExpiredData, getExpiredDateZeroTimeStamp}
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class CleanOldCompactionSuite extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with LakeSoulSQLCommandTest {

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

  test("set and cancel partition.ttl and compaction.ttl") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)

      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)

      assert(sm.snapshot.getTableInfo.configuration("partition.ttl").toInt == 2 && sm.snapshot.getTableInfo.configuration("compaction.ttl").toInt == 1)

      LakeSoulTable.forPath(tableName).setCompactionTtl(3).setPartitionTtl(4)

      assert(sm.updateSnapshot().getTableInfo.configuration("partition.ttl").toInt == 4 && sm.updateSnapshot().getTableInfo.configuration("compaction.ttl").toInt == 3)

      LakeSoulTable.forPath(tableName).cancelCompactionTtl()
      LakeSoulTable.forPath(tableName).cancelPartitionTtl()

      assert(!sm.updateSnapshot().getTableInfo.configuration.contains("partition.ttl") && !sm.updateSnapshot().getTableInfo.configuration.contains("compaction.ttl"))
    })
  }

  test("compaction with cleanOldCompaction=true") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)

      LakeSoulTable.forPath(tableName).compaction()

      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")

      LakeSoulTable.forPath(tableName).upsert(df1)
      LakeSoulTable.forPath(tableName).compaction(true)

      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      assert(LakeSoulTable.forPath(tableName).toDF.count() == 4)
      assert(fileCount(sm.table_path, fs) == 6)
    })
  }

  test("read after compaction with cleanOldCompaction=true") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)
      LakeSoulTable.forPath(tableName).compaction()

      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")

      LakeSoulTable.forPath(tableName).upsert(df1)
      LakeSoulTable.forPath(tableName).compaction(true)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      assert(LakeSoulTable.forPath(tableName).toDF.count() == 4)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      assert(fileCount(sm.table_path, fs) == 6)
    })
  }

  test("Clear redundant data with only set compaction.ttl") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        //        .option("partition.ttl", "2")
        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val tableId = sm.updateSnapshot().getTableInfo.table_id
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(6), 0)

      LakeSoulTable.forPath(tableName).compaction()
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(5), 1)

      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")
      LakeSoulTable.forPath(tableName).upsert(df1)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(3), 2)
      LakeSoulTable.forPath(tableName).compaction(true)
      cleanAllPartitionExpiredData(spark)
      assert(readPartitionInfo(tableId, spark).count() == 6)
      assert(readDataCommitInfo(tableId, spark).count() == 6)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      assert(fileCount(sm.table_path, fs) == 4)
    })
  }

  test("Clear partition data with only set partition.ttl and all partiton data is expired") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        //        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val tableId = sm.updateSnapshot().getTableInfo.table_id


      LakeSoulTable.forPath(tableName).compaction()
      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")
      LakeSoulTable.forPath(tableName).upsert(df1)

      //LakeSoulTable.forPath(tableName).compaction()
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(6), 0)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(5), 1)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(3), 2)
      cleanAllPartitionExpiredData(spark)
      assert(readPartitionInfo(tableId, spark).count() == 0)
      assert(readDataCommitInfo(tableId, spark).count() == 0)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      assert(fileCount(sm.table_path, fs) == 0)
    })
  }

  test("Clear partition data with only set partition.ttl and partiton data is not expired") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        //        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val tableId = sm.updateSnapshot().getTableInfo.table_id

      LakeSoulTable.forPath(tableName).compaction()

      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")
      LakeSoulTable.forPath(tableName).upsert(df1)

      //LakeSoulTable.forPath(tableName).compaction()
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(6), 0)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(5), 1)
      cleanAllPartitionExpiredData(spark)
      assert(readPartitionInfo(tableId, spark).count() == 6)
      assert(readDataCommitInfo(tableId, spark).count() == 6)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      assert(fileCount(sm.table_path, fs) == 6)
    })
  }

  test("Clear partition data and partiton data is expired") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath
      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val tableId = sm.updateSnapshot().getTableInfo.table_id

      LakeSoulTable.forPath(tableName).compaction()

      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")
      LakeSoulTable.forPath(tableName).upsert(df1)

      //LakeSoulTable.forPath(tableName).compaction()
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(6), 0)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(5), 1)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(4), 2)
      cleanAllPartitionExpiredData(spark)
      assert(readPartitionInfo(tableId, spark).count() == 0)
      assert(readDataCommitInfo(tableId, spark).count() == 0)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      assert(fileCount(sm.table_path, fs) == 0)
    })
  }

  test("clear partition data and partiton data is not expired but have redundant data") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath
      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val tableId = sm.updateSnapshot().getTableInfo.table_id

      LakeSoulTable.forPath(tableName).compaction()

      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")
      LakeSoulTable.forPath(tableName).upsert(df1)

      LakeSoulTable.forPath(tableName).compaction(true)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(6), 0)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(5), 1)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(4), 2)
      cleanAllPartitionExpiredData(spark)
      assert(readPartitionInfo(tableId, spark).count() == 6)
      assert(readDataCommitInfo(tableId, spark).count() == 6)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      assert(fileCount(sm.table_path, fs) == 4)
    })
  }

  test("partition.ttl and compaction.ttl is not expired") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath
      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val tableId = sm.updateSnapshot().getTableInfo.table_id

      LakeSoulTable.forPath(tableName).compaction()

      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")
      LakeSoulTable.forPath(tableName).upsert(df1)

      LakeSoulTable.forPath(tableName).compaction()
      cleanAllPartitionExpiredData(spark)
      assert(readPartitionInfo(tableId, spark).count() == 8)
      assert(readDataCommitInfo(tableId, spark).count() == 8)
    })
  }

  test("compaction.ttl or partition.ttl has been set but there is no compaction action") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath
      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val tableId = sm.updateSnapshot().getTableInfo.table_id

      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")
      LakeSoulTable.forPath(tableName).upsert(df1)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(6), 0)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(5), 1)
      cleanAllPartitionExpiredData(spark)
      //      assert(readPartitionInfo(tableId, spark).count() == 4)
      //      assert(readDataCommitInfo(tableId, spark).count() == 4)
      //if parttition.ttl has benn set and all partition is expired
      assert(readPartitionInfo(tableId, spark).count() == 0)
      assert(readDataCommitInfo(tableId, spark).count() == 0)
    })
  }

  test("test whether the disk data is deleted") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath
      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val tableId = sm.updateSnapshot().getTableInfo.table_id
      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")
      LakeSoulTable.forPath(tableName).upsert(df1)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(6), 0)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(5), 1)
      cleanAllPartitionExpiredData(spark)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      assert(fileCount(sm.table_path, fs) == 0)
    })
  }

  test("clear partition data after update") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath
      val df = Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
        .toDF("date", "id", "value")
      df.write
        .mode("append")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .option("partition.ttl", "2")
        .option("compaction.ttl", "1")
        .format("lakesoul")
        .save(tableName)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val tableId = sm.updateSnapshot().getTableInfo.table_id

      //LakeSoulTable.forPath(tableName).compaction()

      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")
      LakeSoulTable.forPath(tableName).upsert(df1)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(6), 0)
      setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(5), 1)
      LakeSoulTable.forPath(tableName).update(col("date") > "2020-01-01", Map("date" -> lit("2021-01-03")))
      cleanAllPartitionExpiredData(spark)
      LakeSoulTable.forPath(tableName).toDF.show()
      assert(readPartitionInfo(tableId, spark).count() == 4)
      assert(readDataCommitInfo(tableId, spark).count() == 4)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      assert(fileCount(sm.table_path, fs) == 3)
    })

  }

  def fileCount(path: String, fs: FileSystem): Long = {
    val fileList: Array[FileStatus] = fs.listStatus(new Path(path))

    fileList.foldLeft(0L) { (acc, fileStatus) =>
      if (fileStatus.isFile) {
        println(fileStatus.getPath.toString)
        acc + 1
      } else if (fileStatus.isDirectory) {
        acc + fileCount(fileStatus.getPath.toString, fs)
      } else {
        acc
      }
    }
  }
}