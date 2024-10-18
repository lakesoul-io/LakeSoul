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

  test("set and cancel partition.ttl and compaction.ttl and only_save_once_compaction value") {
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
        .option("only_save_once_compaction", false)
        .format("lakesoul")
        .save(tableName)

      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)

      assert(
        sm.snapshot.getTableInfo.configuration("partition.ttl").toInt == 2
          && sm.snapshot.getTableInfo.configuration("compaction.ttl").toInt == 1
          && sm.snapshot.getTableInfo.configuration("only_save_once_compaction").toBoolean == false
      )

      LakeSoulTable.forPath(tableName).setCompactionTtl(3).setPartitionTtl(4).onlySaveOnceCompaction(true)

      assert(
        sm.updateSnapshot().getTableInfo.configuration("partition.ttl").toInt == 4
          && sm.updateSnapshot().getTableInfo.configuration("compaction.ttl").toInt == 3
          && sm.updateSnapshot().getTableInfo.configuration("only_save_once_compaction").toBoolean == true
      )

      LakeSoulTable.forPath(tableName).cancelCompactionTtl()
      LakeSoulTable.forPath(tableName).cancelPartitionTtl()

      assert(!sm.updateSnapshot().getTableInfo.configuration.contains("partition.ttl") && !sm.updateSnapshot().getTableInfo.configuration.contains("compaction.ttl"))
    })
  }

  Seq(true, false).distinct.foreach { onlySaveOnceCompaction =>
    test(s"compaction with cleanOldCompaction = true and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
          .format("lakesoul")
          .save(tableName)

        LakeSoulTable.forPath(tableName).compaction()

        val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
          .toDF("date", "id", "value")

        LakeSoulTable.forPath(tableName).upsert(df1)
        LakeSoulTable.forPath(tableName).compaction(cleanOldCompaction = true)

        val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        assert(LakeSoulTable.forPath(tableName).toDF.count() == 4)
        assert(fileCount(sm.table_path, fs) == 6)
      })
    }

    test(s"read after compaction with cleanOldCompaction=true and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
          .format("lakesoul")
          .save(tableName)
        LakeSoulTable.forPath(tableName).compaction()

        val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
          .toDF("date", "id", "value")

        LakeSoulTable.forPath(tableName).upsert(df1)
        LakeSoulTable.forPath(tableName).compaction(cleanOldCompaction = true)
        val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
        assert(LakeSoulTable.forPath(tableName).toDF.count() == 4)
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        assert(fileCount(sm.table_path, fs) == 6)
      })
    }

    test(s"Clear redundant data with only set compaction.ttl and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
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
        LakeSoulTable.forPath(tableName).compaction(cleanOldCompaction = true)
        cleanAllPartitionExpiredData(spark)
        if (onlySaveOnceCompaction) {
          assert(readPartitionInfo(tableId, spark).count() == 2)
          assert(readDataCommitInfo(tableId, spark).count() == 2)
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          assert(fileCount(sm.table_path, fs) == 2)
        } else {
          assert(readPartitionInfo(tableId, spark).count() == 6)
          assert(readDataCommitInfo(tableId, spark).count() == 6)
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          assert(fileCount(sm.table_path, fs) == 4)
        }
      })
    }

    test(s"Clear partition data with only set partition.ttl and all partition data is expired and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
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

    test(s"Clear partition data with only set partition.ttl and partition data is not expired and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
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

    test(s"Clear partition data and partition data is expired and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
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

    test(s"clear partition data and partition data is not expired but have redundant data and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
          .format("lakesoul")
          .save(tableName)
        val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
        val tableId = sm.updateSnapshot().getTableInfo.table_id

        LakeSoulTable.forPath(tableName).compaction()

        val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
          .toDF("date", "id", "value")
        LakeSoulTable.forPath(tableName).upsert(df1)

        LakeSoulTable.forPath(tableName).compaction(cleanOldCompaction = true)
        setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(6), 0)
        setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(5), 1)
        setPartitionInfoTimestamp(tableId, getExpiredDateZeroTimeStamp(4), 2)
        cleanAllPartitionExpiredData(spark)
        if (onlySaveOnceCompaction) {
          assert(readPartitionInfo(tableId, spark).count() == 2)
          assert(readDataCommitInfo(tableId, spark).count() == 2)
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          assert(fileCount(sm.table_path, fs) == 2)
        } else {
          assert(readPartitionInfo(tableId, spark).count() == 6)
          assert(readDataCommitInfo(tableId, spark).count() == 6)
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          assert(fileCount(sm.table_path, fs) == 4)
        }
      })
    }

    test(s"partition.ttl and compaction.ttl is not expired and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
          .format("lakesoul")
          .save(tableName)
        val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
        val tableId = sm.updateSnapshot().getTableInfo.table_id

        LakeSoulTable.forPath(tableName).compaction(cleanOldCompaction = onlySaveOnceCompaction)

        val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
          .toDF("date", "id", "value")
        LakeSoulTable.forPath(tableName).upsert(df1)

        LakeSoulTable.forPath(tableName).compaction(cleanOldCompaction = onlySaveOnceCompaction)
        cleanAllPartitionExpiredData(spark)
        assert(readPartitionInfo(tableId, spark).count() == 8)
        assert(readDataCommitInfo(tableId, spark).count() == 8)
      })
    }

    test(s"compaction.ttl or partition.ttl has been set but there is no compaction action and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
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

    test(s"test whether the disk data is deleted and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
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

    test(s"clear partition data after update and onlySaveOnceCompaction = $onlySaveOnceCompaction") {
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
          .option("only_save_once_compaction", onlySaveOnceCompaction)
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
        if (onlySaveOnceCompaction) {
          assert(readPartitionInfo(tableId, spark).count() == 2)
          assert(readDataCommitInfo(tableId, spark).count() == 2)
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          assert(fileCount(sm.table_path, fs) == 1)
        } else {
          assert(readPartitionInfo(tableId, spark).count() == 4)
          assert(readDataCommitInfo(tableId, spark).count() == 4)
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          assert(fileCount(sm.table_path, fs) == 3)
        }
      })
    }
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