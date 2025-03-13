// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
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
class CleanOldCompactionSuiteDebug extends QueryTest
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

  test(s"compaction with cleanOldCompaction = true and onlySaveOnceCompaction = true") {
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
        .option("only_save_once_compaction", "true")
        .format("lakesoul")
        .save(tableName)

      LakeSoulTable.forPath(tableName).compaction()

      val df1 = Seq(("2020-01-02", 3, "a"), ("2020-01-01", 4, "b"))
        .toDF("date", "id", "value")

      LakeSoulTable.forPath(tableName).upsert(df1)
      LakeSoulTable.forPath(tableName).compaction(cleanOldCompaction = true)

      LakeSoulTable.uncached(tableName)
      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      assert(LakeSoulTable.forPath(tableName).toDF.count() == 4)
      assert(fileCount(sm.table_path, fs) == 6)
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