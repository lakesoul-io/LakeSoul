// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta.LakeSoulOptions.SHORT_TABLE_NAME
import com.dmetasoul.lakesoul.meta.{DBUtil, DataFileInfo, DataOperation, SparkMetaVersion}
import com.dmetasoul.lakesoul.spark.clean.CleanOldCompaction.splitCompactFilePath
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession}
import org.apache.spark.sql.lakesoul.utils.{SparkUtil, TableInfo}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.junit.JUnitRunner
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

@RunWith(classOf[JUnitRunner])
class NewCompactionSuite extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with LakeSoulSQLCommandTest {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.network.timeout", "10000000")
      .set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .set("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .set("spark.dmetasoul.lakesoul.compaction.level0.file.number.limit","2")
  }

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


  test("simple new compaction without partition") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      df1.write
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      var rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      val df2 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "name")

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      assert(!rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      LakeSoulTable.forPath(tableName).newCompaction()
      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1))
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

    })
  }

  test("simple nwe compaction with partition") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      var rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      val df2 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "name")

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      assert(!rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      LakeSoulTable.forPath(tableName).newCompaction()
      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1))
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

    })
  }

  test("simple new compaction without pk") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df0 = Seq((0, 0)).toDF("hash", "value").withColumn("range", lit(0)).select("range", "hash", "value")
      df0.write
        .option("rangePartitions", "range")
        .format("lakesoul")
        .save(tableName)

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      df1.write
        .option("rangePartitions", "range")
        .format("lakesoul")
        .mode("append")
        .save(tableName)

      val df2 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")

      df2.write
        .format("lakesoul")
        .mode("append")
        .save(tableName)

      LakeSoulTable.forPath(tableName).newCompaction()
    })
  }

  test("simple new compaction - string partition") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq(("", 1, 1), (null, 1, 1), ("3", 1, 1), ("1", 2, 2), ("1", 3, 3))
        .toDF("range", "hash", "value")
      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      var rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      val df2 = Seq(("", 1, 2), (null, 1, 2), ("3", 1, 2), ("1", 2, 3), ("1", 3, 4))
        .toDF("range", "hash", "name")

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      assert(!rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      LakeSoulTable.forPath(tableName).newCompaction()
      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1))
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

    })
  }

  test("simple new compaction - multiple partition") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq(("", "", 1, 1), (null, "", 1, 1), ("3", null, 1, 1), ("1", "2", 2, 2), ("1", "3", 3, 3))
        .toDF("range1", "range2", "hash", "value")
      df1.write
        .option("rangePartitions", "range1,range2")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
      var rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      val df2 = Seq(("", "", 1, 2), (null, "", 1, 2), ("3", null, 1, 2), ("1", "2", 2, 3), ("1", "3", 3, 4))
        .toDF("range1", "range2", "hash", "name")

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      assert(!rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      LakeSoulTable.forPath(tableName).newCompaction()
      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1))
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

    })
  }

  test("new compaction with condition - simple") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      val df2 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "name")

      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)

      val rangeInfo = SparkUtil.allDataInfo(sm.snapshot).filter(_.range_partitions.equals("range=1"))

      assert(!rangeInfo.groupBy(_.file_bucket_id).forall(_._2.length == 1))

      LakeSoulTable.forPath(tableName).newCompaction("range=1")
      Thread.sleep(1000)

      val allDataInfo = SparkUtil.allDataInfo(sm.updateSnapshot())
      println(allDataInfo.mkString("Array(", ", ", ")"))

      assert(allDataInfo
        .filter(_.range_partitions.equals("range=1"))
        .groupBy(_.file_bucket_id).forall(_._2.length == 1)
      )

      assert(allDataInfo
        .filter(!_.range_partitions.equals("range=1"))
        .groupBy(_.file_bucket_id).forall(_._2.length != 1)
      )
    })
  }
  //  test("new compaction with call - simple condition") {
  //    withTempDir(file => {
  //      val tableName = file.getCanonicalPath
  //
  //      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
  //        .toDF("range", "hash", "value")
  //      val df2 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
  //        .toDF("range", "hash", "name")
  //
  //      df1.write
  //        .option("rangePartitions", "range")
  //        .option("hashPartitions", "hash")
  //        .option("hashBucketNum", "2")
  //        .format("lakesoul")
  //        .save(tableName)
  //
  //      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
  //        LakeSoulTable.forPath(tableName).upsert(df2)
  //      }
  //
  //      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
  //
  //      val rangeInfo = SparkUtil.allDataInfo(sm.snapshot).filter(_.range_partitions.equals("range=1"))
  //
  //      assert(!rangeInfo.groupBy(_.file_bucket_id).forall(_._2.length == 1))
  //
  //      /* usage for call compaction
  //      * call LakeSoulTable.compaction(condition=>map('range',1),tablePath=>'file://path')
  //      * call LakeSoulTable.compaction(condition=>map('range',1),tableName=>'lakesoul')
  //      * call LakeSoulTable.compaction(tableName=>'lakesoul',hiveTableName=>'hive')
  //      * call LakeSoulTable.compaction(tableName=>'lakesoul',cleanOld=>true)
  //      */
  //      sql("call LakeSoulTable.newCompaction(condition=>map('range',1),tablePath=>'" + tableName + "')")
  //      Thread.sleep(1000)
  //
  //      val allDataInfo = SparkUtil.allDataInfo(sm.updateSnapshot())
  //      println(allDataInfo.mkString("Array(", ", ", ")"))
  //
  //      assert(allDataInfo
  //        .filter(_.range_partitions.equals("range=1"))
  //        .groupBy(_.file_bucket_id).forall(_._2.length == 1)
  //      )
  //
  //      assert(allDataInfo
  //        .filter(!_.range_partitions.equals("range=1"))
  //        .groupBy(_.file_bucket_id).forall(_._2.length != 1)
  //      )
  //    })
  //  }

  test("new compaction with condition - multi partitions should failed") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      val df2 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "name")

      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

      val e = intercept[AnalysisException] {
        LakeSoulTable.forPath(tableName).newCompaction("range=1 or range=2")
      }
      assert(e.getMessage().contains("Couldn't execute compaction because of your condition") &&
        e.getMessage().contains("we only allow one partition"))
    })
  }

  test("upsert after new compaction") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4))
        .toDF("range", "hash", "value")
      val df2 = Seq((1, 1, 11), (1, 2, 22), (1, 3, 33))
        .toDF("range", "hash", "value")


      val df3 = Seq((1, 2, 222), (1, 3, 333), (1, 4, 444), (1, 5, 555))
        .toDF("range", "hash", "value")

      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

      LakeSoulTable.forPath(tableName).newCompaction("range=1")

      checkAnswer(LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "value"),
        Seq((1, 1, 11), (1, 2, 22), (1, 3, 33), (1, 4, 4)).toDF("range", "hash", "value"))

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df3)
      }

      checkAnswer(LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "value"),
        Seq((1, 1, 11), (1, 2, 222), (1, 3, 333), (1, 4, 444), (1, 5, 555)).toDF("range", "hash", "value"))


      LakeSoulTable.forPath(tableName).newCompaction("range=1")

      checkAnswer(LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "value"),
        Seq((1, 1, 11), (1, 2, 222), (1, 3, 333), (1, 4, 444), (1, 5, 555)).toDF("range", "hash", "value"))

    })
  }

  test("new compaction with limited file number") {
    val rand = new scala.util.Random
    val merge_num_limit = 2 + rand.nextInt(100) % 6
    println(s"merge compacted file num limit $merge_num_limit")
    withSQLConf(LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> merge_num_limit.toString,
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "1",
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "20") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath

        val hashBucketNum = 1
        val compactRounds = 10
        val upsertPerRounds = 10
        val startIdGap = 501
        val rowsPerUpsert = 1000
        val compactGroupSize = 3

        // Create test data
        val df = Seq(
          (1, "2023-01-01", 10, 1),
          (2, "2023-01-02", 20, 1),
          (3, "2023-01-03", 30, 1),
          (4, "2023-01-04", 40, 1),
          (5, "2023-01-05", 50, 1)
        ).toDF("id", "date", "value", "range")

        // Write initial data
        df.write
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "id")
          .option(SHORT_TABLE_NAME, "new_compaction_limit_table")
          .option("hashBucketNum", hashBucketNum.toString)
          .save(tablePath)

        val lakeSoulTable = LakeSoulTable.forPath(tablePath)

        for (c <- 0 until compactRounds) {
          // Simulate multiple append operations
          for (i <- c * upsertPerRounds + 1 to (c + 1) * upsertPerRounds) {
            val startId = i * startIdGap
            val appendDf = createTestDataFrame(startId, i, rowsPerUpsert, false)
            lakeSoulTable.upsert(appendDf)
          }

          // Get initial PartitionInfo count
          val initialFile = getFileList(tablePath)
          val incrementalFileCount = initialFile.filter(file => !file.path.contains("compactdir")).length
          val lastCompactedFileCount = initialFile.filter(file => file.path.contains("compactdir")).length
          println(s"before ${c}th time compact file count=${incrementalFileCount + lastCompactedFileCount}, " +
            s"compact file count=$lastCompactedFileCount, incremental file count=$incrementalFileCount")

          // Perform limited compaction (group every compactGroupSize PartitionInfo)
          lakeSoulTable.newCompaction(fileNumLimit = Some(compactGroupSize))

          // Get PartitionInfo count after compaction
          val compactedFileList = getFileList(tablePath)
          val compactedFileCount = compactedFileList.length

          println(s"after ${c}th time compact file count=$compactedFileCount")

          val afterCompactIncrementalFileNum = (incrementalFileCount - 1) / compactGroupSize + 1
          val firstLevelCompactFileNum = afterCompactIncrementalFileNum + lastCompactedFileCount
          val afterCompactionFileNumber = if (firstLevelCompactFileNum >= 20)
            (firstLevelCompactFileNum) / merge_num_limit + firstLevelCompactFileNum % merge_num_limit
          else firstLevelCompactFileNum
          println(s"first level $firstLevelCompactFileNum, after compact $afterCompactionFileNumber," +
            s"compacted files: ${compactedFileList.mkString("Array(", ", ", ")")}")
          assert(compactedFileCount == afterCompactionFileNumber,
            s"Compaction should produce files number is , but there are $compactedFileCount files")
        }

        // Verify data integrity
        val compactedData = lakeSoulTable.toDF.orderBy("id", "date").collect()
        val expectedRows = 5 + startIdGap * upsertPerRounds * compactRounds - startIdGap + rowsPerUpsert
        assert(compactedData.length == expectedRows,
          s"The compressed data should have $expectedRows rows (initial 5 + ($startIdGap * $upsertPerRounds * $compactRounds - $startIdGap) + $rowsPerUpsert rows), but it actually has ${
            compactedData.length
          } rows")
      }
    }
  }

  test("new compaction cdc table with limited file number") {
    val rand = new scala.util.Random
    val merge_num_limit = 2 + rand.nextInt(100) % 6
    println(s"merge compacted file num limit $merge_num_limit")
    withSQLConf(LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> merge_num_limit.toString,
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "1",
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "20") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath

        val hashBucketNum = 1
        val compactRounds = 10
        val upsertPerRounds = 10
        val startIdGap = 500
        val rowsPerUpsert = 1001
        val compactGroupSize = 3

        // Create test data
        val df = Seq(
          (1, "2023-01-01", 10, 1, "insert"),
          (2, "2023-01-02", 20, 1, "insert"),
          (3, "2023-01-03", 30, 1, "insert"),
          (4, "2023-01-04", 40, 1, "insert"),
          (5, "2023-01-05", 50, 1, "insert"),
          (startIdGap - 1, "2023-01-05", 50, 1, "insert")
        ).toDF("id", "date", "value", "range", "op")

        // Write initial data
        df.write
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "id")
          .option(SHORT_TABLE_NAME, "new_compaction_limit_cdc_table")
          .option("lakesoul_cdc_change_column", "op")
          .option("hashBucketNum", hashBucketNum.toString)
          .save(tablePath)

        val lakeSoulTable = LakeSoulTable.forPath(tablePath)

        for (c <- 0 until compactRounds) {
          // Simulate multiple append operations
          for (i <- c * upsertPerRounds + 1 to (c + 1) * upsertPerRounds) {
            val startId = i * startIdGap
            val appendDf = createTestDataFrame(startId, i, rowsPerUpsert)
            lakeSoulTable.upsert(appendDf)
          }

          // Get initial PartitionInfo count
          val initialFile = getFileList(tablePath)
          val incrementalFileCount = initialFile.filter(file => !file.path.contains("compactdir")).length
          val lastCompactedFileCount = initialFile.filter(file => file.path.contains("compactdir")).length
          println(s"before ${c}th time compact file count=${incrementalFileCount + lastCompactedFileCount}, " +
            s"compact file count=$lastCompactedFileCount, incremental file count=$incrementalFileCount")

          // Perform limited compaction (group every compactGroupSize PartitionInfo)
          lakeSoulTable.newCompaction(fileNumLimit = Some(compactGroupSize))

          // Get PartitionInfo count after compaction
          val compactedFiles = getFileList(tablePath)
          val compactedFileCount = compactedFiles.length

          println(s"after ${c}th time compact file count=$compactedFileCount")


          val afterCompactIncrementalFileNum = (incrementalFileCount - 1) / compactGroupSize + 1
          val firstLevelCompactFileNum = afterCompactIncrementalFileNum + lastCompactedFileCount
          val afterCompactionFileNumber = if (firstLevelCompactFileNum >= 20)
            (firstLevelCompactFileNum) / merge_num_limit + firstLevelCompactFileNum % merge_num_limit
          else firstLevelCompactFileNum
          println("==: " + afterCompactionFileNumber)
          assert(compactedFileCount == afterCompactionFileNumber,
            s"Compaction should produce files number is , but there are $compactedFileCount files")
        }

        LakeSoulTable.uncached(tablePath)
        // Verify data integrity
        val compactedData = lakeSoulTable.toDF.orderBy("id", "date").collect()
        // CDC表中，每两行会产生一行最终数据（delete-insert对），另外一半是单独的insert
        val expectedRows = 5 + (startIdGap * upsertPerRounds * compactRounds - startIdGap + rowsPerUpsert + 1) / 2
        assert(compactedData.length == expectedRows,
          s"The compressed data should have $expectedRows rows (initial 5 + ($startIdGap * $upsertPerRounds * $compactRounds - $startIdGap + $rowsPerUpsert + 1)/2 due to CDC and last odd record has no delete), but it actually has ${
            compactedData.length
          } rows")
      }
    }
  }

  test("new compaction with limited file size") {
    val maxFileSize = "30KB"
    val maxFileSizeValue = DBUtil.parseMemoryExpression(maxFileSize)
    withSQLConf(LakeSoulSQLConf.COMPACTION_LEVEL_MAX_FILE_SIZE.key -> maxFileSize,
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "1",
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "20") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath
        val spark = SparkSession.active

        val hashBucketNum = 1
        val compactRounds = 10
        val upsertPerRounds = 10
        val startIdGap = 501
        val rowsPerUpsert = 1000
        val compactFileSize = "10KB"

        // Create test data
        val df = Seq(
          (1, "2023-01-01", 10, 1),
          (2, "2023-01-02", 20, 1),
          (3, "2023-01-03", 30, 1),
          (4, "2023-01-04", 40, 1),
          (5, "2023-01-05", 50, 1)
        ).toDF("id", "date", "value", "range")

        // Write initial data
        df.write
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "id")
          .option(SHORT_TABLE_NAME, "new_compaction_size_limit_table")
          .option("hashBucketNum", hashBucketNum.toString)
          .save(tablePath)

        val lakeSoulTable = LakeSoulTable.forPath(tablePath)

        for (c <- 0 until compactRounds) {
          // Simulate multiple append operations
          for (i <- c * upsertPerRounds + 1 to (c + 1) * upsertPerRounds) {
            val startId = i * startIdGap
            val appendDf = createTestDataFrame(startId, i, rowsPerUpsert, false)
            lakeSoulTable.upsert(appendDf)
          }

          // Get initial PartitionInfo count
          val initialMaxFileSize = getFileList(tablePath).map(_.size).max
          println(s"before ${c}th compact initialMaxFileSize=$initialMaxFileSize")
          LakeSoulTable.uncached(tablePath)
          spark.time({
            // Perform limited compaction (group every compactGroupSize PartitionInfo)
            lakeSoulTable.newCompaction(fileSizeLimit = Some(compactFileSize))
          })

          // Get PartitionInfo count after compaction
          val compactedFiles = getFileList(tablePath)
          val compactedFileMax = compactedFiles.map(_.size).max

          println(s"after ${c}th compact compactedFileMax=$compactedFileMax")

          assert(compactedFileMax <= maxFileSizeValue + Math.min(initialMaxFileSize, maxFileSizeValue),
            s"Compaction should produce file with upper-bounded size:" +
              s"${maxFileSizeValue + Math.min(initialMaxFileSize, maxFileSizeValue)}, " +
              s"but there is a larger $compactedFileMax file size")

          val (compactDir, _) = splitCompactFilePath(compactedFiles.head.path)
          assert(compactedFiles.forall(file => splitCompactFilePath(file.path)._1.equals(compactDir)),
            s"Compaction should produce file with the same compaction dir, but the file list are ${
              compactedFiles.map(_.path).mkString("Array(", ", ", ")")
            }")
        }

        // Verify data integrity
        LakeSoulTable.uncached(tablePath)
        val compactedData = lakeSoulTable.toDF.orderBy("id", "date").collect()
        val expectedRows = 5 + startIdGap * upsertPerRounds * compactRounds - startIdGap + rowsPerUpsert
        assert(compactedData.length == expectedRows,
          s"The compressed data should have $expectedRows rows (initial 5 + ($startIdGap * $upsertPerRounds * $compactRounds - $startIdGap) + $rowsPerUpsert rows), but it actually has ${
            compactedData.length
          } rows")
      }
    }
  }


  test("L0 files are moved to the bottom level through multiple compactions") {
    withSQLConf(
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "3",
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "3",
      LakeSoulSQLConf.COMPACTION_MAX_BYTES_FOR_LEVEL_MULTIPLIER.key -> "2",
      LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> "3") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath
        val spark = SparkSession.active
        spark.sparkContext.setLogLevel("ERROR")

        import scala.util.Random
        val hashBucketNum = 4
        val compactRounds = 9
        val upsertPerRounds = 40
        val startIdGap = 500
        val rowsPerUpsert = 1000
        val valueSize = 16 * 600
        val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
        val random = new Random
        val value = (1 to valueSize).map(_ => chars(random.nextInt(chars.length))).mkString
        // Create test data
        val df = Seq(
          (1, value, 10, 1),
        ).toDF("id", "data", "value", "range")

        // Write initial data
        df.write
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "id")
          .option(SHORT_TABLE_NAME, "new_compaction_multiple_compact_table")
          .option("hashBucketNum", hashBucketNum.toString)
          .save(tablePath)

        val lakeSoulTable = LakeSoulTable.forPath(tablePath)
        //The first three compactions generate three L1 files (one per compaction).
        //The third compaction generates an L2 file as well.
        //The sixth compaction generates the second L2 file.
        //The Ninth compaction generates the third  L2 file.
        //Then, the three L2 files are merged into L3 via compaction.
        for (c <- 1 until compactRounds) {
          // Simulate multiple append operations
          for (i <- c * upsertPerRounds + 1 to (c + 1) * upsertPerRounds) {
            val startId = i * startIdGap
            val value = (1 to valueSize).map(_ => chars(random.nextInt(chars.length))).mkString
            val appendDf = spark.range(startId, startId + rowsPerUpsert)
              .withColumn("data", lit(value))
              .withColumn("value", lit(i * 100))
              .withColumn("range", lit(1))
              .toDF("id", "data", "value", "range")
            lakeSoulTable.upsert(appendDf)
          } //L1 file 390K 3L1 file = L2 File L2 File 1.2MB

          spark.time({
            lakeSoulTable.newCompaction()
          })

          // Get PartitionInfo count after compaction
          val compactedFiles = getFileList(tablePath)
          compactedFiles.groupBy(_.file_bucket_id).foreach { case (bucketId, dataFileInfoList) =>
            dataFileInfoList.foreach(dataFileInfo => {
              if (dataFileInfo.path.contains("compactdir")) {
                val index = dataFileInfo.path.indexOf("compactdir")
                val end = index + 11;
                val level = dataFileInfo.path.substring(index + 10, end).toInt
                if (c == 1 || c == 2) {
                  assert(level == 1 && level <= spark.conf.get("spark.dmetasoul.lakesoul.max.num.levels.limit").toInt,
                    s"This Level is ${level} compact times is ${c}, but should no this level")
                } else if (c % 3 == 0 && c % 9 != 0) {
                  assert(level == 2 && level <= spark.conf.get("spark.dmetasoul.lakesoul.max.num.levels.limit").toInt,
                    s"This Level is ${level} compact times is ${c}, but should no this level")
                } else if (c % 9 == 0) {
                  assert(level == 3, s"This Level is ${level} compact times is ${c}, but should not this level")
                }
              }
            })
          }
        }
      }
    }
  }

  test("new compaction cdc table with limited file size") {
    val maxFileSize = "30KB"
    val maxFileSizeValue = DBUtil.parseMemoryExpression(maxFileSize)
    withSQLConf(LakeSoulSQLConf.COMPACTION_LEVEL_MAX_FILE_SIZE.key -> maxFileSize,
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "1",
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "20") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath
        val spark = SparkSession.active

        val hashBucketNum = 1
        val compactRounds = 10
        val upsertPerRounds = 10
        val startIdGap = 500
        val rowsPerUpsert = 1001
        val compactFileSize = "10KB"

        // Create test data
        val df = Seq(
          (1, "2023-01-01", 10, 1, "insert"),
          (2, "2023-01-02", 20, 1, "insert"),
          (3, "2023-01-03", 30, 1, "insert"),
          (4, "2023-01-04", 40, 1, "insert"),
          (5, "2023-01-05", 50, 1, "insert"),
          (startIdGap - 1, "2023-01-05", 50, 1, "insert")
        ).toDF("id", "date", "value", "range", "op")

        // Write initial data
        df.write
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "id")
          .option(SHORT_TABLE_NAME, "new_compaction_size_limit_cdc_table")
          .option("lakesoul_cdc_change_column", "op")
          .option("hashBucketNum", hashBucketNum.toString)
          .save(tablePath)

        val lakeSoulTable = LakeSoulTable.forPath(tablePath)

        for (c <- 0 until compactRounds) {
          // Simulate multiple append operations
          for (i <- c * upsertPerRounds + 1 to (c + 1) * upsertPerRounds) {
            val startId = i * startIdGap
            val appendDf = createTestDataFrame(startId, i, rowsPerUpsert)
            lakeSoulTable.upsert(appendDf)
          }

          // Get initial PartitionInfo count
          val initialMaxFileSize = getFileList(tablePath).map(_.size).max
          println(s"before ${c}th compact initialMaxFileSize=$initialMaxFileSize")

          // Perform limited compaction (group every compactGroupSize PartitionInfo)
          LakeSoulTable.uncached(tablePath)
          spark.time({
            lakeSoulTable.newCompaction(fileSizeLimit = Some(compactFileSize))
            //          lakeSoulTable.compaction(fileSizeLimit = Some(compactFileSize), force = true)
          })

          // Get PartitionInfo count after compaction
          val compactedFiles = getFileList(tablePath)
          val compactedFileMax = compactedFiles.map(_.size).max

          println(s"after ${c}th compact compactedFileMax=$compactedFileMax")

          assert(compactedFileMax <= maxFileSizeValue + Math.min(initialMaxFileSize, maxFileSizeValue),
            s"Compaction should produce file with upper-bounded size:" +
              s"${maxFileSizeValue + Math.min(initialMaxFileSize, maxFileSizeValue)}, " +
              s"but there is a larger $compactedFileMax file size")

          val (compactDir, _) = splitCompactFilePath(compactedFiles.head.path)
          assert(compactedFiles.forall(file => splitCompactFilePath(file.path)._1.equals(compactDir)),
            s"Compaction should produce file with the same compaction dir, but the file list are ${
              compactedFiles.map(_.path).mkString("Array(", ", ", ")")
            }")
        }

        // Verify data integrity
        LakeSoulTable.uncached(tablePath)
        val finalData = lakeSoulTable.toDF.orderBy("id", "date")
        //      println(finalData.queryExecution)
        val compactedData = finalData.collect()
        //      println(compactedData.mkString("Array(", ", ", ")"))

        val expectedRows = 5 + (startIdGap * upsertPerRounds * compactRounds - startIdGap + rowsPerUpsert + 1) / 2
        assert(compactedData.length == expectedRows,
          s"The compressed data should have $expectedRows rows (initial 5 + ($startIdGap * $upsertPerRounds * $compactRounds - $startIdGap + $rowsPerUpsert + 1)/2 due to CDC and last odd record has no delete), but it actually has ${
            compactedData.length
          } rows")
      }
    }
  }

  // Auxiliary method: Get the number of files
  def getFileList(tablePath: String): Array[DataFileInfo] = {
    val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tablePath)).toString)
    val partitionList = SparkMetaVersion.getAllPartitionInfo(sm.getTableInfoOnly.table_id)
    DataOperation.getTableDataInfo(partitionList)
  }

  test("new compaction with newBucketNum") {
    withTempDir { tempDir =>
      //      val tempDir = org.apache.spark.util.Utils.createDirectory(System.getProperty("java.io.tmpdir"))
      val tablePath = tempDir.getCanonicalPath
      val spark = SparkSession.active

      val hashBucketNum = 4
      val newHashBucketNum = 7
      val compactRounds = 5
      val dataPerRounds = 10
      val compactGroupSize = 3

      // Create test data
      val df = Seq(
        (1, "2023-01-01", 10, 1),
        (2, "2023-01-02", 20, 1),
        (3, "2023-01-03", 30, 1),
        (4, "2023-01-04", 40, 1),
        (5, "2023-01-05", 50, 1)
      ).toDF("id", "date", "value", "range")

      //    val df = Seq(
      //      (1, "2023-01-01", 10, 1, "insert"),
      //      (2, "2023-01-02", 20, 1, "insert"),
      //      (3, "2023-01-03", 30, 1, "insert"),
      //      (4, "2023-01-04", 40, 1, "insert"),
      //      (5, "2023-01-05", 50, 1, "insert")
      //    ).toDF("id", "date", "value", "range", "op")

      // Write initial data
      df.write
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "id")
        .option(SHORT_TABLE_NAME, "new_change_bucket_num_table")
        .option("hashBucketNum", hashBucketNum.toString)
        //      .option("lakesoul_cdc_change_column", "op")
        .save(tablePath)

      val lakeSoulTable = LakeSoulTable.forPath(tablePath)

      for (i <- 1 to 100) {
        val appendDf = Seq(
          (i * 10, s"2023-02-0$i", i * 100, 1)
        ).toDF("id", "date", "value", "range")
        //        val appendDf = Seq(
        //          (i * 10, s"2023-02-0$i", i * 100, 1, "insert")
        //        ).toDF("id", "date", "value", "range", "op")
        lakeSoulTable.upsert(appendDf)
      }
      assert(getFileList(tablePath).groupBy(_.file_bucket_id).keys.toSet.size == hashBucketNum)
      assert(getTableInfo(tablePath).bucket_num == hashBucketNum)

      lakeSoulTable.newCompaction(newBucketNum = Some(newHashBucketNum))

      LakeSoulTable.uncached(tablePath)
      assert(getFileList(tablePath).groupBy(_.file_bucket_id).keys.toSet.size == newHashBucketNum)
      assert(getTableInfo(tablePath).bucket_num == newHashBucketNum)

      LakeSoulTable.uncached(tablePath)
      val lakeSoulTable2 = LakeSoulTable.forPath(tablePath)
      val compactedData = lakeSoulTable2.toDF.orderBy("id", "date").collect()
      println(compactedData.mkString("Array(", ", ", ")"))
      assert(compactedData.length == 105,
        s"The compressed data should have ${105} rows, but it actually has ${compactedData.length} rows")

    }
  }

  test("new compaction with newBucketNum、limited file size and limited file number") {
    val maxFileSize = "30KB"
    withSQLConf(LakeSoulSQLConf.COMPACTION_LEVEL_MAX_FILE_SIZE.key -> maxFileSize) {
      withTempDir { tempDir =>
        //      val tempDir = org.apache.spark.util.Utils.createDirectory(System.getProperty("java.io.tmpdir"))
        val tablePath = tempDir.getCanonicalPath
        val spark = SparkSession.active

        val hashBucketNum = 1
        val firstChangedHashBucketNum = 2
        val secondChangedHashBucketNum = 5
        val compactGroupSize = 3
        val compactFileSize = "10KB"

        // Create test data
        val df = Seq(
          (1, "2023-01-01", 10, 1),
          (2, "2023-01-02", 20, 1),
          (3, "2023-01-03", 30, 1),
          (4, "2023-01-04", 40, 1),
          (5, "2023-01-05", 50, 1)
        ).toDF("id", "date", "value", "range")

        // Write initial data
        df.write
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "id")
          .option(SHORT_TABLE_NAME, "new_compaction_all_param_table")
          .option("hashBucketNum", hashBucketNum.toString)
          //      .option("lakesoul_cdc_change_column", "op")
          .save(tablePath)

        val lakeSoulTable = LakeSoulTable.forPath(tablePath)

        for (i <- 1 to 20) {
          val appendDf = Seq(
            (i * 10, s"2023-02-0$i", i * 100, 1)
          ).toDF("id", "date", "value", "range")
          //        val appendDf = Seq(
          //          (i * 10, s"2023-02-0$i", i * 100, 1, "insert")
          //        ).toDF("id", "date", "value", "range", "op")
          lakeSoulTable.upsert(appendDf)
        }
        assert(getFileList(tablePath).groupBy(_.file_bucket_id).keys.toSet.size == hashBucketNum)
        assert(getTableInfo(tablePath).bucket_num == hashBucketNum)

        lakeSoulTable.newCompaction(fileNumLimit = Some(compactGroupSize), fileSizeLimit = Some(compactFileSize),
          newBucketNum = Some(firstChangedHashBucketNum))
        assert(getFileList(tablePath).groupBy(_.file_bucket_id).keys.toSet.size == firstChangedHashBucketNum)
        assert(getTableInfo(tablePath).bucket_num == firstChangedHashBucketNum)

        for (i <- 21 to 100) {
          val appendDf = Seq(
            (i * 10, s"2023-02-0$i", i * 100, 1)
          ).toDF("id", "date", "value", "range")
          //        val appendDf = Seq(
          //          (i * 10, s"2023-02-0$i", i * 100, 1, "insert")
          //        ).toDF("id", "date", "value", "range", "op")
          lakeSoulTable.upsert(appendDf)
        }

        lakeSoulTable.newCompaction(fileNumLimit = Some(compactGroupSize), fileSizeLimit = Some(compactFileSize),
          newBucketNum = Some(secondChangedHashBucketNum))
        assert(getFileList(tablePath).groupBy(_.file_bucket_id).keys.toSet.size == secondChangedHashBucketNum)
        assert(getTableInfo(tablePath).bucket_num == secondChangedHashBucketNum)

        val compactedData = lakeSoulTable.toDF.orderBy("id", "date").collect()
        println(compactedData.mkString("Array(", ", ", ")"))
        assert(compactedData.length == 105,
          s"The compressed data should have ${105} rows, but it actually has ${compactedData.length} rows")
      }
    }
  }

  test("check new compaction data") {
    withSQLConf() {
      withTempDir(dir => {
        val tablePath = dir.getCanonicalPath
        val df = Seq(("2021-01-01", 1, "rice", "insert"), ("2021-01-01", 2, "bread", "insert"))
          .toDF("date", "id", "name", "op")
        df.write
          .mode("append")
          .format("lakesoul")
          .option("hashPartitions", "id")
          .option("hashBucketNum", "4")
          .option("lakesoul_cdc_change_column", "op")
          .save(tablePath)

        val df1 = Seq(("2021-01-01", 11, "rice", "insert"), ("2021-01-01", 21, "bread", "insert"))
          .toDF("date", "id", "name", "op")
        val df2 = Seq(("2021-01-01", 12, "rice", "insert"), ("2021-01-01", 22, "bread", "insert"))
          .toDF("date", "id", "name", "op")
        val df3 = Seq(("2021-01-01", 13, "rice", "insert"), ("2021-01-01", 23, "bread", "insert"))
          .toDF("date", "id", "name", "op")

        val df4 = Seq(("2021-01-01", 14, "rice", "insert"), ("2021-01-01", 24, "bread", "insert"))
          .toDF("date", "id", "name", "op")
        val df5 = Seq(("2021-01-01", 15, "rice", "insert"), ("2021-01-01", 25, "bread", "insert"))
          .toDF("date", "id", "name", "op")
        val df6 = Seq(("2021-01-01", 16, "rice", "insert"), ("2021-01-01", 26, "bread", "insert"))
          .toDF("date", "id", "name", "op")
        val df7 = Seq(("2021-01-01", 17, "rice", "insert"), ("2021-01-01", 27, "bread", "insert"))
          .toDF("date", "id", "name", "op")
        val df8 = Seq(("2021-01-01", 18, "rice", "insert"), ("2021-01-01", 28, "bread", "insert"))
          .toDF("date", "id", "name", "op")
        val lake = LakeSoulTable.forPath(tablePath)

        lake.upsert(df1)
        lake.newCompaction()
        val data1 = lake.toDF
        data1.show()
        checkAnswer(data1, Seq(
          ("2021-01-01", 1, "rice", "insert"), ("2021-01-01", 2, "bread", "insert"),
          ("2021-01-01", 11, "rice", "insert"), ("2021-01-01", 21, "bread", "insert")
        ).toDF("date", "id", "name", "op"))

        lake.upsert(df2)
        lake.newCompaction()
        val data2 = lake.toDF
        checkAnswer(data2, Seq(
          ("2021-01-01", 1, "rice", "insert"), ("2021-01-01", 2, "bread", "insert"),
          ("2021-01-01", 11, "rice", "insert"), ("2021-01-01", 21, "bread", "insert"),
          ("2021-01-01", 12, "rice", "insert"), ("2021-01-01", 22, "bread", "insert")
        ).toDF("date", "id", "name", "op"))


        lake.upsert(df3)
        lake.newCompaction()
        val data3 = lake.toDF
        checkAnswer(data3, Seq(
          ("2021-01-01", 1, "rice", "insert"), ("2021-01-01", 2, "bread", "insert"),
          ("2021-01-01", 11, "rice", "insert"), ("2021-01-01", 21, "bread", "insert"),
          ("2021-01-01", 12, "rice", "insert"), ("2021-01-01", 22, "bread", "insert"),
          ("2021-01-01", 13, "rice", "insert"), ("2021-01-01", 23, "bread", "insert")
        ).toDF("date", "id", "name", "op"))

        lake.upsert(df4)
        lake.newCompaction()
        val data4 = lake.toDF
        checkAnswer(data4, Seq(
          ("2021-01-01", 1, "rice", "insert"), ("2021-01-01", 2, "bread", "insert"),
          ("2021-01-01", 11, "rice", "insert"), ("2021-01-01", 21, "bread", "insert"),
          ("2021-01-01", 12, "rice", "insert"), ("2021-01-01", 22, "bread", "insert"),
          ("2021-01-01", 13, "rice", "insert"), ("2021-01-01", 23, "bread", "insert"),
          ("2021-01-01", 14, "rice", "insert"), ("2021-01-01", 24, "bread", "insert")
        ).toDF("date", "id", "name", "op"))

        lake.upsert(df5)
        lake.newCompaction()
        val data5 = lake.toDF
        checkAnswer(data5, Seq(
          ("2021-01-01", 1, "rice", "insert"), ("2021-01-01", 2, "bread", "insert"),
          ("2021-01-01", 11, "rice", "insert"), ("2021-01-01", 21, "bread", "insert"),
          ("2021-01-01", 12, "rice", "insert"), ("2021-01-01", 22, "bread", "insert"),
          ("2021-01-01", 13, "rice", "insert"), ("2021-01-01", 23, "bread", "insert"),
          ("2021-01-01", 14, "rice", "insert"), ("2021-01-01", 24, "bread", "insert"),
          ("2021-01-01", 15, "rice", "insert"), ("2021-01-01", 25, "bread", "insert")
        ).toDF("date", "id", "name", "op"))

        lake.upsert(df6)
        lake.newCompaction()
        val data6 = lake.toDF
        checkAnswer(data6, Seq(
          ("2021-01-01", 1, "rice", "insert"), ("2021-01-01", 2, "bread", "insert"),
          ("2021-01-01", 11, "rice", "insert"), ("2021-01-01", 21, "bread", "insert"),
          ("2021-01-01", 12, "rice", "insert"), ("2021-01-01", 22, "bread", "insert"),
          ("2021-01-01", 13, "rice", "insert"), ("2021-01-01", 23, "bread", "insert"),
          ("2021-01-01", 14, "rice", "insert"), ("2021-01-01", 24, "bread", "insert"),
          ("2021-01-01", 15, "rice", "insert"), ("2021-01-01", 25, "bread", "insert"),
          ("2021-01-01", 16, "rice", "insert"), ("2021-01-01", 26, "bread", "insert")
        ).toDF("date", "id", "name", "op"))

        lake.upsert(df7)
        lake.newCompaction()
        val data7 = lake.toDF
        checkAnswer(data7, Seq(
          ("2021-01-01", 1, "rice", "insert"), ("2021-01-01", 2, "bread", "insert"),
          ("2021-01-01", 11, "rice", "insert"), ("2021-01-01", 21, "bread", "insert"),
          ("2021-01-01", 12, "rice", "insert"), ("2021-01-01", 22, "bread", "insert"),
          ("2021-01-01", 13, "rice", "insert"), ("2021-01-01", 23, "bread", "insert"),
          ("2021-01-01", 14, "rice", "insert"), ("2021-01-01", 24, "bread", "insert"),
          ("2021-01-01", 15, "rice", "insert"), ("2021-01-01", 25, "bread", "insert"),
          ("2021-01-01", 16, "rice", "insert"), ("2021-01-01", 26, "bread", "insert"),
          ("2021-01-01", 17, "rice", "insert"), ("2021-01-01", 27, "bread", "insert")
        ).toDF("date", "id", "name", "op"))

        lake.upsert(df8)
        lake.newCompaction()
        val data8 = lake.toDF
        checkAnswer(data8, Seq(
          ("2021-01-01", 1, "rice", "insert"), ("2021-01-01", 2, "bread", "insert"),
          ("2021-01-01", 11, "rice", "insert"), ("2021-01-01", 21, "bread", "insert"),
          ("2021-01-01", 12, "rice", "insert"), ("2021-01-01", 22, "bread", "insert"),
          ("2021-01-01", 13, "rice", "insert"), ("2021-01-01", 23, "bread", "insert"),
          ("2021-01-01", 14, "rice", "insert"), ("2021-01-01", 24, "bread", "insert"),
          ("2021-01-01", 15, "rice", "insert"), ("2021-01-01", 25, "bread", "insert"),
          ("2021-01-01", 16, "rice", "insert"), ("2021-01-01", 26, "bread", "insert"),
          ("2021-01-01", 17, "rice", "insert"), ("2021-01-01", 27, "bread", "insert"),
          ("2021-01-01", 18, "rice", "insert"), ("2021-01-01", 28, "bread", "insert")
        ).toDF("date", "id", "name", "op"))
      })
    }
  }

  test("check layer compaction data") {
    withSQLConf(LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "3",
      LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> "2",
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "4") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath
        val spark = SparkSession.active
        val df = Seq((1, "value1", 1), (2, "value2", 1), (3, "value3", 1))
          .toDF("id", "value", "range")

        df.write
          .mode("append")
          .format("lakesoul")
          .option("hashPartitions", "id")
          .option("rangePartitions", "range")
          .option(SHORT_TABLE_NAME, "checkdata_table")
          .option("hashBucketNum", "4")
          .save(tablePath)

        val lakeSoulTable = LakeSoulTable.forPath(tablePath)
        val compactRounds = 30
        val upsertRounds = 10
        import scala.util.Random
        val random = new Random()
        val expectedData = new HashMap[Int, (String, Int, Int)]

        val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
        for (i <- 1 to compactRounds) {
          for (j <- 1 to upsertRounds) {
            val randomIdList = List.fill(10)(Random.nextInt(11))
            val data: Seq[(Int, String, Int)] = randomIdList.map { id =>
              val result = (1 to 10).map(_ => chars(random.nextInt(chars.length))).mkString
              expectedData.put(id, (result, i, j))
              (id, result, 1)
            }.toSeq
            val df = data.toDF("id", "value", "range")
            lakeSoulTable.upsert(df)
          }
          lakeSoulTable.newCompaction()
          val actualDf = lakeSoulTable.toDF
          val actualData = actualDf.select("id", "value").collect()
          actualData.foreach { row =>
            val id = row.getInt(0)
            val actualValue = row.getString(1)
            expectedData.get(id) match {
              case Some((expectedValue, round, batch)) =>
                if (actualValue != expectedValue) {
                  assert(false, s"[ERROR] Round $i - ID $id not same! " +
                    s"expectedValue: $expectedValue (from$round-B$batch), " +
                    s"ActualValue: $actualValue")
                }
              case None =>
                assert(false, s"[ERROR] Round $i - This $id should not exists")
            }
          }

        }
      }

    }
  }

  test("simple check new compaction with large file") {
    withSQLConf(LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "3",
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "3",
      LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> "2",
      LakeSoulSQLConf.COMPACTION_LEVEL_MAX_FILE_SIZE.key -> "20KB",
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "4") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath
        val spark = SparkSession.active
        import spark.implicits._

        var df = Seq((1, "value1-insert-first", 1), (2, "value2-insert-first", 1), (3, "value3-insert-first", 1))
          .toDF("id", "value", "range")

        df.write
          .mode("append")
          .format("lakesoul")
          .option("hashPartitions", "id")
          .option("rangePartitions", "range")
          .option(SHORT_TABLE_NAME, "checkdata_largedata_table")
          .option("hashBucketNum", "1")
          .save(tablePath)


        val lakeSoulTable = LakeSoulTable.forPath(tablePath)

        val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
        import scala.util.Random
        val random = new Random()
        val targetSize = 10000
        val largedata = (1 to targetSize).map(_ => chars(random.nextInt(chars.length))).mkString
        df = Seq((1, largedata, 1), (3, "value3-update-first", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        df = Seq((2, "value2-update-first", 1), (7, "value7-insert-first", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        lakeSoulTable.newCompaction() //generate large file

        var data = lakeSoulTable.toDF
        checkAnswer(data, Seq(
          (1, largedata, 1),
          (2, "value2-update-first", 1),
          (3, "value3-update-first", 1),
          (7, "value7-insert-first", 1)
        ).toDF("id", "value", "range"))

        df = Seq((1, "value1-update-second", 1), (2, "value2-update-second", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        df = Seq((4, "value4-insert-first", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        df = Seq((5, "value5-insert-first", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        lakeSoulTable.newCompaction()

        data = lakeSoulTable.toDF
        checkAnswer(data, Seq(
          (1, "value1-update-second", 1), (2, "value2-update-second", 1),
          (3, "value3-update-first", 1), (4, "value4-insert-first", 1),
          (5, "value5-insert-first", 1),
          (7, "value7-insert-first", 1)
        ).toDF("id", "value", "range"))


        df = Seq((1, "value1-update-third", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        df = Seq((4, "value4-update-first", 1), (3, "value3-update-second", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        df = Seq((6, "value6-insert-first", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        lakeSoulTable.newCompaction()

        data = lakeSoulTable.toDF
        checkAnswer(data, Seq(
          (1, "value1-update-third", 1), (2, "value2-update-second", 1),
          (3, "value3-update-second", 1), (4, "value4-update-first", 1),
          (5, "value5-insert-first", 1), (6, "value6-insert-first", 1),
          (7, "value7-insert-first", 1)
        ).toDF("id", "value", "range"))

      }
    }
  }

  test("check new compaction with compact file and incremental file") {
    withSQLConf(LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "3",
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "4",
      LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> "2",
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "1") {
      withTempDir { tempDir =>

        val tablePath = tempDir.getCanonicalPath
        val spark = SparkSession.active
        import spark.implicits._

        var df = Seq((1, "value1-insert-first", 1), (2, "value2-insert-first", 1), (3, "value3-insert-first", 1))
          .toDF("id", "value", "range")

        df.write
          .mode("append")
          .format("lakesoul")
          .option("hashPartitions", "id")
          .option("rangePartitions", "range")
          .option(SHORT_TABLE_NAME, "checkdata_largedata_table")
          .option("hashBucketNum", "1")
          .save(tablePath)


        val lakeSoulTable = LakeSoulTable.forPath(tablePath)
        df = Seq((1, "value1-update-first", 1), (2, "value2-update-first", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        df = Seq((4, "value4-insert-first", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        df = Seq((5, "value5-insert-first", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        lakeSoulTable.newCompaction()

        var data = lakeSoulTable.toDF
        checkAnswer(data, Seq(
          (1, "value1-update-first", 1), (2, "value2-update-first", 1),
          (3, "value3-insert-first", 1), (4, "value4-insert-first", 1),
          (5, "value5-insert-first", 1)
        ).toDF("id", "value", "range"))

        df = Seq((1, "value1-update-second", 1), (3, "value3-update-first", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        df = Seq((4, "value4-update-first", 1), (6, "value6-insert-first", 1)).toDF("id", "value", "range")
        lakeSoulTable.upsert(df)
        lakeSoulTable.newCompaction()


        data = lakeSoulTable.toDF
        checkAnswer(data, Seq(
          (1, "value1-update-second", 1), (2, "value2-update-first", 1),
          (3, "value3-update-first", 1), (4, "value4-update-first", 1),
          (5, "value5-insert-first", 1), (6, "value6-insert-first", 1)
        ).toDF("id", "value", "range"))

      }
    }
  }


  test("check new compaction with last level triggers multiple compactions") {
    withSQLConf(LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "3",
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "2",
      LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> "2",
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "1") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath
        val spark = SparkSession.active

        var randomIdList = (1 to 100).toArray
        val expectedData = new HashMap[Int, String]
        val data: Seq[(Int, String, Int)] = randomIdList.map { id =>
          val result = "data" + id.toString
          expectedData.put(id, result)
          (id, result, 1)
        }.toSeq
        val df = data.toDF("id", "value", "range")
        df.write
          .mode("append")
          .format("lakesoul")
          .option("hashPartitions", "id")
          .option("rangePartitions", "range")
          .option(SHORT_TABLE_NAME, "checkdata_table")
          .option("hashBucketNum", "1")
          .save(tablePath)

        val lakeSoulTable = LakeSoulTable.forPath(tablePath)


        val compactRounds = 36 // L1
        import scala.util.Random
        val random = new Random()
        for (i <- 1 to compactRounds) {
          for (j <- 1 to 2) {
            val upsertIdList = Random.shuffle(1 to 100).take(50).toList
            val data: Seq[(Int, String, Int)] = upsertIdList.map { id =>
              val batchId = "compact-" + i + "-upsert-" + j
              val result = "data" + batchId
              expectedData.put(id, result)
              (id, result, 1)
            }.toSeq
            val df = data.toDF("id", "value", "range")
            lakeSoulTable.upsert(df)
          }
          lakeSoulTable.newCompaction()
          val actualDf = lakeSoulTable.toDF
          val actualData = actualDf.select("id", "value").collect().map { row => (row.getInt(0), row.getString(1)) }.toMap
          actualData.foreach { case (id, value) =>
            expectedData.get(id) match {
              case Some(expectedValue) =>
                if (expectedValue != value) {
                  assert(false, s"The ID $id actual value is $value, it should is $expectedValue")
                }
              case None => assert(false, s"The ID $id not exists, it should be exists")
              case _ => assert(false)
            }
          }
        }
      }
    }
  }

  test("check new compaction data with multi-layer compact") {
    withSQLConf(LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "3",
      LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> "2",
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "3",
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "4") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath
        val spark = SparkSession.active
        val df = Seq((1, "value1", 1), (2, "value2", 1), (3, "value3", 1))
          .toDF("id", "value", "range")

        df.write
          .mode("append")
          .format("lakesoul")
          .option("hashPartitions", "id")
          .option("rangePartitions", "range")
          .option(SHORT_TABLE_NAME, "checkdata_table")
          .option("hashBucketNum", "1")
          .save(tablePath)

        //upsert 1000

        import scala.util.Random
        val random = new Random()
        var randomIdList = (1 to 1000).toArray
        val expectedData = new HashMap[Int, String]
        val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
        val data: Seq[(Int, String, Int)] = randomIdList.map { id =>
          val result = (1 to 100).map(_ => chars(random.nextInt(chars.length))).mkString
          expectedData.put(id, result)
          (id, result, 1)
        }.toSeq
        val df1 = data.toDF("id", "value", "range")
        val lakeSoulTable = LakeSoulTable.forPath(tablePath)
        lakeSoulTable.upsert(df1)

        val compactRounds = 40
        val upsertRounds = 3
        for (i <- 1 to compactRounds) {
          for (j <- 1 to upsertRounds) {
            val upsertListSize = Random.nextInt(451) + 50
            val upsertIdList = Random.shuffle(1 to 1000).take(upsertListSize).toList
            val data: Seq[(Int, String, Int)] = upsertIdList.map { id =>
              val result = (1 to 100).map(_ => chars(random.nextInt(chars.length))).mkString
              expectedData.put(id, result)
              (id, result, 1)
            }.toSeq
            val df = data.toDF("id", "value", "range")
            lakeSoulTable.upsert(df)
          }

          lakeSoulTable.newCompaction()
          val actualDf = lakeSoulTable.toDF
          val actualData = actualDf.select("id", "value").collect().map { row => (row.getInt(0), row.getString(1)) }.toMap
          actualData.foreach { case (id, value) =>
            expectedData.get(id) match {
              case Some(expectedValue) =>
                if (expectedValue != value) {
                  assert(false, s"The ID $id actual value is $value, it should is $expectedValue")
                }
              case None => assert(false, s"The ID $id not exists, it should be exists")
              case _ => assert(false)
            }
          }
        }
      }
    }
  }

  test("check new compaction data with newBucketNum、limited file size and limited file number") {
    withSQLConf(
      LakeSoulSQLConf.COMPACTION_LEVEL_MAX_FILE_SIZE.key -> "10KB") {
      withTempDir(dir => {
        val tablePath = dir.getCanonicalPath
        val hashBucketNum = 1
        val firstChangedHashBucketNum = 2
        val secondChangedHashBucketNum = 5
        val compactFileSize = "10KB"
        val df = Seq(
          (1, "2023-01-01", 10, 1),
        ).toDF("id", "date", "value", "range")

        df.write
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "id")
          .option("hashBucketNum", hashBucketNum.toString)
          .save(tablePath)

        val lakeSoulTable = LakeSoulTable.forPath(tablePath)
        for (i <- 2 to 9) {
          val appendDf = Seq(
            (i, s"2023-02-0$i", i * 10, 1)
          ).toDF("id", "date", "value", "range")
          lakeSoulTable.upsert(appendDf)
        }

        lakeSoulTable.newCompaction(fileNumLimit = Some(3), fileSizeLimit = Some(compactFileSize),
          newBucketNum = Some(firstChangedHashBucketNum))
        val data = lakeSoulTable.toDF
        checkAnswer(data, Seq((1, "2023-01-01", 10, 1), (2, "2023-02-02", 20, 1), (3, "2023-02-03", 30, 1),
          (4, "2023-02-04", 40, 1), (5, "2023-02-05", 50, 1), (6, "2023-02-06", 60, 1),
          (7, "2023-02-07", 70, 1), (8, "2023-02-08", 80, 1), (9, "2023-02-09", 90, 1))
          .toDF("id", "date", "value", "range"))

        for (i <- 1 to 9) {
          val appendDf = Seq(
            (i, s"2023-02-0$i", i * 10 + 1, 1)
          ).toDF("id", "date", "value", "range")
          lakeSoulTable.upsert(appendDf)
        }
        lakeSoulTable.newCompaction(fileNumLimit = Some(3), fileSizeLimit = Some(compactFileSize),
          newBucketNum = Some(secondChangedHashBucketNum))

        val data1 = lakeSoulTable.toDF
        checkAnswer(data1, Seq((1, "2023-02-01", 11, 1), (2, "2023-02-02", 21, 1), (3, "2023-02-03", 31, 1),
          (4, "2023-02-04", 41, 1), (5, "2023-02-05", 51, 1), (6, "2023-02-06", 61, 1),
          (7, "2023-02-07", 71, 1), (8, "2023-02-08", 81, 1), (9, "2023-02-09", 91, 1))
          .toDF("id", "date", "value", "range"))
      })
    }
  }


  // Auxiliary method: Get the bucket number of table
  def getTableInfo(tablePath: String): TableInfo = {
    val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tablePath)).toString)
    sm.getTableInfoOnly
  }

  //  Only use for local test
  //  test("compaction with concurrent data insertion and compaction") {
  //    withTempDir { tempDir =>
  //      //      val tempDir = org.apache.spark.util.Utils.createDirectory(System.getProperty("java.io.tmpdir"))
  //      val tablePath = tempDir.getCanonicalPath
  //      //      val spark = SparkSession.active
  //
  //      val hashBucketNum = 4
  //      val compactRounds = 10
  //      val compactGapMs = 10000
  //      val upsertRounds = 100
  //      val upsertGapMs = 100
  //      val upsertRows = 1024
  //      val compactGroupSize = 3
  //      val cdc = true
  //      val ranges = 2;
  //
  //      val fields: util.List[Field] = if (cdc) {
  //        util.Arrays.asList(
  //          new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
  //          new Field("date", FieldType.nullable(new ArrowType.Utf8), null),
  //          new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null),
  //          new Field("range", FieldType.nullable(new ArrowType.Int(32, true)), null),
  //          new Field(TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT, FieldType.nullable(new ArrowType.Utf8), null),
  //        )
  //      } else {
  //        util.Arrays.asList(
  //          new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
  //          new Field("date", FieldType.nullable(new ArrowType.Utf8), null),
  //          new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null),
  //          new Field("range", FieldType.nullable(new ArrowType.Int(32, true)), null),
  //        )
  //      }
  //      //      if (cdc) fields = util.Arrays.asList(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null), new Field("range", FieldType.nullable(new ArrowType.Int(32, true)), null), new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null), new Field("utf8", FieldType.nullable(new ArrowType.Utf8), null), new Field("decimal", FieldType.nullable(ArrowType.Decimal.createDecimal(10, 3, null)), null), new Field("boolean", FieldType.nullable(new ArrowType.Bool), null), new Field("date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null), new Field("datetimeSec", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, ZoneId.of("UTC").toString)), null), new Field(TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT, FieldType.notNullable(new ArrowType.Utf8), null), new Field("datetimeMilli", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneId.of("UTC").toString)), null))
  //      //      else fields = util.Arrays.asList(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null), new Field("range", FieldType.nullable(new ArrowType.Int(32, true)), null), new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null), new Field("utf8", FieldType.nullable(new ArrowType.Utf8), null), new Field("decimal", FieldType.nullable(ArrowType.Decimal.createDecimal(10, 3, null)), null), new Field("boolean", FieldType.nullable(new ArrowType.Bool), null), new Field("date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null), new Field("datetimeSec", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, ZoneId.of("UTC").toString)), null), new Field("datetimeMilli", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneId.of("UTC").toString)), null))
  //
  //      val numCols = fields.size
  //      // Create test data
  //      val df = if (cdc) {
  //        Seq(
  //          (-1, "2023-01-01", 10, ranges + 1, "insert"),
  //          (-2, "2023-01-02", 20, ranges + 1, "insert"),
  //          (-3, "2023-01-03", 30, ranges + 1, "insert"),
  //          (-4, "2023-01-04", 40, ranges + 1, "insert"),
  //          (-5, "2023-01-05", 50, ranges + 1, "insert")
  //        ).toDF("id", "date", "value", "range", TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT)
  //      } else {
  //        Seq(
  //          (1, "2023-01-01", 10, ranges + 1),
  //          (2, "2023-01-02", 20, ranges + 1),
  //          (3, "2023-01-03", 30, ranges + 1),
  //          (4, "2023-01-04", 40, ranges + 1),
  //          (5, "2023-01-05", 50, ranges + 1)
  //        ).toDF("id", "date", "value", "range")
  //      }
  //
  //      // Write initial data
  //      df.write
  //        .format("lakesoul")
  //        //        .option("rangePartitions", "range")
  //        .option("hashPartitions", "id")
  //        .option(SHORT_TABLE_NAME, "compaction_size_limit_table")
  //        .option("hashBucketNum", hashBucketNum.toString)
  //        .option(TableInfoProperty.CDC_CHANGE_COLUMN, TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT)
  //        .save(tablePath)
  //
  //      val lakeSoulTable = LakeSoulTable.forPath(tablePath)
  //      val insertThread = new Thread {
  //
  //        override def run(): Unit = {
  //          val localWriter = new LakeSoulLocalJavaWriter()
  //          val params = Map(
  //            ("lakesoul.pg.url", "jdbc:postgresql://127.0.0.1:5433/test_lakesoul_meta?stringtype=unspecified"),
  //            ("lakesoul.pg.username", "yugabyte"),
  //            ("lakesoul.pg.password", "yugabyte"),
  //            (LakeSoulLocalJavaWriter.TABLE_NAME, "compaction_size_limit_table")
  //          )
  //          localWriter.init(params.asJava)
  //
  //          for (c <- 0 until upsertRounds) {
  //            println(s"upsertRound = $c")
  //            for (i <- c * upsertRows until c * upsertRows + upsertRows) {
  //              val row: Array[AnyRef] = new Array[AnyRef](if (cdc) {
  //                numCols - 1
  //              }
  //              else {
  //                numCols
  //              })
  //              var j: Int = 0
  //              var k: Int = 0
  //              while (j < numCols) {
  //                if (!fields.get(j).getName.contains(TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT)) {
  //                  if (fields.get(j).getName.contains("id")) {
  //                    row(k) = i.asInstanceOf[AnyRef]
  //                    k += 1
  //                  }
  //                  else {
  //                    if (fields.get(j).getName.contains("range")) {
  //                      row(k) = (i % ranges).asInstanceOf[AnyRef]
  //                      k += 1
  //                    }
  //                    else {
  //                      row(k) = fields.get(j).getType.accept(ArrowTypeMockDataGenerator.INSTANCE)
  //                      k += 1
  //                    }
  //                  }
  //                }
  //
  //                j += 1
  //              }
  //              localWriter.writeAddRow(row)
  //              //              if (cdc && i % 7 == 0) {
  //              //                localWriter.writeDeleteRow(row)
  //              //              }
  //            }
  //            localWriter.commit()
  //            Thread.sleep(upsertGapMs)
  //          }
  //          localWriter.close()
  //        }
  //      }
  //      LakeSoulTable.uncached(tablePath)
  //      val compactionThread = new Thread {
  //        override def run(): Unit = {
  //          for (c <- 1 to compactRounds) {
  //            println(s"compactRound = $c")
  //
  //            lakeSoulTable.compaction(fileNumLimit = Some(2), fileSizeLimit = Some("10KB"), force = false)
  //            Thread.sleep(compactGapMs) // Simulate compaction delay
  //          }
  //        }
  //      }
  //      //      insertThread.start()
  //      //      compactionThread.start()
  //      //      insertThread.join()
  //      //      compactionThread.join()
  //      //      val compactedData = lakeSoulTable.toDF.orderBy("id", "date").collect()
  //      //      assert(compactedData.length == upsertRounds * upsertRows + 5, s"The compressed data should have 105 rows, but it actually has ${compactedData.length} rows")
  //    }
  //  }

  // Add this helper method
  private def createTestDataFrame(
                                   startId: Int,
                                   batchId: Int,
                                   rowsPerBatch: Int,
                                   isCdc: Boolean = true): DataFrame = {
    if (isCdc) {
      (0 until rowsPerBatch)
        .flatMap(j => if ((j + startId) % 2 == 0) {
          // For even j, create a delete-insert pair
          Seq(
            (startId + j - 1, s"2023-02-0$batchId", batchId * 100, 1, "delete"),
            (startId + j, s"2023-02-0$batchId", batchId * 100, 1, "insert")
          )
        } else {
          // For odd j, create single insert
          Seq((startId + j, s"2023-02-0$batchId", batchId * 100, 1, "insert"))
        })
        .toDF("id", "date", "value", "range", "op")
    } else {
      (0 until rowsPerBatch)
        .map(j => (startId + j, s"2023-02-0$batchId", batchId * 100, 1))
        .toDF("id", "date", "value", "range")
    }
  }

}
