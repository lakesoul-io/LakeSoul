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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession, MergeOpInt}
import org.apache.spark.sql.lakesoul.utils.{SparkUtil, TableInfo}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.junit.JUnitRunner

import java.math.MathContext

@RunWith(classOf[JUnitRunner])
class CompactionSuite extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with LakeSoulSQLCommandTest {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.network.timeout", "10000000")
      .set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .set("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
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

  test("partitions are not been compacted by default") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      df1.write
        .option("rangePartitions", "range")
        .format("lakesoul")
        .save(tableName)

      assert(SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString).snapshot.getPartitionInfoArray.forall(_.read_files.size == 1))

    })
  }

  test("simple compaction without partition") {
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


      LakeSoulTable.forPath(tableName).compaction()
      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1))
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

    })
  }

  test("simple compaction with partition") {
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


      LakeSoulTable.forPath(tableName).compaction()
      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1))
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

    })
  }

  test("simple compaction without pk") {
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

      LakeSoulTable.forPath(tableName).compaction()
    })
  }

  test("simple compaction - string partition") {
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


      LakeSoulTable.forPath(tableName).compaction()
      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1))
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

    })
  }

  test("simple compaction - multiple partition") {
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


      LakeSoulTable.forPath(tableName).compaction()
      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1))
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

    })
  }

  test("compaction with condition - simple") {
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

      LakeSoulTable.forPath(tableName).compaction("range=1")
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
  test("compaction with call - simple condition") {
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

      /* usage for call compaction
      * call LakeSoulTable.compaction(condition=>map('range',1),tablePath=>'file://path')
      * call LakeSoulTable.compaction(condition=>map('range',1),tableName=>'lakesoul')
      * call LakeSoulTable.compaction(tableName=>'lakesoul',hiveTableName=>'hive')
      * call LakeSoulTable.compaction(tableName=>'lakesoul',cleanOld=>true)
      */
      sql("call LakeSoulTable.compaction(condition=>map('range',1),tablePath=>'" + tableName + "')")
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

  test("compaction with condition - multi partitions should failed") {
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
        LakeSoulTable.forPath(tableName).compaction("range=1 or range=2")
      }
      assert(e.getMessage().contains("Couldn't execute compaction because of your condition") &&
        e.getMessage().contains("we only allow one partition"))

    })
  }


  test("upsert after compaction") {
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

      LakeSoulTable.forPath(tableName).compaction("range=1")

      checkAnswer(LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "value"),
        Seq((1, 1, 11), (1, 2, 22), (1, 3, 33), (1, 4, 4)).toDF("range", "hash", "value"))

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df3)
      }

      checkAnswer(LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "value"),
        Seq((1, 1, 11), (1, 2, 222), (1, 3, 333), (1, 4, 444), (1, 5, 555)).toDF("range", "hash", "value"))


      LakeSoulTable.forPath(tableName).compaction("range=1")

      checkAnswer(LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "value"),
        Seq((1, 1, 11), (1, 2, 222), (1, 3, 333), (1, 4, 444), (1, 5, 555)).toDF("range", "hash", "value"))

    })
  }


  test("simple compaction with merge operator") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1, "1"), (2, 1, 1, "1"), (3, 1, 1, "1"), (1, 2, 2, "2"), (1, 3, 3, "3"))
        .toDF("range", "hash", "v1", "v2")
      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)


      val df2 = Seq((1, 1, 1, "1"), (2, 1, 1, "1"), (3, 1, 1, "1"), (1, 2, 2, "2"), (1, 3, 3, "3"))
        .toDF("range", "hash", "v1", "v2")
      LakeSoulTable.uncached(tableName)
      val table = LakeSoulTable.forPath(tableName)
      table.upsert(df2)

      val result = Seq((1, 1, 2, "1,1"), (2, 1, 2, "1,1"), (3, 1, 2, "1,1"), (1, 2, 4, "2,2"), (1, 3, 6, "3,3"))
        .toDF("range", "hash", "v1", "v2")

      val mergeOperatorInfo = Map(
        "v1" -> new MergeOpInt(),
        "v2" -> "org.apache.spark.sql.lakesoul.test.MergeOpString")
      table.compaction(mergeOperatorInfo = mergeOperatorInfo, cleanOldCompaction = true)
      LakeSoulTable.uncached(tableName)
      checkAnswer(table.toDF.select("range", "hash", "v1", "v2"), result)

    })
  }


  test("compaction with merge operator should failed if merge operator illegal") {
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

      val table = LakeSoulTable.forPath(tableName)

      val e1 = intercept[AnalysisException] {
        class tmp {}
        val mergeOperatorInfo = Map("value" -> new tmp())
        table.compaction(mergeOperatorInfo = mergeOperatorInfo, cleanOldCompaction = true)
      }
      assert(e1.getMessage().contains("is not a legal merge operator class"))
      val e2 = intercept[ClassNotFoundException] {
        val mergeOperatorInfo = Map("value" -> "ClassWillNeverExsit")
        table.compaction(mergeOperatorInfo = mergeOperatorInfo, cleanOldCompaction = true)
      }
      assert(e2.getMessage.contains("ClassWillNeverExsit"))

    })
  }

  test("Compaction and add partition to external catalog") {
    withTable("spark_catalog.default.external_table", "default.lakesoul_test_table") {
      spark.sql("CREATE TABLE IF NOT EXISTS " +
        "spark_catalog.default.external_table" +
        " (id int, name string, date string)" +
        " using parquet" +
        " PARTITIONED BY(date)")
      checkAnswer(spark.sql("show tables in spark_catalog.default"),
        Seq(Row("default", "external_table", false)))
      val df = Seq(("2021-01-01", 1, "rice"), ("2021-01-01", 2, "bread")).toDF("date", "id", "name")
      df.write
        .mode("append")
        .format("lakesoul")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .saveAsTable("lakesoul_test_table")
      checkAnswer(spark.sql("show tables in spark_catalog.default"),
        Seq(Row("default", "external_table", false)))
      checkAnswer(spark.sql("show tables in default"),
        Seq(Row("default", "lakesoul_test_table", false)))
      val lakeSoulTable = LakeSoulTable.forName("lakesoul_test_table")
      lakeSoulTable.compaction("date='2021-01-01'", hiveTableName = "spark_catalog.default.external_table")
      checkAnswer(spark.sql("show partitions spark_catalog.default.external_table"),
        Seq(Row("date=2021-01-01")))
      checkAnswer(spark.sql("select * from spark_catalog.default.external_table order by id"),
        Seq(Row(1, "rice", "2021-01-01"), Row(2, "bread", "2021-01-01")))
    }
  }

  test("compaction with limited file number") {
    withTempDir { tempDir =>
      //    val tempDir = Utils.createDirectory(System.getProperty("java.io.tmpdir"))
      val tablePath = tempDir.getCanonicalPath
      val spark = SparkSession.active

      val hashBucketNum = 4
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

      // Write initial data
      df.write
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "id")
        .option(SHORT_TABLE_NAME, "compaction_limit_table")
        .option("hashBucketNum", hashBucketNum.toString)
        .save(tablePath)

      val lakeSoulTable = LakeSoulTable.forPath(tablePath)

      for (c <- 0 until compactRounds) {
        // Simulate multiple append operations
        for (i <- c * dataPerRounds + 1 to (c + 1) * dataPerRounds) {
          val appendDf = Seq(
            (i * 10, s"2023-02-0$i", i * 100, 1)
          ).toDF("id", "date", "value", "range")
          lakeSoulTable.upsert(appendDf)
        }

        // Get initial PartitionInfo count
        val initialFileCount = getFileList(tablePath).length
        println(s"before ${c}th time compact file count=$initialFileCount")
        lakeSoulTable.toDF.show

        // Perform limited compaction (group every compactGroupSize PartitionInfo)
        lakeSoulTable.compaction(fileNumLimit = Some(compactGroupSize))

        // Get PartitionInfo count after compaction
        val compactedFileList = getFileList(tablePath)
        val compactedFileCount = compactedFileList.length

        println(s"after ${c}th time compact file count=$compactedFileCount")

        lakeSoulTable.toDF.show

        // Verify results
        assert(compactedFileCount <= hashBucketNum,
          s"Compaction should have hashBucketNum files, but it has $compactedFileCount")


        //        assert(compactedFileCount >= (initialFileCount - 1) / compactGroupSize + 1,
        //          s"Compaction should produce files above a lower bound, but there are ${compactedFileCount} files")
        //
        //        assert(compactedFileCount <= (initialFileCount - 1) / compactGroupSize + 1 + hashBucketNum,
        //          s"Compaction should produce files below a upper bound, but there are ${compactedFileCount} files")
      }

      // Verify data integrity
      val compactedData = lakeSoulTable.toDF.orderBy("id", "date").collect()
      println(compactedData.mkString("Array(", ", ", ")"))
      assert(compactedData.length == 5 + dataPerRounds * compactRounds, s"The compressed data should have ${5 + dataPerRounds * compactRounds} rows, but it actually has ${compactedData.length} rows")
    }
  }

  test("compaction cdc table with limited file number") {
    withTempDir { tempDir =>
      //    val tempDir = org.apache.spark.util.Utils.createDirectory(System.getProperty("java.io.tmpdir"))
      val tablePath = tempDir.getCanonicalPath
      val spark = SparkSession.active

      val hashBucketNum = 4
      val compactRounds = 5
      val dataPerRounds = 10
      val compactGroupSize = 3

      // Create test data
      val df = Seq(
        (1, "2023-01-01", 10, 1, "insert"),
        (2, "2023-01-02", 20, 1, "insert"),
        (3, "2023-01-03", 30, 1, "insert"),
        (4, "2023-01-04", 40, 1, "insert"),
        (5, "2023-01-05", 50, 1, "insert")
      ).toDF("id", "date", "value", "range", "op")

      // Write initial data
      df.write
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "id")
        .option(SHORT_TABLE_NAME, "compaction_limit_cdc_table")
        .option("lakesoul_cdc_change_column", "op")
        .option("hashBucketNum", hashBucketNum.toString)
        .save(tablePath)

      val lakeSoulTable = LakeSoulTable.forPath(tablePath)

      for (c <- 0 until compactRounds) {
        // Simulate multiple append operations
        for (i <- c * dataPerRounds + 1 to (c + 1) * dataPerRounds) {
          val appendDf = if (i % 2 == 0) {
            Seq(
              (i * 10, s"2023-02-0$i", i * 100, 1, "insert")
            ).toDF("id", "date", "value", "range", "op")
          } else {
            Seq(
              (i * 10, s"2023-02-0$i", i * 100, 1, "insert"),
              (i * 10, s"2023-02-0$i", i * 100, 1, "delete")
            ).toDF("id", "date", "value", "range", "op")
          }

          lakeSoulTable.upsert(appendDf)
        }

        // Get initial PartitionInfo count
        val initialFiles = getFileList(tablePath)
        println(initialFiles.mkString("Array(", ", ", ")"))
        val initialFileCount = initialFiles.length
        //        println(s"before compact initialPartitionInfoCount=$initialFileCount")
        lakeSoulTable.toDF.show

        // Perform limited compaction (group every compactGroupSize PartitionInfo)
        lakeSoulTable.compaction(fileNumLimit = Some(compactGroupSize))

        // Get PartitionInfo count after compaction
        val compactedFiles = getFileList(tablePath)
        //        println(compactedFiles.mkString("Array(", ", ", ")"))
        val compactedFileCount = compactedFiles.length

        println(s"after compact compactedPartitionInfoCount=$compactedFileCount")

        lakeSoulTable.toDF.show

        // Verify results
        assert(compactedFileCount <= hashBucketNum,
          s"Compaction should have hashBucketNum files, but it has $compactedFileCount")


        //        assert(compactedFileCount >= (initialFileCount - 1) / compactGroupSize + 1,
        //          s"Compaction should produce files above a lower bound, but there are ${compactedFileCount} files")
        //
        //        assert(compactedFileCount <= (initialFileCount - 1) / compactGroupSize + 1 + hashBucketNum,
        //          s"Compaction should produce files below a upper bound, but there are ${compactedFileCount} files")
      }

      // Verify data integrity
      val compactedData = lakeSoulTable.toDF.orderBy("id", "date").collect()
      println(compactedData.mkString("Array(", ", ", ")"))
      assert(compactedData.length == 5 + dataPerRounds * compactRounds / 2, s"The compressed data should have ${5 + dataPerRounds * compactRounds / 2} rows, but it actually has ${compactedData.length} rows")
    }
  }

  test("compaction with limited file size") {
    withTempDir { tempDir =>
      //    val tempDir = org.apache.spark.util.Utils.createDirectory(System.getProperty("java.io.tmpdir"))
      val tablePath = tempDir.getCanonicalPath
      val spark = SparkSession.active

      val hashBucketNum = 4
      val compactRounds = 5
      val upsertPerRounds = 10
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
        .option(SHORT_TABLE_NAME, "compaction_size_limit_table")
        .option("hashBucketNum", hashBucketNum.toString)
        .save(tablePath)

      val lakeSoulTable = LakeSoulTable.forPath(tablePath)

      for (c <- 0 until compactRounds) {
        // Simulate multiple append operations
        for (i <- c * upsertPerRounds + 1 to (c + 1) * upsertPerRounds) {
          val appendDf = (0 until rowsPerUpsert)
            .map(j => (i * 1000 + j, s"2023-02-0$i", i * 100, 1))

            .toDF("id", "date", "value", "range")
          //          val appendDf = Seq(
          //            (i * 10, s"2023-02-0$i", i * 100, 1)
          //          ).toDF("id", "date", "value", "range")
          lakeSoulTable.upsert(appendDf)
        }

        // Get initial PartitionInfo count
        val initialMaxFileSize = getFileList(tablePath).map(_.size).max
        println(s"before ${c}th compact initialMaxFileSize=$initialMaxFileSize")
        LakeSoulTable.uncached(tablePath)
        spark.time({
          // Perform limited compaction (group every compactGroupSize PartitionInfo)
          lakeSoulTable.compaction(fileSizeLimit = Some(compactFileSize), force = false)
          //          lakeSoulTable.compaction(fileSizeLimit = Some(compactFileSize), force = true)
          //          lakeSoulTable.compaction()
        })

        // Get PartitionInfo count after compaction
        val compactedFiles = getFileList(tablePath)
        val compactedFileMax = compactedFiles.map(_.size).max

        println(s"after ${c}th compact compactedFileMax=$compactedFileMax")

        // Verify results
        //        assert(compactedFileMax >= initialMaxFileSize,
        //          s"Compaction should increase the max size of files, but it changed from ${initialMaxFileSize} to $compactedFileMax")

        assert(compactedFileMax <= DBUtil.parseMemoryExpression(compactFileSize) * 1.1,
          s"Compaction should produce file with upper-bounded size, but there is a larger ${compactedFileMax} file size")

        val (compactDir, _) = splitCompactFilePath(compactedFiles.head.path)
        assert(compactedFiles.forall(file => splitCompactFilePath(file.path)._1.equals(compactDir)),
          s"Compaction should produce file with the same compaction dir, but the file list are ${compactedFiles.map(_.path).mkString("Array(", ", ", ")")}")
      }

      // Verify data integrity
      LakeSoulTable.uncached(tablePath)
      val compactedData = lakeSoulTable.toDF.orderBy("id", "date").collect()
      //      println(compactedData.mkString("Array(", ", ", ")"))

      assert(compactedData.length == 5 + rowsPerUpsert * upsertPerRounds * compactRounds, s"The compressed data should have ${5 + rowsPerUpsert * upsertPerRounds * compactRounds} rows, but it actually has ${compactedData.length} rows")
    }
  }

  test("compaction cdc table with limited file size") {
    withTempDir { tempDir =>
      //    val tempDir = org.apache.spark.util.Utils.createDirectory(System.getProperty("java.io.tmpdir"))
      val tablePath = tempDir.getCanonicalPath
      val spark = SparkSession.active

      val hashBucketNum = 4
      val compactRounds = 5
      val upsertPerRounds = 10
      val rowsPerUpsert = 1000
      val compactFileSize = "10KB"

      // Create test data
      val df = Seq(
        (1, "2023-01-01", 10, 1, "insert"),
        (2, "2023-01-02", 20, 1, "insert"),
        (3, "2023-01-03", 30, 1, "insert"),
        (4, "2023-01-04", 40, 1, "insert"),
        (5, "2023-01-05", 50, 1, "insert"),
        (rowsPerUpsert - 1, "2023-01-05", 50, 1, "insert")
      ).toDF("id", "date", "value", "range", "op")

      // Write initial data
      df.write
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "id")
        .option(SHORT_TABLE_NAME, "compaction_size_limit_cdc_table")
        .option("lakesoul_cdc_change_column", "op")
        .option("hashBucketNum", hashBucketNum.toString)
        .save(tablePath)

      val lakeSoulTable = LakeSoulTable.forPath(tablePath)

      for (c <- 0 until compactRounds) {
        // Simulate multiple append operations
        for (i <- c * upsertPerRounds + 1 to (c + 1) * upsertPerRounds) {
          val appendDf = (0 until rowsPerUpsert)
            .flatMap(j => if (j % 2 == 0) {
              Seq((i * rowsPerUpsert + j - 1, s"2023-02-0$i", i * 100, 1, "delete"), (i * rowsPerUpsert + j, s"2023-02-0$i", i * 100, 1, "insert"))
            } else {
              Seq((i * rowsPerUpsert + j, s"2023-02-0$i", i * 100, 1, "insert"))
            })

            .toDF("id", "date", "value", "range", "op")
          //          val appendDf = Seq(
          //            (i * 10, s"2023-02-0$i", i * 100, 1)
          //          ).toDF("id", "date", "value", "range")
          lakeSoulTable.upsert(appendDf)
        }

        // Get initial PartitionInfo count
        val initialMaxFileSize = getFileList(tablePath).map(_.size).max
        println(s"before ${c}th compact initialMaxFileSize=$initialMaxFileSize")

        // Perform limited compaction (group every compactGroupSize PartitionInfo)
        LakeSoulTable.uncached(tablePath)
        spark.time({
          lakeSoulTable.compaction(fileSizeLimit = Some(compactFileSize), force = false)
          //          lakeSoulTable.compaction(fileSizeLimit = Some(compactFileSize), force = true)
        })

        // Get PartitionInfo count after compaction
        val compactedFiles = getFileList(tablePath)
        val compactedFileMax = compactedFiles.map(_.size).max

        println(s"after ${c}th compact compactedFileMax=$compactedFileMax")

        // Verify results
        //        assert(compactedFileMax >= initialMaxFileSize,
        //          s"Compaction should reduce the number of files, but it changed from ${initialMaxFileSize} to $compactedFileMax")

        assert(compactedFileMax <= DBUtil.parseMemoryExpression(compactFileSize) * 1.1,
          s"Compaction should produce file with upper-bounded size, but there is a larger ${compactedFileMax} file size")

        val (compactDir, _) = splitCompactFilePath(compactedFiles.head.path)
        assert(compactedFiles.forall(file => splitCompactFilePath(file.path)._1.equals(compactDir)),
          s"Compaction should produce file with the same compaction dir, but the file list are ${compactedFiles.map(_.path).mkString("Array(", ", ", ")")}")
      }

      // Verify data integrity
      LakeSoulTable.uncached(tablePath)
      val finalData = lakeSoulTable.toDF.orderBy("id", "date")
      //      println(finalData.queryExecution)
      val compactedData = finalData.collect()
      //      println(compactedData.mkString("Array(", ", ", ")"))

      assert(compactedData.length == 6 + rowsPerUpsert * upsertPerRounds * compactRounds / 2, s"The compressed data should have ${6 + rowsPerUpsert * upsertPerRounds * compactRounds / 2} rows, but it actually has ${compactedData.length} rows")
    }
  }

  // Auxiliary method: Get the number of files
  def getFileList(tablePath: String): Array[DataFileInfo] = {
    val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tablePath)).toString)
    val partitionList = SparkMetaVersion.getAllPartitionInfo(sm.getTableInfoOnly.table_id)
    DataOperation.getTableDataInfo(partitionList)
  }

  test("compaction with newBucketNum") {
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
        .option(SHORT_TABLE_NAME, "rebucket_table")
        .option("hashBucketNum", hashBucketNum.toString)
        //      .option("lakesoul_cdc_change_column", "op")
        .save(tablePath)

      val lakeSoulTable = LakeSoulTable.forPath(tablePath)
      println(s"save table, show")
      lakeSoulTable.toDF.show(100000, false)

      for (i <- 1 to 100) {
        val appendDf = Seq(
          (i * 10, s"2023-02-0$i", i * 100, 1)
        ).toDF("id", "date", "value", "range")
        //        val appendDf = Seq(
        //          (i * 10, s"2023-02-0$i", i * 100, 1, "insert")
        //        ).toDF("id", "date", "value", "range", "op")
        lakeSoulTable.upsert(appendDf)
        println(s"upsert time: $i, show")
        lakeSoulTable.toDF.show(100000, false)
      }
      assert(getFileList(tablePath).groupBy(_.file_bucket_id).keys.toSet.size == hashBucketNum)
      assert(getTableInfo(tablePath).bucket_num == hashBucketNum)

      lakeSoulTable.compaction(newBucketNum = Some(newHashBucketNum))
      println(s"after compaction, show")
      lakeSoulTable.toDF.show(100000, false)

      assert(getFileList(tablePath).groupBy(_.file_bucket_id).keys.toSet.size == newHashBucketNum)
      assert(getTableInfo(tablePath).bucket_num == newHashBucketNum)

      val compactedData = lakeSoulTable.toDF.orderBy("id", "date").collect()
      println(compactedData.mkString("Array(", ", ", ")"))
      assert(compactedData.length == 105, s"The compressed data should have ${105} rows, but it actually has ${compactedData.length} rows")

    }
  }

  // Auxiliary method: Get the bucket number of table
  def getTableInfo(tablePath: String): TableInfo = {
    val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tablePath)).toString)
    sm.getTableInfoOnly
  }

  test("Compaction with schema change") {
    withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath
        val spark = SparkSession.active

        val hashBucketNum = 2
        val mathContext = new MathContext(20)

        {
          // Create test data
          val df = Seq(
            (1, "2023-01-01", 10, BigDecimal(10, 2, mathContext), "insert"),
            (2, "2023-01-02", 20, BigDecimal(20, 2, mathContext), "insert"),
            (3, "2023-01-03", 30, BigDecimal(30, 2, mathContext), "insert"),
            (4, "2023-01-04", 40, BigDecimal(40, 2, mathContext), "insert"),
            (5, "2023-01-05", 50, BigDecimal(50, 2, mathContext), "insert")
          ).toDF("id", "date", "value", "range", "op")
          df.printSchema()

          // Write initial data
          df.write
            .format("lakesoul")
            .option("hashPartitions", "id")
            .option(SHORT_TABLE_NAME, "compaction_test_table")
            .option("lakesoul_cdc_change_column", "op")
            .option("hashBucketNum", hashBucketNum.toString)
            .save(tablePath)
        }

        {
          // change schema
          val df = Seq(
            (1, "1", 1, "insert"),
            (2, "2", 1, "update"),
            (3, "3", 1, "delete"),
            (4, "4", 1, "insert"),
            (5, "5", 1, "update")
          ).toDF("id", "value", "range", "op")

          val table = LakeSoulTable.forPath(tablePath)
          table.upsert(df)
        }

        {
          // compaction
          val table = LakeSoulTable.forPath(tablePath)
          LakeSoulTable.uncached(tablePath)
          table.compaction()
        }

        val table = LakeSoulTable.forPath(tablePath)
        LakeSoulTable.uncached(tablePath)
        val tableDF = table.toDF

        checkAnswer(tableDF, Seq(
          Row(1, "2023-01-01", 1, 1.0, "insert"),
          Row(2, "2023-01-02", 2, 1.0, "insert"),
          Row(4, "2023-01-04", 4, 1.0, "insert"),
          Row(5, "2023-01-05", 5, 1.0, "insert"))
        )
      }
    }
  }
}
