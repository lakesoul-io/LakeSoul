package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession, MergeOpInt}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class CleanOldCompactionSuit extends QueryTest
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

      val df = Seq(("2020-01-02",1,"a"),("2020-01-01",2,"b"))
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

      assert(sm.snapshot.getTableInfo.configuration("partition.ttl").toInt==2 && sm.snapshot.getTableInfo.configuration("compaction.ttl").toInt==1)

      LakeSoulTable.forPath(tableName).setCompactionTtl(3).setPartitionTtl(4)

      assert(sm.updateSnapshot().getTableInfo.configuration("partition.ttl").toInt==4 && sm.updateSnapshot().getTableInfo.configuration("compaction.ttl").toInt==3)

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

      val tablePtah = sm.table_path
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val fileList = fs.listStatus(new Path(tablePtah))
      val filePaths = fileList.map(_.getPath)
      filePaths.foreach(println)
      assert(fileList.length==4)
    })
  }

}
