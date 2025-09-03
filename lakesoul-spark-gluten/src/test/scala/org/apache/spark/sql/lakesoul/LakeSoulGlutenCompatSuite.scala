// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.gluten.config.GlutenConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.execution.ExtendedMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.util.Utils
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.io.File

@RunWith(classOf[JUnitRunner])
class LakeSoulGlutenCompatSuite
  extends QueryTest
    with SharedSparkSession
  with LakeSoulSQLCommandTest {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.adaptive.logLevel", "info")
      .set("spark.network.timeout", "10000000")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .set("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .set(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
  }

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new TestSparkSession(sparkConf)
    session.sparkContext.setLogLevel("INFO")
    session
  }

  import testImplicits._

  test("lakesoul write scan - nopk no partition") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      val df = Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date","id","name")
      df.write
        .mode("overwrite")
        .format("lakesoul")
        .save(tablePath)
      val dfRead = spark.read.format("lakesoul").load(tablePath).select("date", "name", "id")
      val plan = dfRead.queryExecution.explainString(ExtendedMode)
      println(plan)
      assert(plan.matches(".*\n\\bVeloxColumnarToRow\n.*\\bProjectExecTransformer\\b.*\n" +
        ".*\\bInputIteratorTransformer\\b.*\n.*\\bOffloadArrowData\n.*\\bBatchScan\\b.*\\bNativeParquetScan\\b.*"))
      checkAnswer(dfRead, Seq(("2021-01-01","rice",1),("2021-01-01","bread",2)).toDF("date", "name", "id"))
    })
  }

  test("lakesoul write scan - nopk") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      val df = Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date","id","name")
      df.write
        .mode("overwrite")
        .format("lakesoul")
        .option("rangePartitions","date")
        .save(tablePath)
      val dfRead = spark.read.format("lakesoul").load(tablePath).select("date", "id", "name")
      val plan = dfRead.queryExecution.explainString(ExtendedMode)
      println(plan)
      assert(plan.matches(".*\n\\bVeloxColumnarToRow\n.*\\bProjectExecTransformer\\b.*\n" +
        ".*\\bInputIteratorTransformer\\b.*\n.*\\bOffloadArrowData\n.*\\bBatchScan\\b.*\\bNativeParquetScan\\b.*"))
      checkAnswer(dfRead, Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date", "id", "name"))
    })
  }

  test("lakesoul write scan - pk") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      val df = Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date","id","name")
      df.write
        .mode("overwrite")
        .format("lakesoul")
        .option("hashPartitions","id")
        .option("hashBucketNum","2")
        .option("rangePartitions","date")
        .save(tablePath)
      val dfRead = spark.read.format("lakesoul").load(tablePath).select("date","name","id")
      val plan = dfRead.queryExecution.explainString(ExtendedMode)
      println(plan)
      assert(plan.matches(".*\\bColumnarToRow\n.*\\bwithPartitionAndOrdering\\b.*\n.*\\bLoadArrowData\n.*\\bProjectExecTransformer\\b.*\n" +
        ".*\\bInputIteratorTransformer\\b.*\n.*\\bOffloadArrowData\n.*\\bBatchScan\\b.*\\bNativeParquetScan\\b.*"))
      checkAnswer(dfRead, Seq(("2021-01-01","rice",1),("2021-01-01","bread",2)).toDF("date", "name", "id"))
    })
  }

  test("lakesoul write scan - table") {
    withTable("temp")({
      val df = Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date","id","name")
      df.write
        .mode("overwrite")
        .format("lakesoul")
        .option("hashPartitions","id")
        .option("hashBucketNum","2")
        .option("rangePartitions","date")
        .saveAsTable("temp")
      val dfRead = spark.sql(s"select date, id, name from temp")
      val plan = dfRead.queryExecution.explainString(ExtendedMode)
      assert(plan.matches(".*\n\\bVeloxColumnarToRow\n.*\\bProjectExecTransformer\\b.*\n" +
        ".*\\bInputIteratorTransformer\\b.*\n.*\\bOffloadArrowData\n.*\\bBatchScan\\b.*\\bNativeParquetScan\\b.*"))
      checkAnswer(dfRead, Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date", "id", "name"))
    })
  }

  test("lakesoul write scan - table cdc") {
    withTable("temp")({
      val df = Seq(
        ("2021-01-01",1,"rice","insert"),
        ("2021-01-01",2,"bread","insert"),
        ("2021-01-01",1,"rice","delete"),
        ("2021-01-01",2,"noodle","update"),
      ).toDF("date","id","name","rowKinds")
      df.write
        .mode("overwrite")
        .format("lakesoul")
        .option("hashPartitions","id")
        .option("hashBucketNum","2")
        .option("lakesoul_cdc_change_column","rowKinds")
        .option("rangePartitions","date")
        .saveAsTable("temp")
      val dfRead = spark.sql(s"select date, id, name from temp")
      val plan = dfRead.queryExecution.explainString(ExtendedMode)
      println(plan)
//      assert(plan.matches(".*\n\\bVeloxColumnarToRow\n.*\\bProjectExecTransformer\\b.*\n" +
//        ".*\\bInputIteratorTransformer\\b.*\n.*\\bOffloadArrowData\n.*\\bBatchScan\\b.*\\bNativeParquetScan\\b.*"))
      checkAnswer(dfRead, Seq(("2021-01-01",2,"noodle")).toDF("date", "id", "name"))
    })
  }

  override def withTable(tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  override def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir()
    try {
      f(dir)
      waitForTasksToFinish()
    } finally {
      Utils.deleteRecursively(dir)
      try {
        LakeSoulTable.forPath(dir.getCanonicalPath).dropTable()
      } catch {
        case _: Exception =>
      }
    }
  }
}
