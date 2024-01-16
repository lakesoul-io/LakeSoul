// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.rules

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import io.glutenproject.execution.WholeStageTransformerSuite
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.ExtendedMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.util.Utils
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.io.File

//@RunWith(classOf[JUnitRunner])
class LakeSoulGlutenCompatSuite
  extends WholeStageTransformerSuite {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.codegen.wholeStage", "false")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.network.timeout", "10000000")
      .set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .set("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  }

  import testImplicits._

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
      assert(dfRead.queryExecution.explainString(ExtendedMode).contains("InputIteratorTransformer"))
      assert(!dfRead.queryExecution.explainString(ExtendedMode).matches(".*\\bColumnarToRow\\b.*"))
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
      val dfRead = spark.read.format("lakesoul").load(tablePath).select("date", "id", "name")
      assert(dfRead.queryExecution.explainString(ExtendedMode).contains("InputIteratorTransformer"))
      assert(!dfRead.queryExecution.explainString(ExtendedMode).matches(".*\\bColumnarToRow\\b.*"))
      checkAnswer(dfRead, Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date", "id", "name"))
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
      assert(dfRead.queryExecution.explainString(ExtendedMode).contains("InputIteratorTransformer"))
      assert(!dfRead.queryExecution.explainString(ExtendedMode).matches(".*\\bColumnarToRow\\b.*"))
      checkAnswer(dfRead, Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date", "id", "name"))
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

  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"
}
