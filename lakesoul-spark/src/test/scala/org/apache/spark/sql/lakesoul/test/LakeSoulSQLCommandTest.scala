// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.test

import com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SparkSessionExtensions}
import org.apache.spark.util.Utils

import java.io.File

trait LakeSoulTestUtils extends Logging {
  self: SharedSparkSession =>

  val testDatabase = "test_database"

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    sparkConf.set("spark.ui.enabled", "false")
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.sparkContext.setLogLevel("ERROR")
    session
  }

  override def withTable(tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
        if (name.split("\\.").length == 1) {
          val databaseName = if (name.startsWith(testDatabase + ".")) name else s"$testDatabase.$name"
          spark.sql(s"DROP TABLE IF EXISTS $databaseName")
        }
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
        case e: Exception =>
      }
    }
  }

  def createDF(seq: Seq[Product], names: Seq[String],
               types: Seq[String], nullables: Option[Seq[Boolean]] = None): DataFrame = {
    val fields = nullables match {
      case None =>
        names.zip(types).map(nt => StructField(nt._1, CatalystSqlParser.parseDataType(nt._2), nullable = false))
      case Some(nullableSeq) =>
        names.zip(types).zip(nullableSeq).map(
          nt => StructField(nt._1._1, CatalystSqlParser.parseDataType(nt._1._2), nullable = nt._2))
    }

    val rows = seq.map(Row.fromTuple)

    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      StructType(fields)
    )
  }
}

/**
  * Because `TestSparkSession` doesn't pick up the conf `spark.sql.extensions` in Spark 2.4.x, we use
  * this class to inject LakeSoul's extension in our tests.
  *
  * @see https://issues.apache.org/jira/browse/SPARK-25003
  */
class LakeSoulTestSparkSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) {
  override val extensions: SparkSessionExtensions = {
    val extensions = new SparkSessionExtensions
    new LakeSoulSparkSessionExtension().apply(extensions)
    extensions
  }
}

/**
  * A trait for tests that are testing a fully set up SparkSession with all of LakeSoul's requirements,
  * such as the configuration of the LakeSoulCatalog and the addition of all LakeSoul extensions.
  */
trait LakeSoulSQLCommandTest extends LakeSoulTestUtils {
  self: SharedSparkSession =>

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    sparkConf.set("spark.ui.enabled", "false")
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
    session.sparkContext.setLogLevel("ERROR")

    session
  }
}

