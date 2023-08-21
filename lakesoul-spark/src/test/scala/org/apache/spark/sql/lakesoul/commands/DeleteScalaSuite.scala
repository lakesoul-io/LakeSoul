// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul
import com.dmetasoul.lakesoul.tables.{LakeSoulTable, LakeSoulTableTestUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession}
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeleteScalaSuite extends DeleteSuiteBase with LakeSoulSQLCommandTest {

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

  test("delete cached table by path") {
    Seq((2, 2), (1, 4)).toDF("key", "value")
      .write.mode("overwrite").format("lakesoul").save(tempPath)
    spark.read.format("lakesoul").load(tempPath).cache()
    spark.read.format("lakesoul").load(tempPath).collect()
    executeDelete(s"lakesoul.default.`$tempPath`", where = "key = 2")
    checkAnswer(spark.read.format("lakesoul").load(tempPath), Row(1, 4) :: Nil)
  }

  test("delete usage test - without condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = LakeSoulTable.forPath(tempPath)
    table.delete()
    checkAnswer(readLakeSoulTable(tempPath), Nil)
  }

  test("delete usage test - with condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = LakeSoulTable.forPath(tempPath)
    table.delete("key = 1 or key = 2")
    checkAnswer(readLakeSoulTable(tempPath), Row(3, 30) :: Row(4, 40) :: Nil)
  }

  test("delete usage test - with Column condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = lakesoul.tables.LakeSoulTable.forPath(tempPath)
    table.delete(functions.expr("key = 1 or key = 2"))
    checkAnswer(readLakeSoulTable(tempPath), Row(3, 30) :: Row(4, 40) :: Nil)
  }

  override protected def executeDelete(target: String, where: String = null): Unit = {

    def parse(tableNameWithAlias: String): (String, Option[String]) = {
      tableNameWithAlias.split(" ").toList match {
        case tableName :: Nil => tableName -> None // just table name
        case tableName :: alias :: Nil => // tablename SPACE alias OR tab SPACE lename
          val ordinary = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet
          if (!alias.forall(ordinary.contains)) {
            (tableName + " " + alias) -> None
          } else {
            tableName -> Some(alias)
          }
        case _ =>
          fail(s"Could not build parse '$tableNameWithAlias' for table and optional alias")
      }
    }

    val lakeSoulTable: LakeSoulTable = {
      val (tableNameOrPath, optionalAlias) = parse(target)
      val isPath: Boolean = tableNameOrPath.startsWith("lakesoul.")
      val table = if (isPath) {
        val path = tableNameOrPath.stripPrefix("lakesoul.default.`").stripSuffix("`")
        lakesoul.tables.LakeSoulTable.forPath(spark, path)
      } else {
        LakeSoulTableTestUtils.createTable(spark.table(tableNameOrPath),
          SnapshotManagement(tableNameOrPath))
      }
      optionalAlias.map(table.as).getOrElse(table)
    }

    if (where != null) {
      lakeSoulTable.delete(where)
    } else {
      lakeSoulTable.delete()
    }
  }

}
