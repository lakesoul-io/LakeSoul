// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSourceUtils
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestUtils}
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.util.control.NonFatal

@RunWith(classOf[JUnitRunner])
class SQLDropPartitionSuite extends SQLDropPartitionBase with LakeSoulSQLCommandTest

abstract class SQLDropPartitionBase extends QueryTest
  with SharedSparkSession
  with LakeSoulTestUtils {

  import testImplicits._

  val format = "lakesoul"

  val singlePartitionedTableName = "singlePartitionedLakeSoulTbl"

  val multiPartitionedTableName = "multiPartitionedLakeSoulTbl"

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    try {
      println(SQLConf.get.getConf(SQLConf.DEFAULT_CATALOG))
      sql(
        s"""
           |CREATE TABLE $singlePartitionedTableName (a INT, b STRING, p INT)
           |USING $format
           |PARTITIONED BY (p)
          """.stripMargin)

      sql(s"INSERT INTO $singlePartitionedTableName values (1, 'A', 1), (2, 'B', 2)")

      sql(
        s"""
           |CREATE TABLE $multiPartitionedTableName (a INT, b STRING, p1 INT, p2 INT)
           |USING $format
           |PARTITIONED BY (p1, p2)
          """.stripMargin)

      sql(s"INSERT INTO $multiPartitionedTableName values (1, 'A', 1, 1), (2, 'B', 1, 2)")
    } catch {
      case NonFatal(e) =>
        afterAll()
        throw e
    }
  }

  protected override def afterEach(): Unit = {
    try {
      val location = Seq(singlePartitionedTableName, multiPartitionedTableName).map(tbl => {
        try {
          LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(
            TableIdentifier(tbl, Some("default"))
          )
        } catch {
          case _: Exception => None
        }
      })

      location.foreach(loc => {
        if (loc.isDefined) {
          try {
            LakeSoulTable.forPath(loc.get).dropTable()
          } catch {
            case e: Exception => {
              println(e.getMessage)
            }
          }
        }
      })
    } finally {
      super.afterEach()
    }
  }

  protected def verifyTable(tableName: String, expected: DataFrame, colNames: Seq[String]): Unit = {
    checkAnswer(spark.table(tableName).select(colNames.map(col): _*), expected)
    waitForTasksToFinish()
  }

  test("ALTER TABLE DROP SINGLE PARTITION") {
    sql(s"ALTER TABLE $singlePartitionedTableName DROP PARTITION (p=1)")
    val df = Seq((2, "B", 2)).toDF("a", "b", "p")
    verifyTable(singlePartitionedTableName, df, Seq("a", "b", "p"))
  }

  test("ALTER TABLE DROP SINGLE PARTITION WHEN PARTITION NAME NOT EXISTS") {
    val e = intercept[AnalysisException] {
      sql(s"ALTER TABLE $singlePartitionedTableName DROP PARTITION (p=3)")
    }
    assert(e.getMessage.contains("The following partitions not found"))
  }

  test("ALTER TABLE DROP MUlTI-LEVEL PARTITION") {
    sql(s"ALTER TABLE $multiPartitionedTableName DROP PARTITION (p1=1, p2=1)")
    val df = Seq((2, "B", 1, 2)).toDF("a", "b", "p1", "p2")
    verifyTable(multiPartitionedTableName, df, Seq("a", "b", "p1", "p2"))
  }

  test("ALTER TABLE DROP SINGLE-LEVEL PARTITION WHEN TABLE WITH SINGLE-LEVEL PARTITION") {
    val e = intercept[AnalysisException] {
      sql(s"ALTER TABLE $multiPartitionedTableName DROP PARTITION (p1=1)")
    }
    assert(e.getMessage.contains("Partition spec is invalid"))
    assert(e.getMessage.contains("The spec (p1) must match the partition spec (p1, p2)"))
  }
}
