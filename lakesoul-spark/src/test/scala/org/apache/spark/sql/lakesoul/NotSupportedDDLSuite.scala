// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSourceUtils
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestUtils}
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.util.Locale
import scala.util.control.NonFatal


@RunWith(classOf[JUnitRunner])
class NotSupportedDDLSuite
  extends NotSupportedDDLBase
    with LakeSoulSQLCommandTest


abstract class NotSupportedDDLBase extends QueryTest
  with SharedSparkSession
  with LakeSoulTestUtils {

  val format = "lakesoul"

  val nonPartitionedTableName = "lakeSoulTbl"

  val partitionedTableName = "partitionedLakeSoulTbl"

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    try {
      println(SQLConf.get.getConf(SQLConf.DEFAULT_CATALOG))
      sql(
        s"""
           |CREATE TABLE $nonPartitionedTableName
           |USING $format
           |AS SELECT 1 as a, 'a' as b
         """.stripMargin)

      sql(
        s"""
           |CREATE TABLE $partitionedTableName (a INT, b STRING, p1 INT)
           |USING $format
           |PARTITIONED BY (p1)
          """.stripMargin)
      sql(s"INSERT INTO $partitionedTableName SELECT 1, 'A', 2")
    } catch {
      case NonFatal(e) =>
        afterAll()
        throw e
    }
  }

  protected override def afterEach(): Unit = {
    try {
      val location = Seq(nonPartitionedTableName, partitionedTableName).map(tbl => {
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
            case _: Exception =>
          }
        }
      })

    } finally {
      super.afterEach()
    }
  }

  private def assertUnsupported(query: String, messages: String*): Unit = {
    val allErrMessages = "operation not allowed" +: "does not support" +: "is not supported" +: messages
    val e = intercept[AnalysisException] {
      sql(query)
    }
    println(e.getMessage().toLowerCase(Locale.ROOT))
    assert(allErrMessages.exists(err => e.getMessage.toLowerCase(Locale.ROOT).contains(err)))
  }

  test("bucketing is not supported for lakesoul tables") {
    withTable("tbl") {
      assertUnsupported(
        s"""
           |CREATE TABLE tbl(a INT, b INT)
           |USING $format
           |CLUSTERED BY (a) INTO 5 BUCKETS
        """.stripMargin)
    }
  }

  test("CREATE TABLE LIKE") {
    withTable("tbl") {
      assertUnsupported(s"CREATE TABLE tbl LIKE $nonPartitionedTableName")
    }
  }

  test("ANALYZE TABLE PARTITION") {
    assertUnsupported(s"ANALYZE TABLE $partitionedTableName PARTITION (p1) COMPUTE STATISTICS",
      "analyze table is not supported for v2 tables")
  }

  test("ALTER TABLE ADD PARTITION") {
    assertUnsupported(s"ALTER TABLE $partitionedTableName ADD PARTITION (p1=3)",
      "can not alter partitions")
  }

  test("ALTER TABLE DROP PARTITION") {
    assertUnsupported(s"ALTER TABLE $partitionedTableName DROP PARTITION (p1=2)",
      "can not alter partitions")
  }

  test("ALTER TABLE RECOVER PARTITIONS") {
    assertUnsupported(s"ALTER TABLE $partitionedTableName RECOVER PARTITIONS")
    assertUnsupported(s"MSCK REPAIR TABLE $partitionedTableName")
  }

  test("ALTER TABLE SET SERDEPROPERTIES") {
    assertUnsupported(s"ALTER TABLE $nonPartitionedTableName SET SERDEPROPERTIES (s1=3)")
  }

  test("ALTER TABLE RENAME TO") {
    assertUnsupported(s"ALTER TABLE $nonPartitionedTableName RENAME TO newTbl",
      "LakeSoul currently doesn't support rename table")
  }


  test("LOAD DATA") {
    assertUnsupported(
      s"""LOAD DATA LOCAL INPATH '/path/to/home' INTO TABLE $nonPartitionedTableName""",
      "load data is not supported for v2 tables")
  }

  test("INSERT OVERWRITE DIRECTORY") {
    assertUnsupported(s"INSERT OVERWRITE DIRECTORY '/path/to/home' USING $format VALUES (1, 'a')")
  }
}
