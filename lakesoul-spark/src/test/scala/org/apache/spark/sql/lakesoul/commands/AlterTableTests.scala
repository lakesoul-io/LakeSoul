// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta.DataFileInfo
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableCatalog}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSourceUtils
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestUtils}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}
import org.apache.spark.util.Utils
import org.junit.runner.RunWith
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.junit.JUnitRunner

import scala.collection.JavaConverters.mapAsScalaMapConverter

trait AlterTableLakeSoulTestBase
  extends QueryTest
    with SharedSparkSession with LakeSoulTestUtils {

  protected def createTable(schema: String, ns: String, tblProperties: Map[String, String]): String

  protected def createTable(df: DataFrame, ns: String, partitionedBy: Seq[String]): String

  protected def dropTable(identifier: String): Unit

  protected def getSnapshotManagement(identifier: String): SnapshotManagement

  final protected def withLakeSoulTable(schema: String, ns: String)(f: String => Unit): Unit = {
    withLakeSoulTable(schema, ns: String, Map.empty[String, String])(i => f(i))
  }

  final protected def withLakeSoulTable(schema: String,
                                        ns: String,
                                        tblProperties: Map[String, String])(f: String => Unit): Unit = {
    val identifier = createTable(schema, ns, tblProperties)
    try {
      f(identifier)
    } finally {
      dropTable(identifier)
    }
  }

  final protected def withLakeSoulTable(df: DataFrame, ns: String)(f: String => Unit): Unit = {
    withLakeSoulTable(df, ns, Seq.empty[String])(i => f(i))
  }

  final protected def withLakeSoulTable(df: DataFrame,
                                        ns: String,
                                        partitionedBy: Seq[String])(f: String => Unit): Unit = {
    val identifier = createTable(df, ns, partitionedBy)
    try {
      f(identifier)
    } finally {
      dropTable(identifier)
    }
  }

  protected def ddlTest(testName: String)(f: String => Unit): Unit = {
    test(testName)(f("default"))
  }

  protected def assertNotSupported(command: String, messages: String*): Unit = {
    val ex = intercept[AnalysisException] {
      sql(command)
    }.getMessage
    assert(ex.contains("not supported") || ex.contains("Unsupported") || ex.contains("Cannot"))
    messages.foreach(msg => assert(ex.contains(msg)))
  }
}

trait AlterTableTests extends AlterTableLakeSoulTestBase {

  import testImplicits._

  ///////////////////////////////
  // ADD COLUMNS
  ///////////////////////////////

  ddlTest("ADD COLUMNS - simple") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns) { tableName =>
      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long, v4 double)")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("v3", "long").add("v4", "double"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, Option[Long], Option[Double])],
        (1, "a", None, None), (2, "b", None, None))
    }
  }

  ddlTest("ADD COLUMNS into complex types - Array") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("a", array(struct("v1"))), ns) { tableName =>
      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (a.element.v3 long)
         """.stripMargin)

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", ArrayType(new StructType()
          .add("v1", "integer")
          .add("v3", "long"))))

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (a.element.v4 struct<f1:long>)
         """.stripMargin)

      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", ArrayType(new StructType()
          .add("v1", "integer")
          .add("v3", "long")
          .add("v4", new StructType().add("f1", "long")))))

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (a.element.v4.f2 string)
         """.stripMargin)

      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", ArrayType(new StructType()
          .add("v1", "integer")
          .add("v3", "long")
          .add("v4", new StructType()
            .add("f1", "long")
            .add("f2", "string")))))
    }
  }

  ddlTest("ADD COLUMNS into complex types - Map with simple key") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map('v1, struct("v2"))), ns) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (m.value.mvv3 long)
         """.stripMargin)

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(IntegerType,
          new StructType()
            .add("v2", "string")
            .add("mvv3", "long"))))
    }
  }

  ddlTest("ADD COLUMNS into complex types - Map with simple value") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map(struct("v1"), 'v2)), ns) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (m.key.mkv3 long)
         """.stripMargin)

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer")
            .add("mkv3", "long"),
          StringType)))
    }
  }

  ddlTest("ADD COLUMNS should not be able to add column to basic type key/value of " +
    "MapType") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map('v1, 'v2)), ns) { tableName =>
      var ex = intercept[AnalysisException] {
        sql(
          s"""
             |ALTER TABLE $tableName ADD COLUMNS (m.key.mkv3 long)
         """.stripMargin)
      }
      assert(ex.getMessage.contains("is not a struct"))

      ex = intercept[AnalysisException] {
        sql(
          s"""
             |ALTER TABLE $tableName ADD COLUMNS (m.key.mkv3 long)
         """.stripMargin)
      }
      assert(ex.getMessage.contains("is not a struct"))
    }
  }

  ddlTest("ADD COLUMNS into complex types - Map") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map(struct("v1"), struct("v2"))), ns) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (m.key.mkv3 long, m.value.mvv3 long)
         """.stripMargin)

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer")
            .add("mkv3", "long"),
          new StructType()
            .add("v2", "string")
            .add("mvv3", "long"))))
    }
  }

  ddlTest("ADD COLUMNS into complex types - Map (nested)") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map(struct("v1"), struct("v2"))), ns) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS
           |(m.key.mkv3 long, m.value.mvv3 struct<f1: long, f2:array<struct<n:long>>>)
         """.stripMargin)

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer")
            .add("mkv3", "long"),
          new StructType()
            .add("v2", "string")
            .add("mvv3", new StructType()
              .add("f1", "long")
              .add("f2", ArrayType(new StructType()
                .add("n", "long")))))))

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS
           |(m.value.mvv3.f2.element.p string)
         """.stripMargin)

      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer")
            .add("mkv3", "long"),
          new StructType()
            .add("v2", "string")
            .add("mvv3", new StructType()
              .add("f1", "long")
              .add("f2", ArrayType(new StructType()
                .add("n", "long")
                .add("p", "string")))))))
    }
  }

  ddlTest("ADD COLUMNS into Map should fail if key or value not specified") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map(struct("v1"), struct("v2"))), ns) { tableName =>

      val ex = intercept[AnalysisException] {
        sql(
          s"""
             |ALTER TABLE $tableName ADD COLUMNS (m.mkv3 long)
           """.stripMargin)
      }
      assert(ex.getMessage.contains("is not a struct"))
    }
  }

  ddlTest("ADD COLUMNS into Array should fail if element is not specified") { ns =>
    withLakeSoulTable(
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("a", array(struct("v1"))), ns) { tableName =>

      intercept[AnalysisException] {
        sql(
          s"""
             |ALTER TABLE $tableName ADD COLUMNS (a.v3 long)
         """.stripMargin)
      }
    }
  }

  ddlTest("ADD COLUMNS - a partitioned table") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns, Seq("v2")) { tableName =>

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long, v4 double)")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string", false)
        .add("v3", "long").add("v4", "double"))

      checkDatasetUnorderly(
        spark.table(tableName).select("v1", "v2", "v3", "v4").as[(Int, String, Option[Long], Option[Double])],
        (1, "a", None, None), (2, "b", None, None))
    }
  }

  ddlTest("ADD COLUMNS - with a comment") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns) { tableName =>

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long COMMENT 'new column')")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("v3", "long", true, "new column"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, Option[Long])],
        (1, "a", None), (2, "b", None))
    }
  }

  ddlTest("ADD COLUMNS - adding to a non-struct column") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns) { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName ADD COLUMNS (v2.x long)")
      }
      assert(ex.getMessage.contains("is not a struct"))
    }
  }

  ddlTest("ADD COLUMNS - a duplicate name") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns) { tableName =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName ADD COLUMNS (v2 long)")
      }
    }
  }

  ddlTest("ADD COLUMNS - a duplicate name (nested)") { ns =>
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withLakeSoulTable(df, ns) { tableName =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName ADD COLUMNS (struct.v2 long)")
      }
    }
  }

  test("ADD COLUMNS - with positions") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withLakeSoulTable(df, "default") { tableName =>

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long FIRST, v4 long AFTER v1, v5 long)")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v3", "long").add("v1", "integer")
        .add("v4", "long").add("v2", "string")
        .add("v5", "long"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Option[Long], Int, Option[Long], String, Option[Long])],
        (None, 1, None, "a", None), (None, 2, None, "b", None))
    }
  }

  test("ADD COLUMNS - with positions using an added column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withLakeSoulTable(df, "default") { tableName =>

      checkDatasetUnorderly(
        spark.table("lakesoul_test").as[(Int, String)],
        (1, "a"), (2, "b"))

      sql("ALTER TABLE lakesoul_test ADD COLUMNS (v3 long FIRST, v4 long AFTER v3, v5 long AFTER v4)")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v3", "long").add("v4", "long").add("v5", "long")
        .add("v1", "integer").add("v2", "string"))

      checkDatasetUnorderly(
        spark.table("lakesoul_test").as[(Option[Long], Option[Long], Option[Long], Int, String)],
        (None, None, None, 1, "a"), (None, None, None, 2, "b"))
    }
  }

  test("ADD COLUMNS - nested columns") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withLakeSoulTable(df, "default") { tableName =>

      checkDatasetUnorderly(
        spark.table("lakesoul_test").as[(Int, String, (Int, String))],
        (1, "a", (1, "a")), (2, "b", (2, "b")))

      sql("ALTER TABLE lakesoul_test ADD COLUMNS " +
        "(struct.v3 long FIRST, struct.v4 long AFTER v1, struct.v5 long)")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v3", "long").add("v1", "integer")
          .add("v4", "long").add("v2", "string").add("v5", "long")))

      checkDatasetUnorderly(
        spark.table("lakesoul_test")
          .as[(Int, String, (Option[Long], Int, Option[Long], String, Option[Long]))],
        (1, "a", (None, 1, None, "a", None)), (2, "b", (None, 2, None, "b", None)))
    }
  }

  //  test("ADD COLUMNS - special column names with positions") {
  //    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
  //      .withColumn("z", struct("v1", "v2"))
  //    withLakeSoulTable(df) { tableName =>
  //
  //      checkDatasetUnorderly(
  //        spark.table(tableName).as[(Int, String, (Int, String))],
  //        (1, "a", (1, "a")), (2, "b", (2, "b")))
  //
  //      sql(s"ALTER TABLE $tableName ADD COLUMNS (`x` long after v1, `z`.`y` double)")
  //
  //      val snapshotManagement = getSnapshotManagement(tableName)
  //      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
  //        .add("v1", "integer").add("x", "long").add("v2", "string")
  //        .add("z", new StructType()
  //          .add("v1", "integer").add("v2", "string").add("y", "double"))
  //      )
  //
  //      checkDatasetUnorderly(
  //        spark.table(tableName).as[(Int, Option[Long], String, (Int, String, Option[Double]))],
  //        (1, None, "a", (1, "a", None)), (2, None, "b", (2, "b", None)))
  //    }
  //  }

  test("ADD COLUMNS - adding after an unknown column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withLakeSoulTable(df, "default") { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long AFTER unknown)")
      }
      assert(ex.getMessage.contains("Couldn't find"))
    }
  }

  test("ADD COLUMNS - case insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      withLakeSoulTable(df, "default") { tableName =>

        sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long AFTER V1)")

        val snapshotManagement = getSnapshotManagement(tableName)
        assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
          .add("v1", "integer").add("v3", "long").add("v2", "string"))
      }
    }
  }

  test("ADD COLUMNS - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      withLakeSoulTable(df, "default") { tableName =>

        val ex = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long AFTER V1)")
        }
        assert(ex.getMessage.contains("Couldn't find"))
      }
    }
  }

  ///////////////////////////////
  // CHANGE COLUMN
  ///////////////////////////////

  ddlTest("CHANGE COLUMN - add a comment") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer COMMENT 'a comment'")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer", true, "a comment").add("v2", "string"))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to a partitioned table") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns, Seq("v2")) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v2 v2 string COMMENT 'a comment'")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string", false, "a comment"))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to a MapType (nested)") { ns =>
    val table = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("a", array(struct(array(struct(map(struct("v1"), struct("v2")))))))
    withLakeSoulTable(table, ns) { tableName =>
      sql(
        s"""
           |ALTER TABLE $tableName CHANGE COLUMN
           |a.element.col1.element.col1 col1 MAP<STRUCT<v1:int>,
           |STRUCT<v2:string>> COMMENT 'a comment'
         """.stripMargin)

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", ArrayType(new StructType()
          .add("col1", ArrayType(new StructType()
            .add("col1", MapType(
              new StructType()
                .add("v1", "integer"),
              new StructType()
                .add("v2", "string")), nullable = true, "a comment"))))))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to an ArrayType (nested)") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map(struct("v1"), struct(array(struct(struct("v1")))))), ns) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName CHANGE COLUMN
           |m.value.col1.element.col1.v1 v1 integer COMMENT 'a comment'
         """.stripMargin)

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer"),
          new StructType()
            .add("col1", ArrayType(new StructType()
              .add("col1", new StructType()
                .add("v1", "integer", nullable = true, "a comment")))))))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to an ArrayType") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("a", array('v1)), ns) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN a a ARRAY<int> COMMENT 'a comment'")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", ArrayType(IntegerType), nullable = true, "a comment"))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to a MapType") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("a", map('v1, 'v2)), ns) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName CHANGE COLUMN
           |a a MAP<int, string> COMMENT 'a comment'
         """.stripMargin)

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", MapType(IntegerType, StringType), nullable = true, "a comment"))
    }
  }

  ddlTest("CHANGE COLUMN - change name") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns) { tableName =>

      assertNotSupported(s"ALTER TABLE $tableName CHANGE COLUMN v2 v3 string")
    }
  }

  ddlTest("CHANGE COLUMN - incompatible") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns) { tableName =>

      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 long",
        "'v1' with type 'IntegerType (nullable = true)'",
        "'v1' with type 'LongType (nullable = true)'")
    }
  }

  ddlTest("CHANGE COLUMN - incompatible (nested)") { ns =>
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withLakeSoulTable(df, ns) { tableName =>

      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN struct.v1 v1 long",
        "'struct.v1' with type 'IntegerType (nullable = true)'",
        "'v1' with type 'LongType (nullable = true)'")
    }
  }

  test("CHANGE COLUMN - move to first") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withLakeSoulTable(df, "default") { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v2 v2 string FIRST")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v2", "string").add("v1", "integer"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(String, Int)],
        ("a", 1), ("b", 2))
    }
  }

  test("CHANGE COLUMN - move to first (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withLakeSoulTable(df, "default") { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v2 v2 string FIRST")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v2", "string").add("v1", "integer")))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (String, Int))],
        (1, "a", ("a", 1)), (2, "b", ("b", 2)))

      // Can't change the inner ordering
      assertNotSupported(s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
        "STRUCT<v1:integer, v2:string> FIRST")

      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
        "STRUCT<v2:string, v1:integer> FIRST")

      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("struct", new StructType()
          .add("v2", "string").add("v1", "integer"))
        .add("v1", "integer").add("v2", "string"))
    }
  }

  //  test("CHANGE COLUMN - move a partitioned column to first") {
  //    val df = Seq((1, "a", true), (2, "b", false)).toDF("v1", "v2", "v3")
  //    withLakeSoulTable(df, Seq("v2", "v3")) { tableName =>
  //
  //
  //      spark.table(tableName).show()
  //      sql(s"ALTER TABLE $tableName CHANGE COLUMN v3 v3 boolean FIRST")
  //
  //      val snapshotManagement = getSnapshotManagement(tableName)
  //      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
  //        .add("v3", "boolean").add("v1", "integer").add("v2", "string"))
  //
  //      spark.table(tableName).show()
  //      checkDatasetUnorderly(
  //        spark.table(tableName).as[(Boolean, Int, String)],
  //        (true, 1, "a"), (false, 2, "b"))
  //    }
  //  }

  test("CHANGE COLUMN - move to after some column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withLakeSoulTable(df, "default") { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER v2")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v2", "string").add("v1", "integer"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(String, Int)],
        ("a", 1), ("b", 2))
    }
  }

  test("CHANGE COLUMN - move to after some column (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withLakeSoulTable(df, "default") { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v1 v1 integer AFTER v2")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v2", "string").add("v1", "integer")))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (String, Int))],
        (1, "a", ("a", 1)), (2, "b", ("b", 2)))

      // cannot change ordering within the struct
      assertNotSupported(s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
        "STRUCT<v1:integer, v2:string> AFTER v1")

      // can move the struct itself however
      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
        "STRUCT<v2:string, v1:integer> AFTER v1")

      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer")
        .add("struct", new StructType().add("v2", "string").add("v1", "integer"))
        .add("v2", "string"))
    }
  }

  test("CHANGE COLUMN - move to after the same column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withLakeSoulTable(df, "default") { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER v1")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))
    }
  }

  test("CHANGE COLUMN - move to after the same column (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withLakeSoulTable(df, "default") { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v1 v1 integer AFTER v1")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v1", "integer").add("v2", "string")))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (Int, String))],
        (1, "a", (1, "a")), (2, "b", (2, "b")))
    }
  }

  //  test("CHANGE COLUMN - move a partitioned column to after some column") {
  //    val df = Seq((1, "a", true), (2, "b", false)).toDF("v1", "v2", "v3")
  //    withLakeSoulTable(df, Seq("v2", "v3")) { tableName =>
  //
  //      sql(s"ALTER TABLE $tableName CHANGE COLUMN v3 v3 boolean AFTER v1")
  //
  //      val snapshotManagement = getSnapshotManagement(tableName)
  //      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
  //        .add("v1", "integer").add("v3", "boolean").add("v2", "string"))
  //
  //      checkDatasetUnorderly(
  //        spark.table(tableName).as[(Int, Boolean, String)],
  //        (1, true, "a"), (2, false, "b"))
  //    }
  //  }

  test("CHANGE COLUMN - move to after the last column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withLakeSoulTable(df, "default") { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER v2")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v2", "string").add("v1", "integer"))
    }
  }

  //  test("CHANGE COLUMN - special column names with positions") {
  //    val df = Seq((1, "a"), (2, "b")).toDF("x.x", "y.y")
  //    withLakeSoulTable(df) { tableName =>
  //      sql(s"ALTER TABLE $tableName CHANGE COLUMN `x.x` `x.x` integer AFTER `y.y`")
  //
  //      val snapshotManagement = getSnapshotManagement(tableName)
  //      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
  //        .add("y.y", "string").add("x.x", "integer"))
  //
  //      checkDatasetUnorderly(
  //        spark.table(tableName).as[(String, Int)],
  //        ("a", 1), ("b", 2))
  //    }
  //  }
  //
  //  test("CHANGE COLUMN - special column names (nested) with positions") {
  //    val df = Seq((1, "a"), (2, "b")).toDF("x.x", "y.y")
  //      .withColumn("z.z", struct("`x.x`", "`y.y`"))
  //    withLakeSoulTable(df) { tableName =>
  //
  //      sql(s"ALTER TABLE $tableName CHANGE COLUMN `z.z`.`x.x` `x.x` integer AFTER `y.y`")
  //
  //      val snapshotManagement = getSnapshotManagement(tableName)
  //      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
  //        .add("x.x", "integer").add("y.y", "string")
  //        .add("z.z", new StructType()
  //          .add("y.y", "string").add("x.x", "integer")))
  //
  //      checkDatasetUnorderly(
  //        spark.table(tableName).as[(Int, String, (String, Int))],
  //        (1, "a", ("a", 1)), (2, "b", ("b", 2)))
  //    }
  //  }

  test("CHANGE COLUMN - move to after an unknown column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withLakeSoulTable(df, "default") { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER unknown")
      }
      assert(ex.getMessage.contains("Missing field unknown"))
    }
  }

  test("CHANGE COLUMN - move to after an unknown column (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withLakeSoulTable(df, "default") { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v1 v1 integer AFTER unknown")
      }
      assert(ex.getMessage.contains("Missing field struct.unknown"))
    }
  }

  test("CHANGE COLUMN - complex types nullability tests") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))
    withLakeSoulTable(df, "default") { tableName =>
      // not supported to tighten nullabilities.
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN s s STRUCT<v1:int, v2:string NOT NULL>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN a a " +
          "ARRAY<STRUCT<v1:int, v2:string NOT NULL>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string NOT NULL>, STRUCT<v1:int, v2:string>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string NOT NULL>>")

      // not supported to add not-null columns.
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN s s " +
          "STRUCT<v1:int, v2:string, sv3:long, sv4:long NOT NULL>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN a a " +
          "ARRAY<STRUCT<v1:int, v2:string, av3:long, av4:long NOT NULL>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string, mkv3:long, mkv4:long NOT NULL>, " +
          "STRUCT<v1:int, v2:string, mvv3:long>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string, mkv3:long>, " +
          "STRUCT<v1:int, v2:string, mvv3:long, mvv4:long NOT NULL>>")
    }
  }

  ddlTest("CHANGE COLUMN - change name (nested)") { ns =>
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withLakeSoulTable(df, ns) { tableName =>

      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN struct.v2 v3 string")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
          "STRUCT<v1:integer, v3:string>")
    }
  }

  ddlTest("CHANGE COLUMN - add a comment (nested)") { ns =>
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withLakeSoulTable(df, ns) { tableName =>
      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v1 v1 integer COMMENT 'a comment'")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v1", "integer", true, "a comment").add("v2", "string")))

      assertNotSupported(s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
        "STRUCT<v1:integer, v2:string COMMENT 'a comment for v2'>")
    }
  }

  ddlTest("CHANGE COLUMN - complex types not supported because behavior is ambiguous") { ns =>
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))
    withLakeSoulTable(df, ns) { tableName =>
      // not supported to add columns
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN s s STRUCT<v1:int, v2:string, sv3:long>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN a a ARRAY<STRUCT<v1:int, v2:string, av3:long>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string, mkv3:long>, STRUCT<v1:int, v2:string, mvv3:long>>")

      // not supported to remove columns.
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN s s STRUCT<v1:int>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN a a ARRAY<STRUCT<v1:int>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int>, STRUCT<v1:int, v2:string>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int>>")
    }
  }

  test("CHANGE COLUMN - move unknown column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withLakeSoulTable(df, "default") { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName CHANGE COLUMN unknown unknown string FIRST")
      }
      assert(ex.getMessage.contains("Missing field unknown"))
    }
  }

  test("CHANGE COLUMN - move unknown column (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withLakeSoulTable(df, "default") { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.unknown unknown string FIRST")
      }
      assert(ex.getMessage.contains("Missing field struct.unknown"))
    }
  }

  test("CHANGE COLUMN - case insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
      withLakeSoulTable(df, "default") { tableName =>

        val snapshotManagement = getSnapshotManagement(tableName)

        sql(s"ALTER TABLE $tableName CHANGE COLUMN V1 v1 integer")

        assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
          .add("v1", "integer").add("v2", "string")
          .add("s", new StructType().add("v1", "integer").add("v2", "string")))

        sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 V1 integer")

        assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
          .add("v1", "integer").add("v2", "string")
          .add("s", new StructType().add("v1", "integer").add("v2", "string")))

        sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER V2")

        assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
          .add("v2", "string").add("v1", "integer")
          .add("s", new StructType().add("v1", "integer").add("v2", "string")))

        // Since the struct doesn't match the case this fails
        assertNotSupported(
          s"ALTER TABLE $tableName CHANGE COLUMN s s struct<V1:integer,v2:string> AFTER V2")

        sql(
          s"ALTER TABLE $tableName CHANGE COLUMN s s struct<v1:integer,v2:string> AFTER V2")

        assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
          .add("v2", "string")
          .add("s", new StructType().add("v1", "integer").add("v2", "string"))
          .add("v1", "integer"))
      }
    }
  }

  test("CHANGE COLUMN - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
      withLakeSoulTable(df, "default") { tableName =>

        val ex1 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN V1 V1 integer")
        }
        assert(ex1.getMessage.contains("Missing field V1"))

        val ex2 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 V1 integer")
        }
        assert(ex2.getMessage.contains("Renaming column is not supported"))

        val ex3 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER V2")
        }
        assert(ex3.getMessage.contains("Missing field V2"))

        val ex4 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN s s struct<V1:integer,v2:string> AFTER v2")
        }
        assert(ex4.getMessage.contains("Cannot update"))
      }
    }
  }
}

trait AlterTableByNameTests extends AlterTableTests {

  import testImplicits._

  override protected def ddlTest(testName: String)(f: String => Unit): Unit = {
    super.ddlTest(testName)(f)

    test(testName + " with lakesoul_db database") {
      withDatabase("lakesoul_db") {
        spark.sql("CREATE DATABASE IF NOT EXISTS lakesoul_db")
        f("lakesoul_db")
      }
    }
  }

  override protected def createTable(schema: String, ns: String, tblProperties: Map[String, String]): String = {
    val props = tblProperties.map { case (key, value) =>
      s"'$key' = '$value'"
    }.mkString(", ")
    val propsString = if (tblProperties.isEmpty) "" else s" TBLPROPERTIES ($props)"
    sql(s"CREATE TABLE $ns.lakesoul_test ($schema) USING lakesoul$propsString")
    s"$ns.lakesoul_test"
  }

  override protected def createTable(df: DataFrame, ns: String, partitionedBy: Seq[String]): String = {
    df.write.option("rangePartitions", partitionedBy.mkString(","))
      .format("lakesoul").saveAsTable(s"$ns.lakesoul_test")
    s"$ns.lakesoul_test"
  }

  override protected def dropTable(identifier: String): Unit = {
    val parts = identifier.split("\\.")

    val location = if (parts.length == 1) {
      LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(
        TableIdentifier(identifier, Some("default")))
    } else {
      LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(
        TableIdentifier(parts(1), Some(parts(0))))
    }
    if (location.isDefined) {
      LakeSoulTable.forPath(location.get).dropTable()
    }
  }

  override protected def getSnapshotManagement(identifier: String): SnapshotManagement = {
    val parts = identifier.split("\\.")
    if (parts.length == 1) {
      SnapshotManagement.forTable(spark, TableIdentifier(identifier))
    } else {
      SnapshotManagement.forTable(spark, TableIdentifier(parts(1), Some(parts(0))))
    }
  }

  test("ADD COLUMNS - external table") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        val path = dir.getCanonicalPath
        Seq((1, "a"), (2, "b")).toDF("v1", "v2")
          .write
          .format("lakesoul")
          .option("path", path)
          .saveAsTable("lakesoul_test")

        checkDatasetUnorderly(
          spark.table("lakesoul_test").as[(Int, String)],
          (1, "a"), (2, "b"))

        sql("ALTER TABLE lakesoul_test ADD COLUMNS (v3 long, v4 double)")

        val snapshotManagement = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(path)).toString)
        assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
          .add("v1", "integer").add("v2", "string")
          .add("v3", "long").add("v4", "double"))

        checkDatasetUnorderly(
          spark.table("lakesoul_test").as[(Int, String, Option[Long], Option[Double])],
          (1, "a", None, None), (2, "b", None, None))
        checkDatasetUnorderly(
          spark.read.format("lakesoul").load(path).as[(Int, String, Option[Long], Option[Double])],
          (1, "a", None, None), (2, "b", None, None))
      }
    }
  }

  def catalog: TableCatalog = {
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[LakeSoulCatalog]
  }

  protected def getProperties(table: Table): Map[String, String] = {
    table.properties().asScala.toMap.filterKeys(
      !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(_))
  }

  test("SET TBLPROPERTIES") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        val path = dir.getCanonicalPath
        Seq((1, "update"), (2, "insert")).toDF("col1", "cdc_change_kind")
          .write
          .format("lakesoul")
          .option("path", path)
          .saveAsTable("lakesoul_test")
        sql("ALTER TABLE lakesoul_test SET TBLPROPERTIES ('lakesoul_cdc_column'='cdc_change_kind')")

        val table = catalog.loadTable(Identifier.of(Array("default"), "lakesoul_test"))
        getProperties(table) should contain("lakesoul_cdc_column" -> "cdc_change_kind")
      }
    }
  }

}

/**
  * For ByPath tests, we select a test case per ALTER TABLE command to simply test identifier
  * resolution.
  */
trait AlterTableByPathTests extends AlterTableLakeSoulTestBase {
  override protected def createTable(schema: String, ns: String, tblProperties: Map[String, String]): String = {
    val tmpDir = Utils.createTempDir().getCanonicalPath
    val snapshotManagement = getSnapshotManagement(tmpDir)
    val tc = snapshotManagement.startTransaction()
    val newTableInfo = tc.tableInfo.copy(table_schema = StructType.fromDDL(schema).json)
    tc.commit(Seq.empty[DataFileInfo], Seq.empty[DataFileInfo], newTableInfo)
    s"$ns.$tmpDir"
  }

  override protected def createTable(df: DataFrame, ns: String, partitionedBy: Seq[String]): String = {
    val tmpDir = Utils.createTempDir().getCanonicalPath
    df.write.format("lakesoul")
      .option("rangePartitions", partitionedBy.mkString(",")).save(tmpDir)
    s"lakesoul.$ns.`$tmpDir`"
  }

  override protected def dropTable(identifier: String): Unit = {
    LakeSoulTable.forPath(identifier.split("\\.").last.stripPrefix("`").stripSuffix("`")).dropTable()
  }

  override protected def getSnapshotManagement(identifier: String): SnapshotManagement = {
    SnapshotManagement(
      SparkUtil.makeQualifiedTablePath(new Path(identifier.split("\\.")
        .last.stripPrefix("`").stripSuffix("`"))).toString)
  }

  override protected def ddlTest(testName: String)(f: String => Unit): Unit = {
    super.ddlTest(testName)(f)

    test(testName + " with lakesoul_db database") {
      withDatabase("lakesoul_db") {
        spark.sql("CREATE DATABASE IF NOT EXISTS lakesoul_db")
        f("lakesoul_db")
      }
    }
  }

  import testImplicits._


  ddlTest("ADD COLUMNS - simple") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns) { tableName =>
      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long, v4 double)")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("v3", "long").add("v4", "double"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, Option[Long], Option[Double])],
        (1, "a", None, None), (2, "b", None, None))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment") { ns =>
    withLakeSoulTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), ns) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer COMMENT 'a comment'")

      val snapshotManagement = getSnapshotManagement(tableName)
      assert(snapshotManagement.updateSnapshot().getTableInfo.schema == new StructType()
        .add("v1", "integer", true, "a comment").add("v2", "string"))
    }
  }

  test("SET LOCATION is not supported for path based tables") {
    val df = spark.range(1).toDF()
    withLakeSoulTable(df, "default") { identifier =>
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val e = intercept[AnalysisException] {
          sql(s"alter table $identifier set location '$path'")
        }
        assert(e.getMessage.contains("`ALTER TABLE xxx SET LOCATION '/xxx'` is not supported"))
      }
    }
  }
}

@RunWith(classOf[JUnitRunner])
class AlterTableByNameSuite
  extends AlterTableByNameTests
    with LakeSoulSQLCommandTest {


}

@RunWith(classOf[JUnitRunner])
class AlterTableByPathSuite extends AlterTableByPathTests with LakeSoulSQLCommandTest
