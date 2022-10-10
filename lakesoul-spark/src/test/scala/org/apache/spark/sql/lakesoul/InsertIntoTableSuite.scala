/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.lakesoul

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.schema.SchemaUtils
import org.apache.spark.sql.lakesoul.sources.{LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestUtils}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConverters._

class InsertIntoSQLSuite extends InsertIntoTests(false, true)
  with LakeSoulSQLCommandTest {
  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName SELECT * FROM $tmpView")
    }
  }
}

class InsertIntoSQLByPathSuite extends InsertIntoTests(false, true)
  with LakeSoulSQLCommandTest {
  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      val ident = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
      val location = LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(ident)
      assert(location.isDefined)
      sql(s"INSERT $overwrite TABLE lakesoul.default.`${location.get}` SELECT * FROM $tmpView")
    }
  }

  test("insertInto: cannot insert into a table that doesn't exist") {
    import testImplicits._
    Seq(SaveMode.Append, SaveMode.Overwrite).foreach { mode =>
      withTempDir { dir =>
        val t1 = s"lakesoul.`${dir.getCanonicalPath}`"
        val tmpView = "tmp_view"
        withTempView(tmpView) {
          val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
          val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
          df.createOrReplaceTempView(tmpView)

          intercept[AnalysisException] {
            sql(s"INSERT $overwrite TABLE $t1 SELECT * FROM $tmpView")
          }

          intercept[AnalysisException] {
            sql(s"INSERT $overwrite TABLE $t1 SELECT * FROM $tmpView")
          }
        }
      }
    }
  }
}

class InsertIntoDataFrameSuite extends InsertIntoTests(false, false)
  with LakeSoulSQLCommandTest {
  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val dfw = insert.write.format(v2Format)
    if (mode != null) {
      dfw.mode(mode)
    }
    dfw.insertInto(tableName)
  }
}

class InsertIntoDataFrameByPathSuite extends InsertIntoTests(false, false)
  with LakeSoulSQLCommandTest {
  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val dfw = insert.write.format(v2Format)
    if (mode != null) {
      dfw.mode(mode)
    }
    val ident = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    val location = LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(ident)
    assert(location.isDefined)
    dfw.insertInto(s"lakesoul.default.`${location.get}`")
  }

  test("insertInto: cannot insert into a table that doesn't exist") {
    import testImplicits._
    Seq(SaveMode.Append, SaveMode.Overwrite).foreach { mode =>
      withTempDir { dir =>
        val t1 = s"lakesoul.`${dir.getCanonicalPath}`"
        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")

        intercept[AnalysisException] {
          df.write.mode(mode).insertInto(t1)
        }

        intercept[AnalysisException] {
          df.write.mode(mode).insertInto(t1)
        }

        // Test DataFrameWriterV2 as well
        val dfW2 = df.writeTo(t1)
        if (mode == SaveMode.Append) {
          intercept[AnalysisException] {
            dfW2.append()
          }
        } else {
          intercept[AnalysisException] {
            dfW2.overwrite(lit(true))
          }
        }
      }
    }
  }
}

/** These tests come from Apache Spark with some modifications to match LakeSoul behavior. */
abstract class InsertIntoTests(
                                override protected val supportsDynamicOverwrite: Boolean,
                                override protected val includeSQLOnlyTests: Boolean)
  extends InsertIntoSQLOnlyTests {

  import testImplicits._

  override def afterEach(): Unit = {
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[LakeSoulCatalog]
    catalog.listTables(Array("default")).foreach(t => {
      sql(s"drop table default.`${t.name}`")
      sql(s"drop table lakesoul.`${t.name}`")
    })
  }

  // START Apache Spark tests

  /**
    * Insert data into a table using the insertInto statement. Implementations can be in SQL
    * ("INSERT") or using the DataFrameWriter (`df.write.insertInto`). Insertions will be
    * by column ordinal and not by column name.
    */
  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode = null): Unit

  test("insertInto: append") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df)
      verifyTable(t1, df, Seq("id", "data"))
    }
  }

  test("insertInto: append by position") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")

      doInsert(t1, dfr)
      verifyTable(t1, df, Seq("id", "data"))
    }
  }

  test("insertInto: append partitioned table") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data").select("data", "id")
      doInsert(t1, df)
      verifyTable(t1, df, Seq("data", "id"))
    }
  }

  test("insertInto: append non partitioned table and read with filter") {
    val t1 = "default.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df)
      val expected = Seq((1L, "a"), (2L, "b")).toDF("id", "data")
      checkAnswer(spark.table(t1).filter("id <= 2"), expected)
    }
  }

  test("insertInto: append partitioned table and read with partition filter") {
    val t1 = "default.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY(id)")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data").select("data", "id")
      doInsert(t1, df)
      val expected = Seq((1L, "a"), (2L, "b")).toDF("id", "data")
      checkAnswer(spark.table(t1).filter("id <= 2").select("id", "data"), expected)
    }
  }

  test("insertInto: overwrite non-partitioned table") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      val df2 = Seq((4L, "d"), (5L, "e"), (6L, "f")).toDF("id", "data")
      doInsert(t1, df)
      doInsert(t1, df2, SaveMode.Overwrite)
      verifyTable(t1, df2, Seq("id", "data"))
    }
  }

  test("insertInto: overwrite partitioned table in static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").select("data", "id")
        doInsert(t1, init)

        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data").select("data", "id")
        doInsert(t1, df, SaveMode.Overwrite)
        verifyTable(t1, df, Seq("data", "id"))
      }
    }
  }


  test("insertInto: overwrite by position") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").select("data", "id")
        doInsert(t1, init)

        val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id").select("id", "data")
        doInsert(t1, dfr, SaveMode.Overwrite)

        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data").select("data", "id")
        verifyTable(t1, df, Seq("data", "id"))
      }
    }
  }

  test("insertInto: fails when missing a column") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string, missing string) USING $v2Format")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      val exc = intercept[AnalysisException] {
        doInsert(t1, df)
      }

      verifyTable(t1, Seq.empty[(Long, String, String)].toDF("id", "data", "missing"), Seq("id", "data", "missing"))
      assert(exc.getMessage.contains("not enough data columns"))
    }
  }

  // This behavior is specific to LakeSoul
  test("insertInto: fails when an extra column is present but can evolve schema") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a", "mango")).toDF("id", "data", "fruit")
      val exc = intercept[AnalysisException] {
        doInsert(t1, df)
      }

      verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"), Seq("id", "data"))
      assert(exc.getMessage.contains(s"too many data columns"))

      withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
        doInsert(t1, df)
      }
      verifyTable(t1, Seq((1L, "a", "mango")).toDF("id", "data", "fruit"), Seq("id", "data", "fruit"))
    }
  }

  // This behavior is specific to LakeSoul
  test("insertInto: schema enforcement") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq(("a", 1L)).toDF("id", "data") // reverse order

      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> "strict") {
        intercept[AnalysisException] {
          doInsert(t1, df, SaveMode.Overwrite)
        }

        verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"), Seq("id", "data"))

        intercept[AnalysisException] {
          doInsert(t1, df)
        }

        verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"), Seq("id", "data"))
      }

      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> "ansi") {
        intercept[AnalysisException] {
          doInsert(t1, df, SaveMode.Overwrite)
        }

        verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"), Seq("id", "data"))

        intercept[AnalysisException] {
          doInsert(t1, df)
        }

        verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"), Seq("id", "data"))
      }

      /*
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> "legacy") {
        doInsert(t1, df, SaveMode.Overwrite)
        verifyTable(
          t1,
          getDF(Row(null, "1")),
          Seq("id", "data"))

        doInsert(t1, df)

        verifyTable(
          t1,
          getDF(Row(null, "1"), Row(null, "1")),
          Seq("id", "data"))
      }
      */
    }
  }

  test("insertInto: struct types and schema enforcement") {
    val t1 = "tbl"
    withTable(t1) {
      sql(
        s"""CREATE TABLE $t1 (
           |  id bigint,
           |  point struct<x: double, y: double>
           |)
           |USING lakesoul""".stripMargin)
      val init = Seq((1L, (0.0, 1.0))).toDF("id", "point")
      doInsert(t1, init)

      doInsert(t1, Seq((2L, (1.0, 0.0))).toDF("col1", "col2")) // naming doesn't matter

      // can handle null types
      doInsert(t1, Seq((3L, (1.0, null))).toDF("col1", "col2"))
      doInsert(t1, Seq((4L, (null, 1.0))).toDF("col1", "col2"))

      val expected = Seq(
        Row(1L, Row(0.0, 1.0)),
        Row(2L, Row(1.0, 0.0)),
        Row(3L, Row(1.0, null)),
        Row(4L, Row(null, 1.0)))
      verifyTable(
        t1,
        spark.createDataFrame(expected.asJava, spark.table(t1).select("id", "point").schema),
        Seq("id", "point"))

      // schema enforcement
      val complexSchema = Seq((5L, (0.5, 0.5), (2.5, 2.5, 1.0), "a", (0.5, "b")))
        .toDF("long", "struct", "newstruct", "string", "badstruct")
        .select(
          $"long",
          $"struct",
          struct(
            $"newstruct._1".as("x"),
            $"newstruct._2".as("y"),
            $"newstruct._3".as("z")) as "newstruct",
          $"string",
          $"badstruct")

      // new column in root
      intercept[AnalysisException] {
        doInsert(t1, complexSchema.select("long", "struct", "string"))
      }

      // new column in struct not accepted
      intercept[AnalysisException] {
        doInsert(t1, complexSchema.select("long", "newstruct"))
      }

      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> "strict") {
        // bad data type not accepted
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("string", "struct"))
        }

        // nested bad data type in struct not accepted
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("long", "badstruct"))
        }
      }

      // missing column in struct
      intercept[AnalysisException] {
        doInsert(t1, complexSchema.select($"long", struct(lit(0.1))))
      }

      // wrong ordering
      intercept[AnalysisException] {
        doInsert(t1, complexSchema.select("struct", "long"))
      }

      // schema evolution
      withSQLConf(
        LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true",
        SQLConf.STORE_ASSIGNMENT_POLICY.key -> "strict") {
        // ordering should still match
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("struct", "long"))
        }

        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("struct", "long", "string"))
        }

        // new column to the end works
        doInsert(t1, complexSchema.select($"long", $"struct", $"string".as("letter")))

        // still cannot insert missing column
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("long", "struct"))
        }

        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select($"long", struct(lit(0.1)), $"string"))
        }

        // still perform nested data type checks
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("long", "badstruct", "string"))
        }

        // bad column within struct
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select(
            $"long", struct(lit(0.1), lit("a"), lit(0.2)), $"string"))
        }

        // Add column to nested field
        doInsert(t1, complexSchema.select($"long", $"newstruct", lit(null)))

        // cannot insert missing field into struct now
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("long", "struct", "string"))
        }
      }

      val expected2 = Seq(
        Row(1L, Row(0.0, 1.0, null), null),
        Row(2L, Row(1.0, 0.0, null), null),
        Row(3L, Row(1.0, null, null), null),
        Row(4L, Row(null, 1.0, null), null),
        Row(5L, Row(0.5, 0.5, null), "a"),
        Row(5L, Row(2.5, 2.5, 1.0), null))
      verifyTable(
        t1,
        spark.createDataFrame(expected2.asJava, spark.table(t1).schema))

      val expectedSchema = new StructType()
        .add("id", LongType)
        .add("point", new StructType()
          .add("x", DoubleType)
          .add("y", DoubleType)
          .add("z", DoubleType))
        .add("letter", StringType)
      val diff = SchemaUtils.reportDifferences(spark.table(t1).schema, expectedSchema)
      if (diff.nonEmpty) {
        fail(diff.mkString("\n"))
      }
    }
  }

  dynamicOverwriteTest("insertInto: overwrite partitioned table in dynamic mode") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").select("data", "id")
      doInsert(t1, init)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data").select("data", "id")
      doInsert(t1, df, SaveMode.Overwrite)

      verifyTable(t1, df.select("id", "data").union(sql("SELECT 4L, 'keep'")), Seq("id", "data"))
    }
  }

  dynamicOverwriteTest("insertInto: overwrite partitioned table in dynamic mode by position") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = createDF(
        Seq((2L, "dummy"), (4L, "keep")),
        Seq("id", "data"),
        Seq("long", "string")
      ).select("data", "id")
      doInsert(t1, init)

      val dfr = createDF(
        Seq((1L, "a"), (2L, "b"), (3L, "c")),
        Seq("data", "id"),
        Seq("long", "string")
      ).select("data", "id")
      doInsert(t1, dfr, SaveMode.Overwrite)

      val df = createDF(
        Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "keep")),
        Seq("id", "data"),
        Seq("long", "string")
      ).select("data", "id")
      verifyTable(t1, df, Seq("data", "id"))
    }
  }
}

trait InsertIntoSQLOnlyTests
  extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter
    with LakeSoulTestUtils {

  import testImplicits._

  /** Check that the results in `tableName` match the `expected` DataFrame. */
  protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  protected def verifyTable(tableName: String, expected: DataFrame, colNames: Seq[String]): Unit = {
    checkAnswer(spark.table(tableName).select(colNames.map(col): _*), expected)
  }

  protected val v2Format: String = "lakesoul"

  /**
    * Whether dynamic partition overwrites are supported by the `Table` definitions used in the
    * test suites. Tables that leverage the V1 Write interface do not support dynamic partition
    * overwrites.
    */
  protected val supportsDynamicOverwrite: Boolean

  /** Whether to include the SQL specific tests in this trait within the extending test suite. */
  protected val includeSQLOnlyTests: Boolean

  protected def withTableAndData(tableName: String)(testFn: String => Unit): Unit = {
    withTable(tableName) {
      val viewName = "tmp_view"
      val df = spark.createDataFrame(spark.sparkContext.parallelize(
        Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c"))),
        StructType(
          Seq(StructField("id", LongType, nullable = false), StructField("data", StringType, nullable = false))
        ))
      df.createOrReplaceTempView(viewName)
      withTempView(viewName) {
        testFn(viewName)
      }
    }
  }

  protected def dynamicOverwriteTest(testName: String)(f: => Unit): Unit = {
    test(testName) {
      try {
        withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString,
          LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
          f
        }
        if (!supportsDynamicOverwrite) {
          fail("Expected failure from test, because the table doesn't support dynamic overwrites")
        }
      } catch {
        case a: AnalysisException if !supportsDynamicOverwrite =>
          assert(a.getMessage.contains("does not support dynamic overwrite"))
      }
    }
  }

  if (includeSQLOnlyTests) {
    test("InsertInto: when the table doesn't exist") {
      val t1 = "tbl"
      val t2 = "tbl2"
      withTableAndData(t1) { _ =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
        val e = intercept[AnalysisException] {
          sql(s"INSERT INTO $t2 VALUES (2L, 'dummy')")
        }
        assert(e.getMessage.contains(t2))
        assert(e.getMessage.contains("Table not found"))
      }
    }

    test("InsertInto: append to partitioned table - static clause") {
      val t1 = "tbl"
      withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
          sql(s"INSERT INTO $t1 PARTITION (id = 23) SELECT data FROM $view")
          verifyTable(t1, sql(s"SELECT 23, data FROM $view"), Seq("id", "data"))
        }
      }
    }

    test("InsertInto: static PARTITION clause fails with non-partition column") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (data)")

        val exc = intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $t1 PARTITION (id=1) SELECT data FROM $view")
        }

        verifyTable(t1, spark.emptyDataFrame, Seq("id", "data"))
        assert(exc.getMessage.contains(
          "PARTITION clause cannot contain a non-partition column name"))
        assert(exc.getMessage.contains("id"))
      }
    }

    test("InsertInto: dynamic PARTITION clause fails with non-partition column") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")

        val exc = intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $t1 PARTITION (data) SELECT * FROM $view")
        }

        verifyTable(t1, spark.emptyDataFrame, Seq("id", "data"))
        assert(exc.getMessage.contains(
          "PARTITION clause cannot contain a non-partition column name"))
        assert(exc.getMessage.contains("data"))
      }
    }

    test("InsertInto: overwrite - dynamic clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = "tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
          sql(s"INSERT INTO $t1 VALUES ('dummy',2L), ('also-deleted',4L)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id) SELECT data,id FROM $view")
          verifyTable(t1, Seq(
            (1, "a"),
            (2, "b"),
            (3, "c")).toDF(),
            Seq("id", "data"))
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - dynamic clause - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES ('dummy',2L), ('keep',4L)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id) SELECT data,id FROM $view")
        verifyTable(t1, Seq(
          (1, "a"),
          (2, "b"),
          (3, "c"),
          (4, "keep")).toDF("id", "data"),
          Seq("id", "data"))
      }
    }

    test("InsertInto: overwrite - missing clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = "tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
          sql(s"INSERT INTO $t1 VALUES ('dummy',2L), ('also-deleted',4L)")
          sql(s"INSERT OVERWRITE TABLE $t1 SELECT data,id FROM $view")
          verifyTable(t1, Seq(
            (1, "a"),
            (2, "b"),
            (3, "c")).toDF("id", "data"),
            Seq("id", "data"))
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - missing clause - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES ('dummy',2L), ('keep',4L)")
        sql(s"INSERT OVERWRITE TABLE $t1 SELECT data,id FROM $view")
        verifyTable(t1, Seq(
          (1, "a"),
          (2, "b"),
          (3, "c"),
          (4, "keep")).toDF("id", "data"),
          Seq("id", "data"))
      }
    }

    test("InsertInto: overwrite - static clause") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
          sql(s"CREATE TABLE $t1 (id bigint, data string, p1 int) " +
            s"USING $v2Format PARTITIONED BY (p1)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 23), (4L, 'keep', 2)")
          verifyTable(t1, Seq(
            (2L, "dummy", 23),
            (4L, "keep", 2)).toDF("id", "data", "p1"),
            Seq("id", "data", "p1"))
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p1 = 23) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 23),
            (2, "b", 23),
            (3, "c", 23),
            (4, "keep", 2)).toDF("id", "data", "p1"),
            Seq("id", "data", "p1"))
        }
      }
    }

    test("InsertInto: overwrite - mixed clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString,
        LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
        val t1 = "tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES ('dummy', 2L, 2), ('also-deleted', 4L, 2)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id, p = 2) SELECT data,id FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"),
            Seq("id", "data", "p"))
        }
      }
    }

    test("InsertInto: overwrite - mixed clause reordered - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString,
        LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
        val t1 = "tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES ('dummy', 2L, 2), ('also-deleted', 4L, 2)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2, id) SELECT data,id FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"),
            Seq("id", "data", "p"))
        }
      }
    }

    test("InsertInto: overwrite - implicit dynamic partition - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString,
        LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
        val t1 = "tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES ('dummy', 2L, 2), ('also-deleted', 4L, 2)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2) SELECT data,id FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"),
            Seq("id", "data", "p"))
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - mixed clause - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
          s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES ('dummy', 2L, 2), ('keep', 4L, 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2, id) SELECT data,id FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"),
          Seq("id", "data", "p"))
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - mixed clause reordered - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
          s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES ('dummy', 2L, 2), ('keep', 4L, 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id, p = 2) SELECT data,id FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"),
          Seq("id", "data", "p"))
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - implicit dynamic partition - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
          s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES ('dummy', 2L, 2), ('keep', 4L, 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2) SELECT data,id FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"),
          Seq("id", "data", "p"))
      }
    }

    //    test("InsertInto: overwrite - multiple static partitions - dynamic mode") {
    //      // Since all partitions are provided statically, this should be supported by everyone
    //      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
    //        val t1 = "tbl"
    //        withTableAndData(t1) { view =>
    //          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
    //              s"USING $v2Format PARTITIONED BY (id, p)")
    //          sql(s"INSERT INTO $t1 VALUES ('dummy', 2L, 2), ('keep', 4L, 2)")
    //          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id = 2, p = 2) SELECT data FROM $view")
    //          verifyTable(t1, Seq(
    //            (2, "a", 2),
    //            (2, "b", 2),
    //            (2, "c", 2),
    //            (4, "keep", 2)).toDF("id", "data", "p"),
    //            Seq("id", "data", "p"))
    //        }
    //      }
    //    }
  }

  // END Apache Spark tests
}
