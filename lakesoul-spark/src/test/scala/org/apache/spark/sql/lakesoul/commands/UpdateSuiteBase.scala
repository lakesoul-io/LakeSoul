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

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf.NATIVE_IO_ENABLE
import org.apache.spark.sql.lakesoul.test.{LakeSoulTestBeforeAndAfterEach, LakeSoulTestUtils}
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}

import java.util.Locale
import scala.language.implicitConversions

abstract class UpdateSuiteBase
  extends QueryTest
    with SharedSparkSession
    with LakeSoulTestBeforeAndAfterEach
    with SQLTestUtils
    with LakeSoulTestUtils {

  import testImplicits._

  protected def executeUpdate(lakeSoulTable: String, set: Seq[String], where: String): Unit = {
    executeUpdate(lakeSoulTable, set.mkString(", "), where)
  }

  protected def executeUpdate(lakeSoulTable: String, set: String, where: String = null): Unit

  protected def append(df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    val writer = df.write.format("lakesoul").mode("append")
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*)
    }
    writer.save(snapshotManagement.table_path)
  }

  protected def appendHashPartition(df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    val writer = df.write.format("lakesoul").mode("append")
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*)
    }
    writer
      .option("hashPartitions", "hash")
      .option("hashBucketNum", "2")
      .save(snapshotManagement.table_path)
  }

  protected def executeUpsert(df: DataFrame): Unit = {
    LakeSoulTable.forPath(snapshotManagement.table_path)
      .upsert(df)
  }

  implicit def jsonStringToSeq(json: String): Seq[String] = json.split("\n")

  protected def checkUpdate(condition: Option[String],
                            setClauses: String,
                            expectedResults: Seq[Row],
                            colNames: Seq[String],
                            tableName: Option[String] = None): Unit = {
    executeUpdate(tableName.getOrElse(s"lakesoul.default.`$tempPath`"), setClauses, where = condition.orNull)
    LakeSoulTable.uncached(tempPath)
    checkAnswer(readLakeSoulTable(tempPath).select(colNames.map(col): _*), expectedResults)
  }

  test("basic case") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    checkUpdate(condition = None, setClauses = "key = 1, value = 2",
      expectedResults = Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Nil,
      Seq("key", "value"))
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - LakeSoul table by path - Partition=$isPartitioned") {
      withTable("lakeSoulTable") {
        val partitions = if (isPartitioned) "key" :: Nil else Nil
        append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

        checkUpdate(
          condition = Some("key >= 1"),
          setClauses = "value = key + value, key = key + 1",
          expectedResults = Row(0, 3) :: Row(2, 5) :: Row(2, 2) :: Row(3, 4) :: Nil,
          Seq("key", "value"),
          tableName = Some(s"lakesoul.default.`$tempPath`"))
      }
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - LakeSoul table by path with hash partition - Partition=$isPartitioned") {
      withTable("lakeSoulTable") {
        val partitions = if (isPartitioned) "key" :: Nil else Nil
        appendHashPartition(Seq((2, 2, 2), (1, 4, 4), (1, 1, 1), (0, 3, 3))
          .toDF("key", "hash", "value"), partitions)

        checkUpdate(
          condition = Some("key >= 1"),
          setClauses = "value = key + value, key = key + 1",
          expectedResults = Row(0, 3, 3) :: Row(2, 4, 5) :: Row(2, 1, 2) :: Row(3, 2, 4) :: Nil,
          Seq("key", "hash", "value"),
          tableName = Some(s"lakesoul.default.`$tempPath`"))
      }
    }
  }

  Seq(true).foreach { isPartitioned =>
    test(s"basic update - with hash partition - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      appendHashPartition(Seq((2, 2, 2), (1, 1, 4), (1, 2, 1), (0, 0, 3))
        .toDF("key", "hash", "value"), partitions)

      checkUpdate(condition = Some("value > 2"), setClauses = "value = 1",
        expectedResults = Row(2, 2, 2) :: Row(1, 1, 1) :: Row(1, 2, 1) :: Row(0, 0, 1) :: Nil,
        Seq("key", "hash", "value"))
    }

  }

  Seq(true).foreach { isPartitioned =>
    test(s"upsert before update - with hash partition - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      appendHashPartition(Seq((2, 2, 1), (1, 2, 1), (0, 0, 1))
        .toDF("key", "hash", "value"), partitions)

      executeUpsert(Seq((2, 2, 2), (1, 3, 2), (0, 0, 2))
        .toDF("key", "hash", "value"))

      checkUpdate(condition = Some("hash = 2"), setClauses = "value = 3",
        expectedResults = Row(2, 2, 3) :: Row(1, 2, 3) :: Row(1, 3, 2) :: Row(0, 0, 2) :: Nil,
        Seq("key", "hash", "value"))
    }

  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - LakeSoul table by name - Partition=$isPartitioned") {
      withTable("lakesoul_table") {
        val partitionByClause = if (isPartitioned) "PARTITIONED BY (key)" else ""
        sql(
          s"""
             |CREATE TABLE lakesoul_table(key INT, value INT)
             |USING lakesoul
             |OPTIONS('path'='$tempPath')
             |$partitionByClause
           """.stripMargin)

        append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))

        checkUpdate(
          condition = Some("key >= 1"),
          setClauses = "value = key + value, key = key + 1",
          expectedResults = Row(0, 3) :: Row(2, 5) :: Row(2, 2) :: Row(3, 4) :: Nil,
          Seq("key", "value"),
          tableName = Some("lakesoul_table"))
      }
    }
  }


  Seq(true, false).foreach { isPartitioned =>
    test(s"table has null values - partitioned=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq(("a", 1), (null, 2), (null, 3), ("d", 4)).toDF("value", "key"), partitions)

      // predicate evaluates to null; no-op
      checkUpdate(condition = Some("value = null"),
        setClauses = "value = -1",
        expectedResults =
          Row("a", 1) :: Row(null, 2) :: Row(null, 3) :: Row("d", 4) :: Nil,
        Seq("value", "key")
        //          Seq(("a", 1), (null, 2), (null, 3), ("d", 4)).toDF("key", "value")
      )

      checkUpdate(condition = Some("value = 'a'"),
        setClauses = "key = -1",
        expectedResults = Row("a", -1) :: Row(null, 2) :: Row(null, 3) :: Row("d", 4) :: Nil,
        Seq("value", "key"))

      checkUpdate(condition = Some("value is null"),
        setClauses = "key = -2",
        expectedResults = Row("a", -1) :: Row(null, -2) :: Row(null, -2) :: Row("d", 4) :: Nil,
        Seq("value", "key"))

      checkUpdate(condition = Some("value is not null"),
        setClauses = "key = -3",
        expectedResults = Row("a", -3) :: Row(null, -2) :: Row(null, -2) :: Row("d", -3) :: Nil,
        Seq("value", "key"))

      checkUpdate(condition = Some("value <=> null"),
        setClauses = "key = -4",
        expectedResults = Row("a", -3) :: Row(null, -4) :: Row(null, -4) :: Row("d", -3) :: Nil,
        Seq("value", "key"))
    }
  }

  test("basic case - condition is false") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    checkUpdate(condition = Some("1 != 1"), setClauses = "key = 1, value = 2",
      expectedResults = Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil,
      Seq("key", "value"))
  }

  test("basic case - condition is true") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    checkUpdate(condition = Some("1 = 1"), setClauses = "key = 1, value = 2",
      expectedResults = Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Nil,
      Seq("key", "value"))
  }

  test("basic case with hash partition - condition is true") {
    appendHashPartition(Seq((2, 2, 2), (1, 2, 4), (1, 3, 1), (0, 3, 3))
      .toDF("key", "hash", "value"))
    checkUpdate(condition = Some("1 = 1"), setClauses = "key = 1, value = 2",
      expectedResults = Row(1, 2, 2) :: Row(1, 3, 2) :: Nil,
      Seq("key", "hash", "value"))
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - without where - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = None, setClauses = "key = 1, value = 2",
        expectedResults = Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - without where and partial columns - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = None, setClauses = "key = 1",
        expectedResults = Row(1, 1) :: Row(1, 2) :: Row(1, 3) :: Row(1, 4) :: Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - without where and out-of-order columns - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = None, setClauses = "value = 3, key = 1",
        expectedResults = Row(1, 3) :: Row(1, 3) :: Row(1, 3) :: Row(1, 3) :: Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - without where and complex input - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = None, setClauses = "value = key + 3, key = key + 1",
        expectedResults = Row(1, 3) :: Row(2, 4) :: Row(2, 4) :: Row(3, 5) :: Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - with where - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = Some("key = 1"), setClauses = "value = 3, key = 1",
        expectedResults = Row(1, 3) :: Row(2, 2) :: Row(0, 3) :: Row(1, 3) :: Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update with hash partition - with where - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      appendHashPartition(Seq((2, 2, 2), (1, 4, 4), (1, 1, 1), (0, 3, 3))
        .toDF("key", "hash", "value"), partitions)

      checkUpdate(condition = Some("hash = 1"), setClauses = "value = 3, key = 1",
        expectedResults = Row(2, 2, 2) :: Row(1, 4, 4) :: Row(1, 1, 3) :: Row(0, 3, 3) :: Nil,
        Seq("key", "hash", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - with where and complex input - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = Some("key >= 1"), setClauses = "value = key + value, key = key + 1",
        expectedResults = Row(0, 3) :: Row(2, 5) :: Row(2, 2) :: Row(3, 4) :: Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - with where and no row matched - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = Some("key >= 10"), setClauses = "value = key + value, key = key + 1",
        expectedResults = Row(0, 3) :: Row(1, 1) :: Row(1, 4) :: Row(2, 2) :: Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"type mismatch - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = Some("key >= 1"),
        setClauses = "value = key + cast(value as String), key = key + '1'",
        expectedResults = Row(0, 3) :: Row(2, 5) :: Row(3, 4) :: Row(2, 2) :: Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - TypeCoercion twice - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((99, 2), (100, 4), (101, 3)).toDF("key", "value"), partitions)

      checkUpdate(
        condition = Some("cast(key as long) * cast('1.0' as decimal(38, 18)) > 100"),
        setClauses = "value = -3",
        expectedResults = Row(100, 4) :: Row(101, -3) :: Row(99, 2) :: Nil,
        Seq("key", "value"))
    }
  }

  test("different variations of column references") {
    append(
      createDF(
        Seq((99, 2), (100, 4), (101, 3), (102, 5)),
        Seq("key", "value"),
        Seq("int", "int")
      )
    )

    spark.read.format("lakesoul").load(tempPath).createOrReplaceTempView("tblName")

    checkUpdate(condition = Some("key = 99"), setClauses = "value = -1",
      Row(99, -1) :: Row(100, 4) :: Row(101, 3) :: Row(102, 5) :: Nil,
      Seq("key", "value"))
    checkUpdate(condition = Some("`key` = 100"), setClauses = "`value` = -1",
      Row(99, -1) :: Row(100, -1) :: Row(101, 3) :: Row(102, 5) :: Nil,
      Seq("key", "value"))
  }

  test("LakeSoul Table columns can have db and table qualifiers") {
    withTable("lakeSoulTable") {
      spark.read.json(
        """
          {"a": {"b.1": 1, "c.e": 'random'}, "d": 1}
          {"a": {"b.1": 3, "c.e": 'string'}, "d": 2}"""
          .split("\n").toSeq.toDS()).write.format("lakesoul").saveAsTable("`lakeSoulTable`")

      executeUpdate(
        lakeSoulTable = "lakeSoulTable",
        set = "`default`.`lakeSoulTable`.a.`b.1` = -1, lakeSoulTable.a.`c.e` = 'RANDOM'",
        where = "d = 1")

      checkAnswer(spark.table("lakeSoulTable"),
        spark.read.json(
          """
            {"a": {"b.1": -1, "c.e": 'RANDOM'}, "d": 1}
            {"a": {"b.1": 3, "c.e": 'string'}, "d": 2}"""
            .split("\n").toSeq.toDS()))
    }
  }

  test("Negative case - non-lakesoul lakeSoulTable") {
    Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value")
      .write.mode("overwrite").format("parquet").save(tempPath)
    val e = intercept[AnalysisException] {
      executeUpdate(lakeSoulTable = s"lakesoul.default.`$tempPath`", set = "key1 = 3")
    }.getMessage
    assert(e.contains("doesn't exist") || e.contains("Table or view not found"))
  }

  test("Negative case - check lakeSoulTable columns during analysis") {

    withTable("table") {
      sql(s"CREATE TABLE table (s int, t string) USING lakesoul PARTITIONED BY (s) LOCATION '${tempDir.getCanonicalPath}'")
      var ae = intercept[AnalysisException] {
        executeUpdate("table", set = "column_doesnt_exist = 'San Francisco'", where = "t = 'a'")
      }
      assert(ae.message.contains("does not exist"))

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        executeUpdate(lakeSoulTable = "table", set = "S = 1, T = 'b'", where = "T = 'a'")
        ae = intercept[AnalysisException] {
          executeUpdate(lakeSoulTable = "table", set = "S = 1, s = 'b'", where = "s = 1")
        }
        assert(ae.message.contains("There is a conflict from these SET columns"))
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        ae = intercept[AnalysisException] {
          executeUpdate(lakeSoulTable = "table", set = "S = 1", where = "t = 'a'")
        }
        assert(ae.message.contains("does not exist"))

        ae = intercept[AnalysisException] {
          executeUpdate(lakeSoulTable = "table", set = "S = 1, s = 'b'", where = "s = 1")
        }
        assert(ae.message.contains("does not exist"))

        // unresolved column in condition
        ae = intercept[AnalysisException] {
          executeUpdate(lakeSoulTable = "table", set = "s = 1", where = "T = 'a'")
        }
        assert(ae.message.contains("does not exist"))
      }
    }
  }

  test("Negative case - do not support subquery test") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("c", "d").createOrReplaceTempView("source")

    // basic subquery
    val e0 = intercept[AnalysisException] {
      executeUpdate(lakeSoulTable = s"lakesoul.default.`$tempPath`",
        set = "key = 1",
        where = "key < (SELECT max(c) FROM source)")
    }.getMessage
    assert(e0.contains("Subqueries are not supported"))

    // subquery with EXISTS
    val e1 = intercept[AnalysisException] {
      executeUpdate(lakeSoulTable = s"lakesoul.default.`$tempPath`",
        set = "key = 1",
        where = "EXISTS (SELECT max(c) FROM source)")
    }.getMessage
    assert(e1.contains("Subqueries are not supported"))

    // subquery with NOT EXISTS
    val e2 = intercept[AnalysisException] {
      executeUpdate(lakeSoulTable = s"lakesoul.default.`$tempPath`",
        set = "key = 1",
        where = "NOT EXISTS (SELECT max(c) FROM source)")
    }.getMessage
    assert(e2.contains("Subqueries are not supported"))

    // subquery with IN
    val e3 = intercept[AnalysisException] {
      executeUpdate(lakeSoulTable = s"lakesoul.default.`$tempPath`",
        set = "key = 1",
        where = "key IN (SELECT max(c) FROM source)")
    }.getMessage
    assert(e3.contains("Subqueries are not supported"))

    // subquery with NOT IN
    val e4 = intercept[AnalysisException] {
      executeUpdate(lakeSoulTable = s"lakesoul.default.`$tempPath`",
        set = "key = 1",
        where = "key NOT IN (SELECT max(c) FROM source)")
    }.getMessage
    assert(e4.contains("Subqueries are not supported"))
  }

  test("nested data support") {
    // nested data support from ArrowColumnVector is limited
    // set a nested field
    checkUpdateJson(lakeSoulTable =
      """
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "z = 10",
      set = "a.c.d = 'RANDOM'" :: Nil,
      expected =
        """
        {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""")

    // do nothing as condition has no match
    val unchanged =
      """
        {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}"""
    checkUpdateJson(lakeSoulTable = unchanged,
      updateWhere = "z = 30",
      set = "a.c.d = 'RANDOMMMMM'" :: Nil,
      expected = unchanged)

    // set multiple nested fields at different levels
    checkUpdateJson(
      lakeSoulTable =
        """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "z = 20",
      set = "a.c.d = 'RANDOM2'" :: "a.c.e = 'STR2'" :: "a.g = -2" :: "z = -20" :: Nil,
      expected =
        """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM2', "e": 'STR2'}, "g": -2}, "z": -20}""")

    // set nested fields to null
    checkUpdateJson(
      lakeSoulTable =
        """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "a.c.d = 'random2'",
      set = "a.c = null" :: "a.g = null" :: Nil,
      expected =
        """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": null, "g": null}, "z": 20}""")

    // set a top struct type column to null
    checkUpdateJson(
      lakeSoulTable =
        """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "a.c.d = 'random2'",
      set = "a = null" :: Nil,
      expected =
        """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": null, "z": 20}""")

    // set a nested field using named_struct
    checkUpdateJson(
      lakeSoulTable =
        """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "a.g = 2",
      set = "a.c = named_struct('d', 'RANDOM2', 'e', 'STR2')" :: Nil,
      expected =
        """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM2', "e": 'STR2'}, "g": 2}, "z": 20}""")

    // set an integer nested field with a string that can be casted into an integer
    checkUpdateJson(
      lakeSoulTable =
        """
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "z = 10",
      set = "a.g = '-1'" :: "z = '30'" :: Nil,
      expected =
        """
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": -1}, "z": 30}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""")

    // set the nested data that has an Array field
    checkUpdateJson(
      lakeSoulTable =
        """
          {"a": {"c": {"d": 'random', "e": [1, 11]}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM2', "e": [2, 22]}, "g": 2}, "z": 20}""",
      updateWhere = "z = 20",
      set = "a.c.d = 'RANDOM22'" :: "a.g = -2" :: Nil,
      expected =
        """
          {"a": {"c": {"d": 'random', "e": [1, 11]}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM22', "e": [2, 22]}, "g": -2}, "z": 20}""")

    // set an array field
    checkUpdateJson(
      lakeSoulTable =
        """
          {"a": {"c": {"d": 'random', "e": [1, 11]}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM22', "e": [2, 22]}, "g": -2}, "z": 20}""",
      updateWhere = "z = 10",
      set = "a.c.e = array(-1, -11)" :: "a.g = -1" :: Nil,
      expected =
        """
          {"a": {"c": {"d": 'random', "e": [-1, -11]}, "g": -1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM22', "e": [2, 22]}, "g": -2}, "z": 20}""")

    // set an array field as a top-level attribute
    checkUpdateJson(
      lakeSoulTable =
        """
          {"a": [1, 11], "b": 'Z'}
          {"a": [2, 22], "b": 'Y'}""",
      updateWhere = "b = 'Z'",
      set = "a = array(-1, -11, -111)" :: Nil,
      expected =
        """
          {"a": [-1, -11, -111], "b": 'Z'}
          {"a": [2, 22], "b": 'Y'}""")
  }


  test("nested data - negative case") {
    val targetDF = spark.read.json(
      """
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}"""
        .split("\n").toSeq.toDS())

    testAnalysisException(
      targetDF,
      set = "a.c = 'RANDOM2'" :: Nil,
      where = "z = 10",
      errMsgs = "data type mismatch" :: Nil)

    testAnalysisException(
      targetDF,
      set = "a.c.z = 'RANDOM2'" :: Nil,
      errMsgs = "No such struct field" :: Nil)

    testAnalysisException(
      targetDF,
      set = "a.c = named_struct('d', 'rand', 'e', 'str')" :: "a.c.d = 'RANDOM2'" :: Nil,
      errMsgs = "There is a conflict from these SET columns" :: Nil)

    testAnalysisException(
      targetDF,
      set =
        Seq("a = named_struct('c', named_struct('d', 'rand', 'e', 'str'))", "a.c.d = 'RANDOM2'"),
      errMsgs = "There is a conflict from these SET columns" :: Nil)

    val schema = new StructType().add("a", MapType(StringType, IntegerType))
    val mapData = spark.read.schema(schema).json(Seq("""{"a": {"b": 1}}""").toDS())
    testAnalysisException(
      mapData,
      set = "a.b = -1" :: Nil,
      errMsgs = "Updating nested fields is only supported for StructType" :: Nil)

    // Updating an ArrayStruct is not supported
    val arrayStructData = spark.read.json(Seq("""{"a": [{"b": 1}, {"b": 2}]}""").toDS())
    testAnalysisException(
      arrayStructData,
      set = "a.b = -1" :: Nil,
      errMsgs = "data type mismatch" :: Nil)
  }


  protected def checkUpdateJson(
                                 lakeSoulTable: Seq[String],
                                 source: Seq[String] = Nil,
                                 updateWhere: String,
                                 set: Seq[String],
                                 expected: Seq[String]): Unit = {
    withTempDir { dir =>
      withTempView("source") {
        def toDF(jsonStrs: Seq[String]) = spark.read.json(jsonStrs.toDS)

        toDF(lakeSoulTable).write.format("lakesoul").mode("overwrite").save(dir.toString)
        if (source.nonEmpty) {
          toDF(source).createOrReplaceTempView("source")
        }
        executeUpdate(s"lakesoul.default.`$dir`", set, updateWhere)
        checkAnswer(readLakeSoulTable(dir.toString), toDF(expected))
      }
    }
  }

  protected def testAnalysisException(
                                       targetDF: DataFrame,
                                       set: Seq[String],
                                       where: String = null,
                                       errMsgs: Seq[String] = Nil): Unit = {
    withTempDir { dir =>
      targetDF.write.format("lakesoul").save(dir.toString)
      val e = intercept[AnalysisException] {
        executeUpdate(lakeSoulTable = s"lakesoul.default.`$dir`", set, where)
      }
      errMsgs.foreach { msg =>
        assert(e.getMessage.toLowerCase(Locale.ROOT).contains(msg.toLowerCase(Locale.ROOT)))
      }
    }
  }
}
