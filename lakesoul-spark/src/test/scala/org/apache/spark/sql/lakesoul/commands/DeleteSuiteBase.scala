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
import org.apache.spark.sql.lakesoul.test.LakeSoulTestBeforeAndAfterEach
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}

abstract class DeleteSuiteBase extends QueryTest
  with SharedSparkSession with LakeSoulTestBeforeAndAfterEach {

  import testImplicits._

  protected def executeDelete(target: String, where: String = null): Unit

  protected def append(df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    val writer = df.write.format("lakesoul").mode("append")
    if (partitionBy.nonEmpty) {
      writer.option("rangePartitions", partitionBy.mkString(","))
    }
    writer.save(snapshotManagement.table_path)
  }

  protected def appendHashPartition(df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    val writer = df.write.format("lakesoul").mode("append")
    if (partitionBy.nonEmpty) {
      writer.option("rangePartitions", partitionBy.mkString(","))
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

  protected def checkDelete(condition: Option[String],
                            expectedResults: Seq[Row],
                            colNames: Seq[String],
                            tableName: Option[String] = None): Unit = {
    executeDelete(target = tableName.getOrElse(s"lakesoul.default.`$tempPath`"), where = condition.orNull)
    checkAnswer(readLakeSoulTable(tempPath).select(colNames.map(col): _*), expectedResults)
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkDelete(condition = None, Nil, Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case with hash partition - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "range" :: Nil else Nil
      append(Seq((2, 2, 2), (1, 2, 4), (1, 2, 1), (0, 2, 3))
        .toDF("range", "hash", "value"),
        partitions)

      checkDelete(condition = None, Nil, Seq("range", "hash", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case - delete from a LakeSoul table by path - Partition=$isPartitioned") {
      withTable("starTable") {
        val partitions = if (isPartitioned) "key" :: Nil else Nil
        val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
        append(input, partitions)

        checkDelete(Some("value = 4 and key = 3"),
          Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil,
          Seq("key", "value"))
        checkDelete(Some("value = 4 and key = 1"),
          Row(2, 2) :: Row(1, 1) :: Row(0, 3) :: Nil,
          Seq("key", "value"))
        checkDelete(Some("value = 2 or key = 1"),
          Row(0, 3) :: Nil,
          Seq("key", "value"))
        checkDelete(Some("key = 0 or value = 99"), Nil,
          Seq("key", "value"))
      }
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case with hash partition - delete from a LakeSoul table by path - rangePartition=$isPartitioned") {
      withTable("starTable") {
        val partitions = if (isPartitioned) "range" :: Nil else Nil
        val input = Seq((2, 2, 2), (1, 4, 3), (1, 4, 4), (1, 1, 1), (0, 3, 3))
          .toDF("range", "hash", "value")
        appendHashPartition(input, partitions)

        checkDelete(Some("value = 4 and range = 3"),
          Row(2, 2, 2) :: Row(1, 4, 4) :: Row(1, 1, 1) :: Row(0, 3, 3) :: Nil,
          Seq("range", "hash", "value"))
        checkDelete(Some("value = 4 and range = 1"),
          Row(2, 2, 2) :: Row(1, 1, 1) :: Row(0, 3, 3) :: Nil,
          Seq("range", "hash", "value"))
        checkDelete(Some("value = 2 or range = 1"),
          Row(0, 3, 3) :: Nil,
          Seq("range", "hash", "value"))
        checkDelete(Some("range = 0 or value = 99"), Nil,
          Seq("range", "hash", "value"))
      }
    }
  }

  test(s"basic case with hash partition - delete from a LakeSoul table by path") {
    withTable("starTable") {
      val partitions = if (false) "range" :: Nil else Nil
      val input = Seq((2, 2, 2), (1, 4, 4), (1, 1, 1), (0, 3, 3))
        .toDF("range", "hash", "value")
      appendHashPartition(input, partitions)

      checkDelete(Some("value = 4 and range = 3"),
        Row(2, 2, 2) :: Row(1, 4, 4) :: Row(1, 1, 1) :: Row(0, 3, 3) :: Nil,
        Seq("range", "hash", "value"))
      checkDelete(Some("value = 4 and range = 1"),
        Row(2, 2, 2) :: Row(1, 1, 1) :: Row(0, 3, 3) :: Nil,
        Seq("range", "hash", "value"))
      checkDelete(Some("value = 2 or hash = 1"),
        Row(0, 3, 3) :: Nil,
        Seq("range", "hash", "value"))
      checkDelete(Some("hash = 3 or value = 99"), Nil,
        Seq("range", "hash", "value"))
    }
  }

  test("upsert before delete with hash partition") {
    withTable("starTable") {
      val partitions = "range" :: Nil
      val input = Seq((2, 2, 1), (1, 2, 1), (0, 3, 1))
        .toDF("range", "hash", "value")
      appendHashPartition(input, partitions)

      executeUpsert(Seq((2, 2, 2), (1, 3, 2))
        .toDF("range", "hash", "value"))

      checkDelete(Some("value = 2"),
        Row(1, 2, 1) :: Row(0, 3, 1) :: Nil,
        Seq("range", "hash", "value"))
      checkDelete(Some("value = 4 or range = 1"),
        Row(0, 3, 1) :: Nil,
        Seq("range", "hash", "value"))
      checkDelete(Some("hash = 3"),
        Nil,
        Seq("range", "hash", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case - delete from a LakeSoul table by name - Partition=$isPartitioned") {
      withTable("lakesoul_table") {
        val partitionByClause = if (isPartitioned) "PARTITIONED BY (key)" else ""
        sql(
          s"""
             |CREATE TABLE lakesoul_table(key INT, value INT)
             |USING lakesoul
             |OPTIONS('path'='$tempPath')
             |$partitionByClause
           """.stripMargin)

        val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
        append(input)

        checkDelete(Some("value = 4 and key = 3"),
          Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil,
          Seq("key", "value"),
          Some("lakesoul_table"))
        checkDelete(Some("value = 4 and key = 1"),
          Row(2, 2) :: Row(1, 1) :: Row(0, 3) :: Nil,
          Seq("key", "value"),
          Some("lakesoul_table"))
        checkDelete(Some("value = 2 or key = 1"),
          Row(0, 3) :: Nil,
          Seq("key", "value"),
          Some("lakesoul_table"))
        checkDelete(Some("key = 0 or value = 99"),
          Nil,
          Seq("key", "value"),
          Some("lakesoul_table"))
      }
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic key columns - Partition=$isPartitioned") {
      val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(input, partitions)

      checkDelete(Some("key > 2"), Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil,
        Seq("key", "value"))
      checkDelete(Some("key < 2"), Row(2, 2) :: Nil,
        Seq("key", "value"))
      checkDelete(Some("key = 2"), Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic key columns with hash partition - Partition=$isPartitioned") {
      val input = Seq((2, 2, 2), (1, 4, 4), (1, 1, 1), (0, 3, 3))
        .toDF("range", "hash", "value")
      val partitions = if (isPartitioned) "range" :: Nil else Nil
      appendHashPartition(input, partitions)

      checkDelete(Some("range > 2"), Row(2, 2, 2) :: Row(1, 4, 4) :: Row(1, 1, 1) :: Row(0, 3, 3) :: Nil,
        Seq("range", "hash", "value"))
      checkDelete(Some("range < 2"), Row(2, 2, 2) :: Nil,
        Seq("range", "hash", "value"))
      checkDelete(Some("range = 2"), Nil,
        Seq("range", "hash", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"where key columns - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkDelete(Some("key = 1"), Row(2, 2) :: Row(0, 3) :: Nil,
        Seq("key", "value"))
      checkDelete(Some("key = 2"), Row(0, 3) :: Nil,
        Seq("key", "value"))
      checkDelete(Some("key = 0"), Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"where data columns - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkDelete(Some("value <= 2"), Row(1, 4) :: Row(0, 3) :: Nil,
        Seq("key", "value"))
      checkDelete(Some("value = 3"), Row(1, 4) :: Nil,
        Seq("key", "value"))
      checkDelete(Some("value != 0"), Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"where data columns with hash partition - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "range" :: Nil else Nil
      appendHashPartition(Seq((2, 2, 2), (1, 4, 4), (1, 1, 1), (0, 3, 3))
        .toDF("range", "hash", "value"), partitions)

      checkDelete(Some("value <= 2"), Row(1, 4, 4) :: Row(0, 3, 3) :: Nil,
        Seq("range", "hash", "value"))
      checkDelete(Some("value = 3"), Row(1, 4, 4) :: Nil,
        Seq("range", "hash", "value"))
      checkDelete(Some("value != 0"), Nil,
        Seq("range", "hash", "value"))
    }
  }

  test("where data columns and partition columns") {
    val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
    append(input, Seq("key"))

    checkDelete(Some("value = 4 and key = 3"),
      Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil,
      Seq("key", "value"))
    checkDelete(Some("value = 4 and key = 1"),
      Row(2, 2) :: Row(1, 1) :: Row(0, 3) :: Nil,
      Seq("key", "value"))
    checkDelete(Some("value = 2 or key = 1"),
      Row(0, 3) :: Nil,
      Seq("key", "value"))
    checkDelete(Some("key = 0 or value = 99"),
      Nil,
      Seq("key", "value"))
  }


  test("Negative case - non-LakeSoul target") {
    Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value")
      .write.format("parquet").mode("append").save(tempPath)
    val e = intercept[AnalysisException] {
      executeDelete(target = s"lakesoul.default.`$tempPath`")
    }.getMessage
    assert(e.contains("Table or view not found") || e.contains("doesn't exist"))
  }

  test("Negative case - non-deterministic condition") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    val e = intercept[AnalysisException] {
      executeDelete(target = s"lakesoul.default.`$tempPath`", where = "rand() > 0.5")
    }.getMessage
    assert(e.contains("nondeterministic expressions are only allowed in"))
  }

  test("delete cached table by name") {
    withTable("cached_lakesoul_table") {
      Seq((2, 2), (1, 4)).toDF("key", "value")
        .write.format("lakesoul").saveAsTable("cached_lakesoul_table")

      spark.table("cached_lakesoul_table").cache()
      spark.table("cached_lakesoul_table").collect()
      executeDelete(target = "cached_lakesoul_table", where = "key = 2")
      checkAnswer(spark.table("cached_lakesoul_table"), Row(1, 4) :: Nil)
    }
  }


  Seq(true, false).foreach { isPartitioned =>
    test(s"condition having current_date - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(
        Seq((java.sql.Date.valueOf("1969-12-31"), 2),
          (java.sql.Date.valueOf("2099-12-31"), 4))
          .toDF("key", "value"), partitions)

      checkDelete(Some("CURRENT_DATE > key"),
        Row(java.sql.Date.valueOf("2099-12-31"), 4) :: Nil,
        Seq("key", "value"))
      checkDelete(Some("CURRENT_DATE <= key"), Nil,
        Seq("key", "value"))
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"foldable condition - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      val allRows = Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil

      checkDelete(Some("false"), allRows,
        Seq("key", "value"))
      checkDelete(Some("1 <> 1"), allRows,
        Seq("key", "value"))
      checkDelete(Some("1 > null"), allRows,
        Seq("key", "value"))
      checkDelete(Some("true"), Nil,
        Seq("key", "value"))
      checkDelete(Some("1 = 1"), Nil,
        Seq("key", "value"))
    }
  }

  test("should not delete the rows where condition evaluates to null") {
    append(Seq(("a", null), ("b", null), ("c", "v"), ("d", "vv")).toDF("key", "value").coalesce(1))

    // "null = null" evaluates to null
    checkDelete(Some("value = null"),
      Row("a", null) :: Row("b", null) :: Row("c", "v") :: Row("d", "vv") :: Nil,
      Seq("key", "value"))

    // these expressions evaluate to null when value is null
    checkDelete(Some("value = 'v'"),
      Row("a", null) :: Row("b", null) :: Row("d", "vv") :: Nil,
      Seq("key", "value"))
    checkDelete(Some("value <> 'v'"),
      Row("a", null) :: Row("b", null) :: Nil,
      Seq("key", "value"))
  }

  test("delete rows with null values using isNull") {
    append(Seq(("a", null), ("b", null), ("c", "v"), ("d", "vv")).toDF("key", "value").coalesce(1))

    // when value is null, this expression evaluates to true
    checkDelete(Some("value is null"),
      Row("c", "v") :: Row("d", "vv") :: Nil,
      Seq("key", "value"))
  }

  test("delete rows with null values using EqualNullSafe") {
    append(Seq(("a", null), ("b", null), ("c", "v"), ("d", "vv")).toDF("key", "value").coalesce(1))

    // when value is null, this expression evaluates to true
    checkDelete(Some("value <=> null"),
      Row("c", "v") :: Row("d", "vv") :: Nil,
      Seq("key", "value"))
  }

  test("do not support subquery test") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("c", "d").createOrReplaceTempView("source")

    // basic subquery
    val e0 = intercept[AnalysisException] {
      executeDelete(target = s"lakesoul.default.`$tempPath`", "key < (SELECT max(c) FROM source)")
    }.getMessage
    assert(e0.contains("Subqueries are not supported"))

    // subquery with EXISTS
    val e1 = intercept[AnalysisException] {
      executeDelete(target = s"lakesoul.default.`$tempPath`", "EXISTS (SELECT max(c) FROM source)")
    }.getMessage
    assert(e1.contains("Subqueries are not supported"))

    // subquery with NOT EXISTS
    val e2 = intercept[AnalysisException] {
      executeDelete(target = s"lakesoul.default.`$tempPath`", "NOT EXISTS (SELECT max(c) FROM source)")
    }.getMessage
    assert(e2.contains("Subqueries are not supported"))

    // subquery with IN
    val e3 = intercept[AnalysisException] {
      executeDelete(target = s"lakesoul.default.`$tempPath`", "key IN (SELECT max(c) FROM source)")
    }.getMessage
    assert(e3.contains("Subqueries are not supported"))

    // subquery with NOT IN
    val e4 = intercept[AnalysisException] {
      executeDelete(target = s"lakesoul.default.`$tempPath`", "key NOT IN (SELECT max(c) FROM source)")
    }.getMessage
    assert(e4.contains("Subqueries are not supported"))
  }
}
