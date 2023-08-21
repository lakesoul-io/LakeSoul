// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.manual_execute_suites

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.functions.{col, last}
import org.apache.spark.sql.lakesoul.test.TestUtils
import org.apache.spark.util.Utils

class UpsertWithDuplicateDataByDifferent {
  def run(): Unit = {
    execute(true)
    execute(false)
  }

  private def execute(onlyOnePartition: Boolean): Unit = {
    val tableName = Utils.createTempDir().getCanonicalPath

    val spark = TestUtils.getSparkSession()

    import spark.implicits._

    try {
      val nameData = TestUtils.getData1(20000, onlyOnePartition)
        .toDF("hash", "name", "range")
        .persist()


      val nameDataDistinct = nameData.groupBy("range", "hash")
        .agg(
          last("name").as("n"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("name"))
        .persist()


      lazy val ageData = TestUtils.getData1(18000, onlyOnePartition)
        .toDF("hash", "age", "range")
        .persist()
      lazy val ageDataDistinct = ageData.groupBy("range", "hash")
        .agg(
          last("age").as("n"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("age"))
        .persist()

      lazy val stuData = TestUtils.getData1(23000, onlyOnePartition)
        .toDF("hash", "stu", "range")
        .persist()
      lazy val stuDataDistinct = stuData.groupBy("range", "hash")
        .agg(
          last("stu").as("n"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("stu"))
        .persist()

      lazy val gradeData = TestUtils.getData1(15000, onlyOnePartition)
        .toDF("hash", "grade", "range")
        .persist()
      lazy val gradeDataDistinct = gradeData.groupBy("range", "hash")
        .agg(
          last("grade").as("n"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("grade"))
        .persist()


      TestUtils.initTable(tableName,
        nameData.select("range", "hash", "name"),
        "range",
        "hash")

      TestUtils.checkUpsertResult(tableName,
        ageData.select("range", "hash", "age"),
        nameDataDistinct
          .join(ageDataDistinct, Seq("range", "hash"), "full")
          .select("range", "hash", "name", "age"),
        Seq("range", "hash", "name", "age"),
        None)


      TestUtils.checkUpsertResult(tableName,
        stuData.select("range", "hash", "stu"),
        nameDataDistinct
          .join(ageDataDistinct, Seq("range", "hash"), "full")
          .join(stuDataDistinct, Seq("range", "hash"), "full")
          .select("range", "hash", "name", "age", "stu"),
        Seq("range", "hash", "name", "age", "stu"),
        None)

      TestUtils.checkUpsertResult(tableName,
        gradeData.select("range", "hash", "grade"),
        nameDataDistinct
          .join(ageDataDistinct, Seq("range", "hash"), "full")
          .join(stuDataDistinct, Seq("range", "hash"), "full")
          .join(gradeDataDistinct, Seq("range", "hash"), "full")
          .select("range", "hash", "name", "age", "stu", "grade"),
        Seq("range", "hash", "name", "age", "stu", "grade"),
        None)

      LakeSoulTable.forPath(tableName).dropTable()
    } catch {
      case e: Exception =>
        LakeSoulTable.forPath(tableName).dropTable()
        throw e
    }

  }
}
