// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.manual_execute_suites

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.functions.{col, last}
import org.apache.spark.sql.lakesoul.test.TestUtils
import org.apache.spark.util.Utils

class MergeOneFileResult {
  def run(): Unit = {
    execute(true)
    execute(false)
  }

  private def execute(onlyOnePartition: Boolean): Unit = {
    val tableName = Utils.createTempDir().getCanonicalPath

    val spark = TestUtils.getSparkSession()
    import spark.implicits._
    try {
      val allData = TestUtils.getDataNew(20000, onlyOnePartition)
        .toDF("hash", "name", "age", "stu", "grade", "range")
        .persist()


      TestUtils.initTable(tableName,
        allData.select("range", "hash", "name", "age"),
        "range",
        "hash")

      val expectedData = allData.groupBy("range", "hash")
        .agg(
          last("name").as("n"),
          last("age").as("a"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("name"),
          col("a").as("age"))


      TestUtils.checkDFResult(
        LakeSoulTable.forPath(tableName).toDF
          .select("range", "hash", "name", "age"),
        expectedData)

      LakeSoulTable.forPath(tableName).dropTable()

    } catch {
      case e: Exception =>
        LakeSoulTable.forPath(tableName).dropTable()
        throw e
    }
  }
}
