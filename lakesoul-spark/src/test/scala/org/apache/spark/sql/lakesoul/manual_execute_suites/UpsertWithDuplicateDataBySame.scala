// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.manual_execute_suites

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.functions.{col, last}
import org.apache.spark.sql.lakesoul.test.TestUtils
import org.apache.spark.util.Utils

object UpsertWithDuplicateDataBySame {
  def main(args: Array[String]): Unit = {
    new UpsertWithDuplicateDataBySame().execute(true)
  }
}


class UpsertWithDuplicateDataBySame {
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

      val expectedData = allData.groupBy("range", "hash")
        .agg(
          last("name").as("n"),
          last("age").as("a"),
          last("stu").as("s"),
          last("grade").as("g"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("name"),
          col("a").as("age"),
          col("s").as("stu"),
          col("g").as("grade"))


      TestUtils.initTable(tableName,
        allData.select("range", "hash", "name", "age"),
        "range",
        "hash")

      TestUtils.checkDFResult(
        LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "name", "age"),
        expectedData.select("range", "hash", "name", "age"))


      val cols = Seq("range", "hash", "name", "age", "stu", "grade")

      TestUtils.checkUpsertResult(tableName,
        allData.select("range", "hash", "stu", "grade"),
        expectedData,
        cols,
        None)

      LakeSoulTable.forPath(tableName).dropTable()
    } catch {
      case e: Exception =>
        LakeSoulTable.forPath(tableName).dropTable()
        throw e
    }

  }


}
