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
