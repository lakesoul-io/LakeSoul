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
import org.apache.spark.sql.functions.{coalesce, col, last}
import org.apache.spark.sql.lakesoul.test.TestUtils
import org.apache.spark.util.Utils

class UpsertAfterCompaction {
  def run(): Unit = {
    execute(true)
    execute(false)
  }

  private def execute(onlyOnePartition: Boolean): Unit = {
    val tableName = Utils.createTempDir().getCanonicalPath

    val spark = TestUtils.getSparkSession()

    import spark.implicits._


    try {
      val data1 = TestUtils.getData1(13000, onlyOnePartition)
        .toDF("hash", "value", "range")
        .persist()

      val data2 = TestUtils.getData1(12000, onlyOnePartition)
        .toDF("hash", "name", "range")
        .persist()

      val data3 = TestUtils.getData1(15000, onlyOnePartition)
        .toDF("hash", "value", "range")
        .persist()
      val data4 = TestUtils.getData2(10000, onlyOnePartition)
        .toDF("hash", "value", "name", "range")
        .persist()

      val distinctData1 = data1.groupBy("range", "hash")
        .agg(
          last("value").as("v"))
        .select(
          col("range"),
          col("hash"),
          col("v").as("value"))

      val distinctData2 = data2.groupBy("range", "hash")
        .agg(
          last("name").as("n"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("name"))

      val distinctData3 = data3.groupBy("range", "hash")
        .agg(
          last("value").as("v"))
        .select(
          col("range"),
          col("hash"),
          col("v").as("value"))

      val distinctData4 = data4.groupBy("range", "hash")
        .agg(
          last("value").as("v"),
          last("name").as("n"))
        .select(
          col("range"),
          col("hash"),
          col("v").as("value"),
          col("n").as("name"))

      val expectedData = distinctData1
        .join(distinctData2, Seq("range", "hash"), "full")
        .join(distinctData3, Seq("range", "hash"), "full")
        .join(distinctData4, Seq("range", "hash"), "full")
        .select(col("range"),
          col("hash"),
          coalesce(distinctData4("value"), distinctData3("value"), distinctData1("value")).as("value"),
          coalesce(distinctData4("name"), distinctData2("name")).as("name"))
        .persist()


      TestUtils.initTable(tableName,
        data1,
        "range",
        "hash")

      val table = LakeSoulTable.forPath(tableName)

      table.upsert(data2)
      table.compaction()
      table.upsert(data3)
      table.upsert(data4)
      table.compaction()

      TestUtils.checkDFResult(
        LakeSoulTable.forPath(tableName).toDF
          .select("range", "hash", "value", "name"),
        expectedData)


      TestUtils.initTable(tableName,
        data1,
        "range",
        "hash")
      table.upsert(data2)
      table.upsert(data3)
      table.compaction()
      table.upsert(data4)


      TestUtils.checkDFResult(
        LakeSoulTable.forPath(tableName).toDF
          .select("range", "hash", "value", "name"),
        expectedData)

      LakeSoulTable.forPath(tableName).dropTable()

    } catch {
      case e: Exception =>
        LakeSoulTable.forPath(tableName).dropTable()
        throw e
    }
  }


}

object UpsertAfterCompaction {
  def main(args: Array[String]): Unit = {
    new UpsertAfterCompaction().execute(true)
  }
}
