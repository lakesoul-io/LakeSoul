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

class UpsertWithDuplicateDataAndFields {
  def run(): Unit = {
    execute(true)
    execute(false)
  }

  private def execute(onlyOnePartition: Boolean): Unit = {
    val tableName = Utils.createTempDir().getCanonicalPath

    val spark = TestUtils.getSparkSession()

    import spark.implicits._

    try {

      val data1 = TestUtils.getData1(20000, onlyOnePartition)
        .toDF("hash", "name", "range")
        .persist()
      lazy val data2 = TestUtils.getData3(18000, onlyOnePartition)
        .toDF("hash", "name", "age", "grade", "range")
        .persist()
      lazy val data3 = TestUtils.getData1(15000, onlyOnePartition)
        .toDF("hash", "grade", "range")
        .persist()
      lazy val data4 = TestUtils.getData2(23000, onlyOnePartition)
        .toDF("hash", "age", "grade", "range")
        .persist()

      val distinctData1 = data1.groupBy("range", "hash")
        .agg(
          last("name").as("n"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("name"))
        .persist()

      lazy val distinctData2 = data2.groupBy("range", "hash")
        .agg(
          last("name").as("n"),
          last("age").as("a"),
          last("grade").as("g"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("name"),
          col("a").as("age"),
          col("g").as("grade"))
        .persist()

      lazy val distinctData3 = data3.groupBy("range", "hash")
        .agg(
          last("grade").as("g"))
        .select(
          col("range"),
          col("hash"),
          col("g").as("grade"))
        .persist()

      lazy val distinctData4 = data4.groupBy("range", "hash")
        .agg(
          last("age").as("a"),
          last("grade").as("g"))
        .select(
          col("range"),
          col("hash"),
          col("a").as("age"),
          col("g").as("grade"))
        .persist()


      TestUtils.initTable(tableName,
        data1.select("range", "hash", "name"),
        "range",
        "hash")


      TestUtils.checkUpsertResult(
        tableName,
        data2.select("range", "hash", "name", "age", "grade"),
        distinctData1
          .join(distinctData2, Seq("range", "hash"), "full")
          .select(
            col("range"),
            col("hash"),
            coalesce(distinctData2("name"), distinctData1("name")).as("name"),
            distinctData2("age").as("age"),
            distinctData2("grade").as("grade")),
        Seq("range", "hash", "name", "age", "grade"),
        None)

      TestUtils.checkUpsertResult(tableName,
        data3.select("range", "hash", "grade"),
        distinctData1
          .join(distinctData2, Seq("range", "hash"), "full")
          .join(distinctData3, Seq("range", "hash"), "full")
          .select(
            col("range"),
            col("hash"),
            coalesce(distinctData2("name"), distinctData1("name")).as("name"),
            distinctData2("age").as("age"),
            coalesce(distinctData3("grade"), distinctData2("grade")).as("grade")),
        Seq("range", "hash", "name", "age", "grade"),
        None)

      TestUtils.checkUpsertResult(tableName,
        data4.select("range", "hash", "age", "grade"),
        distinctData1
          .join(distinctData2, Seq("range", "hash"), "full")
          .join(distinctData3, Seq("range", "hash"), "full")
          .join(distinctData4, Seq("range", "hash"), "full")
          .select(
            col("range"),
            col("hash"),
            coalesce(distinctData2("name"), distinctData1("name")).as("name"),
            coalesce(distinctData4("age"), distinctData2("age")).as("age"),
            coalesce(distinctData4("grade"), distinctData3("grade"), distinctData2("grade")).as("grade")),
        Seq("range", "hash", "name", "age", "grade"),
        None
      )

      LakeSoulTable.forPath(tableName).dropTable()
    } catch {
      case e: Exception =>
        LakeSoulTable.forPath(tableName).dropTable()
        throw e
    }

  }
}
