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

import com.dmetasoul.lakesoul.meta.{MetaUtils, MetaVersion}
import com.dmetasoul.lakesoul.tables.{LakeSoulTable}
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.scalatest.BeforeAndAfterEach

class DropTableSuite extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with LakeSoulTestUtils {
  import testImplicits._
  test("drop table") {
    withTempDir(f => {
      //val tmpPath = f.getCanonicalPath
      val tmpPath = "/lakesoultest"
      Seq((1, 2), (2, 3), (3, 4)).toDF("key", "value")
        .write
        .format("lakesoul")
        .mode("append")
        .save(tmpPath)
      val data=LakeSoulTable.forPath(tmpPath).toDF
      data.show()
      LakeSoulTable.forPath(tmpPath).dropTable()

    })
  }


  test("drop partition") {
    withTempDir(f => {
      val tmpPath = f.getCanonicalPath
      Seq((1, 2), (2, 3), (3, 4)).toDF("key", "value")
        .write
        .partitionBy("key")
        .format("lakesoul")
        .save(tmpPath)

      val tableInfo = MetaVersion.getTableInfo(tmpPath)
      val partitionInfo = MetaVersion.getAllPartitionInfo(tableInfo.table_id)

      val e1 = intercept[AnalysisException] {
        LakeSoulTable.forPath(tmpPath).dropPartition("key=1 or key=2")
      }
      assert(e1.getMessage().contains("You can only drop one partition once time"))
      val e2 = intercept[AnalysisException] {
        LakeSoulTable.forPath(tmpPath).dropPartition("key=4")
      }
      assert(e2.getMessage().contains("Partition not found by condition"))

      LakeSoulTable.forPath(tmpPath).dropPartition("key=1")
      checkAnswer(
        spark.read.format("lakesoul").load(tmpPath).select("key", "value"),
        Row(2, 3) :: Row(3, 4) :: Nil)

      Seq((1, 22)).toDF("key", "value")
        .write
        .mode("append")
        .format("lakesoul")
        .save(tmpPath)
      checkAnswer(
        spark.read.format("lakesoul").load(tmpPath).select("key", "value"),
        Row(1, 22) :: Row(2, 3) :: Row(3, 4) :: Nil)


      val rangeId = partitionInfo.find(_.range_value.equals("key=1")).get.range_value

    })
  }

}
