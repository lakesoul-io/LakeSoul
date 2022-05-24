///*
// * Copyright [2022] [DMetaSoul Team]
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.spark.sql.lakesoul.commands
//
//import com.dmetasoul.lakesoul.meta.{MetaUtils, MetaVersion}
//import com.dmetasoul.lakesoul.tables.{LakeSoulTable, LakeSoulTableTestUtils}
//import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
//import org.apache.spark.sql.test.SharedSparkSession
//import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
//import org.scalatest.BeforeAndAfterEach
//
//class DropTableSuite extends QueryTest
//  with SharedSparkSession with BeforeAndAfterEach
//  with LakeSoulTestUtils {
//
//  import testImplicits._
//
//  lazy val cassandraConnectot = MetaUtils.cassandraConnector
//  lazy val dataBase = MetaUtils.DATA_BASE
//
//  def tableNotExists(tablePath: String, tableId: String): Boolean = {
//    cassandraConnectot.withSessionDo(session => {
//      val res = session.execute(
//        s"""
//           |select table_id from $dataBase.table_info
//           |where table_name='$tablePath'
//        """.stripMargin).one()
//      var flag = true
//      try {
//        if (res.getString("table_id").equals(tableId)) {
//          flag = false
//        }
//      } catch {
//        case e: Exception =>
//      }
//      flag
//    })
//  }
//
//  def metaNotExists(metaTable: String, tableId: String): Boolean = {
//    if (LakeSoulTableTestUtils.getNumByTableId(metaTable, tableId) != 0) {
//      false
//    } else {
//      true
//    }
//  }
//
//  def partitionNotExists(tableId: String, rangeValue: String, rangeId: String): Boolean = {
//    cassandraConnectot.withSessionDo(session => {
//      val res = session.execute(
//        s"""
//           |select range_id from $dataBase.partition_info
//           |where table_id='$tableId' and range_value='$rangeValue' allow filtering
//        """.stripMargin).one()
//      if (res.getString("range_id").equals(rangeId)) {
//        false
//      } else {
//        true
//      }
//    })
//  }
//
//  def dataNotExists(tableId: String, rangeId: String): Boolean = {
//    if (LakeSoulTableTestUtils.getNumByTableIdAndRangeId("data_info", tableId, rangeId) != 0) {
//      false
//    } else {
//      true
//    }
//  }
//
//
//  test("drop table") {
//    withTempDir(f => {
//      val tmpPath = f.getCanonicalPath
//      Seq((1, 2), (2, 3), (3, 4)).toDF("key", "value")
//        .write
//        .format("lakesoul")
//        .save(tmpPath)
//
//      val tableId = MetaVersion.getTableInfo(MetaUtils.modifyTableString(tmpPath)).table_id
//      LakeSoulTable.forPath(tmpPath).dropTable()
//
//      assert(tableNotExists(tmpPath, tableId))
//      assert(metaNotExists("partition_info", tableId))
//      assert(metaNotExists("data_info", tableId))
//      assert(metaNotExists("fragment_value", tableId))
//    })
//  }
//
//
//  test("drop partition") {
//    withTempDir(f => {
//      val tmpPath = f.getCanonicalPath
//      Seq((1, 2), (2, 3), (3, 4)).toDF("key", "value")
//        .write
//        .partitionBy("key")
//        .format("lakesoul")
//        .save(tmpPath)
//
//      val tableInfo = MetaVersion.getTableInfo(MetaUtils.modifyTableString(tmpPath))
//      val partitionInfo = MetaVersion.getAllPartitionInfo(tableInfo.table_id)
//
//      val e1 = intercept[AnalysisException] {
//        LakeSoulTable.forPath(tmpPath).dropPartition("key=1 or key=2")
//      }
//      assert(e1.getMessage().contains("You can only drop one partition once time"))
//      val e2 = intercept[AnalysisException] {
//        LakeSoulTable.forPath(tmpPath).dropPartition("key=4")
//      }
//      assert(e2.getMessage().contains("Partition not found by condition"))
//
//      LakeSoulTable.forPath(tmpPath).dropPartition("key=1")
//      checkAnswer(
//        spark.read.format("lakesoul").load(tmpPath).select("key", "value"),
//        Row(2, 3) :: Row(3, 4) :: Nil)
//
//      Seq((1, 22)).toDF("key", "value")
//        .write
//        .mode("append")
//        .format("lakesoul")
//        .save(tmpPath)
//      checkAnswer(
//        spark.read.format("lakesoul").load(tmpPath).select("key", "value"),
//        Row(1, 22) :: Row(2, 3) :: Row(3, 4) :: Nil)
//
//
//      val rangeId = partitionInfo.find(_.range_value.equals("key=1")).get.range_id
//      assert(partitionNotExists(tableInfo.table_id, "key=1", rangeId))
//      assert(dataNotExists(tableInfo.table_id, rangeId))
//    })
//  }
//
//}
