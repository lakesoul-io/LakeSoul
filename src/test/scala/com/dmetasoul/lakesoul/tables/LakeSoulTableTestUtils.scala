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
//package com.dmetasoul.lakesoul.tables
//
//import com.dmetasoul.lakesoul.meta.MetaUtils
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.lakesoul.SnapshotManagement
//
//object LakeSoulTableTestUtils {
//  /** A utility method to access the private constructor of [[LakeSoulTable]] in tests. */
//  def createTable(df: DataFrame, snapshotManagement: SnapshotManagement): LakeSoulTable =
//    new LakeSoulTable(df, snapshotManagement)
//
//
//  lazy val cassandraConnectot = MetaUtils.cassandraConnector
//  lazy val dataBase = MetaUtils.DATA_BASE
//
//  def getNumByTableId(metaTable: String, tableId: String): Long = {
//    cassandraConnectot.withSessionDo(session => {
//      session.execute(
//        s"""
//           |select count(1) as num from $dataBase.$metaTable
//           |where table_id='$tableId' allow filtering
//        """.stripMargin)
//        .one()
//        .getLong("num")
//    })
//  }
//
//
//  def getNumByTableIdAndRangeId(metaTable: String, tableId: String, rangeId: String): Long = {
//    cassandraConnectot.withSessionDo(session => {
//      session.execute(
//        s"""
//           |select count(1) as num from $dataBase.$metaTable
//           |where table_id='$tableId' and range_id='$rangeId' allow filtering
//        """.stripMargin)
//        .one()
//        .getLong("num")
//    })
//  }
//
//
//}
