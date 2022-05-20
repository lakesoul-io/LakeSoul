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

package com.dmetasoul.lakesoul.meta

import scala.collection.mutable.ArrayBuffer
//todo
object StreamingRecord {
//  private val cassandraConnector = MetaUtils.cassandraConnector
//  private val database = MetaUtils.DATA_BASE

  def getStreamingInfo(tableId: String): (String, Long) = {
//    cassandraConnector.withSessionDo(session => {
//      try {
//        val res = session.execute(
//          s"""
//             |select query_id,batch_id from $database.streaming_info
//             |where table_id='$tableId' allow filtering
//      """.stripMargin).one()
//        (res.getString("query_id"), res.getLong("batch_id"))
//      } catch {
//        case e: Exception => ("", -1L)
//      }
//    })
    ("", -1L)
  }

  def getBatchId(tableId: String, queryId: String): Long = {
//    cassandraConnector.withSessionDo(session => {
//      try {
//        val res = session.execute(
//          s"""
//             |select batch_id from $database.streaming_info
//             |where table_id='$tableId' and query_id='$queryId'
//      """.stripMargin)
//        res.one().getLong("batch_id")
//      } catch {
//        case e: Exception => -1L
//      }
//    })
    1
  }


  def updateStreamingInfo(tableId: String, queryId: String, batchId: Long, timestamp: Long): Unit = {
//    cassandraConnector.withSessionDo(session => {
//      session.execute(
//        s"""
//           |insert into $database.streaming_info(table_id,query_id,batch_id,timestamp)
//           |values('$tableId','$queryId',$batchId,$timestamp)
//      """.stripMargin)
//    })
  }

  def deleteStreamingInfoByTableId(tableId: String): Unit = {
//    cassandraConnector.withSessionDo(session => {
//      session.execute(
//        s"""
//           |delete from $database.streaming_info
//           |where table_id='$tableId'
//      """.stripMargin)
//    })
  }

  def deleteStreamingInfoByTimestamp(tableId: String, expireTimestamp: Long): Unit = {
//    val expireInfo = getTimeoutStreamingInfo(tableId, expireTimestamp)
//    expireInfo.foreach(info => deleteStreamingInfo(info._1, info._2))
  }


  private def getTimeoutStreamingInfo(tableId: String, expireTimestamp: Long): Seq[(String, String)] = {
//    cassandraConnector.withSessionDo(session => {
//      val res = session.execute(
//        s"""
//           |select table_id,query_id,batch_id from $database.streaming_info
//           |where table_id='$tableId' and timestamp<$expireTimestamp allow filtering
//      """.stripMargin)
//      val itr = res.iterator()
//      val arrBuf = new ArrayBuffer[(String, String)]()
//      while (itr.hasNext) {
//        val re = itr.next()
//        val info = (re.getString("table_id"), re.getString("query_id"))
//        arrBuf += info
//      }
//      arrBuf
//
//    })
    new ArrayBuffer[(String, String)]()
  }


  private def deleteStreamingInfo(tableId: String, query_id: String): Unit = {
//    cassandraConnector.withSessionDo(session => {
//      session.execute(
//        s"""
//           |delete from $database.streaming_info
//           |where table_id='$tableId' and query_id='$query_id'
//      """.stripMargin)
//    })
  }


}
