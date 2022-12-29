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

object StreamingRecord {

  val dbManager = new DBManager()

  def getStreamingInfo(tableId: String): (String, Long) = {
    try {
      val streamingInfo = dbManager.getStreamingInfoByTableId(tableId)
      (streamingInfo.getQueryId, streamingInfo.getBatchId)
    } catch {
      case e: Exception => ("", -1L)
    }
  }

  def getBatchId(tableId: String, queryId: String): Long = {
    try {
      dbManager.getStreamingInfoByTableIdAndQueryId(tableId, queryId).getBatchId
    } catch {
      case e: Exception => -1L
    }
  }

  def updateStreamingInfo(tableId: String, queryId: String, batchId: Long, timestamp: Long): Unit = {
    dbManager.updateStreamingInfo(tableId, queryId, batchId, timestamp)
  }

  def deleteStreamingInfoByTableId(tableId: String): Unit = {
    dbManager.deleteStreamingInfoByTableId(tableId)
  }

  def deleteStreamingInfoByTimestamp(tableId: String, expireTimestamp: Long): Unit = {
    val expireInfo = getTimeoutStreamingInfo(tableId, expireTimestamp)
    expireInfo.foreach(info => deleteStreamingInfo(info._1, info._2))
  }

  private def getTimeoutStreamingInfo(tableId: String, expireTimestamp: Long): Seq[(String, String)] = {
    val streamingRecords = new ArrayBuffer[(String, String)]()
    dbManager.getTimeoutStreamingInfo(tableId, expireTimestamp).forEach(streamingInfo => streamingRecords.append((streamingInfo.getTableId, streamingInfo.getQueryId)))
    streamingRecords
  }

  private def deleteStreamingInfo(tableId: String, query_id: String): Unit = {
    dbManager.deleteStreamingInfoByTableIdAndQueryId(tableId, query_id)
  }

}
