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

  def getStreamingInfo(tableId: String): (String, Long) = {
    ("", -1L)
  }

  def getBatchId(tableId: String, queryId: String): Long = {
    1
  }


  def updateStreamingInfo(tableId: String, queryId: String, batchId: Long, timestamp: Long): Unit = {
  }

  def deleteStreamingInfoByTableId(tableId: String): Unit = {
  }

  def deleteStreamingInfoByTimestamp(tableId: String, expireTimestamp: Long): Unit = {
//    val expireInfo = getTimeoutStreamingInfo(tableId, expireTimestamp)
//    expireInfo.foreach(info => deleteStreamingInfo(info._1, info._2))
  }

  private def getTimeoutStreamingInfo(tableId: String, expireTimestamp: Long): Seq[(String, String)] = {
    new ArrayBuffer[(String, String)]()
  }

  private def deleteStreamingInfo(tableId: String, query_id: String): Unit = {
  }

}
