/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.Newmeta
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import scala.collection.mutable.ArrayBuffer
object NewFragmentValue {
  private val cassandraConnector = MetaCommon.cassandraConnector
  private val database = MetaCommon.DATA_BASE
  private val max_size_per_value = MetaCommon.MAX_SIZE_PER_VALUE

  def splitLargeValueIntoFragmentValues(table_id: String, value: String): String = {
    val arr = new ArrayBuffer[String]()
    var remain_value = value
    var fragment_value = ""

    while (remain_value.length > max_size_per_value) {
      fragment_value = remain_value.substring(0, max_size_per_value)
      remain_value = remain_value.substring(max_size_per_value)

      arr += addFragmentValue(table_id, fragment_value)
    }
    arr += addFragmentValue(table_id, remain_value)
    arr.mkString(",")
  }

  private def addFragmentValue(table_id: String, value: String): String = {
    val key_id = "key_" + java.util.UUID.randomUUID().toString
    val timestamp = System.currentTimeMillis()
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |insert into $database.fragment_value
           |(table_id,key_id,value,timestamp)
           |values ('$table_id','$key_id','$value',$timestamp)
           |if not exists
      """.stripMargin)
      if (!res.wasApplied()) {
        throw LakeSoulErrors.failedAddFragmentValueException(key_id)
      }
    })
    key_id
  }

  def getEntireValue(table_id: String, index: String): String = {
    index.split(",").map(getFragmentValue(table_id, _)._1).mkString("")
  }

  private def getFragmentValue(table_id: String, key_id: String): (String, Long) = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select value,timestamp from $database.fragment_value
           |where table_id='$table_id' and key_id='$key_id'
      """.stripMargin).one()
      (res.getString("value"), res.getLong("timestamp"))
    })
  }

  //get fragment value by table_id
  def getFragmentValue(table_id: String): Seq[(String, String)] = {
    cassandraConnector.withSessionDo(session => {
      val resultBuffer = new ArrayBuffer[(String, String)]()
      val res = session.execute(
        s"""
           |select key_id,value from $database.fragment_value
           |where table_id='$table_id' allow filtering
      """.stripMargin).iterator()
      while (res.hasNext) {
        val nextRow = res.next()
        resultBuffer += ((nextRow.getString("key_id"), nextRow.getString("value")))
      }
      resultBuffer
    })
  }

  def deleteFragmentValueByTableId(table_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.fragment_value
           |where table_id='$table_id'
      """.stripMargin)
    })
  }


}
