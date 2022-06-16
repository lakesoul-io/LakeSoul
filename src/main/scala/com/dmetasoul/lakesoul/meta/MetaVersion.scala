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

import com.alibaba.fastjson.JSONObject
import com.google.common.base.Splitter
import org.apache.spark.sql.lakesoul.utils.{PartitionInfo, TableInfo}

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object MetaVersion {

  val dbManager = new DBManager()

  def isTableExists(table_name: String): Boolean = {
    dbManager.isTableExists(table_name)
  }

  def isTableIdExists(table_name: String, table_id: String): Boolean = {
    dbManager.isTableIdExists(table_name, table_id)
  }

  //check whether short_table_name exists, and return table path if exists
  def isShortTableNameExists(short_table_name: String): (Boolean, String) = {
    val path = dbManager.getTablePathFromShortTableName(short_table_name)
    if (path == null) (false, null) else (true, path)
  }

  //get table path, if not exists, return "not found"
  def getTablePathFromShortTableName(short_table_name: String): String = {
    dbManager.getTablePathFromShortTableName(short_table_name)
  }

  //todo
  def isPartitionExists(table_id: String, range_value: String, range_id: String, commit_id: String): Boolean = {
//    cassandraConnector.withSessionDo(session => {
//      val res = session.execute(
//        s"""
//           |select range_id from $database.partition_info
//           |where table_id='$table_id' and range_value='$range_value'
//      """.stripMargin).iterator()
//      if (res.hasNext) {
//        val exist_range_id = res.next().getString("range_id")
//        if (exist_range_id.equals(range_id)) {
//          true
//        } else {
//          throw MetaRerunErrors.partitionChangedException(range_value, commit_id)
//        }
//      } else {
//        false
//      }
//    })
    false
  }

  //todo 少了configuration参数值
  def createNewTable(table_path: String,
                     short_table_name: String,
                     table_id: String,
                     table_schema: String,
                     range_column: String,
                     hash_column: String,
                     configuration: Map[String, String],
                     bucket_num: Int): Unit = {

    val partitions = range_column + ";" + hash_column
    val json = new JSONObject()
    configuration.foreach(x => json.put(x._1,x._2))
    json.put("hashBucketNum", String.valueOf(bucket_num))
    dbManager.createNewTable(table_id, short_table_name, table_path, table_schema, json, partitions)
  }


  //todo
  def addPartition(table_id: String, table_name: String, range_id: String, range_value: String): Unit = {
//    assert(
//      isTableIdExists(table_name, table_id),
//      s"Can't find table `$table_name` with id=`$table_id`, it may has been dropped.")
//
//    cassandraConnector.withSessionDo(session => {
//      val res = session.execute(
//        s"""
//           |insert into $database.partition_info
//           |(table_id,range_value,range_id,table_name,read_version,pre_write_version,
//           |last_update_timestamp,delta_file_num,be_compacted)
//           |values ('$table_id','$range_value','$range_id','$table_name',0,0,
//           |0,0,true)
//           |if not exists
//    """.stripMargin)
//      if (!res.wasApplied()) {
//        throw LakeSoulErrors.failedAddPartitionVersionException(table_name, range_value, range_id)
//      }
//    })
  }

  def listTables(): util.List[String] = {
    dbManager.listTables()
  }

  //todo
  def getTableInfo(table_path: String): TableInfo = {
    val info = dbManager.getTableInfo(table_path)
    if (info == null) {
      return null
    }
    val short_table_name = info.getTableName
    val partitions = info.getPartitions
    val properties = info.getProperties.toString()

    import scala.util.parsing.json.JSON
    val configuration = JSON.parseFull(properties)
    val configurationMap = configuration match {
      case Some(map: collection.immutable.Map[String, String]) => map
    }

    // table may have no partition at all or only have range or hash partition
    val partitionCols = Splitter.on(';').split(partitions).asScala.toArray
    val (range_column, hash_column) = partitionCols match {
      case Array(range, hash) => (range, hash)
      case _ => ("", "")
    }
    val bucket_num = configurationMap.get("hashBucketNum") match {
      case Some(value) => value.toInt
      case _ => -1
    }
    TableInfo(
      Some(table_path),
      info.getTableId,
      info.getTableSchema,
      range_column,
      hash_column,
      bucket_num,
      configurationMap,//todo
      if (short_table_name.equals("")) None else Some(short_table_name)
    )
  }

  //todo
  def getSinglePartitionInfo(table_id: String, range_value: String, range_id: String): PartitionInfo = {
    val info = dbManager.getSinglePartitionInfo(table_id, range_value)
    PartitionInfo(
      table_id = info.getTableId,
      range_value = range_value,
      version = info.getVersion,
      read_files = info.getSnapshot.asScala.toArray,
      expression = info.getExpression
    )
  }
  def getSinglePartitionInfoForVersion(table_id: String, range_value: String, version: Int):  Array[PartitionInfo] = {
    val partitionVersionBuffer = new ArrayBuffer[PartitionInfo]()
    val info = dbManager.getSinglePartitionInfo(table_id, range_value,version)
    partitionVersionBuffer += PartitionInfo(
      table_id = info.getTableId,
      range_value = range_value,
      version = info.getVersion,
      read_files = info.getSnapshot.asScala.toArray,
      expression = info.getExpression
    )
    partitionVersionBuffer.toArray

  }
  def getOnePartitionVersions(table_id: String, range_value: String):  Array[PartitionInfo] = {
    val partitionVersionBuffer = new ArrayBuffer[PartitionInfo]()
    val res_itr = dbManager.getOnePartitionVersions(table_id, range_value).iterator()
    while (res_itr.hasNext) {
      val res = res_itr.next()
      partitionVersionBuffer += PartitionInfo(
        table_id = res.getTableId,
        range_value = res.getPartitionDesc,
        version = res.getVersion,
      )
    }
    partitionVersionBuffer.toArray

  }
  //todo
  def getPartitionId(table_id: String, range_value: String): (Boolean, String) = {
    (false, "")
  }

  def getAllPartitionInfo(table_id: String): Array[PartitionInfo] = {
    val partitionVersionBuffer = new ArrayBuffer[PartitionInfo]()
    val res_itr = dbManager.getAllPartitionInfo(table_id).iterator()
    while (res_itr.hasNext) {
      val res = res_itr.next()
      partitionVersionBuffer += PartitionInfo(
        table_id = res.getTableId,
        range_value = res.getPartitionDesc,
        version = res.getVersion,
        read_files = res.getSnapshot.asScala.toArray,
        expression = res.getExpression
      )
    }
    partitionVersionBuffer.toArray
  }

  def rollbackPartitionInfoByVersion(table_id: String, range_value: String, toVersion: Int): Unit = {
    if(dbManager.rollbackPartitionByVersion(table_id, range_value, toVersion)){
      println(range_value+" toVersion "+toVersion+" success")
    }else{
      println(range_value+" toVersion "+toVersion+" failed. Please check partition value or versionNum is right")
    }

  }

  def updateTableSchema(table_name: String,
                        table_id: String,
                        table_schema: String,
                        config: Map[String, String],
                        new_read_version: Int): Unit = {
    dbManager.updateTableSchema(table_id, table_schema)
  }


  def deleteTableInfo(table_name: String, table_id: String): Unit = {
    dbManager.deleteTableInfo(table_name, table_id)
  }

  def deletePartitionInfoByTableId(table_id: String): Unit = {
    dbManager.logicDeletePartitionInfoByTableId(table_id)
  }

  def deletePartitionInfoByRangeId(table_id: String, range_value: String, range_id: String): Unit = {
    dbManager.logicDeletePartitionInfoByRangeId(table_id, range_value)
  }

  def dropPartitionInfoByTableId(table_id: String): Unit = {
    dbManager.deletePartitionInfoByTableId(table_id)
  }

  def dropPartitionInfoByRangeId(table_id: String, range_value: String): Unit = {
    dbManager.deletePartitionInfoByTableAndPartition(table_id, range_value)
  }

  def deleteShortTableName(short_table_name: String, table_name: String): Unit = {
    dbManager.deleteShortTableName(short_table_name, table_name)
  }

  def addShortTableName(short_table_name: String,
                        table_name: String): Unit = {
    dbManager.addShortTableName(short_table_name, table_name)
  }

  def updateTableShortName(table_name: String,
                           table_id: String,
                           short_table_name: String): Unit = {
    dbManager.updateTableShortName(table_name, table_id, short_table_name)
  }

}

