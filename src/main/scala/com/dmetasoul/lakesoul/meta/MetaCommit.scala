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

import com.alibaba.fastjson.{JSON}

import java.util
import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.Snapshot
import org.apache.spark.sql.lakesoul.utils._

import scala.collection.JavaConverters

object MetaCommit extends Logging {
  //meta commit process
  def doMetaCommit(meta_info: MetaInfo,
                   changeSchema: Boolean,
                   times: Int = 0): Unit = {

    val table_info = meta_info.table_info
    val partitionInfoArray = meta_info.partitionInfoArray
    val commit_type = meta_info.commit_type
    val table_schema = meta_info.table_info.table_schema


    val info = new com.dmetasoul.lakesoul.meta.entity.MetaInfo()
    val tableInfo = new com.dmetasoul.lakesoul.meta.entity.TableInfo()

    tableInfo.setTableId(table_info.table_id)
    tableInfo.setTableName(table_info.short_table_name.toString)
    tableInfo.setTablePath(table_info.table_name.toString)
    tableInfo.setTableSchema(table_info.table_schema)
    tableInfo.setPartitions(table_info.range_column + ";" + table_info.hash_column)
    tableInfo.setProperties(JSON.parseObject(table_info.configuration.toString()))
    info.setTableInfo(tableInfo)

    val javaPartitionInfoList: util.List[entity.PartitionInfo] = new util.ArrayList[entity.PartitionInfo]()
    for (partition_info <- partitionInfoArray) {
      val partitionInfo = new entity.PartitionInfo()
      partitionInfo.setTableId(table_info.table_id)
      partitionInfo.setPartitionDesc(partition_info.range_value)
      partitionInfo.setSnapshot(JavaConverters.bufferAsJavaList(partition_info.read_files.toBuffer))
      partitionInfo.setCommitOp(commit_type.name)
      javaPartitionInfoList.add(partitionInfo)
    }
    info.setListPartition(javaPartitionInfoList)

    var result = MetaVersion.dbManager.commitData(info, changeSchema, commit_type.name)
    if (result) {
      result = addDataInfo(meta_info)
    } else {
      //todo throw error
    }
    if (result && changeSchema) {
      MetaVersion.dbManager.updateTableSchema(table_info.table_id, table_schema)
    }

  }


  //add commit type undo log, generate commit id
  def generateCommitIdToAddUndoLog(table_name: String,
                                   table_id: String,
                                   queryId: String,
                                   batchId: Long): String = {
    val commit_id = "commit_" + java.util.UUID.randomUUID().toString
    commit_id
  }

  def isCommitTimeout(timestamp: Long): Boolean = {
    val timeout = System.currentTimeMillis() - timestamp
    if (timeout > MetaUtils.COMMIT_TIMEOUT) {
      true
    } else {
      false
    }
  }

  def takePartitionsWriteLock(meta_info: MetaInfo, commit_id: String, times: Int = 0): MetaInfo = {
    meta_info
  }

  def takeSchemaLock(meta_info: MetaInfo): Unit = {
  }

  def addDataInfo(meta_info: MetaInfo): Boolean = {
    val table_id = meta_info.table_info.table_name.get
    val dataCommitInfoArray = meta_info.dataCommitInfo
    val commitType = meta_info.commit_type.name

    val metaDataCommitInfoList = new util.ArrayList[entity.DataCommitInfo]()
    for (dataCommitInfo <- dataCommitInfoArray) {
      val metaDataCommitInfo = new entity.DataCommitInfo()
      metaDataCommitInfo.setTableId(table_id)
      metaDataCommitInfo.setPartitionDesc(dataCommitInfo.range_value)
      metaDataCommitInfo.setCommitOp(commitType)
      metaDataCommitInfo.setCommitId(dataCommitInfo.commit_id)
      metaDataCommitInfo.setFileOps(JavaConverters.bufferAsJavaList(dataCommitInfo.file_ops.toBuffer))
      metaDataCommitInfoList.add(metaDataCommitInfo)
    }
    MetaVersion.dbManager.batchCommitDataCommitInfo(metaDataCommitInfoList)
  }

  //check and redo timeout commits before read
  def checkAndRedoCommit(snapshot: Snapshot): Unit = {
    //    val table_id: String = snapshot.getTableInfo.table_id
    //    val limit_timestamp = System.currentTimeMillis() - MetaUtils.COMMIT_TIMEOUT
    //
    //    //drop table command
    //    val dropTableInfo = getUndoLogInfo(
    //      UndoLogType.DropTable.toString,
    //      table_id,
    //      UndoLogType.DropTable.toString)
    //    if (dropTableInfo.nonEmpty) {
    //      DropTableCommand.dropTable(snapshot)
    //      throw LakeSoulErrors.tableNotExistsException(snapshot.getTableName)
    //    }
    //    //drop partition command
    //    val dropPartitionInfo = getUndoLogInfo(
    //      UndoLogType.DropPartition.toString,
    //      table_id,
    //      UndoLogType.DropPartition.toString)
    //    if (dropPartitionInfo.nonEmpty) {
    //      dropPartitionInfo.foreach(d => {
    //        DropPartitionCommand.dropPartition(d.table_name, d.table_id, d.range_value, d.range_id)
    //      })
    //    }
    //
    //
    //    //commit
    //    val logInfo = getTimeoutUndoLogInfo(UndoLogType.Commit.toString, table_id, limit_timestamp)
    //
    //    var flag = true
    //    logInfo.foreach(log => {
    //      if (log.tag == -1) {
    //        if (!Redo.redoCommit(log.table_name, log.table_id, log.commit_id)) {
    //          flag = false
    //        }
    //      } else {
    //        RollBack.rollBackCommit(log.table_id, log.commit_id, log.tag, log.timestamp)
    //      }
    //    })
    //
    //    if (!flag) {
    //      checkAndRedoCommit(snapshot)
    //    }
  }


  def updatePartitionInfoAndGetNewMetaInfo(meta_info: MetaInfo): MetaInfo = {
    meta_info
  }


}
