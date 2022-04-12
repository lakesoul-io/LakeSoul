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

import java.util.concurrent.TimeUnit

import com.dmetasoul.lakesoul.Newmeta.NewMetaCommit.{unlockMaterialRelation, unlockMaterialViewName}
import com.dmetasoul.lakesoul.Newmeta.NewRedo.logInfo
import com.dmetasoul.lakesoul.Newmeta.NewUndolog.{deleteUndoLog, getUndoLogInfo, updateRedoTimestamp}
import com.dmetasoul.lakesoul.Newmeta.NewMetaUtil
import com.dmetasoul.lakesoul.Newmeta.NewMetaLock
import com.dmetasoul.lakesoul.meta.UndoLogType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.utils.RelationTable

object NewRedo extends Logging {
  def redoCommit(table_name: String, table_id: String, commit_id: String): Boolean = {
    logInfo("redo other commit~~~   ")

    if (updateRedoTimestamp(table_id, commit_id)) {
      redoSchemaLock(table_name, table_id, commit_id)
      redoAddedFile(table_id, commit_id)
      redoExpiredFile(table_id, commit_id)
      redoPartitionLock(table_id, commit_id)
      redoStreamingRecord(table_id, commit_id)
      redoShortTableName(table_id, commit_id)
      redoMaterialView(table_id, commit_id)

      deleteUndoLog(UndoLogType.Commit.toString, table_id, commit_id)
      SnapshotManagement(table_name).updateSnapshot()
      true
    } else {
      TimeUnit.SECONDS.sleep(10)
      false
    }
  }

  private def redoPartitionLock(table_id: String, commit_id: String): Unit = {
    val partition_undo_arr = getUndoLogInfo(UndoLogType.Partition.toString, table_id, commit_id)
    for (partition_undo <- partition_undo_arr) {
      NewMetaUtil.updatePartitionInfo(partition_undo)
      NewMetaLock.unlock(partition_undo.range_id, partition_undo.commit_id)
      deleteUndoLog(UndoLogType.Partition.toString, table_id, commit_id, partition_undo.range_id)
    }
  }

  private def redoSchemaLock(table_name: String, table_id: String, commit_id: String): Unit = {
    val schema_undo_arr = getUndoLogInfo(UndoLogType.Schema.toString, table_id, commit_id)

    for (schema_undo <- schema_undo_arr) {
      NewMetaUtil.updateTableSchema(
        table_name,
        table_id,
        schema_undo.table_schema,
        schema_undo.setting,
        schema_undo.write_version.toInt)

      NewMetaLock.unlock(table_id, commit_id)
      deleteUndoLog(UndoLogType.Schema.toString, table_id, commit_id)
    }
  }

  private def redoAddedFile(table_id: String, commit_id: String): Unit = {
    val add_file_undo_arr = getUndoLogInfo(UndoLogType.AddFile.toString, table_id, commit_id)
    for (add_file_undo <- add_file_undo_arr) {
      NewDataOperation.redoAddedNewDataFile(add_file_undo)
      deleteUndoLog(
        UndoLogType.AddFile.toString,
        table_id,
        commit_id,
        add_file_undo.range_id,
        add_file_undo.file_path)
    }
  }

  private def redoExpiredFile(table_id: String, commit_id: String): Unit = {
    val expire_file_undo_arr = getUndoLogInfo(UndoLogType.ExpireFile.toString, table_id, commit_id)
    for (expire_file_undo <- expire_file_undo_arr) {
      NewDataOperation.redoExpireDataFile(expire_file_undo)
      deleteUndoLog(
        UndoLogType.ExpireFile.toString,
        table_id,
        commit_id,
        expire_file_undo.range_id,
        expire_file_undo.file_path)
    }
  }

  private def redoStreamingRecord(table_id: String, commit_id: String): Unit = {
    val streaming_undo_arr = getUndoLogInfo(UndoLogType.Commit.toString, table_id, commit_id)

    for (streaming_undo <- streaming_undo_arr) {
      if (streaming_undo.query_id != null
        && streaming_undo.query_id.nonEmpty
        && !streaming_undo.query_id.equals(MetaCommon.UNDO_LOG_DEFAULT_VALUE)
        && streaming_undo.batch_id >= 0) {

        NewStreamingRecord.updateStreamingInfo(
          table_id,
          streaming_undo.query_id,
          streaming_undo.batch_id,
          streaming_undo.timestamp)
      }
    }
  }

  private def redoShortTableName(table_id: String, commit_id: String): Unit = {
    val info = getUndoLogInfo(UndoLogType.ShortTableName.toString, table_id, commit_id)
    if (info.nonEmpty) {
      //add short name to table_info
      NewMetaUtil.updateTableShortName(
        info.head.table_name,
        table_id,
        info.head.short_table_name)
      deleteUndoLog(UndoLogType.ShortTableName.toString, table_id, commit_id)
    }
  }

  private def redoMaterialView(table_id: String, commit_id: String): Unit = {
    val undoInfo = getUndoLogInfo(UndoLogType.Material.toString, table_id, commit_id)
    if (undoInfo.nonEmpty) {
      val info = undoInfo.head
      if (info.is_creating_view) {
        //add material view
        NewMaterialView.addMaterialView(
          info.short_table_name,
          info.table_name,
          table_id,
          info.relation_tables,
          info.sql_text,
          info.auto_update,
          info.view_info)
        //unlock material view
        unlockMaterialViewName(commit_id, info.short_table_name)

        //update material relation
        info.relation_tables.split(",").map(m => RelationTable.build(m)).foreach(table => {
          //update
          NewMetaCommit.updateMaterialRelation(
            table_id = table.tableId,
            table_name = table.tableName,
            view_name = info.short_table_name)
          //unlock material relation
          unlockMaterialRelation(commit_id = commit_id, table_id = table.tableId)
        })
      } else {
        //update material view
        NewMaterialView.updateMaterialView(info.short_table_name, info.relation_tables, info.auto_update)
        //unlock material view
        unlockMaterialViewName(commit_id, info.short_table_name)
      }
      //delete undo log
      deleteUndoLog(
        commit_type = UndoLogType.Material.toString,
        table_id = table_id,
        commit_id = commit_id)
    }
  }


}

