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

import java.util.concurrent.TimeUnit

import MetaCommit.{unlockMaterialRelation, unlockMaterialViewName}
import UndoLog._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.utils.{CommitOptions, MetaInfo, RelationTable}

object RollBack extends Logging {
  def rollBackUpdate(meta_info: MetaInfo,
                     commit_id: String,
                     changeSchema: Boolean,
                     commitOptions: CommitOptions): Unit = {
    val table_id = meta_info.table_info.table_id
    val (last_timestamp, tag) = getCommitTimestampAndTag(
      UndoLogType.Commit.toString,
      table_id,
      commit_id)

    if (tag != -1) {
      val partition_info_arr = meta_info.partitionInfoArray
      logInfo("============commit failed, commit id=" + commit_id)
      if (changeSchema) {
        MetaLock.unlock(table_id, commit_id)
        deleteUndoLog(UndoLogType.Schema.toString, table_id, commit_id)
      }
      if (commitOptions.shortTableName.isDefined) {
        MetaVersion.deleteShortTableName(commitOptions.shortTableName.get, meta_info.table_info.table_name)
        deleteUndoLog(UndoLogType.ShortTableName.toString, table_id, commit_id)
      }

      if (commitOptions.materialInfo.isDefined) {
        val shortTableName = if (commitOptions.shortTableName.isDefined) {
          commitOptions.shortTableName.get
        } else {
          meta_info.table_info.short_table_name.get
        }
        //unlock material view
        unlockMaterialViewName(commit_id, shortTableName)
        //unlock material relation
        commitOptions.materialInfo.get.relationTables.foreach(table => {
          unlockMaterialRelation(commit_id = commit_id, table_id = table.tableId)
        })
        //delete undo log
        deleteUndoLog(
          commit_type = UndoLogType.Material.toString,
          table_id = meta_info.table_info.table_id,
          commit_id = commit_id)
      }

      for (partition_info <- partition_info_arr) {
        for (file <- partition_info.expire_files) {
          deleteUndoLog(
            commit_type = UndoLogType.ExpireFile.toString,
            table_id = table_id,
            commit_id = commit_id,
            range_id = partition_info.range_id,
            file_path = file.file_path
          )
        }
        for (file <- partition_info.add_files) {
          deleteUndoLog(
            commit_type = UndoLogType.AddFile.toString,
            table_id = table_id,
            commit_id = commit_id,
            range_id = partition_info.range_id,
            file_path = file.file_path
          )
        }

        MetaLock.unlock(partition_info.range_id, commit_id)
        deleteUndoLog(UndoLogType.Partition.toString, table_id, commit_id, partition_info.range_id)
      }

      deleteUndoLog(UndoLogType.Commit.toString, table_id, commit_id)
    }
  }

  def rollBackCommit(table_id: String, commit_id: String, tag: Int, timestamp: Long): Unit = {
    logInfo("roll back other commit~~~   ")
    if (markOtherCommitRollBack(table_id, commit_id, tag, timestamp)) {
      rollBackShortTableName(table_id, commit_id)
      rollBackMaterialView(table_id, commit_id)
      rollBackSchemaLock(table_id, commit_id)
      rollBackAddedFile(table_id, commit_id)
      rollBackExpiredFile(table_id, commit_id)
      rollBackPartitionLock(table_id, commit_id)

      deleteUndoLog(UndoLogType.Commit.toString, table_id, commit_id)
    } else {
      TimeUnit.SECONDS.sleep(10)
    }
  }

  def cleanRollBackCommit(table_id: String, commit_id: String, lock_id: String): Unit = {
    logInfo("clean roll back other commit~~~  ")

    rollBackShortTableName(table_id, commit_id)
    rollBackMaterialView(table_id, commit_id)
    rollBackSchemaLock(table_id, commit_id)
    rollBackAddedFile(table_id, commit_id)
    rollBackExpiredFile(table_id, commit_id)
    rollBackPartitionLock(table_id, commit_id)

    MetaLock.unlock(lock_id, commit_id)
  }


  def rollBackPartitionLock(table_id: String, commit_id: String): Unit = {
    val partition_undo_arr = getUndoLogInfo(UndoLogType.Partition.toString, table_id, commit_id)
    for (partition_undo <- partition_undo_arr) {
      MetaLock.unlock(partition_undo.range_id, commit_id)
      deleteUndoLog(UndoLogType.Partition.toString, table_id, commit_id, partition_undo.range_id)
    }
  }

  private def rollBackSchemaLock(table_id: String, commit_id: String): Unit = {
    MetaLock.unlock(table_id, commit_id)
    deleteUndoLog(UndoLogType.Schema.toString, table_id, commit_id)
  }

  private def rollBackAddedFile(table_id: String, commit_id: String): Unit = {
    val addFiles = getUndoLogInfo(UndoLogType.AddFile.toString, table_id, commit_id)
    for (file <- addFiles) {
      deleteUndoLog(UndoLogType.AddFile.toString, table_id, commit_id, file.range_id, file.file_path)
    }
  }

  private def rollBackExpiredFile(table_id: String, commit_id: String): Unit = {
    val expireFiles = getUndoLogInfo(UndoLogType.ExpireFile.toString, table_id, commit_id)
    for (file <- expireFiles) {
      deleteUndoLog(UndoLogType.ExpireFile.toString, table_id, commit_id, file.range_id, file.file_path)
    }
  }

  private def rollBackShortTableName(table_id: String, commit_id: String): Unit = {
    val info = getUndoLogInfo(UndoLogType.ShortTableName.toString, table_id, commit_id)
    if (info.nonEmpty) {
      MetaVersion.deleteShortTableName(info.head.short_table_name, info.head.table_name)
      deleteUndoLog(UndoLogType.ShortTableName.toString, table_id, commit_id)
    }
  }

  private def rollBackMaterialView(table_id: String, commit_id: String): Unit = {
    val info = getUndoLogInfo(UndoLogType.Material.toString, table_id, commit_id)
    if (info.nonEmpty) {
      //unlock material view
      unlockMaterialViewName(commit_id, info.head.short_table_name)

      if (info.head.is_creating_view) {
        //unlock material relation
        info.head.relation_tables.split(",").map(m => RelationTable.build(m))
          .foreach(table => {
            unlockMaterialRelation(commit_id = commit_id, table_id = table.tableId)
          })
      }

      //delete undo log
      deleteUndoLog(
        commit_type = UndoLogType.Material.toString,
        table_id = table_id,
        commit_id = commit_id)
    }
  }


}
