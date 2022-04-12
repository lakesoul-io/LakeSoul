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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.exception.{MetaRetryErrors, LakeSoulErrors}
import org.apache.spark.sql.lakesoul.utils.undoLogInfo

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import com.dmetasoul.lakesoul.meta.UndoLogType;
object NewUndolog  extends Logging {
  private val cassandraConnector = MetaCommon.cassandraConnector
  private val database = MetaCommon.DATA_BASE
  private val default_value = MetaCommon.UNDO_LOG_DEFAULT_VALUE
  private val default_setting = MetaCommon.UNDO_LOG_DEFAULT_SETTING
  private val max_size_per_value = MetaCommon.MAX_SIZE_PER_VALUE

  def addCommitUndoLog(table_name: String,
                       table_id: String,
                       commit_id: String,
                       timestamp: Long,
                       queryId: String, //for streaming commit
                       batchId: Long): Boolean = {
    insertUndoLogWithLWT(
      commit_type = UndoLogType.Commit.toString,
      table_id = table_id,
      commit_id = commit_id,
      table_name = table_name,
      tag = 0,
      timestamp = timestamp,
      query_id = queryId,
      batch_id = batchId)
  }

  def addPartitionUndoLog(table_name: String,
                          range_value: String,
                          table_id: String,
                          range_id: String,
                          commit_id: String,
                          delta_file_num: Int,
                          be_compacted: Boolean): Unit = {
    insertUndoLog(
      commit_type = UndoLogType.Partition.toString,
      table_id = table_id,
      commit_id = commit_id,
      range_id = range_id,
      table_name = table_name,
      range_value = range_value,
      delta_file_num = delta_file_num,
      be_compacted = be_compacted)
  }

  def addSchemaUndoLog(table_name: String,
                       table_id: String,
                       commit_id: String,
                       write_version: Long,
                       table_schema: String,
                       setting: String): Unit = {
    val table_schema_index = if (table_schema.length > max_size_per_value) {
      NewFragmentValue.splitLargeValueIntoFragmentValues(table_id, table_schema)
    } else {
      table_schema
    }

    insertUndoLog(
      commit_type = UndoLogType.Schema.toString,
      table_id = table_id,
      commit_id = commit_id,
      table_name = table_name,
      write_version = write_version,
      table_schema = table_schema_index,
      setting = setting)
  }

  def addShortTableNameUndoLog(table_name: String,
                               table_id: String,
                               commit_id: String,
                               short_table_name: String): Unit = {
    insertUndoLog(
      commit_type = UndoLogType.ShortTableName.toString,
      table_id = table_id,
      commit_id = commit_id,
      table_name = table_name,
      short_table_name = short_table_name)
  }

  def addMaterialUndoLog(table_name: String,
                         table_id: String,
                         commit_id: String,
                         short_table_name: String,
                         sql_text: String,
                         relation_tables: String,
                         auto_update: Boolean,
                         is_creating_view: Boolean,
                         view_info: String): Unit = {
    //queryInfo may very big and exceed 64kb limit, so try to split it to some fragment if value is too long
    val view_info_index = if (view_info.length > max_size_per_value) {
      NewFragmentValue.splitLargeValueIntoFragmentValues(table_id, view_info)
    } else {
      view_info
    }

    insertUndoLog(
      commit_type = UndoLogType.Material.toString,
      table_id = table_id,
      commit_id = commit_id,
      table_name = table_name,
      short_table_name = short_table_name,
      sql_text = sql_text,
      relation_tables = relation_tables,
      auto_update = auto_update,
      is_creating_view = is_creating_view,
      view_info = view_info_index)
  }

  def addFileUndoLog(table_name: String,
                     table_id: String,
                     range_id: String,
                     commit_id: String,
                     file_path: String,
                     write_version: Long,
                     size: Long,
                     modification_time: Long,
                     file_exist_cols: String,
                     is_base_file: Boolean): Unit = {
    insertUndoLog(
      commit_type = UndoLogType.AddFile.toString,
      table_id = table_id,
      commit_id = commit_id,
      range_id = range_id,
      file_path = file_path,
      table_name = table_name,
      write_version = write_version,
      size = size,
      modification_time = modification_time,
      file_exist_cols = file_exist_cols,
      is_base_file = is_base_file)
  }

  def expireFileUndoLog(table_name: String,
                        table_id: String,
                        range_id: String,
                        commit_id: String,
                        file_path: String,
                        write_version: Long,
                        modification_time: Long): Unit = {
    insertUndoLog(
      commit_type = UndoLogType.ExpireFile.toString,
      table_id = table_id,
      commit_id = commit_id,
      range_id = range_id,
      file_path = file_path,
      table_name = table_name,
      write_version = write_version,
      modification_time = modification_time)
  }

  def addDropTableUndoLog(table_name: String,
                          table_id: String): Boolean = {
    insertUndoLogWithLWT(
      commit_type = UndoLogType.DropTable.toString,
      table_id = table_id,
      commit_id = UndoLogType.DropTable.toString,
      table_name = table_name)
  }

  def addDropPartitionUndoLog(table_name: String,
                              table_id: String,
                              range_value: String,
                              range_id: String): Boolean = {
    insertUndoLogWithLWT(
      commit_type = UndoLogType.DropPartition.toString,
      table_id = table_id,
      commit_id = UndoLogType.DropPartition.toString,
      range_id = range_id,
      table_name = table_name,
      range_value = range_value)
  }

  def insertUndoLogWithLWT(commit_type: String,
                           table_id: String,
                           commit_id: String,
                           range_id: String = default_value,
                           file_path: String = default_value,
                           table_name: String,
                           range_value: String = default_value,
                           tag: Int = default_value.toInt,
                           write_version: Long = default_value.toLong,
                           timestamp: Long = System.currentTimeMillis(),
                           size: Long = default_value.toLong,
                           modification_time: Long = default_value.toLong,
                           table_schema: String = default_value,
                           setting: String = default_setting,
                           query_id: String = default_value, //for streaming commit
                           batch_id: Long = default_value.toLong,
                           short_table_name: String = default_value): Boolean = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |insert into $database.undo_log
           |(commit_type,table_id,commit_id,range_id,file_path,table_name,range_value,tag,write_version,timestamp,
           |size,modification_time,table_schema,setting,query_id,batch_id,short_table_name)
           |values ('$commit_type','$table_id','$commit_id','$range_id','$file_path','$table_name','$range_value',$tag,
           |$write_version,$timestamp,$size,$modification_time,'$table_schema',$setting,'$query_id',$batch_id,
           |'$short_table_name')
           |if not exists
      """.stripMargin)
      res.wasApplied()
    })
  }

  def insertUndoLog(commit_type: String,
                    table_id: String,
                    commit_id: String,
                    range_id: String = default_value,
                    file_path: String = default_value,
                    table_name: String,
                    range_value: String = default_value,
                    tag: Int = default_value.toInt,
                    write_version: Long = default_value.toLong,
                    timestamp: Long = System.currentTimeMillis(),
                    size: Long = default_value.toLong,
                    modification_time: Long = default_value.toLong,
                    table_schema: String = default_value,
                    setting: String = default_setting,
                    file_exist_cols: String = default_value,
                    delta_file_num: Int = default_value.toInt,
                    be_compacted: Boolean = true,
                    is_base_file: Boolean = false,
                    short_table_name: String = default_value,
                    sql_text: String = default_value,
                    relation_tables: String = default_value,
                    auto_update: Boolean = false,
                    is_creating_view: Boolean = false,
                    view_info: String = default_value): Unit = {
    cassandraConnector.withSessionDo(session => {
      val format_sql_text = MetaCommon.formatSqlTextToCassandra(sql_text)
      session.execute(
        s"""
           |insert into $database.undo_log
           |(commit_type,table_id,commit_id,range_id,file_path,table_name,range_value,tag,write_version,timestamp,
           |size,modification_time,table_schema,setting,file_exist_cols,delta_file_num,be_compacted,is_base_file,
           |short_table_name,sql_text,relation_tables,auto_update,is_creating_view,view_info)
           |values ('$commit_type','$table_id','$commit_id','$range_id','$file_path','$table_name','$range_value',$tag,
           |$write_version,$timestamp,$size,$modification_time,'$table_schema',$setting,'$file_exist_cols',
           |$delta_file_num,$be_compacted,$is_base_file,'$short_table_name','$format_sql_text','$relation_tables',
           |$auto_update,$is_creating_view,'$view_info')
      """.stripMargin)
    })

  }

  def getCommitTimestampAndTag(commit_type: String,
                               table_id: String,
                               commit_id: String,
                               range_id: String = default_value,
                               file_path: String = default_value): (Long, Int) = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select timestamp,tag from $database.undo_log where commit_type='$commit_type'
           |and table_id='$table_id' and commit_id='$commit_id'
           |and range_id='$range_id' and file_path='$file_path'
      """.stripMargin).iterator()
      if (res.hasNext) {
        val row = res.next()
        (row.getLong("timestamp"), row.getInt("tag"))
      } else {
        (-5, -5)
      }
    })
  }

  def updateCommitTimestamp(table_id: String, commit_id: String): Long = {
    cassandraConnector.withSessionDo(session => {
      val new_timestamp = System.currentTimeMillis()
      val res = session.execute(
        s"""
           |update $database.undo_log set timestamp=$new_timestamp
           |where commit_type='${UndoLogType.Commit.toString}'
           |and table_id='$table_id' and commit_id='$commit_id'
           |and range_id='$default_value' and file_path='$default_value'
           |if tag=0
      """.stripMargin)
      if (res.wasApplied()) {
        new_timestamp
      } else {
        throw MetaRetryErrors.failedUpdateCommitTimestampException(table_id, commit_id)
      }
    })

  }

  def updateRedoTimestamp(table_id: String, commit_id: String): Boolean = {
    cassandraConnector.withSessionDo(session => {
      val new_timestamp = System.currentTimeMillis()
      val res = session.execute(
        s"""
           |update $database.undo_log set timestamp=$new_timestamp
           |where commit_type='${UndoLogType.Commit.toString}'
           |and table_id='$table_id' and commit_id='$commit_id'
           |and range_id='$default_value' and file_path='$default_value'
           |if tag=-1
      """.stripMargin)
      res.wasApplied()
    })
  }

  def updateUndoLogTimestamp(commit_type: String,
                             table_id: String,
                             commit_id: String,
                             range_id: String = default_value,
                             file_path: String = default_value,
                             last_timestamp: Long): (Boolean, Long) = {
    cassandraConnector.withSessionDo(session => {
      val new_timestamp = System.currentTimeMillis()
      val res = session.execute(
        s"""
           |update $database.undo_log set timestamp=$new_timestamp
           |where commit_type='$commit_type'
           |and table_id='$table_id' and commit_id='$commit_id'
           |and range_id='$range_id' and file_path='$file_path'
           |if timestamp=$last_timestamp
      """.stripMargin)
      if (res.wasApplied()) {
        (true, new_timestamp)
      } else {
        (false, -5)
      }
    })
  }

  def updatePartitionLogInfo(table_id: String,
                             commit_id: String,
                             range_id: String,
                             write_version: Long,
                             be_compacted: Boolean,
                             delta_file_num: Int): Unit = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |update $database.undo_log
           |set write_version=$write_version,be_compacted=$be_compacted,delta_file_num=$delta_file_num
           |where commit_type='${UndoLogType.Partition.toString}'
           |and table_id='$table_id' and commit_id='$commit_id'
           |and range_id='$range_id' and file_path='$default_value'
           |if write_version=$default_value
      """.stripMargin)
      if (!res.wasApplied()) {
        logInfo(res.one().toString)
        throw LakeSoulErrors.failedUpdatePartitionUndoLogException()
      }
    })
  }

  def markOtherCommitRollBack(table_id: String, commit_id: String, tag: Int, timestamp: Long): Boolean = {
    cassandraConnector.withSessionDo(session => {
      val new_tag = tag + 1
      val now = System.currentTimeMillis()

      val res = session.execute(
        s"""
           |update $database.undo_log set tag=$new_tag,timestamp=$now
           |where commit_type='${UndoLogType.Commit.toString}' and table_id='$table_id'
           |and commit_id='$commit_id'
           |and range_id='$default_value' and file_path='$default_value'
           |if tag=$tag and timestamp=$timestamp
      """.stripMargin)
      res.wasApplied()
    })

  }

  def markSelfCommitSuccess(table_id: String, commit_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      val now = System.currentTimeMillis()
      val res = session.execute(
        s"""
           |update $database.undo_log set tag=-1,timestamp=$now
           |where commit_type='${UndoLogType.Commit.toString}' and table_id='$table_id'
           |and commit_id='$commit_id'
           |and range_id='$default_value' and file_path='$default_value'
           |if tag=0
      """.stripMargin)
      if (!res.wasApplied()) {
        throw MetaRetryErrors.failedMarkCommitTagException(table_id, commit_id)
      }
    })
  }

  def getUndoLogInfo(commit_type: String,
                     table_id: String,
                     commit_id: String): Array[undoLogInfo] = {
    cassandraConnector.withSessionDo(session => {
      val res = session.executeAsync(
        s"""
           |select table_name,range_id,range_value,file_path,tag,write_version,timestamp,size,modification_time,
           |table_schema,setting,file_exist_cols,delta_file_num,be_compacted,is_base_file,query_id,batch_id,
           |short_table_name,sql_text,relation_tables,auto_update,is_creating_view,view_info
           |from $database.undo_log where commit_type='$commit_type' and table_id='$table_id'
           |and commit_id='$commit_id'
      """.stripMargin).getUninterruptibly()
      val itr = res.iterator()
      val arr_buf = new ArrayBuffer[undoLogInfo]()

      while (itr.hasNext) {
        val re = itr.next()
        arr_buf += undoLogInfo(
          commit_type,
          table_id,
          commit_id,
          re.getString("range_id"),
          re.getString("file_path"),
          re.getString("table_name"),
          re.getString("range_value"),
          re.getInt("tag"),
          re.getLong("write_version"),
          re.getLong("timestamp"),
          re.getLong("size"),
          re.getLong("modification_time"),
          re.getString("table_schema"),
          re.getMap("setting", classOf[String], classOf[String]).toMap,
          re.getString("file_exist_cols"),
          re.getInt("delta_file_num"),
          re.getBool("be_compacted"),
          re.getBool("is_base_file"),
          re.getString("query_id"),
          re.getLong("batch_id"),
          re.getString("short_table_name"),
          MetaCommon.formatSqlTextFromCassandra(re.getString("sql_text")),
          re.getString("relation_tables"),
          re.getBool("auto_update"),
          re.getBool("is_creating_view"),
          re.getString("view_info"))
      }
      arr_buf.toArray
    })
  }

  def deleteUndoLogByCommitId(commit_type: String,
                              table_id: String,
                              commit_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.undo_log where commit_type='$commit_type' and
           |table_id='$table_id' and commit_id='$commit_id'
      """.stripMargin)
    })
  }

  def getTimeoutUndoLogInfo(commit_type: String,
                            table_id: String,
                            limit_timestamp: Long): Array[undoLogInfo] = {
    cassandraConnector.withSessionDo(session => {
      val res = session.executeAsync(
        s"""
           |select commit_id,range_id,file_path,table_name,range_value,tag,write_version,timestamp,size,
           |modification_time,table_schema,setting,file_exist_cols,delta_file_num,be_compacted,
           |is_base_file,query_id,batch_id,short_table_name,sql_text,relation_tables,auto_update,
           |is_creating_view,view_info
           |from $database.undo_log where commit_type='$commit_type' and table_id='$table_id'
           |and timestamp<$limit_timestamp allow filtering
      """.stripMargin).getUninterruptibly()
      val itr = res.iterator()
      val arr_buf = new ArrayBuffer[undoLogInfo]()

      while (itr.hasNext) {
        val re = itr.next()
        arr_buf += undoLogInfo(
          commit_type,
          table_id,
          re.getString("commit_id"),
          re.getString("range_id"),
          re.getString("file_path"),
          re.getString("table_name"),
          re.getString("range_value"),
          re.getInt("tag"),
          re.getLong("write_version"),
          re.getLong("timestamp"),
          re.getLong("size"),
          re.getLong("modification_time"),
          re.getString("table_schema"),
          re.getMap("setting", classOf[String], classOf[String]).toMap,
          re.getString("file_exist_cols"),
          re.getInt("delta_file_num"),
          re.getBool("be_compacted"),
          re.getBool("is_base_file"),
          re.getString("query_id"),
          re.getLong("batch_id"),
          re.getString("short_table_name"),
          MetaCommon.formatSqlTextFromCassandra(re.getString("sql_text")),
          re.getString("relation_tables"),
          re.getBool("auto_update"),
          re.getBool("is_creating_view"),
          re.getString("view_info"))
      }
      arr_buf.toArray
    })
  }

  def hasCommitTypeLog(table_id: String, commit_id: String): Boolean = {
    cassandraConnector.withSessionDo(session => {
      val res = session.executeAsync(
        s"""
           |select commit_id
           |from $database.undo_log where commit_type='${UndoLogType.Commit.toString}' and table_id='$table_id'
           |and commit_id='$commit_id'
      """.stripMargin).getUninterruptibly()

      if (res.iterator().nonEmpty) {
        true
      } else {
        false
      }
    })

  }

  def deleteUndoLogByTableId(commit_type: String,
                             table_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.undo_log where commit_type='$commit_type' and
           |table_id='$table_id'
      """.stripMargin)
    })
  }

  def deleteUndoLogByRangeId(commit_type: String,
                             table_id: String,
                             range_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select commit_id,range_id from $database.undo_log
           |where commit_type='$commit_type' and table_id='$table_id'
      """.stripMargin).iterator()

      while (res.hasNext) {
        val row = res.next()
        if (row.getString("range_id").equals(range_id)) {
          deleteUndoLogByCommitId(commit_type, table_id, row.getString("commit_id"))
        }
      }
    })
  }

  def deleteUndoLogByRangeId(commit_type: String,
                             table_id: String,
                             commit_id: String,
                             range_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.undo_log where commit_type='$commit_type' and
           |table_id='$table_id' and commit_id='$commit_id' and range_id='$range_id'
      """.stripMargin)
    })
  }

  def deleteUndoLog(commit_type: String,
                    table_id: String,
                    commit_id: String,
                    range_id: String = default_value,
                    file_path: String = default_value): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.undo_log where commit_type='$commit_type' and
           |table_id='$table_id' and commit_id='$commit_id' and range_id='$range_id' and file_path='$file_path'
      """.stripMargin)
    })
  }


}
