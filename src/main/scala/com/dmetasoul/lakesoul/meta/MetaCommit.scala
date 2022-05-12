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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.Snapshot
import org.apache.spark.sql.lakesoul.commands.{DropPartitionCommand, DropTableCommand}
import org.apache.spark.sql.lakesoul.exception._
import org.apache.spark.sql.lakesoul.material_view.ConstructQueryInfo
import org.apache.spark.sql.lakesoul.utils._

import scala.collection.mutable.ArrayBuffer

object MetaCommit extends Logging {
  //meta commit process
  def doMetaCommit(meta_info: MetaInfo,
                   changeSchema: Boolean,
                   commitOptions: CommitOptions,
                   times: Int = 0): Unit = {
    //add commit type undo log, generate commit_id
    val commit_id = generateCommitIdToAddUndoLog(
      meta_info.table_info.table_name,
      meta_info.table_info.table_id,
      meta_info.query_id,
      meta_info.batch_id)

    try {
      //add undo log and take locks for short table name and material view
      takeLockOfShortTableNameAndMaterialView(meta_info.table_info, commitOptions, commit_id)

      //hold all needed partition lock
      var new_meta_info = takePartitionsWriteLock(meta_info, commit_id)

      //if table schema will be changed in this commit, hold schema lock
      if (changeSchema) {
        takeSchemaLock(new_meta_info)
      }

      //update write version, compaction info, delta file num
      new_meta_info = updatePartitionInfoAndGetNewMetaInfo(new_meta_info)

      //check files conflict
      fileConflictDetection(new_meta_info)

      //add file changing info to table undo_log
      addDataInfo(new_meta_info)

      //change commit state to redo, roll back is forbidden in this time, this commit must be success
      markSelfCommitSuccess(new_meta_info.table_info.table_id, new_meta_info.commit_id)

      //change schema, release schema lock, and delete corresponding undo log
      if (changeSchema) {
        updateSchema(new_meta_info)
      }

      //process short table name and material view
      processShortTableNameAndMaterialView(new_meta_info, commitOptions)

      //add/update files info to table data_info, release partition lock and delete undo log
      commitMetaInfo(new_meta_info, changeSchema)

    } catch {
      case e: MetaRetryException =>
        e.printStackTrace()
        logInfo("!!!!!!! Error Retry commit id: " + commit_id)
        rollBackUpdate(meta_info, commit_id, changeSchema, commitOptions)
        if (times < MetaUtils.MAX_COMMIT_ATTEMPTS) {
          doMetaCommit(meta_info, changeSchema, commitOptions, times + 1)
        } else {
          throw LakeSoulErrors.commitFailedReachLimit(
            meta_info.table_info.table_name,
            commit_id,
            MetaUtils.MAX_COMMIT_ATTEMPTS)
        }

      case e: Throwable =>
        rollBackUpdate(meta_info, commit_id, changeSchema, commitOptions)
        throw e
    }

  }


  //add commit type undo log, generate commit id
  def generateCommitIdToAddUndoLog(table_name: String,
                                   table_id: String,
                                   queryId: String,
                                   batchId: Long): String = {
    val commit_id = "commit_" + java.util.UUID.randomUUID().toString
    logInfo(s"{{{{{{commit: $commit_id begin!!!}}}}}}")
    val timestamp = System.currentTimeMillis()
    if (!addCommitUndoLog(table_name, table_id, commit_id, timestamp, queryId, batchId)) {
      generateCommitIdToAddUndoLog(table_name, table_id, queryId, batchId)
    } else {
      commit_id
    }
  }


  def isCommitTimeout(timestamp: Long): Boolean = {
    val timeout = System.currentTimeMillis() - timestamp
    if (timeout > MetaUtils.COMMIT_TIMEOUT) {
      true
    } else {
      false
    }
  }

  def getCommitState(table_name: String,
                     table_id: String,
                     commit_id: String,
                     time_limit: Long = MetaUtils.COMMIT_TIMEOUT): commitStateInfo = {
    val (last_timestamp, tag) = getCommitTimestampAndTag(UndoLogType.Commit.toString, table_id, commit_id)
    if (last_timestamp < 1) {
      commitStateInfo(CommitState.Clean, table_name, table_id, commit_id, tag, last_timestamp)
    } else {
      val timeout = System.currentTimeMillis() - last_timestamp

      //not time out
      if (timeout < time_limit) {
        if (tag == 0) {
          commitStateInfo(CommitState.Committing, table_name, table_id, commit_id, tag, last_timestamp)
        }
        else if (tag == -1) {
          commitStateInfo(CommitState.Redoing, table_name, table_id, commit_id, tag, last_timestamp)
        }
        else {
          commitStateInfo(CommitState.RollBacking, table_name, table_id, commit_id, tag, last_timestamp)
        }
      }
      else {
        if (tag == 0) {
          commitStateInfo(CommitState.CommitTimeout, table_name, table_id, commit_id, tag, last_timestamp)
        }
        else if (tag == -1) {
          commitStateInfo(CommitState.RedoTimeout, table_name, table_id, commit_id, tag, last_timestamp)
        }
        else {
          commitStateInfo(CommitState.RollBackTimeout, table_name, table_id, commit_id, tag, last_timestamp)
        }
      }
    }
  }

  def takeLockOfShortTableNameAndMaterialView(tableInfo: TableInfo,
                                              commitOptions: CommitOptions,
                                              commit_id: String): Unit = {
    //lock short table name if defined, it will quickly failed when another user committing the same name
    if (commitOptions.shortTableName.isDefined) {
      //add short table name undo log
      addShortTableNameUndoLog(table_name = tableInfo.table_name,
        table_id = tableInfo.table_id,
        commit_id = commit_id,
        short_table_name = commitOptions.shortTableName.get
      )
      //add short name relation
      MetaVersion.addShortTableName(commitOptions.shortTableName.get, tableInfo.table_name)
    }
    //if this table is a material view
    if (commitOptions.materialInfo.isDefined) {
      val materialInfo = commitOptions.materialInfo.get
      val shortTableName = if (materialInfo.isCreatingView) {
        commitOptions.shortTableName.get
      } else {
        tableInfo.short_table_name.get
      }
      //add material undo log
      addMaterialUndoLog(table_name = tableInfo.table_name,
        table_id = tableInfo.table_id,
        commit_id = commit_id,
        short_table_name = shortTableName,
        sql_text = materialInfo.sqlText,
        relation_tables = materialInfo.relationTables.map(_.toString).mkString(","),
        auto_update = materialInfo.autoUpdate,
        is_creating_view = materialInfo.isCreatingView,
        view_info = ConstructQueryInfo.buildJson(materialInfo.info))

      //lock material view table to prevent changes by other commit
      lockMaterialViewName(
        commit_id,
        shortTableName,
        materialInfo.isCreatingView)

      if (materialInfo.isCreatingView) {
        //lock material relation table to prevent changes on relation tables by other commit
        commitOptions.materialInfo.get.relationTables.foreach(table => {
          lockMaterialRelation(commit_id = commit_id, table_id = table.tableId, table_name = table.tableName)
        })
      }
    }
  }

  def processShortTableNameAndMaterialView(new_meta_info: MetaInfo, commitOptions: CommitOptions): Unit = {
    //set short name for this table
    if (commitOptions.shortTableName.isDefined) {
      //add short name to table_info
      MetaVersion.updateTableShortName(
        new_meta_info.table_info.table_name,
        new_meta_info.table_info.table_id,
        commitOptions.shortTableName.get)

      deleteUndoLog(
        commit_type = UndoLogType.ShortTableName.toString,
        table_id = new_meta_info.table_info.table_id,
        commit_id = new_meta_info.commit_id
      )
    }
    //if this table is a material view
    if (commitOptions.materialInfo.isDefined) {
      val materialInfo = commitOptions.materialInfo.get
      val shortTableName = if (commitOptions.shortTableName.isDefined) {
        commitOptions.shortTableName.get
      } else {
        new_meta_info.table_info.short_table_name.get
      }
      if (materialInfo.isCreatingView) {
        //queryInfo may very big and exceed 64kb limit, so try to split it to some fragment if value is too long
        val view_info_index = getUndoLogInfo(
          UndoLogType.Material.toString,
          new_meta_info.table_info.table_id,
          new_meta_info.commit_id)
          .head.view_info
        //add material view
        MaterialView.addMaterialView(
          shortTableName,
          new_meta_info.table_info.table_name,
          new_meta_info.table_info.table_id,
          materialInfo.relationTables.map(_.toString).mkString(","),
          materialInfo.sqlText,
          materialInfo.autoUpdate,
          view_info_index)
        //unlock material view
        unlockMaterialViewName(new_meta_info.commit_id, shortTableName)

        //update material relation
        materialInfo.relationTables.foreach(table => {
          //update
          updateMaterialRelation(
            table_id = table.tableId,
            table_name = table.tableName,
            view_name = shortTableName)
          //unlock material relation
          unlockMaterialRelation(commit_id = new_meta_info.commit_id, table_id = table.tableId)
        })
      } else {
        //update material view
        val view_name = new_meta_info.table_info.short_table_name.get
        MaterialView.updateMaterialView(
          view_name,
          materialInfo.relationTables.map(_.toString).mkString(","),
          materialInfo.autoUpdate
        )
        //unlock material view
        unlockMaterialViewName(new_meta_info.commit_id, view_name)
      }

      //delete undo log
      deleteUndoLog(
        commit_type = UndoLogType.Material.toString,
        table_id = new_meta_info.table_info.table_id,
        commit_id = new_meta_info.commit_id)

    }
  }

  def lockSinglePartition(partition_info: PartitionInfo,
                          commit_id: String,
                          times: Int = 0): Unit = {

    //update timestamp, if timeout, exit the loop directly and rollback, retry after a random sleep
    updateCommitTimestamp(partition_info.table_id, commit_id)

    //lock
    val (lock_flag, last_commit_id) = MetaLock.lock(partition_info.range_id, commit_id)

    if (!lock_flag) {
      //if lock failed, it prove that another job is committing, check its state
      val state_info = getCommitState(partition_info.table_name, partition_info.table_id, last_commit_id)

      logInfo(s"Another job's commit state is ${state_info.state.toString} ~~")

      state_info.state match {
        //wait a moment, increase count and retry
        case CommitState.Committing =>
          if (times < MetaUtils.GET_LOCK_MAX_ATTEMPTS) {
            TimeUnit.SECONDS.sleep(MetaUtils.WAIT_LOCK_INTERVAL)
            lockSinglePartition(partition_info, commit_id, times + 1)
          } else {
            //it may has deadlock
            throw new MetaNonfatalException("There may have deadlock.")
          }
        //wait a moment and retry
        case CommitState.RollBacking | CommitState.Redoing =>
          TimeUnit.SECONDS.sleep(MetaUtils.WAIT_LOCK_INTERVAL)
          lockSinglePartition(partition_info, commit_id, times)

        //rollback and retry
        case CommitState.CommitTimeout | CommitState.RollBackTimeout =>
          rollBackCommit(state_info.table_id, state_info.commit_id, state_info.tag, state_info.timestamp)
          lockSinglePartition(partition_info, commit_id)

        //clean
        case CommitState.Clean =>
          cleanRollBackCommit(state_info.table_id, state_info.commit_id, partition_info.range_id)
          lockSinglePartition(partition_info, commit_id)

        //redo and retry
        case CommitState.RedoTimeout =>
          redoCommit(state_info.table_name, state_info.table_id, state_info.commit_id)
          lockSinglePartition(partition_info, commit_id)
      }
    }
  }

  def takePartitionsWriteLock(meta_info: MetaInfo, commit_id: String, times: Int = 0): MetaInfo = {
    val new_partition_info_arr_buf = new ArrayBuffer[PartitionInfo]()
    val partition_info_itr = meta_info.partitionInfoArray.sortBy(_.range_id).iterator

    try {
      while (partition_info_itr.hasNext) {
        val partition_info = partition_info_itr.next()

        var rangeId = ""
        var changed = false

        try {
          if (!MetaVersion.isPartitionExists(
            partition_info.table_id,
            partition_info.range_value,
            partition_info.range_id,
            commit_id)) {
            MetaVersion.addPartition(
              partition_info.table_id,
              partition_info.table_name,
              partition_info.range_id,
              partition_info.range_value)
          }
        } catch {
          //create partition concurrently
          case e: MetaRerunException if (e.getMessage.contains("another job drop and create a newer one.")
            && partition_info.read_version == 1
            && partition_info.pre_write_version == 1) =>
            //the latter committer can use exist range_id
            val (partitionExists, now_range_id) =
              MetaVersion.getPartitionId(partition_info.table_id, partition_info.range_value)
            if (partitionExists) {
              rangeId = now_range_id
              changed = true
            } else {
              throw e
            }

          case e: MetaException if (e.getMessage.contains("Failed to add partition version for table:")
            && partition_info.read_version == 1
            && partition_info.pre_write_version == 1) =>
            //the latter committer can use exist range_id
            val (partitionExists, now_range_id) =
              MetaVersion.getPartitionId(partition_info.table_id, partition_info.range_value)
            if (partitionExists) {
              rangeId = now_range_id
              changed = true
            } else {
              throw e
            }

          case e: Exception => throw e

        }

        val newPartitionInfo = if (changed) {
          partition_info.copy(range_id = rangeId)
        } else {
          partition_info
        }

        addPartitionUndoLog(
          newPartitionInfo.table_name,
          newPartitionInfo.range_value,
          newPartitionInfo.table_id,
          newPartitionInfo.range_id,
          commit_id,
          newPartitionInfo.delta_file_num,
          newPartitionInfo.be_compacted)

        lockSinglePartition(newPartitionInfo, commit_id)

        new_partition_info_arr_buf += newPartitionInfo
      }

      meta_info.copy(
        partitionInfoArray = new_partition_info_arr_buf.toArray,
        commit_id = commit_id)
    } catch {
      case e: MetaNonfatalException =>
        if (times < MetaUtils.GET_LOCK_MAX_ATTEMPTS) {
          //it may has deadlock, release all hold locks
          rollBackPartitionLock(meta_info.table_info.table_id, commit_id)
          updateCommitTimestamp(meta_info.table_info.table_id, commit_id)
          logInfo("  too many jobs require to update,sleep a while ~~~~")
          TimeUnit.SECONDS.sleep(scala.util.Random.nextInt((times + 1) * MetaUtils.RETRY_LOCK_INTERVAL))

          //increase count and retry
          takePartitionsWriteLock(meta_info, commit_id, times + 1)
        } else {
          throw MetaRetryErrors.tooManyCommitException()
        }

      case e: Throwable => throw e
    }

  }

  def updateMaterialRelation(table_id: String,
                             table_name: String,
                             view_name: String): Unit = {
    val existsViews = MaterialView.getMaterialRelationInfo(table_id)
    val newViews = if (existsViews.isEmpty) {
      view_name
    } else {
      existsViews + "," + view_name
    }

    MaterialView.updateMaterialRelationInfo(table_id, table_name, newViews)
  }


  def formatLockMaterialView(name: String): String = {
    s"material_$name"
  }

  def lockMaterialViewName(commit_id: String, materialViewName: String, isCreating: Boolean): Unit = {
    val (lock_flag, last_commit_id) = MetaLock.lock(formatLockMaterialView(materialViewName), commit_id)
    lazy val table_exists = MaterialView.isMaterialViewExists(materialViewName)
    if (!lock_flag || (isCreating && table_exists)) {
      //filed quickly if another user is creating or material view is already created very recently
      throw LakeSoulErrors.failedLockMaterialViewName(materialViewName)
    }
  }

  def unlockMaterialViewName(commit_id: String, materialViewName: String): Unit = {
    MetaLock.unlock(formatLockMaterialView(materialViewName), commit_id)
  }

  def lockMaterialRelation(commit_id: String, table_id: String, table_name: String): Unit = {
    val (lock_flag, last_commit_id) = MetaLock.lock(formatLockMaterialView(table_id), commit_id)
    if (!lock_flag) {
      //check state and resolve last commit
      resolveLastCommit(table_name, table_id, last_commit_id)

      //retry
      lockMaterialRelation(commit_id, table_id, table_name)
    }
  }

  def unlockMaterialRelation(commit_id: String, table_id: String): Unit = {
    MetaLock.unlock(formatLockMaterialView(table_id), commit_id)
  }

  def lockSchema(meta_info: MetaInfo): Unit = {
    val (lock_flag, last_commit_id) = MetaLock.lock(meta_info.table_info.table_id, meta_info.commit_id)
    if (!lock_flag) {
      //check state and resolve last commit
      resolveLastCommit(meta_info.table_info.table_name, meta_info.table_info.table_id, last_commit_id)

      //retry
      lockSchema(meta_info)
    }
  }

  //check the state of this commit, and resolve it according to its state
  def resolveLastCommit(table_name: String, table_id: String, last_commit_id: String): Unit = {
    //get state of last commit
    val state_info = getCommitState(
      table_name,
      table_id,
      last_commit_id)

    logInfo(s"Another job's commit state is ${state_info.state.toString} ~~")

    state_info.state match {
      case CommitState.Committing | CommitState.RollBacking | CommitState.Redoing =>
        TimeUnit.SECONDS.sleep(MetaUtils.WAIT_LOCK_INTERVAL)

      case CommitState.CommitTimeout | CommitState.RollBackTimeout =>
        rollBackCommit(state_info.table_id, state_info.commit_id, state_info.tag, state_info.timestamp)

      case CommitState.Clean =>
        cleanRollBackCommit(state_info.table_id, state_info.commit_id, state_info.table_id)

      case CommitState.RedoTimeout =>
        redoCommit(state_info.table_name, state_info.table_id, state_info.commit_id)
    }
  }

  def takeSchemaLock(meta_info: MetaInfo): Unit = {
    val schema_write_version = meta_info.table_info.schema_version + 1
    addSchemaUndoLog(
      meta_info.table_info.table_name,
      meta_info.table_info.table_id,
      meta_info.commit_id,
      schema_write_version,
      meta_info.table_info.table_schema,
      MetaUtils.toCassandraSetting(meta_info.table_info.configuration))

    //lock
    lockSchema(meta_info)

    //if schema had been changed, throw exception directly
    val current_schema_version = MetaVersion.getTableInfo(meta_info.table_info.table_name).schema_version
    if (meta_info.table_info.schema_version != current_schema_version) {
      throw LakeSoulErrors.schemaChangedException(meta_info.table_info.table_name)
    }
  }

  def addDataInfo(meta_info: MetaInfo): Unit = {
    val table_name = meta_info.table_info.table_name
    val table_id = meta_info.table_info.table_id
    val commit_id = meta_info.commit_id

    //update timestamp to avoid committing timeout
    updateCommitTimestamp(table_id, commit_id)

    //add all files change undo log
    for (partition_info <- meta_info.partitionInfoArray) {
      val range_id = partition_info.range_id
      val write_version = getWriteVersionFromMetaInfo(meta_info, partition_info)
      for (file <- partition_info.expire_files) {
        expireFileUndoLog(
          table_name,
          table_id,
          range_id,
          commit_id,
          file.file_path,
          write_version,
          file.modification_time)
      }
      for (file <- partition_info.add_files) {
        addFileUndoLog(
          table_name,
          table_id,
          range_id,
          commit_id,
          file.file_path,
          write_version,
          file.size,
          file.modification_time,
          file.file_exist_cols,
          file.is_base_file)
      }
    }
  }

  def updateSchema(meta_info: MetaInfo): Unit = {
    val table_name = meta_info.table_info.table_name
    val table_id = meta_info.table_info.table_id
    val commit_id = meta_info.commit_id

    val new_read_version = meta_info.table_info.schema_version + 1

    //schema may be too long to store in a cassandra field(64KB),in this case the schema value will be split,
    //and the value here is a collection of index in table fragment_value
    val schema_index = getUndoLogInfo(UndoLogType.Schema.toString, table_id, commit_id).head.table_schema
    MetaVersion.updateTableSchema(
      table_name,
      table_id,
      schema_index,
      meta_info.table_info.configuration,
      new_read_version)

    MetaLock.unlock(table_id, commit_id)
    deleteUndoLog(UndoLogType.Schema.toString, table_id, commit_id)
  }


  def commitMetaInfo(meta_info: MetaInfo,
                     changeSchema: Boolean): Unit = {
    val table_name = meta_info.table_info.table_name
    val table_id = meta_info.table_info.table_id
    val commit_id = meta_info.commit_id

    for (partition_info <- meta_info.partitionInfoArray) {
      val range_value = partition_info.range_value
      val range_id = partition_info.range_id
      val write_version = getWriteVersionFromMetaInfo(meta_info, partition_info)

      for (file <- partition_info.expire_files) {
        DataOperation.deleteExpireDataFile(
          table_id,
          range_id,
          file.file_path,
          write_version,
          commit_id,
          file.modification_time)

        deleteUndoLog(
          commit_type = UndoLogType.ExpireFile.toString,
          table_id = table_id,
          commit_id = commit_id,
          range_id = range_id,
          file_path = file.file_path
        )
      }
      for (file <- partition_info.add_files) {
        DataOperation.addNewDataFile(
          table_id,
          range_id,
          file.file_path,
          write_version,
          commit_id,
          file.size,
          file.modification_time,
          file.file_exist_cols,
          file.is_base_file)

        deleteUndoLog(
          commit_type = UndoLogType.AddFile.toString,
          table_id = table_id,
          commit_id = commit_id,
          range_id = range_id,
          file_path = file.file_path
        )
      }

      MetaVersion.updatePartitionInfo(
        table_id = table_id,
        range_value = range_value,
        range_id = range_id,
        write_version = write_version,
        delta_file_num = partition_info.delta_file_num,
        be_compacted = partition_info.be_compacted)

      MetaLock.unlock(partition_info.range_id, commit_id)
      deleteUndoLog(
        commit_type = UndoLogType.Partition.toString,
        table_id = table_id,
        commit_id = commit_id,
        range_id = range_id
      )
    }

    //if it is streaming job, streaming info should be updated
    if (meta_info.query_id.nonEmpty && meta_info.batch_id >= 0) {
      StreamingRecord.updateStreamingInfo(table_id, meta_info.query_id, meta_info.batch_id, System.currentTimeMillis())
    }

    deleteUndoLog(
      commit_type = UndoLogType.Commit.toString,
      table_id = table_id,
      commit_id = commit_id
    )

    logInfo(s"{{{{{{commit: $commit_id success!!!}}}}}}")
  }


  //before write files info to table data_info, the new write_version should be got from metaInfo
  def getWriteVersionFromMetaInfo(meta_info: MetaInfo, partition_info: PartitionInfo): Long = {
    var write_version = -1L
    for (meta_partition_info <- meta_info.partitionInfoArray) {
      if (meta_partition_info.range_value.equals(partition_info.range_value)) {
        write_version = meta_partition_info.pre_write_version
      }
    }

    if (write_version == -1) {
      val partitions_arr_buf = new ArrayBuffer[String]()
      for (partition_info <- meta_info.partitionInfoArray) {
        partitions_arr_buf += partition_info.range_value
      }
      throw LakeSoulErrors.getWriteVersionError(
        meta_info.table_info.table_name,
        partition_info.range_value,
        partitions_arr_buf.toArray.mkString(","),
        meta_info.commit_id
      )
    }
    write_version
  }

  //check files conflict
  def fileConflictDetection(meta_info: MetaInfo): Unit = {
    val update_file_info_arr = meta_info.partitionInfoArray
    meta_info.commit_type match {
      case DeltaCommit => //files conflict checking is not required when committing delta files
      case CompactionCommit | PartCompactionCommit =>
        //when committing compaction job, it just need to check that the read files have not been deleted
        DataOperation.checkDataInfo(meta_info.commit_id, update_file_info_arr, false, true)
      case _ =>
        DataOperation.checkDataInfo(meta_info.commit_id, update_file_info_arr, true, true)
    }
  }


  //check and redo timeout commits before read
  def checkAndRedoCommit(snapshot: Snapshot): Unit = {
    val table_id: String = snapshot.getTableInfo.table_id
    val limit_timestamp = System.currentTimeMillis() - MetaUtils.COMMIT_TIMEOUT

    //drop table command
    val dropTableInfo = getUndoLogInfo(
      UndoLogType.DropTable.toString,
      table_id,
      UndoLogType.DropTable.toString)
    if (dropTableInfo.nonEmpty) {
      DropTableCommand.dropTable(snapshot)
      throw LakeSoulErrors.tableNotExistsException(snapshot.getTableName)
    }
    //drop partition command
    val dropPartitionInfo = getUndoLogInfo(
      UndoLogType.DropPartition.toString,
      table_id,
      UndoLogType.DropPartition.toString)
    if (dropPartitionInfo.nonEmpty) {
      dropPartitionInfo.foreach(d => {
        DropPartitionCommand.dropPartition(d.table_name, d.table_id, d.range_value, d.range_id)
      })
    }


    //commit
    val logInfo = getTimeoutUndoLogInfo(UndoLogType.Commit.toString, table_id, limit_timestamp)

    var flag = true
    logInfo.foreach(log => {
      if (log.tag == -1) {
        if (!Redo.redoCommit(log.table_name, log.table_id, log.commit_id)) {
          flag = false
        }
      } else {
        RollBack.rollBackCommit(log.table_id, log.commit_id, log.tag, log.timestamp)
      }
    })

    if (!flag) {
      checkAndRedoCommit(snapshot)
    }

  }

  def cleanUndoLog(snapshot: Snapshot): Unit = {
    val table_id: String = snapshot.getTableInfo.table_id
    checkAndRedoCommit(snapshot)
    val limit_timestamp = System.currentTimeMillis() - MetaUtils.UNDO_LOG_TIMEOUT

    val allType = UndoLogType.getAllType
    val log_info_seq = allType
      .map(UndoLog.getTimeoutUndoLogInfo(_, table_id, limit_timestamp))
      .flatMap(_.toSeq)

    val commit_log_info_seq = log_info_seq.filter(_.commit_type.equalsIgnoreCase(UndoLogType.Commit.toString))
    val other_log_info_seq = log_info_seq.filter(!_.commit_type.equalsIgnoreCase(UndoLogType.Commit.toString))

    //delete undo log directly if table is not exists
    commit_log_info_seq
      .filter(log => !MetaVersion.isTableIdExists(log.table_name, log.table_id))
      .foreach(log => deleteUndoLog(UndoLogType.Commit.toString, table_id, log.commit_id))

    //if a commit_id don't has corresponding undo log with commit type, it should be clean
    val skip_commit_id =
      other_log_info_seq
        .map(_.commit_id).toSet
        .filter(hasCommitTypeLog(table_id, _))

    other_log_info_seq
      .filter(log => !skip_commit_id.contains(log.commit_id))
      .foreach(log => {
        log.commit_type match {
          case t: String if t.equalsIgnoreCase(UndoLogType.Schema.toString) =>
            MetaLock.unlock(log.table_id, log.commit_id)
          case t: String if t.equalsIgnoreCase(UndoLogType.Partition.toString) =>
            MetaLock.unlock(log.range_id, log.commit_id)
          case t: String if t.equalsIgnoreCase(UndoLogType.Material.toString) =>
            unlockMaterialViewName(commit_id = log.commit_id, log.short_table_name)
            log.relation_tables.split(",").map(m => RelationTable.build(m))
              .foreach(table => {
                unlockMaterialRelation(commit_id = log.commit_id, table_id = table.tableId)
              })

        }
        deleteUndoLog(log.commit_type, table_id, log.commit_id, log.range_id, log.file_path)
      })

    //cleanup the timeout streaming info
    val streaming_expire_timestamp = System.currentTimeMillis() - MetaUtils.STREAMING_INFO_TIMEOUT
    StreamingRecord.deleteStreamingInfoByTimestamp(table_id, streaming_expire_timestamp)

  }


  def updatePartitionInfoAndGetNewMetaInfo(meta_info: MetaInfo): MetaInfo = {
    val new_partition_info_arr_buf = new ArrayBuffer[PartitionInfo]()
    val partition_info_itr = meta_info.partitionInfoArray.iterator
    val commit_id = meta_info.commit_id


    while (partition_info_itr.hasNext) {
      val partition_info = partition_info_itr.next()


      //update the write_version/be_compacted/delta_file_num info in partition undo log
      val currentPartitionInfo = MetaVersion
        .getSinglePartitionInfo(partition_info.table_id, partition_info.range_value, partition_info.range_id)
      val write_version = currentPartitionInfo.read_version + 1
      val versionDiff = currentPartitionInfo.read_version - partition_info.read_version
      val (compactInfo, deltaFileNum) = meta_info.commit_type match {
        case CompactionCommit =>
          if (versionDiff > 0) {
            (currentPartitionInfo.be_compacted, versionDiff.toInt)
          } else if (versionDiff == 0) {
            (partition_info.be_compacted, 0)
          } else {
            throw LakeSoulErrors.readVersionIllegalException(
              currentPartitionInfo.read_version,
              partition_info.read_version)
          }

        case PartCompactionCommit =>
          if (versionDiff >= 0) {
            (currentPartitionInfo.be_compacted, partition_info.delta_file_num + versionDiff.toInt)
          } else {
            throw LakeSoulErrors.readVersionIllegalException(
              currentPartitionInfo.read_version,
              partition_info.read_version)
          }

        case _ => (partition_info.be_compacted, currentPartitionInfo.delta_file_num + 1)
      }
      UndoLog.updatePartitionLogInfo(
        partition_info.table_id,
        commit_id,
        partition_info.range_id,
        write_version,
        compactInfo,
        deltaFileNum)

      new_partition_info_arr_buf += partition_info.copy(
        pre_write_version = write_version,
        be_compacted = compactInfo,
        delta_file_num = deltaFileNum)
    }

    meta_info.copy(partitionInfoArray = new_partition_info_arr_buf.toArray)


  }


}
