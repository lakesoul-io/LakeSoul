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

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta._
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.utils.FileOperation
import org.apache.spark.sql.lakesoul.{PartitionFilter, Snapshot, SnapshotManagement}

import java.util.concurrent.TimeUnit

object DropTableCommand {

  val MAX_ATTEMPTS: Int = MetaUtils.GET_LOCK_MAX_ATTEMPTS
  val WAIT_TIME: Int = MetaUtils.DROP_TABLE_WAIT_SECONDS

  def run(snapshot: Snapshot): Unit = {
    val table_id = snapshot.getTableInfo.table_id
    val table_name = snapshot.getTableInfo.table_name

    var i = 0
    while (i < MAX_ATTEMPTS) {
      if (UndoLog.addDropTableUndoLog(table_name, table_id)) {
        dropTable(snapshot)
        i = MAX_ATTEMPTS
      } else {
        i = checkAndDropTable(snapshot, i)
      }
    }

  }


  private def checkAndDropTable(snapshot: Snapshot, i: Int): Int = {
    val table_id = snapshot.getTableInfo.table_id

    val (timestamp, _) = UndoLog.getCommitTimestampAndTag(
      UndoLogType.DropTable.toString,
      table_id,
      "dropTable")

    if (timestamp < 0) {
      MAX_ATTEMPTS
    } else if (timestamp > System.currentTimeMillis() - MetaUtils.COMMIT_TIMEOUT) {
      TimeUnit.SECONDS.sleep(WAIT_TIME)
      checkAndDropTable(snapshot, i)
    } else {
      val update_timestamp = UndoLog.updateUndoLogTimestamp(
        commit_type = UndoLogType.DropTable.toString,
        table_id = table_id,
        commit_id = "dropTable",
        last_timestamp = timestamp
      )
      if (update_timestamp._1) {
        dropTable(snapshot)
        MAX_ATTEMPTS
      }
      else {
        i + 1
      }

    }
  }

  def dropTable(snapshot: Snapshot): Unit = {
    val tableInfo = snapshot.getTableInfo
    val table_id = tableInfo.table_id
    val table_name = tableInfo.table_name
    val short_table_name = tableInfo.short_table_name

    //delete material views associated with this table
    if (!tableInfo.is_material_view) {
      val materialViews = MaterialView.getMaterialRelationInfo(table_id)
      if (materialViews.nonEmpty) {
        materialViews.split(",").foreach(view => LakeSoulTable.forName(view).dropTable())
      }
    }
    MetaVersion.deleteTableInfo(table_name, table_id)

    if (short_table_name.isDefined) {
      MetaVersion.deleteShortTableName(short_table_name.get, table_name)
      //if this table is material view, clean relation info
      if (tableInfo.is_material_view) {
        //clean material_relation info
        val materialViewInfo = MaterialView.getMaterialViewInfo(short_table_name.get)
        if (materialViewInfo.isDefined) {
          materialViewInfo.get.relationTables.foreach(table => {
            val relationTableInfo = MetaVersion.getTableInfo(table.tableName)
            //drop view from relation info
            dropMaterialViewFromRelation(
              relationTableInfo.table_id,
              relationTableInfo.table_name,
              short_table_name.get)
          })
        }

        //clean material_view info
        MaterialView.deleteMaterialView(short_table_name.get)
      }
    }

    UndoLog.deleteUndoLogByTableId(UndoLogType.Commit.toString, table_id)
    UndoLog.deleteUndoLogByTableId(UndoLogType.Material.toString, table_id)
    UndoLog.deleteUndoLogByTableId(UndoLogType.ShortTableName.toString, table_id)
    UndoLog.deleteUndoLogByTableId(UndoLogType.Partition.toString, table_id)
    UndoLog.deleteUndoLogByTableId(UndoLogType.Schema.toString, table_id)
    UndoLog.deleteUndoLogByTableId(UndoLogType.AddFile.toString, table_id)
    UndoLog.deleteUndoLogByTableId(UndoLogType.ExpireFile.toString, table_id)
    UndoLog.deleteUndoLogByTableId(UndoLogType.DropPartition.toString, table_id)

    TimeUnit.SECONDS.sleep(WAIT_TIME)

    val partition_info_arr = snapshot.getPartitionInfoArray
    MetaVersion.deletePartitionInfoByTableId(table_id)

    partition_info_arr.foreach(part => DataOperation.deleteDataInfoByRangeId(table_id, part.range_id))
    FragmentValue.deleteFragmentValueByTableId(table_id)
    StreamingRecord.deleteStreamingInfoByTableId(table_id)

    val path = new Path(table_name)
    val sessionHadoopConf = SparkSession.active.sessionState.newHadoopConf()
    val fs = path.getFileSystem(sessionHadoopConf)

    UndoLog.deleteUndoLogByTableId(UndoLogType.DropTable.toString, table_id)
    SnapshotManagement.invalidateCache(table_name)
    fs.delete(path, true);
  }

  private def dropMaterialViewFromRelation(table_id: String,
                                           table_name: String,
                                           view_name: String): Unit = {
    val existsViews = MaterialView.getMaterialRelationInfo(table_id)
    val newViews = existsViews.split(",").toSet.-(view_name).mkString(",")
    if (newViews.isEmpty) {
      MaterialView.deleteMaterialRelationInfo(table_id)
    } else {
      MaterialView.updateMaterialRelationInfo(table_id, table_name, newViews)
    }
  }


}

object DropPartitionCommand extends PredicateHelper {
  val MAX_ATTEMPTS: Int = MetaUtils.GET_LOCK_MAX_ATTEMPTS
  val WAIT_TIME: Int = MetaUtils.DROP_TABLE_WAIT_SECONDS

  def run(snapshot: Snapshot, condition: Expression): Unit = {
    val table_name = snapshot.getTableName
    val table_id = snapshot.getTableInfo.table_id

    val candidatePartitions = PartitionFilter.partitionsForScan(snapshot, Seq(condition))
    //only one partition is allowed to drop at a time
    if (candidatePartitions.isEmpty) {
      LakeSoulErrors.partitionNotFoundException(snapshot.getTableName, condition.toString())
    } else if (candidatePartitions.length > 1) {
      LakeSoulErrors.tooMuchPartitionException(
        snapshot.getTableName,
        condition.toString(),
        candidatePartitions.length)
    }

    val range_id = candidatePartitions.head.range_id
    val range_value = candidatePartitions.head.range_value


    var i = 0
    while (i < MAX_ATTEMPTS) {

      if (UndoLog.addDropPartitionUndoLog(table_name, table_id, range_value, range_id)) {
        dropPartition(table_name, table_id, range_value, range_id)
        i = MAX_ATTEMPTS
      } else {
        i = checkAndDropPartition(table_name, table_id, range_value, range_id, i)
      }
    }

  }

  private def checkAndDropPartition(table_name: String, table_id: String, range_value: String, range_id: String, i: Int): Int = {
    val (timestamp, _) = UndoLog.getCommitTimestampAndTag(
      UndoLogType.DropPartition.toString,
      table_id,
      UndoLogType.DropPartition.toString,
      range_id)
    if (timestamp < 0) {
      MAX_ATTEMPTS
    } else if (timestamp > System.currentTimeMillis() - MetaUtils.COMMIT_TIMEOUT) {
      TimeUnit.SECONDS.sleep(WAIT_TIME)
      checkAndDropPartition(table_name, table_id, range_value, range_id, i)
    } else {
      val update_timestamp = UndoLog.updateUndoLogTimestamp(
        commit_type = UndoLogType.DropPartition.toString,
        table_id = table_id,
        commit_id = UndoLogType.DropPartition.toString,
        range_id = range_id,
        last_timestamp = timestamp
      )
      if (update_timestamp._1) {
        dropPartition(table_name, table_id, range_value, range_id)
        MAX_ATTEMPTS
      }
      else {
        i + 1
      }

    }
  }

  def dropPartition(table_name: String, table_id: String, range_value: String, range_id: String): Unit = {
    MetaVersion.deletePartitionInfoByRangeId(table_id, range_value, range_id)
    UndoLog.deleteUndoLogByRangeId(UndoLogType.Partition.toString, table_id, range_id)
    UndoLog.deleteUndoLogByRangeId(UndoLogType.AddFile.toString, table_id, range_id)
    UndoLog.deleteUndoLogByRangeId(UndoLogType.ExpireFile.toString, table_id, range_id)

    TimeUnit.SECONDS.sleep(WAIT_TIME)

    DataOperation.deleteDataInfoByRangeId(table_id, range_id)

    UndoLog.deleteUndoLogByRangeId(
      UndoLogType.DropPartition.toString,
      table_id,
      UndoLogType.DropPartition.toString,
      range_id)
    SnapshotManagement(table_name).updateSnapshot()
  }


}
