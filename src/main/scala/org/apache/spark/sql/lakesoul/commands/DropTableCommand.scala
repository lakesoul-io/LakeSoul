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
//import com.dmetasoul.lakesoul.Newmeta._
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
       dropTable(snapshot)
  }
  def dropTable(snapshot: Snapshot): Unit = {
    val tableInfo = snapshot.getTableInfo
    val table_id = tableInfo.table_id
    val table_name = tableInfo.table_name
    val short_table_name = tableInfo.short_table_name
    MetaVersion.deleteTableInfo(table_name.get, table_id)
    if (short_table_name.isDefined) {
      MetaVersion.deleteShortTableName(short_table_name.get, table_name.get)
    }
    TimeUnit.SECONDS.sleep(WAIT_TIME)
    val partition_info_arr = snapshot.getPartitionInfoArray
    MetaVersion.deletePartitionInfoByTableId(table_id)
    partition_info_arr.foreach(part => DataOperation.deleteDataInfoByRangeId(table_id, part.range_value))
    val path = new Path(table_name.get)
    val sessionHadoopConf = SparkSession.active.sessionState.newHadoopConf()
    val fs = path.getFileSystem(sessionHadoopConf)
    SnapshotManagement.invalidateCache(table_name.get)
    fs.delete(path, true);
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
    val range_value = candidatePartitions.head.range_value
    dropPartition(table_name, table_id, range_value)
  }

  def dropPartition(table_name: String, table_id: String, range_value: String): Unit = {
    //just add partition version with non-value snapshot;not delete related datainfo for SCD
    MetaVersion.deletePartitionInfoByRangeId(table_id, range_value,"")
  }


}
